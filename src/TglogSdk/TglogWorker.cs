using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;

namespace Tencent.Tglog;

/// <summary>
/// Core worker thread that manages message batching, network I/O, retries, and connections.
///
/// Architecture:
/// - Runs a dedicated thread with a Socket.Select() event loop
/// - Consumes messages from a lock-free MPSC queue (fed by application threads)
/// - Batches messages and sends via TCP or UDP
/// - Manages connection pool with automatic replacement and recovery
/// - Implements exponential backoff retry logic
/// </summary>
internal sealed class TglogWorker
{
    // --- Constants (matching Java) ---
    private const int MsgPerRun = 3000;
    private const long EndpointRecoverIntervalMs = 5000;
    private const long ConnectionReplaceCheckIntervalMs = 20000;
    private const long EmergencySupplementIntervalMs = 5000;
    private const long MetricsUpdateIntervalMs = 5000;
    private const long PendingCloseCheckIntervalMs = 1000;
    private const int DefaultConnCloseDelayMs = 30000;
    private const int ErrorConnCloseDelayWithDataMs = 30000;
    private const int ErrorConnCloseDelayNoDataMs = 5000;
    private const long EmergencySupplementMinIntervalMs = 2000;

    private enum WorkerState { Init, Ready, Closing, Closed }

    /// <summary>Result of a batch send attempt.</summary>
    private readonly struct SendResult
    {
        public TglogErrorCode ErrorCode { get; init; }
        public bool IsOk => ErrorCode == TglogErrorCode.Ok;
        public static SendResult Ok() => new() { ErrorCode = TglogErrorCode.Ok };
        public static SendResult Error(TglogErrorCode code) => new() { ErrorCode = code };
    }

    /// <summary>Connection scheduled for delayed closure.</summary>
    private readonly struct DelayedClose
    {
        public TglogConnection Connection { get; init; }
        public long DeadlineTicks { get; init; }
        public bool IsError { get; init; }
    }

    // --- Fields ---
    private readonly int _workerId;
    private readonly TglogConfig _config;
    private readonly TglogMetrics _metrics;
    private readonly ILogger _logger;

    // State
    private volatile WorkerState _state = WorkerState.Init;
    private readonly ManualResetEventSlim _readyEvent = new(false);

    // Queues
    private readonly LockFreeQueue<LogMessage> _messageQueue;
    private readonly ConcurrentQueue<IReadOnlyList<IPEndPoint>> _endpointUpdateQueue = new();

    // Flush requests submitted by application threads; resolved on the worker thread.
    // Timeout handling lives entirely in TglogClient.Flush — the worker only decides
    // whether all pending work has drained.
    private readonly ConcurrentQueue<TaskCompletionSource<bool>> _flushRequestQueue = new();

    // Connections
    private readonly List<TglogConnection> _connections = new(8);
    private readonly List<TglogConnection> _connectingConns = new(8);
    private readonly Dictionary<Socket, TglogConnection> _socketToConnection = new();
    private long _connectionIndex;
    private int _connectionsPerEndpoint;
    private int _endpointReplaceIndex;

    // Tracks the unavailability of an endpoint: when it was (last) marked
    // unavailable and how many recovery attempts have been made. Bundling both
    // fields keeps the timestamp and retry count from drifting out of sync, since
    // they always change together.
    private struct UnavailableState
    {
        public long UnavailableTime;
        public int RetryCount;

        public UnavailableState(long unavailableTime)
        {
            UnavailableTime = unavailableTime;
            RetryCount = 0;
        }
    }

    // Endpoints
    private List<IPEndPoint> _currentEndpoints = new();
    private readonly Dictionary<string, UnavailableState> _unavailableEndpoints = new();

    // Initial endpoints (set before start)
    private IReadOnlyList<IPEndPoint>? _initialEndpoints;

    // Batch management
    private BatchBuffer _currentBatch;
    private LogMessage? _pendingMessage;

    // Delayed closes
    private readonly List<DelayedClose> _pendingCloseList = new(8);

    // Timing
    private long _lastEndpointRecoverTime;
    private long _lastConnectionReplaceTime;
    private long _lastEmergencySupplementTime;
    private long _lastMetricsUpdateTime;
    private long _lastPendingCloseCheckTime;

    // Select lists (reused each iteration to avoid allocation)
    private readonly List<Socket> _selectReadList = new(8);
    private readonly List<Socket> _selectWriteList = new(8);
    private readonly List<Socket> _selectErrorList = new(8);

    // Worker thread
    private Thread? _workerThread;

    public TglogWorker(int workerId, TglogConfig config, TglogMetrics metrics, ILoggerFactory loggerFactory)
    {
        _workerId = workerId;
        _config = config;
        _metrics = metrics;
        _logger = loggerFactory.CreateLogger($"TGLog.Worker-{workerId}");

        // Create lock-free queue (power-of-2 capacity, matching Java: per-worker capacity)
        _messageQueue = new LockFreeQueue<LogMessage>(config.MaxPendingMessages);

        _currentBatch = new BatchBuffer(config, _logger);

        _metrics.SetMsgQueueCapacity(workerId, _messageQueue.Capacity);
    }

    /// <summary>Sets the initial endpoints before the worker starts.</summary>
    public void SetInitialEndpoints(IReadOnlyList<IPEndPoint> endpoints)
    {
        _initialEndpoints = endpoints;
    }

    /// <summary>
    /// Enqueues a message for sending. Called from application threads.
    /// Returns <see cref="TglogErrorCode.Ok"/> if enqueued,
    /// <see cref="TglogErrorCode.SendQueueIsFull"/> if the bounded queue is full, or
    /// <see cref="TglogErrorCode.ProducerClosed"/> if the worker is no longer running
    /// (closing/closed, including after a fatal exit). Distinguishing the latter lets
    /// the caller report an accurate error instead of a misleading "queue full".
    /// </summary>
    public TglogErrorCode Enqueue(LogMessage message)
    {
        if (_state != WorkerState.Ready)
            return TglogErrorCode.ProducerClosed;

        if (!_messageQueue.Offer(message))
        {
            return TglogErrorCode.SendQueueIsFull;
        }

        _metrics.IncrementEnqueuedMsg();
        return TglogErrorCode.Ok;
    }

    /// <summary>Notifies the worker of an endpoint change (called from EndpointManager thread).</summary>
    public void NotifyEndpointUpdate(IReadOnlyList<IPEndPoint> newEndpoints)
    {
        _endpointUpdateQueue.Enqueue(newEndpoints);
    }

    /// <summary>
    /// Submits a flush request to the worker. The returned task completes with
    /// <c>true</c> once the worker has drained all pending work, or <c>false</c>
    /// if the worker stopped before draining. Does NOT block and does NOT inspect
    /// worker-internal state on the caller thread — the worker thread completes the
    /// task. Timeout handling is the caller's responsibility (see TglogClient.Flush).
    /// </summary>
    public Task<bool> RequestFlush()
    {
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Enqueue first, then re-check state. Ordering matters: if we enqueue before
        // the worker transitions to Closing, the event loop or the shutdown path
        // (CompleteFlushRequestsOnStop) will service it. If the worker has already
        // stopped, there is no loop to service it, so we complete it here. We never
        // call HasPendingWorkInternal() on this (caller) thread.
        _flushRequestQueue.Enqueue(tcs);

        if (_state != WorkerState.Ready && _state != WorkerState.Init)
        {
            // Worker is closing/closed and may never poll the queue again. Drain it
            // here ourselves and complete with false: we cannot guarantee a full
            // flush once the worker is no longer processing.
            while (_flushRequestQueue.TryDequeue(out var pending))
            {
                pending.TrySetResult(false);
            }
        }

        return tcs.Task;
    }

    /// <summary>
    /// Worker-thread-only check: returns true if there are pending messages,
    /// an in-flight batch, or buffered connection data. MUST only be called from
    /// the worker thread, as it reads worker-owned state without synchronization.
    /// </summary>
    private bool HasPendingWorkInternal()
    {
        if (!_messageQueue.IsEmpty) return true;
        if (_pendingMessage != null) return true;
        if (!_currentBatch.IsEmpty) return true;

        for (int i = 0; i < _connections.Count; i++)
        {
            if (_connections[i].HasBufferedData) return true;
        }

        // Delayed-close connections may still hold unsent output data; BuildSelectLists()
        // keeps flushing them, so Flush() must not report "drained" while they have data.
        for (int i = 0; i < _pendingCloseList.Count; i++)
        {
            if (_pendingCloseList[i].Connection.HasBufferedData) return true;
        }
        return false;
    }

    /// <summary>Starts the worker thread.</summary>
    public void Start()
    {
        _workerThread = new Thread(Run)
        {
            Name = $"TglogWorker-{_workerId}",
            IsBackground = true
        };
        _workerThread.Start();
    }

    /// <summary>Waits for the worker to become ready.</summary>
    public bool WaitForReady(int timeoutMs)
    {
        return _readyEvent.Wait(timeoutMs);
    }

    /// <summary>Signals the worker to begin shutdown.</summary>
    public void Stop()
    {
        _state = WorkerState.Closing;
    }

    /// <summary>Waits for the worker thread to complete. Returns true if the thread stopped within the timeout.</summary>
    public bool Join(int timeoutMs)
    {
        return _workerThread?.Join(timeoutMs) ?? true;
    }

    // ==================== Main Thread Entry ====================

    private void Run()
    {
        try
        {
            InitializeConnections();
            _state = WorkerState.Ready;
            _readyEvent.Set();
            RunEventLoop();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Worker {WorkerId}: fatal error", _workerId);
        }
        finally
        {
            try
            {
                Cleanup();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Worker {WorkerId}: error during cleanup", _workerId);
            }
            // Guarantee terminal state for all remaining messages, even if Cleanup threw
            FailRemainingMessages();

            // Release any flush waiters before flipping to Closed, so a concurrent
            // RequestFlush() that observed Ready/Closing has its task completed here.
            CompleteFlushRequestsOnStop();

            _state = WorkerState.Closed;

            // A RequestFlush() racing with the state flip above may have enqueued
            // after CompleteFlushRequestsOnStop ran but before _state became Closed.
            // RequestFlush itself drains when it sees a non-Ready state, but to cover
            // the interleaving where it enqueued just before that check, drain again.
            CompleteFlushRequestsOnStop();
        }
    }

    // ==================== Initialization ====================

    private void InitializeConnections()
    {
        if (_initialEndpoints == null || _initialEndpoints.Count == 0)
        {
            _logger.LogWarning("Worker {WorkerId}: no initial endpoints", _workerId);
            return;
        }

        _logger.LogInformation("Worker {WorkerId}: initializing connections, endpoints={EndpointCount}", _workerId, _initialEndpoints.Count);

        _currentEndpoints = new List<IPEndPoint>(_initialEndpoints);
        _connectionsPerEndpoint = _currentEndpoints.Count <= 2 ? 2 : 1;

        foreach (var endpoint in _currentEndpoints)
        {
            bool anyConnected = false;
            for (int i = 0; i < _connectionsPerEndpoint; i++)
            {
                var conn = CreateConnection(endpoint);
                if (conn != null)
                {
                    anyConnected = true;
                }
            }

            if (!anyConnected)
            {
                MarkEndpointUnavailable(endpoint);
            }
        }

        // Wait briefly for connecting connections to complete
        if (_connectingConns.Count > 0)
        {
            WaitForConnectingConnections(_config.ConnectionTimeoutMs);
        }

        // TCP requires at least one established connection to be useful.
        // If all connection attempts failed, abort worker startup so that
        // WaitForReady() returns false and the client constructor fails.
        if (_config.TransportProtocol == TglogConfig.Protocol.Tcp && _connections.Count == 0)
        {
            throw new TglogException(TglogErrorCode.NewConnFailed,
                $"Worker {_workerId}: no connections established during initialization");
        }

        _logger.LogInformation("Worker {WorkerId}: using {ConnectionsPerEndpoint} connections per endpoint (total endpoints: {EndpointCount})", _workerId, _connectionsPerEndpoint, _currentEndpoints.Count);
    }

    private void WaitForConnectingConnections(int timeoutMs)
    {
        long deadline = Environment.TickCount64 + timeoutMs;

        while (_connectingConns.Count > 0 && Environment.TickCount64 < deadline)
        {
            var checkWrite = new List<Socket>(_connectingConns.Count);
            var checkError = new List<Socket>(_connectingConns.Count);

            foreach (var conn in _connectingConns)
            {
                if (conn.Socket != null)
                {
                    checkWrite.Add(conn.Socket);
                    checkError.Add(conn.Socket);
                }
            }

            if (checkWrite.Count == 0) break;

            try
            {
                Socket.Select(null, checkWrite, checkError, 100_000); // 100ms
                ProcessSelectedConnectingSockets(checkWrite, checkError);
                CheckConnectingConnections();
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Worker {WorkerId}: WaitForConnecting error: {Message}", _workerId, ex.Message);
                break;
            }
        }

        if (_connectingConns.Count > 0)
        {
            _logger.LogWarning("Worker {WorkerId}: {Count} connections timed out during initialization", _workerId, _connectingConns.Count);
        }

        for (int i = _connectingConns.Count - 1; i >= 0; i--)
        {
            var conn = _connectingConns[i];
            if (conn.IsConnecting)
                conn.MarkConnectFailed();
        }

        CheckConnectingConnections();
    }

    // ==================== Event Loop ====================

    private void RunEventLoop()
    {
        long now = Environment.TickCount64;
        _lastEndpointRecoverTime = now;
        _lastConnectionReplaceTime = now;
        _lastEmergencySupplementTime = now;
        _lastMetricsUpdateTime = now;
        _lastPendingCloseCheckTime = now;

        while (_state == WorkerState.Ready)
        {
            // 1. Process endpoint updates
            ProcessEndpointUpdates();

            // 2. Build select lists
            BuildSelectLists();

            // 3. Select with adaptive timeout
            int timeoutMicros = _messageQueue.IsEmpty ? 10_000 : 5_000;
            DoSelect(timeoutMicros);

            // 4. Process select results
            ProcessSelectResults();

            // 5. Check connecting connections
            CheckConnectingConnections();

            // 6. Process messages
            ProcessMessages();

            // 7. Resolve any pending flush requests now that work has progressed
            ProcessFlushRequests();

            // 8. Periodic maintenance
            now = Environment.TickCount64;

            if (now - _lastEndpointRecoverTime >= EndpointRecoverIntervalMs)
            {
                RecoverUnavailableEndpoints();
                _lastEndpointRecoverTime = now;
            }

            if (_config.MaxConnectionLifetimeSec > 0 &&
                now - _lastConnectionReplaceTime >= ConnectionReplaceCheckIntervalMs)
            {
                ReplaceExpiredConnections();
                _lastConnectionReplaceTime = now;
            }

            if (now - _lastEmergencySupplementTime >= EmergencySupplementIntervalMs)
            {
                if (_connections.Count == 0)
                {
                    SupplementConnection();
                }
                _lastEmergencySupplementTime = now;
            }

            if (now - _lastPendingCloseCheckTime >= PendingCloseCheckIntervalMs)
            {
                ProcessPendingCloses();
                _lastPendingCloseCheckTime = now;
            }

            if (now - _lastMetricsUpdateTime >= MetricsUpdateIntervalMs)
            {
                UpdateMetrics();
                _lastMetricsUpdateTime = now;
            }
        }

        _logger.LogInformation("Worker {WorkerId}: event loop ended", _workerId);
    }

    private void BuildSelectLists()
    {
        _selectReadList.Clear();
        _selectWriteList.Clear();
        _selectErrorList.Clear();

        // Connecting connections → check write + error
        foreach (var conn in _connectingConns)
        {
            if (conn.Socket != null)
            {
                _selectWriteList.Add(conn.Socket);
                _selectErrorList.Add(conn.Socket);
            }
        }

        // Active connections
        foreach (var conn in _connections)
        {
            if (conn.Socket == null) continue;

            if (_config.TransportProtocol == TglogConfig.Protocol.Tcp)
            {
                _selectReadList.Add(conn.Socket); // Monitor for peer close
            }
            if (conn.HasBufferedData)
            {
                _selectWriteList.Add(conn.Socket); // Has data to flush
            }
        }

        // Also check non-error delayed close connections for writability
        foreach (var dc in _pendingCloseList)
        {
            if (!dc.IsError && dc.Connection.Socket != null && dc.Connection.HasBufferedData)
            {
                _selectWriteList.Add(dc.Connection.Socket);
            }
        }
    }

    private void DoSelect(int timeoutMicros)
    {
        if (_selectReadList.Count == 0 && _selectWriteList.Count == 0 && _selectErrorList.Count == 0)
        {
            // No sockets to watch — sleep briefly
            Thread.Sleep(_messageQueue.IsEmpty ? 10 : 5);
            return;
        }

        try
        {
            Socket.Select(_selectReadList, _selectWriteList, _selectErrorList, timeoutMicros);
        }
        catch (SocketException ex)
        {
            _logger.LogWarning("Worker {WorkerId}: select error: {Message}", _workerId, ex.Message);
            _selectReadList.Clear();
            _selectWriteList.Clear();
            _selectErrorList.Clear();
        }
    }

    private void ProcessSelectResults()
    {
        // Process errors
        foreach (var socket in _selectErrorList)
        {
            if (_socketToConnection.TryGetValue(socket, out var conn))
            {
                if (conn.IsConnecting)
                {
                    conn.MarkConnectFailed();
                    continue;
                }

                HandleConnectionError(conn, true);
            }
        }

        // Process readable (TCP: detect peer close)
        foreach (var socket in _selectReadList)
        {
            if (_socketToConnection.TryGetValue(socket, out var conn))
            {
                if (!conn.HandleRead())
                {
                    HandleConnectionError(conn, true);
                }
            }
        }

        // Process writable
        foreach (var socket in _selectWriteList)
        {
            if (_socketToConnection.TryGetValue(socket, out var conn))
            {
                if (conn.IsConnecting)
                {
                    conn.FinishConnect();
                    continue;
                }

                if (conn.State == TglogConnection.ConnectionState.Error)
                    continue;

                conn.TryFlush();

                if (conn.State == TglogConnection.ConnectionState.Error)
                {
                    HandleConnectionError(conn, true);
                }
            }
        }
    }

    private void ProcessSelectedConnectingSockets(List<Socket> selectedWrite, List<Socket> selectedError)
    {
        foreach (var socket in selectedError)
        {
            if (_socketToConnection.TryGetValue(socket, out var conn) && conn.IsConnecting)
            {
                conn.MarkConnectFailed();
            }
        }

        foreach (var socket in selectedWrite)
        {
            if (_socketToConnection.TryGetValue(socket, out var conn) && conn.IsConnecting)
            {
                conn.FinishConnect();
            }
        }
    }

    private void CheckConnectingConnections()
    {
        for (int i = _connectingConns.Count - 1; i >= 0; i--)
        {
            var conn = _connectingConns[i];

            if (conn.State == TglogConnection.ConnectionState.Connected)
            {
                _connectingConns.RemoveAt(i);
                _connections.Add(conn);
                ClearEndpointUnavailable(conn.Endpoint);
                _logger.LogInformation("Worker {WorkerId}: connection ready: {Endpoint} (total: {Count})", _workerId, conn.Endpoint, _connections.Count);
            }
            else if (conn.State == TglogConnection.ConnectionState.Error || conn.Socket == null)
            {
                _connectingConns.RemoveAt(i);
                MarkEndpointUnavailable(conn.Endpoint);
                UnregisterSocket(conn);
                conn.Close();
                _metrics.IncrementErrorConn(_workerId);
            }
        }
    }

    // ==================== Message Processing ====================

    private void ProcessMessages() => ProcessMessages(false);

    /// <summary>
    /// Processes messages from the queue.
    /// </summary>
    /// <param name="shutdown">
    /// When true the worker is draining during <see cref="Cleanup"/>: the batch is sent
    /// immediately without honoring retry backoff, and a failed send fails the batch
    /// instead of re-arming a retry. This guarantees forward progress on every call so
    /// the cleanup drain loop cannot busy-spin until the stop timeout elapses (it would
    /// otherwise, because a batch stuck in backoff makes ProcessMessages return without
    /// consuming the queue).
    /// </param>
    private void ProcessMessages(bool shutdown)
    {
        int processed = 0;

        // During shutdown we never re-enter the retry/backoff state: one send attempt,
        // accept its outcome.
        bool retry = !shutdown;

        while (processed < MsgPerRun && (_state == WorkerState.Ready || _state == WorkerState.Closing))
        {
            // Handle current batch retry state
            if (_currentBatch.IsRetrying)
            {
                // Check if retry has timed out
                long totalBackoff = Backoff.BatchRetry.GetTotalBackoffTime(_config.MaxRetries);
                if (_currentBatch.IsRetryTimeout((int)totalBackoff) ||
                    _currentBatch.IsSendTimeout(_config.SendTimeoutMs))
                {
                    _logger.LogWarning("Worker {WorkerId}: batch retry/send timeout", _workerId);
                    FailCurrentBatch(TglogErrorCode.SendTimeout);
                    // Continue to process new messages
                }
                else if (shutdown || _currentBatch.ShouldRetryNow(Backoff.BatchRetry))
                {
                    // Time to retry - attempt to send the batch again. During shutdown
                    // we ignore the backoff window and retry now so the drain loop can
                    // finish fast instead of waiting out backoff.
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug($"Worker {_workerId}: attempting retry {_currentBatch.RetryCount}/{_config.MaxRetries}");
                    if (!SendBatch(retry))
                    {
                        break; // Still retrying (will check again in next iteration)
                    }
                    // Retry succeeded, continue to process new messages
                }
                else
                {
                    // Still in backoff period, skip processing new messages
                    break;
                }
            }

            // Check batch send timeout
            if (_currentBatch.IsSendTimeout(_config.SendTimeoutMs))
            {
                _logger.LogWarning("Worker {WorkerId}: batch send timeout", _workerId);
                FailCurrentBatch(TglogErrorCode.SendTimeout);
            }

            // Send current batch if ready (count/size/timeout triggers)
            if (_currentBatch.ShouldFlush())
            {
                if (!SendBatch(retry))
                {
                    break; // Batch is retrying
                }
            }

            // Get next message
            LogMessage? msg = _pendingMessage;
            if (msg == null)
            {
                msg = _messageQueue.Poll();
                if (msg == null)
                {
                    break; // No more messages
                }
                _metrics.IncrementDequeuedMsg();
            }
            _pendingMessage = null;
            processed++;

            // Check if message fits in current batch (request size limit)
            int msgSize = msg.PayloadSize;
            int batchSize = _currentBatch.CurrentSize;
            int maxSize = _config.MaxRequestSize;

            if (batchSize + msgSize > maxSize)
            {
                // Save message and send current batch first
                _pendingMessage = msg;
                if (!SendBatch(retry))
                {
                    break; // Batch is retrying
                }
                continue;
            }

            // Add message to batch
            if (!_currentBatch.Add(msg))
            {
                // Batch full - save message and send batch
                _pendingMessage = msg;
                if (!SendBatch(retry))
                {
                    break; // Batch is retrying
                }
                continue;
            }

            // Check if batch should be sent now (count/size triggers, NOT timeout)
            if (!_currentBatch.IsEmpty &&
                (_currentBatch.MessageCount >= _config.BatchingMaxMessages ||
                 _currentBatch.CurrentSize >= _config.BatchingMaxSize))
            {
                if (!SendBatch(retry))
                {
                    break; // Batch is retrying
                }
            }
        }
    }

    /// <summary>
    /// Sends the current batch.
    /// Returns true if batch is done (success or permanently failed), false if retrying.
    /// </summary>
    private bool SendBatch(bool retry)
    {
        if (_currentBatch.IsEmpty) return true;

        var result = DoSendBatch();

        if (result.IsOk)
        {
            // Success - notify and update metrics
            int msgCount = _currentBatch.MessageCount;
            _currentBatch.NotifyAllSucceeded();
            _metrics.AddSucceededMsg(msgCount);
            return true;
        }

        // Failed
        if (!retry)
        {
            FailCurrentBatch(result.ErrorCode);
            return true;
        }

        // Retry
        return RetryBatch();
    }

    private SendResult DoSendBatch()
    {
        var conn = GetConnection();
        if (conn == null)
        {
            _logger.LogError("Worker {WorkerId}: no available connections", _workerId);
            return SendResult.Error(TglogErrorCode.NoAvailableConn);
        }

        var buffers = _currentBatch.PrepareForSend();

        if (_config.TransportProtocol == TglogConfig.Protocol.Tcp)
        {
            return SendBatchTcp(conn, buffers);
        }
        else
        {
            return SendBatchUdp(conn, buffers);
        }
    }

    private SendResult SendBatchTcp(TglogConnection startConn, ArraySegment<byte>[] buffers)
    {
        // Snapshot the active connections up front. HandleConnectionError() removes
        // the failing connection from _connections, so iterating against the live
        // list (or relying on the failing conn for "next") would skip still-healthy
        // connections. We try every connection that was active at send time, starting
        // from the round-robin pick, until one accepts the batch.
        foreach (var conn in EnumerateConnectionsFrom(startConn))
        {
            long sent = conn.Send(buffers);

            if (sent < 0)
            {
                // Connection error — drop it and try the next snapshot entry.
                _logger.LogError("Worker {WorkerId}: TCP send error on {Endpoint}, trying next", _workerId, conn.Endpoint);
                HandleConnectionError(conn, true);
                continue;
            }

            if (sent == 0)
            {
                // High watermark exceeded — try the next connection.
                _logger.LogWarning("Worker {WorkerId}: connection {Endpoint} exceeded high watermark", _workerId, conn.Endpoint);
                continue;
            }

            // sent > 0: Success (data sent directly or buffered)
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug($"Worker {_workerId}: partial TCP send, data buffered");
            return SendResult.Ok();
        }

        return SendResult.Error(TglogErrorCode.SendFailed);
    }

    private SendResult SendBatchUdp(TglogConnection startConn, ArraySegment<byte>[] buffers)
    {
        // Try every connection that was active at send time (one full round), starting
        // from the round-robin pick. Only if none accept the datagram do we report a
        // failure and let the batch enter the retry path. Snapshotting up front avoids
        // skipping healthy connections when HandleConnectionError() removes a failing one.
        foreach (var conn in EnumerateConnectionsFrom(startConn))
        {
            long sent = conn.Send(buffers);

            if (sent > 0)
            {
                return SendResult.Ok();
            }

            if (sent == 0)
            {
                // EAGAIN — try the next connection.
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug($"Worker {_workerId}: UDP send would block on {conn.Endpoint}, trying another");
                continue;
            }

            // sent < 0: error — drop it and try the next snapshot entry.
            HandleConnectionError(conn, true);
        }

        return SendResult.Error(TglogErrorCode.SendFailed);
    }

    /// <summary>
    /// Yields the active connections for one send attempt, starting at <paramref name="startConn"/>
    /// (the round-robin pick) and wrapping around. The set of connections is snapshotted at
    /// enumeration start, so connections removed mid-iteration (e.g. by HandleConnectionError)
    /// do not shorten the walk or cause healthy connections to be skipped.
    /// </summary>
    private IEnumerable<TglogConnection> EnumerateConnectionsFrom(TglogConnection startConn)
    {
        var snapshot = _connections.ToArray();
        if (snapshot.Length == 0) yield break;

        int startIdx = Array.IndexOf(snapshot, startConn);
        if (startIdx < 0) startIdx = 0;

        for (int i = 0; i < snapshot.Length; i++)
        {
            yield return snapshot[(startIdx + i) % snapshot.Length];
        }
    }

    /// <summary>
    /// Marks the current batch for retry. Returns true if done (max retries exceeded), false if retrying.
    /// </summary>
    private bool RetryBatch()
    {
        // Check max retries
        if (_currentBatch.RetryCount >= _config.MaxRetries)
        {
            FailCurrentBatch(TglogErrorCode.SendTimeout);
            return true;
        }

        // Mark batch for retry (increments retry count and sets retry timestamps)
        _currentBatch.MarkForRetry();
        _metrics.IncrementRetryMsg();

        long backoffMs = Backoff.BatchRetry.GetNextInterval(_currentBatch.RetryCount);
        _logger.LogInformation("Worker {WorkerId}: retrying batch in {BackoffMs}ms (attempt {RetryCount}/{MaxRetries})", _workerId, backoffMs, _currentBatch.RetryCount, _config.MaxRetries);

        return false; // Still retrying
    }

    private void FailCurrentBatch(TglogErrorCode errorCode)
    {
        if (_currentBatch.IsEmpty) return;

        int msgCount = _currentBatch.MessageCount;
        _currentBatch.NotifyAllFailed(errorCode);

        if (errorCode == TglogErrorCode.SendTimeout)
            _metrics.AddTimeoutMsg(msgCount);
        else
            _metrics.AddFailedMsg(msgCount);
    }

    // ==================== Connection Management ====================

    private TglogConnection? GetConnection()
    {
        if (_connections.Count == 0)
        {
            SupplementConnection();
            if (_connections.Count == 0)
            {
                _logger.LogError("Worker {WorkerId}: no available connections", _workerId);
                return null;
            }
        }

        long index = Interlocked.Increment(ref _connectionIndex) & long.MaxValue;
        return _connections[(int)(index % _connections.Count)];
    }

    private TglogConnection? CreateConnection(IPEndPoint endpoint)
    {
        var conn = new TglogConnection(endpoint, _config, _metrics, _workerId, _logger);
        if (conn.Connect())
        {
            RegisterSocket(conn);
            if (conn.IsConnected)
            {
                _connections.Add(conn);
                ClearEndpointUnavailable(conn.Endpoint);
            }
            else if (conn.IsConnecting)
            {
                _connectingConns.Add(conn);
            }
            return conn;
        }
        else
        {
            _metrics.IncrementErrorConn(_workerId);
            return null;
        }
    }

    private void HandleConnectionError(TglogConnection conn, bool supplement)
    {
        // A delayed-close connection has already been condemned (DNS removal, lifetime
        // rotation, or a prior error). A write failure / peer close on it is expected,
        // not a reason to penalize its endpoint or grow the pool. Only rescue its data.
        // Marking the endpoint unavailable here could even resurrect a DNS-removed
        // endpoint via the recovery path, so we must not do it.
        if (RemoveFromPendingClose(conn))
        {
            FlushOrRescueAndClose(conn);
            return;
        }

        // Active / connecting connection error: remove and stop watching network events
        _connections.Remove(conn);
        _connectingConns.Remove(conn);
        UnregisterSocket(conn);

        // Mark endpoint unavailable (like Java)
        MarkEndpointUnavailable(conn.Endpoint);

        // Schedule delayed close for data rescue opportunity
        CloseConnectionWithDelay(conn, true);

        // Update metrics
        _metrics.IncrementErrorConn(_workerId);

        // Try to supplement connection (like Java) — only for live connections
        if (supplement)
            SupplementConnection();
    }

    /// <summary>
    /// Removes <paramref name="conn"/> from the delayed-close list if present.
    /// Returns true when it was found (i.e. the connection was a pending-close one).
    /// </summary>
    private bool RemoveFromPendingClose(TglogConnection conn)
    {
        for (int i = 0; i < _pendingCloseList.Count; i++)
        {
            if (_pendingCloseList[i].Connection == conn)
            {
                _pendingCloseList.RemoveAt(i);
                return true;
            }
        }
        return false;
    }

    private void CloseConnectionWithDelay(TglogConnection conn, bool isError)
    {
        for (int i = 0; i < _pendingCloseList.Count; i++)
        {
            var dc = _pendingCloseList[i];
            if (dc.Connection != conn) continue;

            if (isError && !dc.IsError)
            {
                int errorDelayMs = conn.HasBufferedData ? ErrorConnCloseDelayWithDataMs : ErrorConnCloseDelayNoDataMs;
                _pendingCloseList[i] = new DelayedClose
                {
                    Connection = dc.Connection,
                    DeadlineTicks = Math.Min(dc.DeadlineTicks, Environment.TickCount64 + errorDelayMs),
                    IsError = true
                };
            }
            return;
        }

        int delayMs;
        if (isError)
        {
            delayMs = conn.HasBufferedData ? ErrorConnCloseDelayWithDataMs : ErrorConnCloseDelayNoDataMs;
        }
        else
        {
            delayMs = DefaultConnCloseDelayMs;
        }

        _logger.LogInformation("Worker {WorkerId}: scheduling connection close in {DelayMs}ms: {Endpoint}", _workerId, delayMs, conn.Endpoint);

        _pendingCloseList.Add(new DelayedClose
        {
            Connection = conn,
            DeadlineTicks = Environment.TickCount64 + delayMs,
            IsError = isError
        });
    }

    /// <summary>
    /// Closes delayed-close connections whose deadline has elapsed, rescuing any
    /// buffered data first. Called from the worker event loop. The shutdown path in
    /// <see cref="Cleanup"/> rescue-closes the remaining entries directly.
    /// </summary>
    private void ProcessPendingCloses()
    {
        long now = Environment.TickCount64;

        for (int i = _pendingCloseList.Count - 1; i >= 0; i--)
        {
            var dc = _pendingCloseList[i];
            if (now >= dc.DeadlineTicks)
            {
                _pendingCloseList.RemoveAt(i);
                FlushOrRescueAndClose(dc.Connection);
            }
        }
    }

    /// <summary>
    /// Flushes-or-rescues a delayed-close connection, then unregisters and closes it.
    /// Self-flush is preferred: if the connection can still write its own buffered data
    /// we let it, preserving order and avoiding a needless cross-connection transfer.
    /// Only data it cannot send itself (it is full, or already in Error and unable to
    /// flush) is rescued onto a healthy active connection. Shared by the delayed-close
    /// path, the pending-close error split, and shutdown cleanup.
    /// </summary>
    private void FlushOrRescueAndClose(TglogConnection conn)
    {
        // 1. Self-flush first — keeps data on its original connection (ordered) and
        //    avoids moving bytes to another connection when this one can still send.
        if (conn.State == TglogConnection.ConnectionState.Connected && conn.HasBufferedData)
            conn.TryFlush();

        // 2. Whatever it could not send (full socket, or Error and unflushable) is
        //    rescued onto a still-healthy active connection.
        if (conn.HasBufferedData)
            RescueData(conn);

        UnregisterSocket(conn);
        conn.Close();
        _metrics.IncrementClosedConn(_workerId);

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug($"Worker {_workerId}: flushed/rescued and closed connection: {conn.Endpoint}");
    }

    private void RescueData(TglogConnection closingConn)
    {
        var pendingData = closingConn.RemovePendingData();
        if (pendingData == null || pendingData.Count == 0) return;

        int rescuedBytes = 0;
        foreach (var seg in pendingData)
            rescuedBytes += seg.Remaining;

        // Find an active connection to receive the data
        foreach (var conn in _connections)
        {
            if (conn != closingConn && conn.IsConnected)
            {
                if (conn.WritePendingData(pendingData))
                {
                    _metrics.AddRescuedMsgBytes(rescuedBytes);
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug($"Worker {_workerId}: rescued {rescuedBytes} bytes to {conn.Endpoint}");
                    return;
                }
            }
        }

        // No connection available to rescue data
        _logger.LogWarning("Worker {WorkerId}: lost {RescuedBytes} bytes - no rescue connection", _workerId, rescuedBytes);
    }

    private void ReplaceExpiredConnections()
    {
        for (int i = _connections.Count - 1; i >= 0; i--)
        {
            var conn = _connections[i];
            if (conn.IsExpired())
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug($"Worker {_workerId}: replacing expired connection to {conn.Endpoint}");

                ReplaceConnection(conn, i);
            }
        }
    }

    /// <summary>
    /// Replaces a connection using round-robin endpoint selection (matching Java).
    /// Tries each available endpoint to create a new connection, then closes the old one.
    /// </summary>
    private void ReplaceConnection(TglogConnection oldConn, int oldConnIndex)
    {
        // Round-robin across available endpoints (like Java)
        for (int j = 0; j < _currentEndpoints.Count; j++)
        {
            int index = (_endpointReplaceIndex++ & int.MaxValue) % _currentEndpoints.Count;
            var endpoint = _currentEndpoints[index];

            string key = GetEndpointKey(endpoint);
            if (_unavailableEndpoints.ContainsKey(key))
                continue;

            var newConn = CreateConnection(endpoint);
            if (newConn != null)
            {
                // Remove old connection from active list
                _connections.Remove(oldConn);
                CloseConnectionWithDelay(oldConn, false);
                return;
            }
            else
            {
                MarkEndpointUnavailable(endpoint);
            }
        }

        // No available endpoint found — keep the old connection
    }

    private void RecoverUnavailableEndpoints()
    {
        if (_unavailableEndpoints.Count == 0) return;

        long now = Environment.TickCount64;

        foreach (var kvp in _unavailableEndpoints.ToArray())
        {
            string endpointKey = kvp.Key;
            var ustate = kvp.Value;
            long nextInterval = Backoff.EndpointRecovery.GetNextInterval(ustate.RetryCount + 1);
            long elapsed = now - ustate.UnavailableTime;

            if (elapsed >= nextInterval)
            {
                var endpoint = ParseEndpointKey(endpointKey);
                if (endpoint == null) continue;

                int initiatedConns = 0;
                for (int i = 0; i < _connectionsPerEndpoint; i++)
                {
                    if (CreateConnection(endpoint) != null)
                        initiatedConns++;
                }

                if (_unavailableEndpoints.TryGetValue(endpointKey, out var current))
                {
                    current.RetryCount = ustate.RetryCount + 1;
                    current.UnavailableTime = now;
                    _unavailableEndpoints[endpointKey] = current;
                }

                if (initiatedConns == 0)
                {
                    _logger.LogWarning("Worker {WorkerId}: failed to initiate any recovery connection for {Endpoint}", _workerId, endpointKey);
                }
            }
        }
    }

    private void SupplementConnection()
    {
        if (_currentEndpoints.Count == 0) return;

        // Try each endpoint
        foreach (var endpoint in _currentEndpoints)
        {
            string key = GetEndpointKey(endpoint);
            if (_unavailableEndpoints.ContainsKey(key)) continue;

            // Count existing connections for this endpoint
            int existing = CountConnectionsForEndpoint(endpoint);
            if (existing < _connectionsPerEndpoint)
            {
                if (CreateConnection(endpoint) != null)
                    return;
                else
                    _logger.LogWarning("Worker {WorkerId}: supplement connection failed for {Endpoint}", _workerId, endpoint);
            }
        }

        // If all at target, try random endpoint
        if (_currentEndpoints.Count > 0)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug($"Worker {_workerId}: force supplement connection");

            var endpoint = _currentEndpoints[Random.Shared.Next(_currentEndpoints.Count)];
            string key = GetEndpointKey(endpoint);
            if (!_unavailableEndpoints.ContainsKey(key))
            {
                if (CreateConnection(endpoint) == null)
                    _logger.LogWarning("Worker {WorkerId}: force supplement connection failed for {Endpoint}", _workerId, endpoint);
            }
        }
    }

    private int CountConnectionsForEndpoint(IPEndPoint endpoint)
    {
        int count = 0;
        string key = GetEndpointKey(endpoint);
        foreach (var conn in _connections)
            if (GetEndpointKey(conn.Endpoint) == key) count++;
        foreach (var conn in _connectingConns)
            if (GetEndpointKey(conn.Endpoint) == key) count++;
        return count;
    }

    // ==================== Flush Requests ====================

    /// <summary>
    /// Resolves pending flush requests (worker thread only). If any work is still
    /// pending, the entire queue is left untouched to be re-checked on the next
    /// event-loop iteration; otherwise every outstanding request completes with
    /// <c>true</c>. Timeouts are handled by the caller, so the worker never expires
    /// a request itself.
    /// </summary>
    private void ProcessFlushRequests()
    {
        if (_flushRequestQueue.IsEmpty) return;

        // Evaluate pending work once per pass — identical for all waiting requests.
        if (HasPendingWorkInternal()) return;

        while (_flushRequestQueue.TryDequeue(out var tcs))
        {
            tcs.TrySetResult(true);
        }
    }

    /// <summary>
    /// Completes all outstanding flush requests on worker shutdown so callers
    /// awaiting a flush never hang. Reports <c>true</c> only if no work remains at
    /// this point, otherwise <c>false</c>. (Worker thread, cleanup path.)
    /// </summary>
    private void CompleteFlushRequestsOnStop()
    {
        if (_flushRequestQueue.IsEmpty) return;

        bool drained = !HasPendingWorkInternal();
        while (_flushRequestQueue.TryDequeue(out var tcs))
        {
            tcs.TrySetResult(drained);
        }
    }

    // ==================== Endpoint Updates ====================

    private void ProcessEndpointUpdates()
    {
        while (_endpointUpdateQueue.TryDequeue(out var newEndpoints))
        {
            HandleEndpointUpdate(newEndpoints);
        }
    }

    private void HandleEndpointUpdate(IReadOnlyList<IPEndPoint> newEndpoints)
    {
        // Ignore empty/null updates. Applying them would wipe currentEndpoints and
        // close every connection, leaving the worker with nowhere to send. An empty
        // endpoint list almost always means a transient DNS resolution failure rather
        // than a genuine "no servers" topology, so keep the existing endpoints.
        if (newEndpoints == null || newEndpoints.Count == 0)
        {
            _logger.LogWarning("Worker {WorkerId}: ignoring empty endpoint update, keeping {Count} current endpoints",
                _workerId, _currentEndpoints.Count);
            return;
        }

        _logger.LogInformation("Worker {WorkerId}: processing endpoint update: {Count} endpoints", _workerId, newEndpoints.Count);

        var oldSet = new HashSet<string>(_currentEndpoints.Select(GetEndpointKey));
        var newSet = new HashSet<string>(newEndpoints.Select(GetEndpointKey));

        // Find added endpoints
        var added = newEndpoints.Where(ep => !oldSet.Contains(GetEndpointKey(ep))).ToList();
        // Find removed endpoints
        var removed = _currentEndpoints.Where(ep => !newSet.Contains(GetEndpointKey(ep))).ToList();

        // Recalculate connections per endpoint
        _connectionsPerEndpoint = newEndpoints.Count <= 2 ? 2 : 1;

        // Update the endpoint view BEFORE adding/removing connections. A newly added endpoint may
        // fail to connect synchronously, in which case MarkEndpointUnavailable() is called below;
        // that method consults _currentEndpoints.Count to decide whether backoff applies, so it must
        // already reflect the new topology, otherwise the new endpoint would be checked against the
        // old (smaller) list and wrongly skipped (e.g. A -> A+B, B unreachable, Count was still 1).
        _currentEndpoints = new List<IPEndPoint>(newEndpoints);
        _metrics.SetEndpointCount(_currentEndpoints.Count);

        // Add connections for new endpoints first
        foreach (var endpoint in added)
        {
            bool anyConnected = false;
            for (int i = 0; i < _connectionsPerEndpoint; i++)
            {
                if (CreateConnection(endpoint) != null)
                {
                    anyConnected = true;
                }
            }

            if (!anyConnected)
            {
                MarkEndpointUnavailable(endpoint);
            }

            _metrics.IncrementAddEndpoint();
        }

        // Remove connections for removed endpoints
        foreach (var endpoint in removed)
        {
            string key = GetEndpointKey(endpoint);

            // Remove from active connections
            for (int i = _connections.Count - 1; i >= 0; i--)
            {
                if (GetEndpointKey(_connections[i].Endpoint) == key)
                {
                    var conn = _connections[i];
                    _connections.RemoveAt(i);
                    CloseConnectionWithDelay(conn, false);
                }
            }

            // Remove from connecting connections (like Java)
            for (int i = _connectingConns.Count - 1; i >= 0; i--)
            {
                if (GetEndpointKey(_connectingConns[i].Endpoint) == key)
                {
                    var conn = _connectingConns[i];
                    _connectingConns.RemoveAt(i);
                    UnregisterSocket(conn);
                    conn.Close();
                    _metrics.IncrementClosedConn(_workerId);
                }
            }

            _unavailableEndpoints.Remove(key);
            _metrics.IncrementRemoveEndpoint();
        }

        _logger.LogInformation("Worker {WorkerId}: endpoint update complete, added={Added}, removed={Removed}, total={Total}", _workerId, added.Count, removed.Count, _currentEndpoints.Count);
    }

    // ==================== Cleanup ====================

    private void Cleanup()
    {
        int remaining = _messageQueue.Size + (_pendingMessage != null ? 1 : 0) + _currentBatch.MessageCount;
        _logger.LogInformation("Worker {WorkerId}: flushing {Remaining} remaining messages", _workerId, remaining);

        // 1. Drain queued/pending/batch messages. ProcessMessages(shutdown:true) sends
        //    without retry/backoff so a batch stuck in backoff is flushed immediately and
        //    the loop always makes forward progress (otherwise it would busy-spin until
        //    the deadline). Send the final partial batch after each pass.
        long deadline = Environment.TickCount64 + _config.StopTimeoutMs;
        bool flushTimedOut = false;
        while (!_messageQueue.IsEmpty || _pendingMessage != null)
        {
            if (Environment.TickCount64 >= deadline)
            {
                flushTimedOut = true;
                break;
            }

            ProcessMessages(shutdown: true);

            // Send final batch (no retry on shutdown)
            if (!_currentBatch.IsEmpty)
            {
                SendBatch(false);
            }
        }

        // 2. Send the final partial batch (no retry on shutdown).
        if (!_currentBatch.IsEmpty)
        {
            SendBatch(false);
        }

        // 3. Fail any messages that could not be flushed in time so their callbacks
        //    complete instead of hanging.
        if (flushTimedOut || !_messageQueue.IsEmpty || _pendingMessage != null || !_currentBatch.IsEmpty)
        {
            _logger.LogWarning("Worker {WorkerId}: cleanup timed out, failing remaining messages", _workerId);
            FailRemainingMessages();
        }

        // 4. Drain connection output buffers (active + delayed-close) while sockets
        //    are still alive.
        DrainConnectionBuffers();

        // 5. Rescue-and-close all delayed-close connections, moving leftover buffered
        //    data into the still-open active connections. Must run while _connections
        //    is still populated, since RescueData picks its target from that list.
        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug($"Worker {_workerId}: rescue-closing {_pendingCloseList.Count} pending delayed-close connections");
        foreach (var dc in _pendingCloseList)
        {
            FlushOrRescueAndClose(dc.Connection);
        }
        _pendingCloseList.Clear();

        // 6. Rescue may have moved data onto active connections — drain once more.
        DrainConnectionBuffers();

        // 7. Close all active and connecting connections.
        foreach (var conn in _connections)
        {
            UnregisterSocket(conn);
            conn.Close();
            _metrics.IncrementClosedConn(_workerId);
        }
        _connections.Clear();

        foreach (var conn in _connectingConns)
        {
            UnregisterSocket(conn);
            conn.Close();
        }
        _connectingConns.Clear();

        _socketToConnection.Clear();

        _logger.LogInformation("Worker {WorkerId}: cleanup completed", _workerId);
    }

    /// <summary>
    /// Drains buffered data from active and delayed-close connections, waiting on
    /// socket writability via <see cref="Socket.Select"/> (mirroring the steady-state
    /// event loop) rather than busy-waiting. Returns when all buffers are empty or
    /// the stop timeout elapses.
    /// </summary>
    private void DrainConnectionBuffers()
    {
        long deadline = Environment.TickCount64 + _config.StopTimeoutMs;

        while (Environment.TickCount64 < deadline)
        {
            _selectWriteList.Clear();
            bool anyBuffered = false;

            foreach (var conn in _connections)
            {
                if (conn.Socket != null && conn.HasBufferedData)
                {
                    conn.TryFlush();
                    if (conn.HasBufferedData)
                    {
                        _selectWriteList.Add(conn.Socket);
                        anyBuffered = true;
                    }
                }
            }
            foreach (var dc in _pendingCloseList)
            {
                if (dc.Connection.Socket != null && dc.Connection.HasBufferedData)
                {
                    dc.Connection.TryFlush();
                    if (dc.Connection.HasBufferedData)
                    {
                        _selectWriteList.Add(dc.Connection.Socket);
                        anyBuffered = true;
                    }
                }
            }

            if (!anyBuffered) return;

            // Wait for any buffered connection to become writable (5ms), like the event loop.
            try
            {
                Socket.Select(null, _selectWriteList, null, 5_000);
            }
            catch (SocketException)
            {
                // A socket faulted while waiting; loop will re-flush survivors.
            }
        }

        _logger.LogWarning("Worker {WorkerId}: drain timeout, some data may be unflushed", _workerId);
    }

    // ==================== Terminal State Guarantee ====================

    /// <summary>
    /// Fails all remaining messages (in queue, pending, and current batch) with a shutdown error.
    /// Final safety net so that, under the documented usage contract (all Send/SendAsync
    /// threads have stopped before Dispose/Close), every accepted message gets a terminal
    /// callback. This does NOT cover a Send that runs concurrently with shutdown — that
    /// scenario is explicitly outside the SDK's guarantees (callers must not send + close
    /// concurrently). Idempotent: safe to call even if messages have already been drained.
    /// </summary>
    private void FailRemainingMessages()
    {
        var shutdownException = new TglogException(TglogErrorCode.ProducerClosed, "Worker shutdown");

        // 1. Fail _pendingMessage
        if (_pendingMessage != null)
        {
            InvokeCallbackSafe(_pendingMessage, shutdownException);
            _metrics.IncrementFailedMsg();
            _pendingMessage = null;
        }

        // 2. Fail _currentBatch
        if (!_currentBatch.IsEmpty)
        {
            int batchCount = _currentBatch.MessageCount;
            _currentBatch.NotifyAllFailed(TglogErrorCode.ProducerClosed);
            _metrics.AddFailedMsg(batchCount);
        }

        // 3. Drain and fail all messages remaining in _messageQueue.
        // A producer may have CAS-claimed a slot (writeIndex++) but not yet written
        // the item (Volatile.Write). Poll() returns null for such slots.
        // We spin briefly when Poll() returns null but the queue reports non-empty
        // (writeIndex > readIndex), to wait for the in-flight write to complete.
        // The bounded spin is best-effort for an in-process write that is already
        // committed under normal operation; it is NOT meant to cover a producer
        // racing with shutdown (send + close concurrency is outside the contract).
        LogMessage? msg;
        int failedCount = 0;
        int nullCount = 0;
        while (true)
        {
            msg = _messageQueue.Poll();
            if (msg != null)
            {
                InvokeCallbackSafe(msg, shutdownException);
                failedCount++;
                nullCount = 0;
            }
            else
            {
                if (_messageQueue.IsEmpty)
                    break;
                // writeIndex > readIndex but slot is null: producer mid-write
                nullCount++;
                if (nullCount > 128)
                    break; // Give up after reasonable spin
                Thread.SpinWait(4);
            }
        }

        if (failedCount > 0)
        {
            _metrics.AddFailedMsg(failedCount);
            _logger.LogWarning("Worker {WorkerId}: failed {FailedCount} remaining queued messages during shutdown", _workerId, failedCount);
        }
    }

    /// <summary>
    /// Invokes a message callback safely — catches and swallows exceptions
    /// to prevent one bad callback from affecting shutdown or other messages.
    /// </summary>
    private void InvokeCallbackSafe(LogMessage message, TglogException exception)
    {
        try
        {
            message.Callback?.Invoke(message.Payload, exception);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Exception in user callback: {Message}", ex.Message);
        }
    }

    // ==================== Metrics ====================

    private void UpdateMetrics()
    {
        _metrics.SetMsgQueueSize(_workerId, _messageQueue.Size);
        _metrics.SetActiveConnCount(_workerId, _connections.Count);
    }

    // ==================== Helper Methods ====================

    private void RegisterSocket(TglogConnection conn)
    {
        if (conn.Socket != null)
        {
            _socketToConnection[conn.Socket] = conn;
        }
    }

    private void UnregisterSocket(TglogConnection conn)
    {
        if (conn.Socket != null)
        {
            _socketToConnection.Remove(conn.Socket);
        }
    }

    private void MarkEndpointUnavailable(IPEndPoint endpoint)
    {
        // Don't mark if only one endpoint (like Java)
        if (_currentEndpoints.Count <= 1) return;

        string key = GetEndpointKey(endpoint);

        // Don't mark if there are still live connections for this endpoint (like Java)
        foreach (var conn in _connections)
        {
            if (GetEndpointKey(conn.Endpoint) == key)
                return;
        }

        if (!_unavailableEndpoints.ContainsKey(key))
        {
            _unavailableEndpoints[key] = new UnavailableState(Environment.TickCount64);
            _logger.LogWarning("Worker {WorkerId}: marked endpoint unavailable: {Key}", _workerId, key);
        }
    }

    private void ClearEndpointUnavailable(IPEndPoint endpoint)
    {
        string key = GetEndpointKey(endpoint);
        if (_unavailableEndpoints.Remove(key))
        {
            _logger.LogInformation("Worker {WorkerId}: endpoint recovered: {Endpoint}", _workerId, endpoint);
        }
    }

    private static string GetEndpointKey(IPEndPoint endpoint) => $"{endpoint.Address}:{endpoint.Port}";

    private static IPEndPoint? ParseEndpointKey(string key)
    {
        int colonIdx = key.LastIndexOf(':');
        if (colonIdx < 0) return null;
        if (IPAddress.TryParse(key.AsSpan(0, colonIdx), out var addr) &&
            int.TryParse(key.AsSpan(colonIdx + 1), out int port))
        {
            return new IPEndPoint(addr, port);
        }
        return null;
    }
}
