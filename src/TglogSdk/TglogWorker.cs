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
    private const long EndpointRecoverIntervalMs = 30000;
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

    // Connections
    private readonly List<TglogConnection> _connections = new(8);
    private readonly List<TglogConnection> _connectingConns = new(8);
    private readonly Dictionary<Socket, TglogConnection> _socketToConnection = new();
    private long _connectionIndex;
    private int _connectionsPerEndpoint;
    private int _endpointReplaceIndex;

    // Endpoints
    private List<IPEndPoint> _currentEndpoints = new();
    private readonly Dictionary<string, long> _unavailableEndpoints = new();
    private readonly Dictionary<string, int> _endpointRetryCount = new();

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
    /// Returns true if enqueued, false if the queue is full.
    /// </summary>
    public bool Enqueue(LogMessage message)
    {
        if (_state != WorkerState.Ready)
            return false;

        if (!_messageQueue.Offer(message))
        {
            return false;
        }

        _metrics.IncrementEnqueuedMsg();
        return true;
    }

    /// <summary>Notifies the worker of an endpoint change (called from EndpointManager thread).</summary>
    public void NotifyEndpointUpdate(IReadOnlyList<IPEndPoint> newEndpoints)
    {
        _endpointUpdateQueue.Enqueue(newEndpoints);
    }

    /// <summary>Returns true if the worker has pending messages or buffered data.</summary>
    public bool HasPendingWork()
    {
        if (!_messageQueue.IsEmpty) return true;
        if (_pendingMessage != null) return true;
        if (!_currentBatch.IsEmpty) return true;

        for (int i = 0; i < _connections.Count; i++)
        {
            if (_connections[i].HasBufferedData) return true;
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
            _logger.LogError($"Worker {_workerId}: fatal error: {ex}");
        }
        finally
        {
            try
            {
                Cleanup();
            }
            catch (Exception ex)
            {
                _logger.LogError($"Worker {_workerId}: error during cleanup: {ex}");
            }
            // Guarantee terminal state for all remaining messages, even if Cleanup threw
            FailRemainingMessages();
            _state = WorkerState.Closed;
        }
    }

    // ==================== Initialization ====================

    private void InitializeConnections()
    {
        if (_initialEndpoints == null || _initialEndpoints.Count == 0)
        {
            _logger.LogWarning($"Worker {_workerId}: no initial endpoints");
            return;
        }

        _logger.LogInformation($"Worker {_workerId}: initializing connections, endpoints={_initialEndpoints.Count}");

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

        _logger.LogInformation($"Worker {_workerId}: using {_connectionsPerEndpoint} connections per endpoint (total endpoints: {_currentEndpoints.Count})");
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

                // Process results
                for (int i = _connectingConns.Count - 1; i >= 0; i--)
                {
                    var conn = _connectingConns[i];
                    if (conn.Socket == null) continue;

                    bool inError = checkError.Contains(conn.Socket);
                    bool inWrite = checkWrite.Contains(conn.Socket);

                    if (inError)
                    {
                        _connectingConns.RemoveAt(i);
                        UnregisterSocket(conn);
                        conn.Close();
                    }
                    else if (inWrite)
                    {
                        if (conn.FinishConnect())
                        {
                            _connectingConns.RemoveAt(i);
                            _connections.Add(conn);
                        }
                        else
                        {
                            _connectingConns.RemoveAt(i);
                            UnregisterSocket(conn);
                            conn.Close();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Worker {_workerId}: WaitForConnecting error: {ex.Message}");
                break;
            }
        }

        // Timeout: close remaining connecting connections
        if (_connectingConns.Count > 0)
        {
            _logger.LogWarning($"Worker {_workerId}: {_connectingConns.Count} connections timed out during initialization");
        }

        for (int i = _connectingConns.Count - 1; i >= 0; i--)
        {
            var conn = _connectingConns[i];
            UnregisterSocket(conn);
            conn.Close();
        }
        _connectingConns.Clear();
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

            // 7. Periodic maintenance
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

        _logger.LogInformation($"Worker {_workerId}: event loop ended");
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

        // Also check delayed close connections for writability
        foreach (var dc in _pendingCloseList)
        {
            if (dc.Connection.Socket != null && dc.Connection.HasBufferedData)
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
            _logger.LogWarning($"Worker {_workerId}: select error: {ex.Message}");
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
                HandleConnectionError(conn, false);
            }
        }

        // Process readable (TCP: detect peer close)
        foreach (var socket in _selectReadList)
        {
            if (_socketToConnection.TryGetValue(socket, out var conn))
            {
                if (!conn.HandleRead())
                {
                    HandleConnectionError(conn, false);
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
                    // Already handled in CheckConnectingConnections
                    continue;
                }
                conn.TryFlush();

                if (conn.State == TglogConnection.ConnectionState.Error)
                {
                    HandleConnectionError(conn, false);
                }
            }
        }
    }

    private void CheckConnectingConnections()
    {
        for (int i = _connectingConns.Count - 1; i >= 0; i--)
        {
            var conn = _connectingConns[i];
            if (conn.Socket == null)
            {
                _connectingConns.RemoveAt(i);
                UnregisterSocket(conn);
                continue;
            }

            // Check if connection completed (socket was in write list)
            if (_selectWriteList.Contains(conn.Socket) || _selectErrorList.Contains(conn.Socket))
            {
                if (_selectErrorList.Contains(conn.Socket))
                {
                    _connectingConns.RemoveAt(i);
                    MarkEndpointUnavailable(conn.Endpoint);
                    UnregisterSocket(conn);
                    conn.Close();
                    _metrics.IncrementErrorConn(_workerId);
                    continue;
                }

                if (conn.FinishConnect())
                {
                    _connectingConns.RemoveAt(i);
                    _connections.Add(conn);
                    _logger.LogInformation($"Worker {_workerId}: connection ready: {conn.Endpoint} (total: {_connections.Count})");
                }
                else
                {
                    _connectingConns.RemoveAt(i);
                    MarkEndpointUnavailable(conn.Endpoint);
                    UnregisterSocket(conn);
                    conn.Close();
                    _metrics.IncrementErrorConn(_workerId);
                }
            }
        }
    }

    // ==================== Message Processing ====================

    private void ProcessMessages()
    {
        int processed = 0;

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
                    _logger.LogWarning($"Worker {_workerId}: batch retry/send timeout");
                    FailCurrentBatch(TglogErrorCode.SendTimeout);
                    // Continue to process new messages
                }
                else if (_currentBatch.ShouldRetryNow(Backoff.BatchRetry))
                {
                    // Time to retry - attempt to send the batch again
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug($"Worker {_workerId}: attempting retry {_currentBatch.RetryCount}/{_config.MaxRetries}");
                    if (!SendBatch(true))
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
                _logger.LogWarning($"Worker {_workerId}: batch send timeout");
                FailCurrentBatch(TglogErrorCode.SendTimeout);
            }

            // Send current batch if ready (count/size/timeout triggers)
            if (_currentBatch.ShouldFlush())
            {
                if (!SendBatch(true))
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
                if (!SendBatch(true))
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
                if (!SendBatch(true))
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
                if (!SendBatch(true))
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
            _logger.LogError($"Worker {_workerId}: no available connections");
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
        int tryCount = 0;
        var conn = startConn;

        while (tryCount < _connections.Count)
        {
            long sent = conn.Send(buffers);

            if (sent < 0)
            {
                // Connection error
                _logger.LogError($"Worker {_workerId}: TCP send error on {conn.Endpoint}, trying next");
                HandleConnectionError(conn, true);
                tryCount++;
                conn = GetConnection()!;
                if (conn == null) break;
                continue;
            }

            if (sent == 0)
            {
                // High watermark exceeded — try next connection
                _logger.LogWarning($"Worker {_workerId}: connection {conn.Endpoint} exceeded high watermark");
                tryCount++;
                conn = GetNextConnection(conn);
                if (conn == null) break;
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
        int maxTries = Math.Min(_connections.Count, 3);
        int tryCount = 0;
        var conn = startConn;

        while (tryCount < maxTries)
        {
            long sent = conn.Send(buffers);

            if (sent > 0)
            {
                return SendResult.Ok();
            }

            if (sent == 0)
            {
                // EAGAIN — try next
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug($"Worker {_workerId}: UDP send would block, trying another (attempt {tryCount + 1}/{maxTries})");
                tryCount++;
                conn = GetNextConnection(conn);
                if (conn == null) break;
                continue;
            }

            // sent < 0: error
            HandleConnectionError(conn, true);
            tryCount++;
            conn = GetNextConnection(conn);
            if (conn == null) break;
        }

        return SendResult.Error(TglogErrorCode.SendFailed);
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
        _logger.LogInformation($"Worker {_workerId}: retrying batch in {backoffMs}ms (attempt {_currentBatch.RetryCount}/{_config.MaxRetries})");

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
                _logger.LogError($"Worker {_workerId}: no available connections");
                return null;
            }
        }

        long index = Interlocked.Increment(ref _connectionIndex) & long.MaxValue;
        return _connections[(int)(index % _connections.Count)];
    }

    private TglogConnection? GetNextConnection(TglogConnection current)
    {
        if (_connections.Count <= 1) return null;

        int currentIdx = _connections.IndexOf(current);
        if (currentIdx < 0) return _connections.Count > 0 ? _connections[0] : null;

        int nextIdx = (currentIdx + 1) % _connections.Count;
        return _connections[nextIdx];
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

    private void HandleConnectionError(TglogConnection conn, bool fromSend)
    {
        // Remove from active connections
        _connections.Remove(conn);
        _connectingConns.Remove(conn);

        // Mark endpoint unavailable (like Java)
        MarkEndpointUnavailable(conn.Endpoint);

        // Schedule delayed close for data rescue opportunity
        CloseConnectionWithDelay(conn, true);

        // Update metrics
        _metrics.IncrementErrorConn(_workerId);

        // Try to supplement connection (like Java)
        SupplementConnection();
    }

    private void CloseConnectionWithDelay(TglogConnection conn, bool isError)
    {
        // Remove any existing pending close for this connection
        _pendingCloseList.RemoveAll(dc => dc.Connection == conn);

        int delayMs;
        if (isError)
        {
            delayMs = conn.HasBufferedData ? ErrorConnCloseDelayWithDataMs : ErrorConnCloseDelayNoDataMs;
        }
        else
        {
            delayMs = DefaultConnCloseDelayMs;
        }

        _logger.LogInformation($"Worker {_workerId}: scheduling connection close in {delayMs}ms: {conn.Endpoint}");

        _pendingCloseList.Add(new DelayedClose
        {
            Connection = conn,
            DeadlineTicks = Environment.TickCount64 + delayMs
        });
    }

    private void ProcessPendingCloses()
    {
        long now = Environment.TickCount64;

        for (int i = _pendingCloseList.Count - 1; i >= 0; i--)
        {
            var dc = _pendingCloseList[i];
            if (now >= dc.DeadlineTicks)
            {
                _pendingCloseList.RemoveAt(i);

                // Try to rescue buffered data
                RescueData(dc.Connection);

                UnregisterSocket(dc.Connection);
                dc.Connection.Close();
                _metrics.IncrementClosedConn(_workerId);

                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug($"Worker {_workerId}: closed delayed connection: {dc.Connection.Endpoint}");
            }
        }
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
        _logger.LogWarning($"Worker {_workerId}: lost {rescuedBytes} bytes - no rescue connection");
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

        var toRemove = new List<string>();
        long now = Environment.TickCount64;

        foreach (var kvp in _unavailableEndpoints)
        {
            string endpointKey = kvp.Key;
            int retryCount = _endpointRetryCount.GetValueOrDefault(endpointKey, 0);
            long nextInterval = Backoff.EndpointRecovery.GetNextInterval(retryCount + 1);
            long elapsed = now - kvp.Value;

            if (elapsed >= nextInterval)
            {
                var endpoint = ParseEndpointKey(endpointKey);
                if (endpoint == null) continue;

                bool success = false;
                for (int i = 0; i < _connectionsPerEndpoint; i++)
                {
                    if (CreateConnection(endpoint) != null)
                        success = true;
                }

                if (success)
                {
                    toRemove.Add(endpointKey);
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug($"Worker {_workerId}: recovered endpoint {endpointKey}");
                }
                else
                {
                    _endpointRetryCount[endpointKey] = retryCount + 1;
                    _unavailableEndpoints[endpointKey] = now;
                }
            }
        }

        foreach (var key in toRemove)
        {
            _unavailableEndpoints.Remove(key);
            _endpointRetryCount.Remove(key);
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
                    _logger.LogWarning($"Worker {_workerId}: supplement connection failed for {endpoint}");
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
                    _logger.LogWarning($"Worker {_workerId}: force supplement connection failed for {endpoint}");
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
        _logger.LogInformation($"Worker {_workerId}: processing endpoint update: {newEndpoints.Count} endpoints");

        var oldSet = new HashSet<string>(_currentEndpoints.Select(GetEndpointKey));
        var newSet = new HashSet<string>(newEndpoints.Select(GetEndpointKey));

        // Find added endpoints
        var added = newEndpoints.Where(ep => !oldSet.Contains(GetEndpointKey(ep))).ToList();
        // Find removed endpoints
        var removed = _currentEndpoints.Where(ep => !newSet.Contains(GetEndpointKey(ep))).ToList();

        // Recalculate connections per endpoint
        _connectionsPerEndpoint = newEndpoints.Count <= 2 ? 2 : 1;

        // Add connections for new endpoints first
        foreach (var endpoint in added)
        {
            for (int i = 0; i < _connectionsPerEndpoint; i++)
            {
                CreateConnection(endpoint);
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
            _endpointRetryCount.Remove(key);
            _metrics.IncrementRemoveEndpoint();
        }

        _currentEndpoints = new List<IPEndPoint>(newEndpoints);
        _metrics.SetEndpointCount(_currentEndpoints.Count);

        _logger.LogInformation($"Worker {_workerId}: endpoint update complete, added={added.Count}, removed={removed.Count}, total={_currentEndpoints.Count}");
    }

    // ==================== Cleanup ====================

    private void Cleanup()
    {
        int remaining = _messageQueue.Size + (_pendingMessage != null ? 1 : 0) + _currentBatch.MessageCount;
        _logger.LogInformation($"Worker {_workerId}: flushing {remaining} remaining messages");

        // Drain remaining messages
        long deadline = Environment.TickCount64 + _config.StopTimeoutMs;

        while (Environment.TickCount64 < deadline)
        {
            // Process remaining messages without retry
            var msg = _pendingMessage;
            _pendingMessage = null;
            if (msg == null) msg = _messageQueue.Poll();
            if (msg == null)
            {
                if (!_currentBatch.IsEmpty)
                {
                    SendBatchNoRetry();
                }
                break;
            }

            _metrics.IncrementDequeuedMsg();

            if (!_currentBatch.Add(msg))
            {
                SendBatchNoRetry();
                _currentBatch.Add(msg);
            }
        }

        if (Environment.TickCount64 >= deadline)
        {
            _logger.LogWarning($"Worker {_workerId}: cleanup timed out, failing remaining messages");
            // Fail all messages still in queue/batch — don't leave them hanging
            FailRemainingMessages();
        }

        // Send final partial batch
        if (!_currentBatch.IsEmpty)
        {
            SendBatchNoRetry();
        }

        // Flush connection buffers
        long flushDeadline = Environment.TickCount64 + Math.Min(_config.StopTimeoutMs, 2000);
        while (Environment.TickCount64 < flushDeadline)
        {
            bool anyBuffered = false;
            foreach (var conn in _connections)
            {
                if (conn.HasBufferedData)
                {
                    conn.TryFlush();
                    anyBuffered = true;
                }
            }
            if (!anyBuffered) break;
            Thread.Sleep(1);
        }

        // Close all pending delayed-close connections
        foreach (var dc in _pendingCloseList)
        {
            UnregisterSocket(dc.Connection);
            dc.Connection.Close();
        }
        _pendingCloseList.Clear();

        // Close all active and connecting connections
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

        _logger.LogInformation($"Worker {_workerId}: cleanup completed");
    }

    private void SendBatchNoRetry()
    {
        if (_currentBatch.IsEmpty) return;
        if (_connections.Count == 0)
        {
            // No available connections — fail the entire batch explicitly
            FailCurrentBatch(TglogErrorCode.NoAvailableConn);
            return;
        }

        // Send without retry (used during cleanup)
        SendBatch(false);
    }

    // ==================== Terminal State Guarantee ====================

    /// <summary>
    /// Fails all remaining messages (in queue, pending, and current batch) with a shutdown error.
    /// Called as a final safety net to ensure every accepted message gets a terminal callback.
    /// Idempotent: safe to call even if messages have already been drained.
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
            _logger.LogWarning($"Worker {_workerId}: failed {failedCount} remaining queued messages during shutdown");
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
            _logger.LogWarning($"Exception in user callback: {ex.Message}");
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
            _unavailableEndpoints[key] = Environment.TickCount64;
            _endpointRetryCount[key] = 0;
            _logger.LogWarning($"Worker {_workerId}: marked endpoint unavailable: {key}");
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
