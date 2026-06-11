using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Tencent.Tglog;

/// <summary>
/// Main client for sending log messages to TGLog servers.
///
/// Features:
/// - Asynchronous, non-blocking send with callback and Task support
/// - Multiple worker threads for high throughput (default 8)
/// - Lock-free message distribution via round-robin
/// - Automatic connection management, retry, and DNS resolution
/// - Prometheus metrics integration
/// - Pluggable logging via <see cref="ILogger"/>
///
/// Usage:
/// <code>
/// var config = TglogConfig.CreateTcp("log-server.example.com", 8080)
///     .SetWorkerNum(8)
///     .SetBatchingMaxMessages(50);
///
/// using var client = new TglogClient(config);
///
/// // Fire and forget
/// client.Send("my log message\n");
///
/// // With callback
/// client.Send("my log message\n", (payload, ex) => {
///     if (ex != null) Console.Error.WriteLine($"Failed: {ex.ErrorCode}");
/// });
///
/// // Async/await
/// await client.SendAsync("my log message\n");
/// </code>
/// </summary>
public sealed class TglogClient : IDisposable
{
    private readonly TglogConfig _config;
    private readonly TglogMetrics _metrics;
    private readonly EndpointManager _endpointManager;
    private readonly TglogWorker[] _workers;
    private readonly ILogger _logger;
    private readonly ILoggerFactory _loggerFactory;

    private int _roundRobinIndex;
    private volatile int _running; // 0 = not started/stopped, 1 = running
    private volatile int _closed;  // 0 = open, 1 = closed

    /// <summary>Access to performance metrics.</summary>
    public TglogMetrics Metrics => _metrics;

    /// <summary>
    /// Creates and starts a new TGLog client with the given configuration.
    /// </summary>
    /// <exception cref="TglogException">Thrown if configuration is invalid or initialization fails.</exception>
    public TglogClient(TglogConfig config)
    {
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _config.Validate();

        // Setup logger factory
        _loggerFactory = config.LoggerFactory
            ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger("TGLog.Client");

        _metrics = new TglogMetrics(config.WorkerNum, config.PrometheusRegistry);
        _endpointManager = new EndpointManager(config, _loggerFactory);
        _workers = new TglogWorker[config.WorkerNum];

        // From this point on, any failure must unregister metrics to keep
        // the shared CollectorRegistry clean for retry / recreation.
        bool initSucceeded = false;
        try
        {
            Start();
            initSucceeded = true;
        }
        finally
        {
            if (!initSucceeded)
            {
                try { _endpointManager.Dispose(); }
                catch (Exception ex)
                {
                    _logger.LogWarning("Failed to dispose endpoint manager during construction cleanup: {Message}", ex.Message);
                }
                try { _metrics.Unregister(); }
                catch (Exception ex)
                {
                    _logger.LogWarning("Failed to unregister metrics during construction cleanup: {Message}", ex.Message);
                }
            }
        }
    }

    /// <summary>
    /// Sends a log payload asynchronously with an optional callback.
    /// Returns a Task that completes when the message is sent or fails.
    ///
    /// The payload MUST end with '\n'.
    /// </summary>
    /// <exception cref="TglogException">Thrown synchronously for validation errors or if client is closed.</exception>
    public Task SendAsync(string logPayload, TglogCallback? callback = null)
    {
        EnsureRunning();
        ValidatePayload(logPayload);

        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);

        TglogCallback wrappedCallback = (payload, exception) =>
        {
            try
            {
                callback?.Invoke(payload, exception);
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Exception in user callback: {Message}", ex.Message);
            }

            if (exception == null)
                tcs.TrySetResult(null);
            else
                tcs.TrySetException(exception);
        };

        var message = new LogMessage(logPayload, wrappedCallback);

        if (message.PayloadSize > _config.MaxRequestSize)
        {
            throw new TglogException(TglogErrorCode.LogTooLong,
                $"Payload size {message.PayloadSize} exceeds max request size {_config.MaxRequestSize}");
        }

        _metrics.IncrementRequestMsg();

        var worker = SelectWorker();
        if (!worker.Enqueue(message))
        {
            _metrics.IncrementDroppedMsg();
            if (_closed != 0)
                throw new TglogException(TglogErrorCode.ProducerClosed, "Client has been closed");
            throw new TglogException(TglogErrorCode.SendQueueIsFull, "Worker queue is full");
        }

        return tcs.Task;
    }

    /// <summary>
    /// Sends a log payload in fire-and-forget mode with an optional callback.
    /// Does not return a Task — use <see cref="SendAsync"/> for awaitable sends.
    ///
    /// The payload MUST end with '\n'.
    /// </summary>
    /// <exception cref="TglogException">Thrown synchronously for validation errors or if client is closed.</exception>
    public void Send(string logPayload, TglogCallback? callback = null)
    {
        EnsureRunning();
        ValidatePayload(logPayload);

        var message = new LogMessage(logPayload, callback);

        if (message.PayloadSize > _config.MaxRequestSize)
        {
            throw new TglogException(TglogErrorCode.LogTooLong,
                $"Payload size {message.PayloadSize} exceeds max request size {_config.MaxRequestSize}");
        }

        _metrics.IncrementRequestMsg();

        var worker = SelectWorker();
        if (!worker.Enqueue(message))
        {
            _metrics.IncrementDroppedMsg();
            if (_closed != 0)
                throw new TglogException(TglogErrorCode.ProducerClosed, "Client has been closed");
            throw new TglogException(TglogErrorCode.SendQueueIsFull, "Worker queue is full");
        }
    }

    /// <summary>
    /// Waits for all pending messages to be sent, up to the specified timeout.
    /// Returns true if all messages were flushed within the timeout.
    /// </summary>
    /// <remarks>
    /// During normal operation you do not need to call <c>Flush</c>; messages are
    /// batched and sent automatically by worker threads. To shut down, just call
    /// <see cref="Dispose"/> — it flushes pending messages (bounded by
    /// <see cref="TglogConfig.StopTimeoutMs"/>) before stopping workers and releasing
    /// resources. <c>Flush</c> is still useful for tests or before a critical
    /// checkpoint where you need to guarantee delivery at a specific point.
    /// </remarks>
    public bool Flush(int timeoutMs = 5000)
    {
        // Post a flush request to each worker rather than inspecting worker-internal
        // state from this (caller) thread. Each worker completes its task from inside
        // the worker thread once it has drained all pending work, so we never touch
        // the worker's non-thread-safe structures cross-thread. The timeout budget is
        // enforced here, in the caller — workers never expire a request themselves.
        var tasks = new Task<bool>[_workers.Length];
        for (int i = 0; i < _workers.Length; i++)
        {
            tasks[i] = _workers[i].RequestFlush();
        }

        long deadline = Environment.TickCount64 + timeoutMs;
        bool allDrained = true;
        foreach (var task in tasks)
        {
            long remaining = deadline - Environment.TickCount64;
            if (remaining <= 0)
            {
                // Out of time budget; only count it if already completed as drained.
                if (!task.IsCompletedSuccessfully || !task.Result)
                    allDrained = false;
            }
            else if (task.Wait((int)remaining))
            {
                if (!task.Result) allDrained = false;
            }
            else
            {
                allDrained = false;
            }
        }

        return allDrained;
    }

    /// <summary>Returns the metrics as a JSON string.</summary>
    public string GetMetricsJson() => _metrics.ToJson();

    /// <summary>Returns the metrics in human-readable format.</summary>
    public string GetMetricsString() => _metrics.ToString();

    /// <summary>
    /// Gracefully shuts down the client: flushes pending messages, then closes all connections.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _closed, 1, 0) != 0)
            return; // Already closed

        _running = 0;
        _logger.LogInformation("Client shutting down");

        // Flush pending messages before tearing down workers. Workers also drain
        // remaining messages during their own cleanup, but flushing first gives
        // them a chance to deliver under the normal (retrying) send path.
        Flush(_config.StopTimeoutMs);

        // Stop endpoint manager
        _endpointManager.Dispose();

        // Stop all workers
        foreach (var worker in _workers)
            worker.Stop();

        // Wait for workers to finish.
        // Worker Cleanup() internally uses StopTimeoutMs for drain + up to 2s for
        // flushing connection buffers, so we allow extra headroom beyond StopTimeoutMs.
        int joinTimeoutMs = _config.StopTimeoutMs + 3000;
        bool allStopped = true;
        foreach (var worker in _workers)
        {
            if (!worker.Join(joinTimeoutMs))
            {
                allStopped = false;
                _logger.LogWarning("Worker did not stop within timeout");
            }
        }

        // Unregister metrics after all workers have stopped.
        // If a worker somehow still hasn't exited (should be extremely rare given
        // the generous join timeout), we unregister anyway — a stuck worker thread
        // is unlikely to be actively calling Inc() at this point.
        if (!allStopped)
        {
            _logger.LogWarning("Not all workers stopped, proceeding with metrics unregister");
        }
        try
        {
            _metrics.Unregister();
        }
        catch (Exception e)
        {
            _logger.LogWarning("Failed to unregister metrics during close: {Message}", e.Message);
        }

        _logger.LogInformation("Client shutdown complete");
    }

    // ==================== Private Methods ====================

    private void Start()
    {
        // Start endpoint manager and resolve initial endpoints
        _endpointManager.AddChangeListener(OnEndpointsChanged);
        _endpointManager.Start();

        var initialEndpoints = _endpointManager.CurrentEndpoints;
        if (initialEndpoints.Count == 0)
        {
            throw new TglogException(TglogErrorCode.NewConnFailed,
                $"No endpoints resolved for {_config.Host}:{_config.Port}");
        }

        _metrics.SetEndpointCount(initialEndpoints.Count);

        // Auto-adjust batching max size based on protocol limits
        if (_config.BatchingMaxSize > _config.MaxRequestSize)
        {
            _logger.LogWarning("Batching max size ({BatchingMaxSize}) exceeds protocol limit ({MaxRequestSize}), adjusting", _config.BatchingMaxSize, _config.MaxRequestSize);
        }

        // Create and start workers
        for (int i = 0; i < _workers.Length; i++)
        {
            _workers[i] = new TglogWorker(i, _config, _metrics, _loggerFactory);
            _workers[i].SetInitialEndpoints(initialEndpoints);
            _workers[i].Start();
        }

        // Wait for workers to be ready
        int waitTimeout = _config.ConnectionTimeoutMs * 2;
        foreach (var worker in _workers)
        {
            if (!worker.WaitForReady(waitTimeout))
            {
                // A worker failed to become ready — abort startup and rollback
                _logger.LogError("Worker failed to become ready within {TimeoutMs}ms, aborting startup", waitTimeout);
                foreach (var w in _workers)
                {
                    if (w != null)
                    {
                        w.Stop();
                        w.Join(waitTimeout);
                    }
                }
                throw new TglogException(TglogErrorCode.NewConnFailed,
                    "One or more workers failed to become ready");
            }
        }

        _running = 1;

        _logger.LogInformation("Client created: {Protocol}://{Host}:{Port} (workers={WorkerCount}, endpoints={EndpointCount})", _config.TransportProtocol, _config.Host, _config.Port, _workers.Length, initialEndpoints.Count);
    }

    private void OnEndpointsChanged(IReadOnlyList<System.Net.IPEndPoint> newEndpoints)
    {
        _logger.LogInformation("Endpoints changed: {Count} endpoints", newEndpoints.Count);
        _metrics.SetEndpointCount(newEndpoints.Count);

        foreach (var worker in _workers)
            worker?.NotifyEndpointUpdate(newEndpoints);
    }

    private TglogWorker SelectWorker()
    {
        int index = (Interlocked.Increment(ref _roundRobinIndex) & int.MaxValue) % _workers.Length;
        return _workers[index];
    }

    private void EnsureRunning()
    {
        if (_closed != 0)
            throw new TglogException(TglogErrorCode.ProducerClosed, "Client has been closed");
        if (_running == 0)
            throw new TglogException(TglogErrorCode.ProducerClosed, "Client is not running");
    }

    private static void ValidatePayload(string logPayload)
    {
        if (string.IsNullOrEmpty(logPayload))
            throw new TglogException(TglogErrorCode.BadLog, "Payload cannot be null or empty");
        if (!logPayload.EndsWith('\n'))
            throw new TglogException(TglogErrorCode.BadLog, "Payload must end with '\\n'");
    }
}
