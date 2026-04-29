using System.Text;
using Prometheus;

namespace Tencent.Tglog;

/// <summary>
/// Performance metrics for TGLog client using prometheus-net.
///
/// Metric names and labels match the Java SDK exactly for cross-platform monitoring consistency.
/// Thread-safe by design (prometheus-net handles concurrency internally).
///
/// <b>Lifecycle:</b> Metrics are registered with the provided
/// <see cref="CollectorRegistry"/> in the constructor and unpublished via
/// <see cref="Unregister"/>. When reusing a single registry across multiple
/// TglogClient instances in the same process (e.g. config hot-reload), the previous
/// client must complete its <see cref="TglogClient.Dispose"/> (which calls <see cref="Unregister"/>)
/// before the new client is created. If overlap is unavoidable, give each client its
/// own <see cref="CollectorRegistry"/> via <see cref="TglogConfig.SetPrometheusRegistry"/>.
/// </summary>
public sealed class TglogMetrics
{
    private readonly int _workerCount;

    /// <summary>The Prometheus CollectorRegistry used by this metrics instance.</summary>
    public CollectorRegistry Registry { get; }

    // ========== Counter Metrics ==========

    private readonly Counter _requestMsgCount;
    private readonly Counter _enqueuedMsgCount;
    private readonly Counter _dequeuedMsgCount;
    private readonly Counter _succeededMsgCount;
    private readonly Counter _failedMsgCount;
    private readonly Counter _timeoutMsgCount;
    private readonly Counter _retryMsgCount;
    private readonly Counter _droppedMsgCount;

    private readonly Counter _openedConnCount;     // label: worker_id
    private readonly Counter _closedConnCount;     // label: worker_id
    private readonly Counter _errorConnCount;      // label: worker_id
    private readonly Counter _eagainErrCount;      // label: worker_id

    private readonly Counter _addEndpointCount;
    private readonly Counter _removeEndpointCount;

    private readonly Counter _rescuedMsgBytes;

    // ========== Gauge Metrics ==========

    private readonly Gauge _msgQueueSize;          // label: worker_id
    private readonly Gauge _msgQueueCapacity;      // label: worker_id
    private readonly Gauge _endpointCount;
    private readonly Gauge _activeConnCount;       // label: worker_id

    /// <summary>
    /// Creates a new TglogMetrics instance. Registers all Prometheus metrics.
    /// </summary>
    /// <param name="workerCount">Number of worker threads.</param>
    /// <param name="registry">Custom Prometheus registry. Null means use default.</param>
    public TglogMetrics(int workerCount, CollectorRegistry? registry = null)
    {
        _workerCount = workerCount;
        Registry = registry ?? Metrics.DefaultRegistry;
        var factory = Metrics.WithCustomRegistry(Registry);

        // --- Counters (no labels) ---
        _requestMsgCount = factory.CreateCounter(
            "tglog_request_messages_total",
            "Total number of messages requested to send");
        _enqueuedMsgCount = factory.CreateCounter(
            "tglog_enqueued_messages_total",
            "Total number of messages successfully enqueued");
        _dequeuedMsgCount = factory.CreateCounter(
            "tglog_dequeued_messages_total",
            "Total number of messages successfully dequeued");
        _succeededMsgCount = factory.CreateCounter(
            "tglog_succeeded_messages_total",
            "Total number of messages successfully sent");
        _failedMsgCount = factory.CreateCounter(
            "tglog_failed_messages_total",
            "Total number of messages failed to send");
        _timeoutMsgCount = factory.CreateCounter(
            "tglog_timeout_messages_total",
            "Total number of messages timed out");
        _retryMsgCount = factory.CreateCounter(
            "tglog_retry_messages_total",
            "Total number of message retries");
        _droppedMsgCount = factory.CreateCounter(
            "tglog_dropped_messages_total",
            "Total number of messages dropped (queue full)");

        _addEndpointCount = factory.CreateCounter(
            "tglog_add_endpoints_total",
            "Total number of endpoints added");
        _removeEndpointCount = factory.CreateCounter(
            "tglog_remove_endpoints_total",
            "Total number of endpoints removed");
        _rescuedMsgBytes = factory.CreateCounter(
            "tglog_rescued_message_bytes_total",
            "Total bytes of rescued messages");

        // --- Counters (with worker_id label) ---
        _openedConnCount = factory.CreateCounter(
            "tglog_opened_connections_total",
            "Total number of connections opened",
            "worker_id");
        _closedConnCount = factory.CreateCounter(
            "tglog_closed_connections_total",
            "Total number of connections closed",
            "worker_id");
        _errorConnCount = factory.CreateCounter(
            "tglog_error_connections_total",
            "Total number of connection errors",
            "worker_id");
        _eagainErrCount = factory.CreateCounter(
            "tglog_eagain_errors_total",
            "Total number of EAGAIN errors (socket buffer full)",
            "worker_id");

        // --- Gauges (with worker_id label) ---
        _msgQueueSize = factory.CreateGauge(
            "tglog_message_queue_size",
            "Current size of the message queue",
            "worker_id");
        _msgQueueCapacity = factory.CreateGauge(
            "tglog_message_queue_capacity",
            "Capacity of the message queue",
            "worker_id");
        _activeConnCount = factory.CreateGauge(
            "tglog_active_connections",
            "Current number of active connections",
            "worker_id");

        // --- Gauges (no labels) ---
        _endpointCount = factory.CreateGauge(
            "tglog_endpoints",
            "Current number of endpoints");
    }

    // ========== Counter Increments ==========

    public void IncrementRequestMsg() => _requestMsgCount.Inc();
    public void IncrementEnqueuedMsg() => _enqueuedMsgCount.Inc();
    public void IncrementDequeuedMsg() => _dequeuedMsgCount.Inc();
    public void IncrementSucceededMsg() => _succeededMsgCount.Inc();
    public void IncrementFailedMsg() => _failedMsgCount.Inc();
    public void IncrementTimeoutMsg() => _timeoutMsgCount.Inc();
    public void IncrementRetryMsg() => _retryMsgCount.Inc();
    public void IncrementDroppedMsg() => _droppedMsgCount.Inc();
    public void IncrementAddEndpoint() => _addEndpointCount.Inc();
    public void IncrementRemoveEndpoint() => _removeEndpointCount.Inc();
    public void AddRescuedMsgBytes(long bytes) => _rescuedMsgBytes.Inc(bytes);

    public void AddSucceededMsg(int count) { if (count > 0) _succeededMsgCount.Inc(count); }
    public void AddFailedMsg(int count) { if (count > 0) _failedMsgCount.Inc(count); }
    public void AddTimeoutMsg(int count) { if (count > 0) _timeoutMsgCount.Inc(count); }
    public void AddRetryMsg(int count) { if (count > 0) _retryMsgCount.Inc(count); }

    // --- Per-Worker Counter Increments ---

    public void IncrementOpenedConn(int workerId) => _openedConnCount.WithLabels(workerId.ToString()).Inc();
    public void IncrementClosedConn(int workerId) => _closedConnCount.WithLabels(workerId.ToString()).Inc();
    public void IncrementErrorConn(int workerId) => _errorConnCount.WithLabels(workerId.ToString()).Inc();
    public void IncrementEagainErr(int workerId) => _eagainErrCount.WithLabels(workerId.ToString()).Inc();

    // ========== Gauge Updates ==========

    public void SetMsgQueueSize(int workerId, long size) => _msgQueueSize.WithLabels(workerId.ToString()).Set(size);
    public void SetMsgQueueCapacity(int workerId, long capacity) => _msgQueueCapacity.WithLabels(workerId.ToString()).Set(capacity);
    public void SetActiveConnCount(int workerId, long count) => _activeConnCount.WithLabels(workerId.ToString()).Set(count);
    public void SetEndpointCount(long count) => _endpointCount.Set(count);

    // ========== Getters (read from Prometheus counters/gauges) ==========

    public long RequestMsgCount => (long)_requestMsgCount.Value;
    public long EnqueuedMsgCount => (long)_enqueuedMsgCount.Value;
    public long DequeuedMsgCount => (long)_dequeuedMsgCount.Value;
    public long SucceededMsgCount => (long)_succeededMsgCount.Value;
    public long FailedMsgCount => (long)_failedMsgCount.Value;
    public long TimeoutMsgCount => (long)_timeoutMsgCount.Value;
    public long RetryMsgCount => (long)_retryMsgCount.Value;
    public long DroppedMsgCount => (long)_droppedMsgCount.Value;
    public long RescuedMsgBytes => (long)_rescuedMsgBytes.Value;
    public long AddEndpointCount => (long)_addEndpointCount.Value;
    public long RemoveEndpointCount => (long)_removeEndpointCount.Value;
    public long EndpointCount => (long)_endpointCount.Value;

    // --- Aggregated Per-Worker Getters ---

    public long OpenedConnCount => SumLabeledCounter(_openedConnCount);
    public long ClosedConnCount => SumLabeledCounter(_closedConnCount);
    public long ErrorConnCount => SumLabeledCounter(_errorConnCount);
    public long EagainErrCount => SumLabeledCounter(_eagainErrCount);
    public long TotalMsgQueueSize => SumLabeledGauge(_msgQueueSize);
    public long TotalMsgQueueCapacity => SumLabeledGauge(_msgQueueCapacity);
    public long TotalActiveConnCount => SumLabeledGauge(_activeConnCount);

    // --- Per-Worker Getters ---

    public long GetMsgQueueSize(int workerId) => (long)_msgQueueSize.WithLabels(workerId.ToString()).Value;
    public long GetMsgQueueCapacity(int workerId) => (long)_msgQueueCapacity.WithLabels(workerId.ToString()).Value;
    public long GetActiveConnCount(int workerId) => (long)_activeConnCount.WithLabels(workerId.ToString()).Value;
    public long GetOpenedConnCount(int workerId) => (long)_openedConnCount.WithLabels(workerId.ToString()).Value;
    public long GetClosedConnCount(int workerId) => (long)_closedConnCount.WithLabels(workerId.ToString()).Value;
    public long GetErrorConnCount(int workerId) => (long)_errorConnCount.WithLabels(workerId.ToString()).Value;

    // ========== Lifecycle ==========

    /// <summary>
    /// Unpublish all metrics from the Prometheus registry.
    ///
    /// <para>Must be called after the owning TglogClient has stopped all workers and will
    /// no longer touch any metric. Otherwise a concurrent Inc() call may race with
    /// unpublish and produce inconsistent state.</para>
    ///
    /// <para>This method is idempotent — calling it twice is safe (Unpublish on an
    /// already-unpublished collector is a no-op).</para>
    ///
    /// <para>Note: prometheus-net registries are append-only (no Remove/Unregister).
    /// We use Unpublish() to hide all metrics from scrape output, which achieves the
    /// same practical effect as Java's registry.unregister(). The collector objects
    /// remain in the registry but produce no output.</para>
    /// </summary>
    public void Unregister()
    {
        SafeUnpublish(_requestMsgCount);
        SafeUnpublish(_enqueuedMsgCount);
        SafeUnpublish(_dequeuedMsgCount);
        SafeUnpublish(_succeededMsgCount);
        SafeUnpublish(_failedMsgCount);
        SafeUnpublish(_timeoutMsgCount);
        SafeUnpublish(_retryMsgCount);
        SafeUnpublish(_droppedMsgCount);
        SafeUnpublish(_openedConnCount);
        SafeUnpublish(_closedConnCount);
        SafeUnpublish(_errorConnCount);
        SafeUnpublish(_eagainErrCount);
        SafeUnpublish(_addEndpointCount);
        SafeUnpublish(_removeEndpointCount);
        SafeUnpublish(_rescuedMsgBytes);
        SafeUnpublish(_msgQueueSize);
        SafeUnpublish(_msgQueueCapacity);
        SafeUnpublish(_endpointCount);
        SafeUnpublish(_activeConnCount);
    }

    private static void SafeUnpublish(Counter counter)
    {
        try { counter.Unpublish(); } catch { /* best-effort */ }
    }

    private static void SafeUnpublish(Gauge gauge)
    {
        try { gauge.Unpublish(); } catch { /* best-effort */ }
    }

    // ========== Derived Metrics ==========

    /// <summary>Success rate: succeeded / (succeeded + failed + dropped). Returns 0.0 if no messages processed.</summary>
    public double SuccessRate
    {
        get
        {
            long succeeded = SucceededMsgCount;
            long failed = FailedMsgCount;
            long dropped = DroppedMsgCount;
            long total = succeeded + failed + dropped;
            return total == 0 ? 0.0 : (double)succeeded / total;
        }
    }

    /// <summary>Queue utilization: totalQueueSize / totalQueueCapacity.</summary>
    public double QueueUtilization
    {
        get
        {
            long capacity = TotalMsgQueueCapacity;
            return capacity == 0 ? 0.0 : (double)TotalMsgQueueSize / capacity;
        }
    }

    // ========== Export ==========

    /// <summary>Returns metrics as a JSON string.</summary>
    public string ToJson()
    {
        var sb = new StringBuilder(1024);
        sb.Append('{');
        sb.Append("\"counters\":{");
        AppendJsonField(sb, "request_messages_total", RequestMsgCount); sb.Append(',');
        AppendJsonField(sb, "enqueued_messages_total", EnqueuedMsgCount); sb.Append(',');
        AppendJsonField(sb, "dequeued_messages_total", DequeuedMsgCount); sb.Append(',');
        AppendJsonField(sb, "succeeded_messages_total", SucceededMsgCount); sb.Append(',');
        AppendJsonField(sb, "failed_messages_total", FailedMsgCount); sb.Append(',');
        AppendJsonField(sb, "timeout_messages_total", TimeoutMsgCount); sb.Append(',');
        AppendJsonField(sb, "retry_messages_total", RetryMsgCount); sb.Append(',');
        AppendJsonField(sb, "dropped_messages_total", DroppedMsgCount); sb.Append(',');
        AppendJsonField(sb, "rescued_message_bytes_total", RescuedMsgBytes); sb.Append(',');
        AppendJsonField(sb, "opened_connections_total", OpenedConnCount); sb.Append(',');
        AppendJsonField(sb, "closed_connections_total", ClosedConnCount); sb.Append(',');
        AppendJsonField(sb, "error_connections_total", ErrorConnCount); sb.Append(',');
        AppendJsonField(sb, "eagain_errors_total", EagainErrCount); sb.Append(',');
        AppendJsonField(sb, "add_endpoints_total", AddEndpointCount); sb.Append(',');
        AppendJsonField(sb, "remove_endpoints_total", RemoveEndpointCount);
        sb.Append("},");
        sb.Append("\"gauges\":{");
        AppendJsonField(sb, "message_queue_size", TotalMsgQueueSize); sb.Append(',');
        AppendJsonField(sb, "message_queue_capacity", TotalMsgQueueCapacity); sb.Append(',');
        AppendJsonField(sb, "active_connections", TotalActiveConnCount); sb.Append(',');
        AppendJsonField(sb, "endpoints", EndpointCount);
        sb.Append("},");
        sb.Append("\"derived\":{");
        sb.Append($"\"success_rate\":{SuccessRate:F6},");
        sb.Append($"\"queue_utilization\":{QueueUtilization:F6}");
        sb.Append("}}");
        return sb.ToString();
    }

    /// <summary>Returns metrics in human-readable format.</summary>
    public override string ToString()
    {
        var sb = new StringBuilder(512);
        sb.AppendLine("TGLog Metrics:");
        sb.AppendLine("====================");
        sb.AppendLine($"  Requests:       {RequestMsgCount:N0}");
        sb.AppendLine($"  Enqueued:       {EnqueuedMsgCount:N0}");
        sb.AppendLine($"  Dequeued:       {DequeuedMsgCount:N0}");
        sb.AppendLine($"  Succeeded:      {SucceededMsgCount:N0}");
        sb.AppendLine($"  Failed:         {FailedMsgCount:N0}");
        sb.AppendLine($"  Timeout:        {TimeoutMsgCount:N0}");
        sb.AppendLine($"  Retried:        {RetryMsgCount:N0}");
        sb.AppendLine($"  Dropped:        {DroppedMsgCount:N0}");
        sb.AppendLine($"  Success Rate:   {SuccessRate:P2}");
        sb.AppendLine("--------------------");
        sb.AppendLine($"  Opened Conns:   {OpenedConnCount:N0}");
        sb.AppendLine($"  Closed Conns:   {ClosedConnCount:N0}");
        sb.AppendLine($"  Error Conns:    {ErrorConnCount:N0}");
        sb.AppendLine($"  EAGAIN Errors:  {EagainErrCount:N0}");
        sb.AppendLine($"  Rescued Bytes:  {RescuedMsgBytes:N0}");
        sb.AppendLine("--------------------");
        sb.AppendLine($"  Queue Size:     {TotalMsgQueueSize:N0}");
        sb.AppendLine($"  Queue Capacity: {TotalMsgQueueCapacity:N0}");
        sb.AppendLine($"  Queue Util:     {QueueUtilization:P2}");
        sb.AppendLine($"  Active Conns:   {TotalActiveConnCount:N0}");
        sb.AppendLine($"  Endpoints:      {EndpointCount:N0}");
        return sb.ToString();
    }

    // ========== Private Helpers ==========

    private long SumLabeledCounter(Counter counter)
    {
        long sum = 0;
        for (int i = 0; i < _workerCount; i++)
            sum += (long)counter.WithLabels(i.ToString()).Value;
        return sum;
    }

    private long SumLabeledGauge(Gauge gauge)
    {
        long sum = 0;
        for (int i = 0; i < _workerCount; i++)
            sum += (long)gauge.WithLabels(i.ToString()).Value;
        return sum;
    }

    private static void AppendJsonField(StringBuilder sb, string name, long value)
    {
        sb.Append('"').Append(name).Append("\":").Append(value);
    }
}
