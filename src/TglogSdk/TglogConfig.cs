using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Prometheus;

namespace Tencent.Tglog;

/// <summary>
/// Configuration for <see cref="TglogClient"/>.
/// Use <see cref="CreateTcp"/> or <see cref="CreateUdp"/> factory methods to create,
/// then chain Set* methods for customization.
/// </summary>
public sealed class TglogConfig
{
    /// <summary>Maximum UDP request size: 65535 - 8 (UDP header) - 20 (IP header)</summary>
    public const int MaxUdpReqSize = 65507;

    /// <summary>Maximum TCP request size: 512KB</summary>
    public const int MaxTcpReqSize = 512 * 1024;

    /// <summary>Transport protocol.</summary>
    public enum Protocol
    {
        Tcp,
        Udp
    }

    // --- Network Configuration ---

    /// <summary>Transport protocol (TCP or UDP).</summary>
    public Protocol TransportProtocol { get; private set; }

    /// <summary>Server hostname or IP address.</summary>
    public string Host { get; private set; }

    /// <summary>Server port.</summary>
    public int Port { get; private set; }

    /// <summary>Connection timeout in milliseconds. Default: 3000.</summary>
    public int ConnectionTimeoutMs { get; private set; } = 3000;

    /// <summary>Maximum connection lifetime in seconds (0 = persistent). Default: 300.</summary>
    public int MaxConnectionLifetimeSec { get; private set; } = 300;

    /// <summary>DNS resolution interval in seconds. Default: 60.</summary>
    public int DnsUpdateIntervalSec { get; private set; } = 60;

    // --- Batching Configuration ---

    /// <summary>Maximum messages per batch. Default: 50.</summary>
    public int BatchingMaxMessages { get; private set; } = 50;

    /// <summary>Maximum batch size in bytes. Default: 32KB.</summary>
    public int BatchingMaxSize { get; private set; } = 32 * 1024;

    /// <summary>Maximum batch delay in milliseconds. Default: 10.</summary>
    public int BatchingMaxDelayMs { get; private set; } = 10;

    // --- Worker Configuration ---

    /// <summary>Number of worker threads. Default: 8.</summary>
    public int WorkerNum { get; private set; } = 8;

    /// <summary>Maximum pending messages per worker. Default: 409600.</summary>
    public int MaxPendingMessages { get; private set; } = 409600;

    // --- Buffer Configuration ---

    /// <summary>Output buffer high watermark in bytes. Default: 32MB.</summary>
    public int WriteHighWatermark { get; private set; } = 32 * 1024 * 1024;

    /// <summary>Socket send buffer size (SO_SNDBUF). Default: 64MB.</summary>
    public int SocketSendBufferSize { get; private set; } = 64 * 1024 * 1024;

    /// <summary>Socket receive buffer size (SO_RCVBUF). Default: 16KB.</summary>
    public int SocketRecvBufferSize { get; private set; } = 16 * 1024;

    // --- Retry Configuration ---

    /// <summary>Send timeout in milliseconds. Default: 30000.</summary>
    public int SendTimeoutMs { get; private set; } = 30000;

    /// <summary>Maximum retry attempts per batch. Default: 10.</summary>
    public int MaxRetries { get; private set; } = 10;

    /// <summary>Shutdown timeout in milliseconds. Default: 3000.</summary>
    public int StopTimeoutMs { get; private set; } = 3000;

    // --- Debug ---

    /// <summary>Enable debug logging. Default: false.</summary>
    public bool DebugEnabled { get; private set; }

    // --- Logger ---

    /// <summary>
    /// Microsoft.Extensions.Logging logger factory for creating logger instances.
    /// If null, <see cref="NullLoggerFactory.Instance"/> is used (no logging output).
    /// Users can inject any logging framework (Serilog, NLog, etc.) via this factory.
    /// </summary>
    public ILoggerFactory? LoggerFactory { get; private set; }

    // --- Prometheus ---

    /// <summary>
    /// Prometheus CollectorRegistry for metric registration.
    /// If null, <see cref="Prometheus.Metrics.DefaultRegistry"/> is used (matching Java behavior).
    /// Provide a custom registry if running multiple TglogClient instances in the same process.
    /// </summary>
    public CollectorRegistry? PrometheusRegistry { get; private set; }

    /// <summary>Maximum single request size based on protocol.</summary>
    public int MaxRequestSize => TransportProtocol == Protocol.Udp ? MaxUdpReqSize : MaxTcpReqSize;

    // --- Factory Methods ---

    /// <summary>Create a TCP configuration for the given host and port.</summary>
    public static TglogConfig CreateTcp(string host, int port)
    {
        return new TglogConfig(Protocol.Tcp, host, port);
    }

    /// <summary>Create a UDP configuration for the given host and port.</summary>
    public static TglogConfig CreateUdp(string host, int port)
    {
        return new TglogConfig(Protocol.Udp, host, port);
    }

    private TglogConfig(Protocol protocol, string host, int port)
    {
        TransportProtocol = protocol;
        Host = host ?? throw new ArgumentNullException(nameof(host));
        Port = port;
    }

    // --- Fluent Setters ---

    public TglogConfig SetProtocol(Protocol protocol) { TransportProtocol = protocol; return this; }
    public TglogConfig SetHost(string host) { Host = host ?? throw new ArgumentNullException(nameof(host)); return this; }
    public TglogConfig SetPort(int port) { Port = port; return this; }
    public TglogConfig SetConnectionTimeoutMs(int ms) { ConnectionTimeoutMs = ms; return this; }
    public TglogConfig SetMaxConnectionLifetimeSec(int sec) { MaxConnectionLifetimeSec = sec; return this; }
    public TglogConfig SetDnsUpdateIntervalSec(int sec) { DnsUpdateIntervalSec = sec; return this; }
    public TglogConfig SetBatchingMaxMessages(int count) { BatchingMaxMessages = count; return this; }
    public TglogConfig SetBatchingMaxSize(int bytes) { BatchingMaxSize = bytes; return this; }
    public TglogConfig SetBatchingMaxDelayMs(int ms) { BatchingMaxDelayMs = ms; return this; }
    public TglogConfig SetWorkerNum(int num) { WorkerNum = num; return this; }
    public TglogConfig SetMaxPendingMessages(int count) { MaxPendingMessages = count; return this; }
    public TglogConfig SetWriteHighWatermark(int bytes) { WriteHighWatermark = bytes; return this; }
    public TglogConfig SetSocketSendBufferSize(int bytes) { SocketSendBufferSize = bytes; return this; }
    public TglogConfig SetSocketRecvBufferSize(int bytes) { SocketRecvBufferSize = bytes; return this; }
    public TglogConfig SetSendTimeoutMs(int ms) { SendTimeoutMs = ms; return this; }
    public TglogConfig SetMaxRetries(int count) { MaxRetries = count; return this; }
    public TglogConfig SetStopTimeoutMs(int ms) { StopTimeoutMs = ms; return this; }
    public TglogConfig SetDebugEnabled(bool enabled) { DebugEnabled = enabled; return this; }
    public TglogConfig SetLoggerFactory(ILoggerFactory factory) { LoggerFactory = factory; return this; }
    public TglogConfig SetPrometheusRegistry(CollectorRegistry registry) { PrometheusRegistry = registry; return this; }

    // --- Validation ---

    /// <summary>Validates all configuration parameters. Throws <see cref="TglogException"/> on invalid config.</summary>
    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(Host))
            throw new TglogException(TglogErrorCode.BadFlag, "Host cannot be null or empty");
        if (Port < 1 || Port > 65535)
            throw new TglogException(TglogErrorCode.BadFlag, $"Port must be 1-65535, got {Port}");
        if (WorkerNum < 1)
            throw new TglogException(TglogErrorCode.BadFlag, $"WorkerNum must be >= 1, got {WorkerNum}");
        if (BatchingMaxMessages < 1)
            throw new TglogException(TglogErrorCode.BadFlag, $"BatchingMaxMessages must be >= 1, got {BatchingMaxMessages}");
        if (BatchingMaxSize < 1)
            throw new TglogException(TglogErrorCode.BadFlag, $"BatchingMaxSize must be >= 1, got {BatchingMaxSize}");
        if (BatchingMaxDelayMs < 0)
            throw new TglogException(TglogErrorCode.BadFlag, $"BatchingMaxDelayMs must be >= 0, got {BatchingMaxDelayMs}");
        if (MaxPendingMessages < 1)
            throw new TglogException(TglogErrorCode.BadFlag, $"MaxPendingMessages must be >= 1, got {MaxPendingMessages}");
        if (ConnectionTimeoutMs < 0)
            throw new TglogException(TglogErrorCode.BadFlag, $"ConnectionTimeoutMs must be >= 0, got {ConnectionTimeoutMs}");
        if (SendTimeoutMs < 0)
            throw new TglogException(TglogErrorCode.BadFlag, $"SendTimeoutMs must be >= 0, got {SendTimeoutMs}");
        if (MaxRetries < 0)
            throw new TglogException(TglogErrorCode.BadFlag, $"MaxRetries must be >= 0, got {MaxRetries}");

        // Auto-adjust batching max size to fit within protocol limits
        if (BatchingMaxSize > MaxRequestSize)
        {
            BatchingMaxSize = MaxRequestSize;
        }
    }
}
