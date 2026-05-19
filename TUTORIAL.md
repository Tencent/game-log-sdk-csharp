# TGLog C# SDK Tutorial

A step-by-step guide to using the TGLog SDK for .NET to ship game-server telemetry into TGLog infrastructure with high throughput.

> Audience: backend / game-server engineers who want to integrate TGLog into a .NET 8 application.

---

## 1. What you get

- `TglogClient` — a process-wide, thread-safe sender (TCP or UDP).
- Round-robin distribution to N worker threads, each with a lock-free MPSC queue and its own connection pool.
- Batching by message count, byte size, or max delay (whichever fires first).
- Three send modes: fire-and-forget, callback, `async`/`await`.
- Built-in Prometheus metrics (prometheus-net) and pluggable `ILoggerFactory` (Serilog / NLog / MEL).
- Automatic DNS refresh, connection recycling, retries with backoff.

Send semantics: TGLog is a request-only protocol — once the SDK has buffered a message into the OS socket, it counts as a successful send. There is no server-side ACK.

---

## 2. Prerequisites

- .NET 8 SDK
- Network access to a TGLog endpoint (host + port)
- A reference to the SDK project, for example:

```xml
<ProjectReference Include="..\..\src\TglogSdk\TglogSdk.csproj" />
```

---

## 3. Hello, TGLog (60 seconds)

```csharp
using Tencent.Tglog;

var config = TglogConfig.CreateTcp("dev.tglog.com", 20001);

using var client = new TglogClient(config);

// Payload MUST be non-empty and end with '\n'
client.Send("login|1001|player_abc|success\n");
```

That's the entire happy path. `using` calls `Dispose`, which flushes pending messages and closes connections.

---

## 4. Configuring the client

`TglogConfig` is built fluently. Start from `CreateTcp` or `CreateUdp`, then chain setters:

```csharp
var config = TglogConfig.CreateTcp("dev.tglog.com", 20001)
    .SetWorkerNum(8)                     // 8 background worker threads
    .SetBatchingMaxMessages(50)          // flush a batch at 50 messages
    .SetBatchingMaxSize(32 * 1024)       // ...or 32 KB
    .SetBatchingMaxDelayMs(10)           // ...or after 10 ms idle
    .SetMaxRetries(5)                    // retry a failing batch up to 5 times
    .SetSendTimeoutMs(10_000)            // give up on a batch after 10 s
    .SetMaxConnectionLifetimeSec(300)    // recycle each connection every 5 min
    .SetMaxPendingMessages(409_600)      // per-worker queue capacity
    .SetDebugEnabled(true);              // chattier internal logs
```

Defaults and limits worth remembering:

| Setting | Default | Notes |
|---|---|---|
| Workers | 8 | More workers ⇒ more parallel connections |
| Batch messages | 50 | |
| Batch size | 32 KB | Auto-clamped to protocol max |
| Batch delay | 10 ms | |
| Pending messages / worker | 409 600 | Backpressure boundary |
| TCP max request size | 512 KB | `TglogConfig.MaxTcpReqSize` |
| UDP max request size | 65 507 B | `TglogConfig.MaxUdpReqSize` |
| Connection timeout | 3 000 ms | |
| Connection lifetime | 300 s | 0 = persistent |
| DNS refresh | 60 s | |

`Send` will throw `TglogException(BadFlag)` at construction time if `Host`/`Port` are invalid; other illegal numeric settings get silently reset to defaults by `Validate()`.

---

## 5. The three ways to send

The payload contract is the same for all three:

1. Non-null, non-empty.
2. Must end with `\n`.
3. Must not exceed `MaxRequestSize` for the chosen protocol.

### 5.1 Fire-and-forget

Best for high-throughput hot paths where you don't care about per-message status.

```csharp
client.Send("tick|frame=42\n");
```

If the worker queue is full, `Send` throws `TglogException(SendQueueIsFull)` — wrap it if you don't want the call site to crash.

### 5.2 Callback

The callback runs on a worker thread when the batch containing your message has been buffered for transmission (success) or has exhausted retries (failure).

```csharp
client.Send("purchase|1001|sword|100\n", (payload, ex) =>
{
    if (ex != null)
        Console.Error.WriteLine($"Send failed [{ex.ErrorCode}]: {ex.Message}");
});
```

Callbacks must be cheap; anything throwing inside them is caught and logged but otherwise ignored.

### 5.3 `async` / `await`

When you genuinely need to await the result of a single send (e.g. tests, critical bootstrap events):

```csharp
try
{
    await client.SendAsync("game_start|1001|map_01|team_a\n");
}
catch (TglogException ex)
{
    // surfaces queue-full / closed / payload-validation failures
}
```

Don't `await` every send in a tight loop — you'll throttle yourself to the round-trip latency. Prefer fire-and-forget plus a single `Flush` (or rely on `Dispose`) for hot loops.

---

## 6. UDP

UDP swaps `CreateTcp` for `CreateUdp`. Each batch is concatenated into a single datagram, so the per-batch limit drops to 65 507 bytes:

```csharp
var config = TglogConfig.CreateUdp("dev.tglog.com", 20001)
    .SetWorkerNum(2)
    .SetBatchingMaxMessages(20)
    .SetBatchingMaxDelayMs(5);
```

Pick UDP when you can tolerate occasional message loss in exchange for lower overhead. TCP is the default and recommended for most pipelines.

---

## 7. Logging

By default the SDK uses `NullLoggerFactory` and is silent. Inject any `ILoggerFactory` to see what it's doing:

```csharp
using Microsoft.Extensions.Logging;
using Serilog;

var serilogLogger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .CreateLogger();

using var loggerFactory = new LoggerFactory().AddSerilog(serilogLogger);

var config = TglogConfig.CreateTcp("dev.tglog.com", 20001)
    .SetLoggerFactory(loggerFactory)
    .SetDebugEnabled(true); // include internal DBG entries
```

Any `ILoggerFactory` works (Microsoft.Extensions.Logging built-ins, Serilog, NLog, …).

---

## 8. Prometheus metrics

`TglogClient.Metrics` exposes counters and gauges named the same as the Java SDK so dashboards stay portable. Common ones:

| Metric | Meaning |
|---|---|
| `tglog_request_messages_total` | `Send` / `SendAsync` calls accepted |
| `tglog_enqueued_messages_total` | Successfully enqueued |
| `tglog_succeeded_messages_total` | Buffered for transmission |
| `tglog_failed_messages_total` | Exhausted retries |
| `tglog_dropped_messages_total` | Rejected because queue was full |
| `tglog_msg_queue_size{worker_id}` | Current per-worker queue depth |
| `tglog_active_conn_count{worker_id}` | Live connections per worker |
| `tglog_endpoint_count` | DNS-resolved endpoints |

Quick inspection during development:

```csharp
Console.WriteLine(client.GetMetricsString());
Console.WriteLine(client.GetMetricsJson());
```

If you run **multiple** `TglogClient` instances in the same process (e.g. config hot-reload), give each a private registry to avoid duplicate-registration errors:

```csharp
var config = TglogConfig.CreateTcp("dev.tglog.com", 20001)
    .SetPrometheusRegistry(Prometheus.Metrics.NewCustomRegistry());
```

To expose metrics over HTTP, add prometheus-net's `MetricServer` (or scraper of choice) on top of the registry.

---

## 9. Shutdown and `Flush`

`Dispose` does the right thing: it stops accepting new sends, drains queues up to `StopTimeoutMs` (default 3 s), waits a bit longer for sockets to flush, then unregisters metrics.

`Flush(timeoutMs)` is available but rarely needed in production — it returns `true` if every worker queue, in-flight batch, and socket buffer drained within `timeoutMs`. Useful in tests or before a critical checkpoint:

```csharp
if (!client.Flush(5_000))
{
    logger.LogWarning("TGLog did not drain in 5s; some messages may still be in flight");
}
```

Always wrap `TglogClient` in `using` (or call `Dispose` explicitly) so the metrics registry is cleaned up.

---

## 10. Error handling cheat sheet

`TglogException.ErrorCode` is the easy switch:

| Code | When | What to do |
|---|---|---|
| `BadLog` | Empty payload or missing `\n` | Fix the call site |
| `LogTooLong` | Payload > `MaxRequestSize` | Split the log or switch to TCP |
| `SendQueueIsFull` | Worker queue saturated | Backpressure, drop, or grow `MaxPendingMessages` |
| `ProducerClosed` | `Send` after `Dispose` | Don't send after shutdown |
| `BadFlag` | Bad host/port at construction | Fix config |
| `NewConnFailed` | No endpoints resolved on startup | Check DNS / firewall |
| `SendTimeout`, `SendFailed`, `ConnWriteFailed` | Surfaces only via callback | Already retried `MaxRetries` times |

Validation errors (`BadLog`, `LogTooLong`) are thrown synchronously from `Send` / `SendAsync`. Network errors only reach you through the callback or the awaited `Task`.

---

## 11. Putting it together — production template

```csharp
using Microsoft.Extensions.Logging;
using Tencent.Tglog;

public sealed class TelemetryService : IAsyncDisposable
{
    private readonly TglogClient _client;
    private readonly ILogger<TelemetryService> _log;

    public TelemetryService(ILoggerFactory loggerFactory, ILogger<TelemetryService> log)
    {
        _log = log;

        var config = TglogConfig.CreateTcp("tglog.internal", 20001)
            .SetWorkerNum(Math.Max(4, Environment.ProcessorCount))
            .SetBatchingMaxMessages(100)
            .SetBatchingMaxSize(64 * 1024)
            .SetBatchingMaxDelayMs(20)
            .SetMaxRetries(5)
            .SetMaxPendingMessages(1_000_000)
            .SetLoggerFactory(loggerFactory)
            .SetPrometheusRegistry(Prometheus.Metrics.NewCustomRegistry());

        _client = new TglogClient(config);
    }

    public void Track(string line)
    {
        try
        {
            _client.Send(line.EndsWith('\n') ? line : line + "\n");
        }
        catch (TglogException ex) when (ex.ErrorCode == TglogErrorCode.SendQueueIsFull)
        {
            _log.LogWarning("TGLog queue full; dropping event");
        }
    }

    public ValueTask DisposeAsync()
    {
        _client.Flush(5_000);
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
```

---

## 12. Smoke-testing your integration

The repo ships a CLI harness for ad-hoc verification:

```bash
# Tiny TCP smoke test against a real endpoint
dotnet run --project integration-test/TglogSdk.IntegrationTest -- \
    --host 127.0.0.1 --port 20001 --messages 1 --tcp

# 100k messages, 8 workers, callbacks on, debug logs
dotnet run --project integration-test/TglogSdk.IntegrationTest -- \
    --host dev.tglog.com --port 20001 \
    --messages 100000 --workers 8 --batch-size 50 --callback --debug
```

Common flags: `--tcp` / `--udp`, `--rate-limit <msg/s>` (0 = unlimited), `--callback`, `--debug`. See `--help` for the full list.

---

## 13. FAQ

**Q: Do I need to call `Flush` before exiting?**
No — `Dispose` already calls it. Use `Flush` explicitly only when you must guarantee drain at a specific point (e.g. before a synchronous checkpoint).

**Q: A successful callback fires before bytes hit the wire — is that a bug?**
No. TGLog is request-only; the SDK treats "buffered into the OS socket" as success on purpose.

**Q: Can I share one `TglogClient` across the whole process?**
Yes, that's the intended pattern. It is thread-safe and amortizes worker threads, connections, and metrics.

**Q: Can I create multiple clients?**
Yes, but give each a separate Prometheus registry (`SetPrometheusRegistry`) to avoid duplicate metric registration.

**Q: Why must payloads end with `\n`?**
TGLog's framing is line-delimited; the SDK validates the terminator instead of mutating your payload, so you stay in control of the bytes on the wire.
