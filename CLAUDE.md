# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**TGLog SDK for C#** - A high-performance, production-grade game server telemetry SDK written in pure C#. This is a port of the Java TGLog SDK, providing identical APIs and behavior for .NET applications.

- **Language**: C# (.NET 8.0+)
- **Namespace**: `Tencent.Tglog`
- **Primary Use Case**: Asynchronous, batched logging for game server telemetry to TGLog infrastructure
- **Key Dependencies**:
  - `prometheus-net` (v8.x) - Prometheus metrics
  - `Microsoft.Extensions.Logging.Abstractions` (v8.x) - Logging abstraction
  - .NET 8.0 target framework with LangVersion 10 and nullable reference types enabled

## Build and Test Commands

### Solution Structure

The solution contains three projects:
- **TglogSdk** (src/TglogSdk/) - Core SDK library
- **TglogSdk.Examples** (examples/TglogSdk.Examples/) - Usage examples
- **TglogSdk.IntegrationTest** (integration-test/TglogSdk.IntegrationTest/) - Performance tests

### Build Commands

```bash
# Build entire solution (Debug)
dotnet build TglogSdk.sln

# Build Release
dotnet build TglogSdk.sln -c Release

# Build specific project
dotnet build src/TglogSdk/TglogSdk.csproj
```

### Run Examples and Tests

```bash
# Run the simple example
dotnet run --project examples/TglogSdk.Examples

# Run example with custom host/port
dotnet run --project examples/TglogSdk.Examples -- 127.0.0.1 8080

# Run integration test
dotnet run --project integration-test/TglogSdk.IntegrationTest -- --help
```

## Architecture

### High-Level Design

The SDK uses an **asynchronous, non-blocking worker-thread** architecture:

```
Application Thread              Worker Thread(s)           Network I/O
------------------------------------------------------------------
  Send()/SendAsync() 
      +---> LockFreeQueue -----> TglogWorker -----> Socket (TCP/UDP)
             (MPSC, lock-free)   (Event loop)
```

**Key principles:**
1. **Non-blocking enqueue**: Lock-free MPSC queue lets app threads enqueue without blocking
2. **Dedicated workers**: Background thread pool (default 8) each runs Socket.Select() event loop
3. **Message batching**: Batches by message count, size, or time for throughput
4. **Connection pooling**: Each worker manages pool of TCP/UDP connections with auto-recovery
5. **Prometheus metrics**: Deep observability matching Java SDK exactly
6. **Pluggable logging**: Any ILogger implementation (Serilog, NLog, MEL, etc.)

### Core Components

**TglogClient** (TglogClient.cs) - Main entry point
- Lifecycle: Create -> Start workers -> Accept messages -> Flush -> Dispose
- Methods: Send(), SendAsync(), Flush(), GetMetrics*(), Dispose()
- Thread-safe for concurrent Send() calls (lock-free queue)
- Callbacks invoked from worker threads

**TglogConfig** (TglogConfig.cs) - Fluent configuration builder
- Factory methods: CreateTcp(host, port) / CreateUdp(host, port)
- Settings: network, batching, workers, buffers, retry, logging, Prometheus
- Validation called in TglogClient constructor

**TglogWorker** (TglogWorker.cs) - Background worker thread
- Event loop using Socket.Select() for efficient I/O multiplexing
- Consumes messages from lock-free queue, batches, sends via TCP/UDP
- Manages connection pool with exponential backoff retry
- Auto-recovery from connection/endpoint failures
- State machine: Init -> Ready -> Closing -> Closed

**LockFreeQueue** (LockFreeQueue.cs) - High-performance MPSC queue
- Power-of-2 ring buffer with atomic CAS for producers
- Cache-line padding (128 bytes) prevents false sharing
- Offer() - non-blocking enqueue (multiple producers)
- Poll() - single consumer dequeue

**BatchBuffer** (BatchBuffer.cs) - Message batching container
- Three flush triggers: message count, total size, time elapsed
- Caches send buffers across retries (no re-serialization)
- Tracks retry state for backoff

**TglogConnection** (TglogConnection.cs) - Single connection wrapper
- Non-blocking socket I/O (Blocking = false)
- Output buffer: LinkedList of BufferSegment for partial writes (libevent-style)
- High watermark check prevents memory exhaustion
- Connection lifetime with jitter avoids thundering herd
- State machine: Disconnected -> Connecting -> Connected -> Error

**EndpointManager** (EndpointManager.cs) - DNS resolution
- Periodic hostname resolution (default 60s interval)
- Notifies workers on endpoint list changes
- Handles failover and autoscaling scenarios

**TglogMetrics** (TglogMetrics.cs) - Prometheus integration
- Counters: request/enqueued/dequeued/succeeded/failed/timeout/retry/dropped messages
- Per-worker counters: connections opened/closed/error, EAGAIN errors
- Gauges: queue size/capacity, active connections, endpoint count
- Unregistered on Dispose to keep shared registry clean


### Message Flow

```csharp
var config = TglogConfig.CreateTcp("server", 8080).SetWorkerNum(4);
using var client = new TglogClient(config);

// Non-blocking enqueue to lock-free queue
client.Send("my log\n", (payload, ex) => {
    if (ex == null) Console.WriteLine("Success");
});

// Worker thread dequeues, batches, sends via TCP/UDP
// On completion, invokes callback from worker thread
client.Flush(5000); // Wait for all pending work

// Dispose: stops workers, flushes, closes connections, unregisters metrics
```

### Error Handling

- **TglogException** with TglogErrorCode enum
- Error codes: SendTimeout, ProducerClosed, SendQueueIsFull, NewConnFailed, LogTooLong, BadLog, etc.
- Validation: Payload must end with newline; size must fit protocol limit
- Callback exceptions: Caught and logged (don't propagate)

### Thread Safety

| Operation | Safe |
|-----------|------|
| Send() / SendAsync() | Yes (lock-free queue) |
| Flush() | Yes (any thread) |
| GetMetrics*() | Yes (prometheus-net) |
| Dispose() | One-time (not reentrant) |
| Callbacks | Called from worker thread |

### Performance Notes

1. **Lock-free queue** - Multiple threads Send() concurrently without locks
2. **Cache-line padding** - Prevents false sharing between producer/consumer indices
3. **Batching** - Reduces context switches and network packets
4. **Non-blocking I/O** - Single thread multiplexes multiple connections
5. **Exponential backoff** - Prevents thundering herd on failures
6. **Connection pooling** - Reduces TCP handshake overhead
7. **DNS caching** - Periodic updates avoid per-message resolution

### Logging Integration

Inject any ILoggerFactory (Serilog, NLog, MEL):

```csharp
var config = TglogConfig.CreateTcp("server", 8080)
    .SetLoggerFactory(myLoggerFactory);
```

SDK logs at Debug (DNS, sockets), Info (client lifecycle, endpoints), Warning (timeouts, failures), Error (init failures).

## Testing

- **SimpleExample.cs** - Comprehensive demo: fire-and-forget, callbacks, async, metrics, high throughput
- **TglogClientTest.cs** - Stress test with configurable parameters (host, port, protocol, message count, workers, batch size, callback, rate-limit, debug)

## Special Notes

### Unsafe Code
Performance-critical buffer and socket operations use unsafe blocks (allowed in csproj).

### Nullable Reference Types
Strictly enforced globally. All ref types must be Type? (nullable) or Type (non-null). Prevents null ref exceptions.

### Prometheus Registry
When creating multiple TglogClient instances in same process:
- **Option 1**: Dispose previous client (unregisters metrics) before creating next
- **Option 2**: Give each client custom CollectorRegistry via SetPrometheusRegistry()

Avoids "metric already registered" exceptions.

### Async Model
- SendAsync() returns Task completing when message sent/failed
- Callbacks invoked from worker threads (not SendAsync caller thread)
- Multiple concurrent SendAsync calls safe (each gets TaskCompletionSource)
