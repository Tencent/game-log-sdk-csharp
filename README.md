# TGLog SDK for C#

High-performance game server remote logging SDK for .NET, providing asynchronous, batched log delivery to TGLog infrastructure.

This is a C# port of the Java TGLog SDK, offering identical APIs and behavior for .NET applications.

## Features

- **High Performance** - Lock-free MPSC queue, non-blocking I/O, message batching
- **TCP / UDP** - Supports both transport protocols
- **Async** - Fire-and-forget, callback, and async/await send modes
- **Connection Pooling** - Automatic connection management with retry and backoff
- **Prometheus Metrics** - Built-in observability (request counts, queue depth, connections, etc.)
- **Pluggable Logging** - Integrates with any `ILoggerFactory` (Serilog, NLog, MEL, etc.)

## Requirements

- .NET 8.0+

## Quick Start

### Build

```bash
dotnet build TglogSdk.sln
```

### Basic Usage

```csharp
using Tencent.Tglog;

// Create config
var config = TglogConfig.CreateTcp("server-host", 20001)
    .SetWorkerNum(4)
    .SetBatchingMaxMessages(50)
    .SetBatchingMaxDelayMs(10);

// Create client and send
using var client = new TglogClient(config);

// Fire-and-forget
client.Send("{\"event\":\"login\",\"user_id\":123}\n");

// With callback
client.Send("{\"event\":\"purchase\",\"item\":\"sword\"}\n", (payload, ex) =>
{
    if (ex != null) Console.WriteLine($"Failed: {ex.Message}");
});

// Async/await
await client.SendAsync("{\"event\":\"game_start\"}\n");

// Flush before shutdown
client.Flush(5000);
```

### UDP Mode

```csharp
var config = TglogConfig.CreateUdp("server-host", 20001)
    .SetWorkerNum(2);
```

### Inject Logger

```csharp
var config = TglogConfig.CreateTcp("server-host", 20001)
    .SetLoggerFactory(myLoggerFactory); // Serilog, NLog, etc.
```

### Custom Prometheus Registry

```csharp
var config = TglogConfig.CreateTcp("server-host", 20001)
    .SetPrometheusRegistry(Prometheus.Metrics.NewCustomRegistry());
```

## Configuration

| Method | Default | Description |
|--------|---------|-------------|
| `SetWorkerNum` | 8 | Number of background worker threads |
| `SetBatchingMaxMessages` | 50 | Max messages per batch |
| `SetBatchingMaxSize` | 32KB | Max batch size in bytes |
| `SetBatchingMaxDelayMs` | 10 | Max batch wait time (ms) |
| `SetMaxRetries` | 3 | Max send retries per batch |
| `SetSendTimeoutMs` | 30000 | Send timeout (ms) |
| `SetMaxConnectionLifetimeSec` | 300 | Connection lifetime before recycling (s) |
| `SetConnectionTimeoutMs` | 3000 | TCP connection timeout (ms) |
| `SetMaxPendingMessages` | 409600 | Max queued messages per worker |
| `SetDnsUpdateIntervalSec` | 60 | DNS re-resolve interval (s) |
| `SetDebugEnabled` | false | Enable debug logging |

## Project Structure

```
TglogSdk.sln
  src/TglogSdk/                  - Core SDK library
  examples/TglogSdk.Examples/    - Usage examples
  integration-test/              - Performance/integration tests
```

## Run Examples

```bash
# Default (127.0.0.1:20001)
dotnet run --project examples/TglogSdk.Examples

# Custom host/port
dotnet run --project examples/TglogSdk.Examples -- 10.0.0.1 9090
```

## License

Proprietary - Tencent Internal Use
