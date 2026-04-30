# CODEBUDDY.md

This file provides guidance to CodeBuddy Code when working with code in this repository.

## Project overview

TGLog SDK for C# is a .NET 8 telemetry client in namespace `Tencent.Tglog`. It is a C# port of the Java TGLog SDK and focuses on high-throughput asynchronous, batched delivery of game server logs to TGLog infrastructure over TCP or UDP.

The solution has three projects:

- `src/TglogSdk/` — core SDK library (`TglogSdk.csproj`).
- `examples/TglogSdk.Examples/` — executable usage examples.
- `integration-test/TglogSdk.IntegrationTest/` — executable performance/integration test harness.

Core dependencies are `prometheus-net` 8.x and `Microsoft.Extensions.Logging.Abstractions` 8.x. The library targets `net8.0`, uses C# language version 10, enables nullable reference types, and allows unsafe blocks.

## Commands

```bash
# Restore dependencies
dotnet restore TglogSdk.sln

# Build the full solution
dotnet build TglogSdk.sln

# Build Release
dotnet build TglogSdk.sln -c Release

# Build only the SDK library
dotnet build src/TglogSdk/TglogSdk.csproj

# Run the example app (defaults are handled by the app)
dotnet run --project examples/TglogSdk.Examples

# Run the example app with host/port
dotnet run --project examples/TglogSdk.Examples -- 127.0.0.1 8080

# Show integration/performance test options
dotnet run --project integration-test/TglogSdk.IntegrationTest -- --help

# Run the integration/performance harness with defaults
dotnet run --project integration-test/TglogSdk.IntegrationTest

# Run a small TCP smoke test against a target host/port
dotnet run --project integration-test/TglogSdk.IntegrationTest -- --host 127.0.0.1 --port 20001 --messages 1 --tcp

# Optional formatting check if dotnet-format is available in the SDK/environment
dotnet format TglogSdk.sln --verify-no-changes
```

There are no xUnit/NUnit/MSTest test projects in the solution, so `dotnet test` and single unit-test filters are not applicable unless a test project is added later. The current "single test" equivalent is running the integration harness with a tiny message count and explicit host/port.

No repository-specific CI, lint, or packaging scripts were present when this file was created.

## Runtime architecture

Application code creates a `TglogConfig` via `CreateTcp(host, port)` or `CreateUdp(host, port)`, optionally chains `Set*` methods, then creates a `TglogClient`. Payloads sent through `Send`, callback-based `Send`, or `SendAsync` must be non-empty and end with `\n`.

High-level flow:

```text
Application threads
  -> TglogClient.Send/SendAsync
  -> round-robin worker selection
  -> per-worker LockFreeQueue<LogMessage>
  -> TglogWorker batching/event loop
  -> TglogConnection non-blocking TCP/UDP socket
  -> TGLog server
```

Important components:

- `TglogClient` is the public entry point. It validates config, starts the DNS manager and worker threads, distributes messages round-robin, exposes `Flush`, metrics helpers, and disposes workers/connections/metrics.
- `TglogConfig` is the fluent configuration object for protocol, host/port, batching, worker count, queue size, socket buffers, retry/timeouts, debug, logger factory, and Prometheus registry.
- `EndpointManager` resolves the configured host initially and on a timer (`DnsUpdateIntervalSec`), then notifies workers when endpoint sets change.
- `TglogWorker` owns one background thread, one MPSC queue, batching state, endpoint updates, and a pool of `TglogConnection` objects. It uses `Socket.Select()` for non-blocking I/O multiplexing and handles retries, endpoint recovery, connection replacement, delayed close, and queue/connection metrics.
- `LockFreeQueue<T>` is a bounded MPSC ring buffer: multiple application producer threads call `Offer`, and exactly one worker thread calls `Poll`. Capacity is rounded to a power of two and producer/consumer indices are cache-line padded.
- `BatchBuffer` groups messages by max message count, max byte size, or max delay. TCP sends one buffer per message; UDP concatenates the batch into one datagram-sized buffer. It caches prepared buffers for retries and invokes callbacks on success/failure.
- `TglogConnection` wraps a single non-blocking TCP or UDP socket, manages connect/connected/error states, buffers partial writes with a linked-list output buffer, enforces a high watermark, and applies connection lifetime jitter.
- `TglogMetrics` registers Prometheus counters/gauges matching the Java SDK naming. Metrics are unpublished during `TglogClient.Dispose`; when multiple clients overlap in one process, use `SetPrometheusRegistry(Prometheus.Metrics.NewCustomRegistry())` to avoid shared-registry conflicts.

## Behavioral notes

- `Send` is fire-and-forget unless a callback is supplied; `SendAsync` completes when the worker reports success or failure.
- Callbacks run on worker threads and are caught/logged so callback exceptions do not break delivery processing.
- `Flush(timeoutMs)` waits until worker queues, current batches, pending messages, and connection output buffers drain or the timeout expires.
- TCP max request size is 512 KB; UDP max request size is 65,507 bytes. `TglogConfig.Validate()` clamps `BatchingMaxSize` to the protocol limit.
- Default logging is `NullLoggerFactory`; inject an `ILoggerFactory` with `SetLoggerFactory` to see SDK logs.
- The example project demonstrates TCP and UDP config, custom logger factory injection, custom Prometheus registry, fire-and-forget send, callbacks, `SendAsync`, `Flush`, metrics, and high-throughput enqueueing.
- The integration harness supports `--host`, `--port`, `--messages`, `--workers`, `--batch-size`, `--rate-limit`, `--tcp`, `--udp`, `--callback`, and `--debug`.
