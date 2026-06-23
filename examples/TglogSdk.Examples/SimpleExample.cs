using Microsoft.Extensions.Logging;
using Tencent.Tglog;

namespace Tencent.Tglog.Examples;

/// <summary>
/// Simple example demonstrating TGLog SDK usage for game server telemetry.
/// Shows: TCP/UDP config, fire-and-forget, callback, async/await, metrics,
/// custom ILoggerFactory injection, Prometheus registry.
/// </summary>
class SimpleExample
{
    /// <summary>
    /// Minimal ILoggerFactory that creates console loggers with colored output.
    /// In real projects, use Microsoft.Extensions.Logging built-in providers,
    /// Serilog, NLog, or any other framework that implements ILoggerFactory.
    /// </summary>
    class ColorConsoleLoggerFactory : ILoggerFactory
    {
        public ILogger CreateLogger(string categoryName) => new ColorConsoleLogger(categoryName);
        public void AddProvider(ILoggerProvider provider) { }
        public void Dispose() { }

        class ColorConsoleLogger : ILogger
        {
            private readonly string _category;
            public ColorConsoleLogger(string category) => _category = category;

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Debug;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
                Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (!IsEnabled(logLevel)) return;

                var color = logLevel switch
                {
                    LogLevel.Debug => ConsoleColor.Gray,
                    LogLevel.Information => ConsoleColor.Green,
                    LogLevel.Warning => ConsoleColor.Yellow,
                    _ => ConsoleColor.Red,
                };
                var tag = logLevel switch
                {
                    LogLevel.Debug => "DBG",
                    LogLevel.Information => "INF",
                    LogLevel.Warning => "WRN",
                    _ => "ERR",
                };

                Console.ForegroundColor = color;
                Console.WriteLine($"[{tag}] [{_category}] {formatter(state, exception)}");
                if (exception != null)
                    Console.WriteLine($"  {exception.Message}");
                Console.ResetColor();
            }
        }
    }

    static async Task Main(string[] args)
    {
        string host = args.Length > 0 ? args[0] : "127.0.0.1";
        int port = args.Length > 1 ? int.Parse(args[1]) : 8080;

        Console.WriteLine($"TGLog C# SDK Example - connecting to {host}:{port}");
        Console.WriteLine();

        // ==========================================
        // Example 1: TCP with Custom ILoggerFactory & Prometheus Registry
        // ==========================================
        Console.WriteLine("=== Example 1: TCP with Custom Logger & Prometheus ===");

        var config = TglogConfig.CreateTcp(host, port)
            .SetWorkerNum(4)
            .SetBatchingMaxMessages(50)
            .SetBatchingMaxSize(32 * 1024)
            .SetBatchingMaxDelayMs(10)
            .SetMaxRetries(5)
            .SetSendTimeoutMs(10000)
            .SetMaxConnectionLifetimeSec(300)
            // Inject any ILoggerFactory — works with Serilog, NLog, MEL built-in, etc.
            .SetLoggerFactory(new ColorConsoleLoggerFactory())
            // Use custom Prometheus registry (optional — default uses global registry)
            .SetPrometheusRegistry(Prometheus.Metrics.NewCustomRegistry());

        Console.WriteLine($"  Protocol: TCP");
        Console.WriteLine($"  Workers: {config.WorkerNum}");
        Console.WriteLine($"  Batch size: {config.BatchingMaxMessages} msgs / {config.BatchingMaxSize} bytes");
        Console.WriteLine($"  Logger: ColorConsoleLoggerFactory (custom ILoggerFactory)");
        Console.WriteLine($"  Prometheus: custom registry");
        Console.WriteLine();

        try
        {
            using var client = new TglogClient(config);

            // --- Fire-and-forget send ---
            Console.WriteLine("=== Example 2: Fire-and-Forget Send ===");
            for (int i = 0; i < 10; i++)
            {
                client.Send($"login|{i}|{DateTime.UtcNow:yyyy-MM-ddTHH:mm:ss.fffZ}\n");
            }
            Console.WriteLine("  Sent 10 messages (fire-and-forget)");

            // --- Send with callback ---
            Console.WriteLine("=== Example 3: Send with Callback ===");
            int successCount = 0;
            int failCount = 0;

            for (int i = 0; i < 10; i++)
            {
                client.Send(
                    $"item_purchase|{i}|{1000 + i}|1|99\n",
                    (payload, exception) =>
                    {
                        if (exception == null)
                            Interlocked.Increment(ref successCount);
                        else
                            Interlocked.Increment(ref failCount);
                    });
            }

            // --- Async/Await send ---
            Console.WriteLine("=== Example 4: Async/Await Send ===");
            try
            {
                await client.SendAsync("game_start|abc123|1|0\n");
                Console.WriteLine("  Async send succeeded");
            }
            catch (TglogException ex)
            {
                Console.WriteLine($"  Async send failed: {ex.ErrorCode} - {ex.Message}");
            }

            // --- Flush and check metrics ---
            Console.WriteLine("=== Example 5: Flush & Metrics ===");
            bool flushed = client.Flush(5000);
            Console.WriteLine($"  Flush completed: {flushed}");
            Console.WriteLine($"  Callback results: {successCount} success, {failCount} failed");
            Console.WriteLine();
            Console.WriteLine(client.GetMetricsString());

            // --- High throughput test ---
            Console.WriteLine("=== Example 6: High Throughput Test ===");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            int totalMessages = 100000;

            for (int i = 0; i < totalMessages; i++)
            {
                client.Send($"tick|{i}\n");
            }

            sw.Stop();
            double msgsPerSec = totalMessages / sw.Elapsed.TotalSeconds;
            Console.WriteLine($"  Sent {totalMessages:N0} messages in {sw.ElapsedMilliseconds}ms");
            Console.WriteLine($"  Throughput: {msgsPerSec:N0} msg/sec (enqueue rate)");

            // Flush all
            client.Flush(10000);
            Console.WriteLine($"  All messages flushed");
            Console.WriteLine();

            // --- Prometheus metrics access ---
            Console.WriteLine("=== Prometheus Integration ===");
            Console.WriteLine($"  Registry: {client.Metrics.Registry.GetType().Name}");
            Console.WriteLine($"  Metrics can be exposed via prometheus-net HTTP server or pushed to Pushgateway");
            Console.WriteLine();

            // Final metrics
            Console.WriteLine("=== Final Metrics ===");
            Console.WriteLine(client.GetMetricsString());
            Console.WriteLine("JSON metrics:");
            Console.WriteLine(client.GetMetricsJson());
        }
        catch (TglogException ex)
        {
            Console.WriteLine($"TGLog Error: {ex.ErrorCode} ({ex.ErrorCodeValue}) - {ex.Message}");
        }

        // ==========================================
        // Example 7: Default (no logger injection)
        // ==========================================
        Console.WriteLine();
        Console.WriteLine("=== Example 7: Default (NullLogger) ===");
        Console.WriteLine("  When no ILoggerFactory is set, the SDK uses NullLoggerFactory (no log output).");
        Console.WriteLine("  In production, inject your app's ILoggerFactory to see SDK logs.");

        var defaultConfig = TglogConfig.CreateUdp(host, port)
            .SetWorkerNum(2)
            .SetBatchingMaxMessages(10)
            .SetBatchingMaxDelayMs(5);
        // No SetLoggerFactory — uses NullLoggerFactory.Instance

        Console.WriteLine($"  Protocol: UDP, Workers: {defaultConfig.WorkerNum}");
        Console.WriteLine($"  Max request size: {defaultConfig.MaxRequestSize} bytes");
        Console.WriteLine();

        Console.WriteLine("Done. Press any key to exit.");
        Console.ReadKey();
    }
}
