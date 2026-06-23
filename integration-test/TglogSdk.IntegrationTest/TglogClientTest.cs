using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Serilog;
using Tencent.Tglog;

namespace Tencent.Tglog.IntegrationTest;

/// <summary>
/// Performance test for TglogClient.
///
/// This test demonstrates the usage and performance of the pure C# TGLog client.
/// Mirrors the Java SDK's TglogClientTest.java exactly.
/// </summary>
class TglogClientTest
{
    static int Main(string[] args)
    {
        // Parse command line arguments
        TestConfig testConfig = ParseArgs(args);

        PrintSeparator();
        Console.WriteLine("TGLog Pure C# Client Performance Test");
        PrintSeparator();
        Console.WriteLine("Configuration:");
        Console.WriteLine($"  Protocol:      {testConfig.Protocol}");
        Console.WriteLine($"  Host:          {testConfig.Host}");
        Console.WriteLine($"  Port:          {testConfig.Port}");
        Console.WriteLine($"  Messages:      {testConfig.MessageCount:N0}");
        Console.WriteLine($"  Workers:       {testConfig.WorkerNum}");
        Console.WriteLine($"  Batch Size:    {testConfig.BatchSize}");
        Console.WriteLine($"  With Callback: {testConfig.WithCallback}");
        Console.WriteLine($"  Rate Limit:    {(testConfig.RateLimit > 0 ? $"{testConfig.RateLimit:N0} msg/s" : "unlimited")}");
        PrintSeparator();

        try
        {
            RunTest(testConfig);
            return 0;
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"Test failed: {e.Message}");
            Console.Error.WriteLine(e.StackTrace);
            return 1;
        }
    }

    private static void RunTest(TestConfig testConfig)
    {
        // Setup Serilog as the logging provider.
        // This demonstrates how to inject an external logging framework into TglogClient.
        var serilogLogger = new LoggerConfiguration()
            .MinimumLevel.Is(testConfig.Debug
                ? Serilog.Events.LogEventLevel.Debug
                : Serilog.Events.LogEventLevel.Information)
            .WriteTo.Console(outputTemplate:
                "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {SourceContext}: {Message:lj}{NewLine}{Exception}")
            .CreateLogger();

        using var loggerFactory = new LoggerFactory().AddSerilog(serilogLogger);

        // Create configuration
        TglogConfig config = (testConfig.Protocol == TglogConfig.Protocol.Tcp
                ? TglogConfig.CreateTcp(testConfig.Host, testConfig.Port)
                : TglogConfig.CreateUdp(testConfig.Host, testConfig.Port))
            .SetWorkerNum(testConfig.WorkerNum)
            .SetBatchingMaxMessages(testConfig.BatchSize)
            .SetBatchingMaxDelayMs(10)
            .SetLoggerFactory(loggerFactory);

        // Create client
        Console.WriteLine("\nCreating client...");
        using var client = new TglogClient(config);

        // Wait a bit for connections to establish
        Thread.Sleep(500);

        Console.WriteLine("Client created successfully");
        Console.WriteLine("\nStarting performance test...\n");

        // Prepare test data - pre-generate log strings for better performance
        Console.WriteLine("Preparing test data...");
        int logArraySize = Math.Min(testConfig.MessageCount, 10000);
        string[] logs = new string[logArraySize];
        long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        for (int i = 0; i < logs.Length; i++)
        {
            logs[i] = $"gametest|userid={i}|CarryOutItemFlow|/cnprod-a/36/settlementsvr/nanjing/11.176.45.170/1|2025-07-14 13:39:23|wx39308a0a1bd83d22|1|36|18182882159345025898|18182882159345025898|2340077323481036542|11090000053|1|648520772929162293|0|4|0.100|109|1|0|||0|0|||648530419497318933|2340077323481036542||ts={timestamp}\n";
        }
        Console.WriteLine("Test data prepared");

        // Counters
        long successCount = 0;
        long failureCount = 0;
        CountdownEvent? latch = testConfig.WithCallback
            ? new CountdownEvent(testConfig.MessageCount) : null;

        // Callback
        TglogCallback? callback = testConfig.WithCallback ? (payload, exception) =>
        {
            if (exception == null)
            {
                Interlocked.Increment(ref successCount);
            }
            else
            {
                Interlocked.Increment(ref failureCount);
                Console.Error.WriteLine($"Send failed: {exception.Message}");
            }
            latch!.Signal();
        } : null;

        // Create rate limiter if rate limit is specified
        RateLimiter? rateLimiter = testConfig.RateLimit > 0
            ? new RateLimiter(testConfig.RateLimit) : null;

        // Send messages
        long startTime = Stopwatch.GetTimestamp();
        long lastReportTime = startTime;
        int reportInterval = testConfig.MessageCount / 10; // Report every 10%
        if (reportInterval == 0) reportInterval = 1;

        for (int i = 0; i < testConfig.MessageCount; i++)
        {
            // Apply rate limiting if enabled
            rateLimiter?.Acquire();

            string log = logs[i % logs.Length];

            try
            {
                client.Send(log, callback);
            }
            catch (TglogException e)
            {
                Console.Error.WriteLine($"Failed to send message {i}: {e.Message}");
                Interlocked.Increment(ref failureCount);
                latch?.Signal();
            }

            // Progress report
            if ((i + 1) % reportInterval == 0)
            {
                long now = Stopwatch.GetTimestamp();
                double progress = (i + 1) * 100.0 / testConfig.MessageCount;
                long intervalElapsedMs = ElapsedMs(lastReportTime, now);
                long totalElapsedMs = ElapsedMs(startTime, now);
                long intervalRate = reportInterval * 1000L / Math.Max(intervalElapsedMs, 1);
                long avgRate = (long)(i + 1) * 1000L / Math.Max(totalElapsedMs, 1);
                Console.WriteLine($"Progress: {progress:F0}% ({i + 1:N0} messages) - Interval: {intervalRate:N0} msg/s, Average: {avgRate:N0} msg/s");
                lastReportTime = now;
            }
        }

        long sendEndTime = Stopwatch.GetTimestamp();
        long sendDuration = ElapsedMs(startTime, sendEndTime);

        Console.WriteLine("\nAll messages sent to queue");
        Console.WriteLine($"Send duration: {sendDuration:N0} ms");
        Console.WriteLine($"Send rate: {(long)testConfig.MessageCount * 1000L / Math.Max(sendDuration, 1):N0} msg/s");

        // Wait for callbacks if enabled
        if (testConfig.WithCallback)
        {
            Console.WriteLine("\nWaiting for all callbacks...");
            bool completed = latch!.Wait(TimeSpan.FromSeconds(60));
            if (!completed)
            {
                Console.Error.WriteLine("Warning: Not all callbacks completed within timeout");
            }
        }
        else
        {
            // Wait for queues to drain
            Console.WriteLine("\nWaiting for queues to drain...");
            client.Flush(30000);
        }

        long endTime = Stopwatch.GetTimestamp();
        long totalDuration = ElapsedMs(startTime, endTime);

        // Print results
        Console.WriteLine();
        PrintSeparator();
        Console.WriteLine("Test Results");
        PrintSeparator();

        TglogMetrics metrics = client.Metrics;

        Console.WriteLine($"Total Duration:     {totalDuration:N0} ms ({totalDuration / 1000.0:F2} seconds)");
        Console.WriteLine($"Overall Throughput: {(long)testConfig.MessageCount * 1000L / Math.Max(totalDuration, 1):N0} msg/s");
        Console.WriteLine();
        Console.WriteLine($"Messages Requested: {metrics.RequestMsgCount:N0}");
        Console.WriteLine($"Messages Enqueued:  {metrics.EnqueuedMsgCount:N0}");
        Console.WriteLine($"Messages Succeeded: {metrics.SucceededMsgCount:N0}");
        Console.WriteLine($"Messages Failed:    {metrics.FailedMsgCount:N0}");
        Console.WriteLine($"Messages Dropped:   {metrics.DroppedMsgCount:N0}");
        Console.WriteLine($"Success Rate:       {metrics.SuccessRate * 100:F2}%");
        Console.WriteLine();
        Console.WriteLine($"Connections Opened: {metrics.OpenedConnCount:N0}");
        Console.WriteLine($"Connections Closed: {metrics.ClosedConnCount:N0}");
        Console.WriteLine($"Connection Errors:  {metrics.ErrorConnCount:N0}");

        if (testConfig.WithCallback)
        {
            Console.WriteLine();
            Console.WriteLine($"Callback Success:   {Interlocked.Read(ref successCount):N0}");
            Console.WriteLine($"Callback Failure:   {Interlocked.Read(ref failureCount):N0}");
        }

        PrintSeparator();

        // Print detailed metrics if debug enabled
        if (testConfig.Debug)
        {
            Console.WriteLine("\nDetailed Metrics:");
            Console.WriteLine(client.GetMetricsString());
            Console.WriteLine("\nMetrics JSON:");
            Console.WriteLine(client.GetMetricsJson());
        }

        // Close client (Dispose is called by using statement, but we print status)
        Console.WriteLine("\nClosing client...");
        client.Dispose();
        Console.WriteLine("Client closed");

        // Cleanup
        latch?.Dispose();
    }

    private static TestConfig ParseArgs(string[] args)
    {
        TestConfig config = new TestConfig();

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "-h":
                case "--host":
                    config.Host = args[++i];
                    break;
                case "-p":
                case "--port":
                    config.Port = int.Parse(args[++i]);
                    break;
                case "-m":
                case "--messages":
                    config.MessageCount = int.Parse(args[++i]);
                    break;
                case "-w":
                case "--workers":
                    config.WorkerNum = int.Parse(args[++i]);
                    break;
                case "-b":
                case "--batch-size":
                    config.BatchSize = int.Parse(args[++i]);
                    break;
                case "--tcp":
                    config.Protocol = TglogConfig.Protocol.Tcp;
                    break;
                case "--udp":
                    config.Protocol = TglogConfig.Protocol.Udp;
                    break;
                case "-c":
                case "--callback":
                    config.WithCallback = true;
                    break;
                case "-d":
                case "--debug":
                    config.Debug = true;
                    break;
                case "-r":
                case "--rate-limit":
                    config.RateLimit = long.Parse(args[++i]);
                    break;
                case "--help":
                    PrintUsage();
                    Environment.Exit(0);
                    break;
                default:
                    Console.Error.WriteLine($"Unknown option: {args[i]}");
                    PrintUsage();
                    Environment.Exit(1);
                    break;
            }
        }

        return config;
    }

    private static void PrintSeparator()
    {
        Console.WriteLine(new string('=', 60));
    }

    private static void PrintUsage()
    {
        Console.WriteLine("Usage: TglogClientTest [options]");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  -h, --host <host>        Server host (default: dev.tglog.com)");
        Console.WriteLine("  -p, --port <port>        Server port (default: 20001)");
        Console.WriteLine("  -m, --messages <count>   Number of messages to send (default: 100000)");
        Console.WriteLine("  -w, --workers <count>    Number of worker threads (default: 8)");
        Console.WriteLine("  -b, --batch-size <size>  Batch size (default: 50)");
        Console.WriteLine("  -r, --rate-limit <rate>  Rate limit in messages/sec (default: 500000, 0 = unlimited)");
        Console.WriteLine("  --tcp                    Use TCP protocol (default)");
        Console.WriteLine("  --udp                    Use UDP protocol");
        Console.WriteLine("  -c, --callback           Enable callbacks (slower but tracks results)");
        Console.WriteLine("  -d, --debug              Enable debug logging");
        Console.WriteLine("  --help                   Show this help message");
    }

    private static long ElapsedMs(long startTimestamp, long endTimestamp)
    {
        return (long)((endTimestamp - startTimestamp) * 1000.0 / Stopwatch.Frequency);
    }

    // ==================== Inner Classes ====================

    class TestConfig
    {
        public string Host = "dev.tglog.com";
        public int Port = 20001;
        public int MessageCount = 100000;
        public int WorkerNum = 8;
        public int BatchSize = 50;
        public long RateLimit = 500000; // Default: 500,000 messages/sec
        public TglogConfig.Protocol Protocol = TglogConfig.Protocol.Tcp;
        public bool WithCallback = false;
        public bool Debug = false;
    }
}

/// <summary>
/// Simple token-bucket rate limiter matching Guava's RateLimiter behavior.
/// Uses a smooth-bursty algorithm: permits accumulate up to 1 second of burst.
/// </summary>
sealed class RateLimiter
{
    private readonly double _intervalTicks; // ticks between permits
    private readonly long _maxStoredPermitsTicks; // max stored permits in ticks
    private long _nextFreeTicket;
    private readonly object _lock = new();

    public RateLimiter(long permitsPerSecond)
    {
        _intervalTicks = (double)Stopwatch.Frequency / permitsPerSecond;
        _maxStoredPermitsTicks = Stopwatch.Frequency; // 1 second of burst
        _nextFreeTicket = Stopwatch.GetTimestamp();
    }

    /// <summary>
    /// Acquires one permit, blocking until it is available.
    /// </summary>
    public void Acquire()
    {
        long waitUntil;
        lock (_lock)
        {
            long now = Stopwatch.GetTimestamp();
            if (now >= _nextFreeTicket)
            {
                // Accumulate stored permits, but cap at max burst
                long excess = now - _nextFreeTicket;
                if (excess > _maxStoredPermitsTicks)
                    _nextFreeTicket = now;
                // No waiting needed
                _nextFreeTicket += (long)_intervalTicks;
                return;
            }

            waitUntil = _nextFreeTicket;
            _nextFreeTicket += (long)_intervalTicks;
        }

        // Spin-wait for short durations, Thread.Sleep for longer
        long waitTicks = waitUntil - Stopwatch.GetTimestamp();
        if (waitTicks > 0)
        {
            double waitMs = waitTicks * 1000.0 / Stopwatch.Frequency;
            if (waitMs > 1.0)
            {
                Thread.Sleep((int)waitMs);
            }
            else
            {
                // Spin-wait for sub-millisecond precision
                SpinWait spinner = new();
                while (Stopwatch.GetTimestamp() < waitUntil)
                    spinner.SpinOnce();
            }
        }
    }
}
