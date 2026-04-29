namespace Tencent.Tglog;

/// <summary>
/// Calculates exponential backoff intervals for retry logic.
/// Formula: interval = min(initial * multiplier^(retryCount-1), max)
/// </summary>
internal sealed class Backoff
{
    /// <summary>Batch retry: 100ms -> 200ms -> ... -> 5s max</summary>
    public static readonly Backoff BatchRetry = new(100, 2.0, 5000);

    /// <summary>Connection retry: 1s -> 2s -> ... -> 60s max</summary>
    public static readonly Backoff ConnectionRetry = new(1000, 2.0, 60000);

    /// <summary>Endpoint recovery: 5s -> 10s -> ... -> 300s max</summary>
    public static readonly Backoff EndpointRecovery = new(5000, 2.0, 300000);

    public long InitialIntervalMs { get; }
    public double Multiplier { get; }
    public long MaxIntervalMs { get; }

    public Backoff(long initialIntervalMs, double multiplier, long maxIntervalMs)
    {
        InitialIntervalMs = initialIntervalMs;
        Multiplier = multiplier;
        MaxIntervalMs = maxIntervalMs;
    }

    /// <summary>
    /// Gets the backoff interval for the given retry count.
    /// retryCount starts at 1 for the first retry.
    /// </summary>
    public long GetNextInterval(int retryCount)
    {
        if (retryCount <= 0) return InitialIntervalMs;

        // Prevent overflow for very high retry counts
        if (retryCount - 1 > 30) return MaxIntervalMs;

        double interval = InitialIntervalMs * Math.Pow(Multiplier, retryCount - 1);
        return (long)Math.Min(interval, MaxIntervalMs);
    }

    /// <summary>
    /// Returns true if enough time has passed since the last attempt to allow a retry.
    /// </summary>
    public bool ShouldRetry(long lastAttemptTicks, int retryCount)
    {
        long elapsed = Environment.TickCount64 - lastAttemptTicks;
        return elapsed >= GetNextInterval(retryCount);
    }

    /// <summary>
    /// Calculates the total backoff time across all retries up to maxRetries.
    /// </summary>
    public long GetTotalBackoffTime(int maxRetries)
    {
        long total = 0;
        for (int i = 1; i <= maxRetries; i++)
        {
            total += GetNextInterval(i);
        }
        return total;
    }
}
