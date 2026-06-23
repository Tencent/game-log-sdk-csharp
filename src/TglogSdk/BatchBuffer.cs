using Microsoft.Extensions.Logging;

namespace Tencent.Tglog;

/// <summary>
/// Accumulates log messages into batches for efficient network sending.
/// Supports three flush triggers: message count, size, and timeout.
/// Caches prepared send buffers for retry reuse.
/// </summary>
internal sealed class BatchBuffer
{
    private readonly TglogConfig _config;
    private readonly ILogger _logger;

    private readonly List<LogMessage> _messages = new();
    private int _currentSize;
    private long _createTimeTicks;  // When first message was added

    // Retry state
    private int _retryCount;
    private long _retryStartTicks;
    private long _lastRetryAttemptTicks;

    // Cached send buffers (reused on retry to avoid re-serialization)
    private ArraySegment<byte>[]? _sendBuffers;
    private int _sendBufferCount;

    public BatchBuffer(TglogConfig config, ILogger logger)
    {
        _config = config;
        _logger = logger;
    }

    /// <summary>Number of messages currently in this batch.</summary>
    public int MessageCount => _messages.Count;

    /// <summary>Total size of messages in bytes.</summary>
    public int CurrentSize => _currentSize;

    /// <summary>True if the batch contains no messages.</summary>
    public bool IsEmpty => _messages.Count == 0;

    /// <summary>Current retry count.</summary>
    public int RetryCount => _retryCount;

    /// <summary>True if the batch is in retry state.</summary>
    public bool IsRetrying => _retryStartTicks > 0;

    /// <summary>
    /// Tries to add a message to the batch. Returns false if the message
    /// would exceed batch limits (caller should flush first).
    /// </summary>
    public bool Add(LogMessage message)
    {
        int msgSize = message.PayloadSize;

        if (_messages.Count == 0)
        {
            // First message: set creation time
            _createTimeTicks = Environment.TickCount64;

            // Only enforce MaxRequestSize for the first message.
            // This allows a single large message (> BatchingMaxSize but <= MaxRequestSize)
            // to form a batch by itself, preventing it from being stuck forever.
            if (msgSize > _config.MaxRequestSize)
                return false;
        }
        else
        {
            // Subsequent messages: enforce all batch limits
            if (_messages.Count >= _config.BatchingMaxMessages)
                return false;
            if (_currentSize + msgSize > _config.BatchingMaxSize)
                return false;
            if (_currentSize + msgSize > _config.MaxRequestSize)
                return false;
        }

        _messages.Add(message);
        _currentSize += msgSize;
        return true;
    }

    /// <summary>
    /// Returns true if the batch should be flushed (any trigger met):
    /// 1. Message count >= max
    /// 2. Size >= max
    /// 3. Age >= max delay
    /// </summary>
    public bool ShouldFlush()
    {
        if (_messages.Count == 0) return false;

        if (_messages.Count >= _config.BatchingMaxMessages)
            return true;
        if (_currentSize >= _config.BatchingMaxSize)
            return true;
        if (_config.BatchingMaxDelayMs > 0 && GetAge() >= _config.BatchingMaxDelayMs)
            return true;

        return false;
    }

    /// <summary>
    /// Returns true if this message cannot fit in the current batch
    /// (used for size check before adding).
    /// </summary>
    public bool WouldExceedLimits(LogMessage message)
    {
        // Empty batch: same logic as Add() — only MaxRequestSize matters
        if (_messages.Count == 0)
            return message.PayloadSize > _config.MaxRequestSize;

        if (_messages.Count >= _config.BatchingMaxMessages) return true;
        int newSize = _currentSize + message.PayloadSize;
        if (newSize > _config.BatchingMaxSize) return true;
        if (newSize > _config.MaxRequestSize) return true;
        return false;
    }

    /// <summary>
    /// Prepares send buffers for transmission. Caches the result for retries.
    /// </summary>
    public ArraySegment<byte>[] PrepareForSend()
    {
        if (_sendBuffers != null)
        {
            // Retry: reuse cached buffers (reset offsets)
            return _sendBuffers;
        }

        return _config.TransportProtocol == TglogConfig.Protocol.Tcp
            ? PrepareForTcp()
            : PrepareForUdp();
    }

    /// <summary>
    /// TCP: One buffer per message for gathering write (writev/WSASend).
    /// </summary>
    private ArraySegment<byte>[] PrepareForTcp()
    {
        _sendBuffers = new ArraySegment<byte>[_messages.Count];
        for (int i = 0; i < _messages.Count; i++)
        {
            byte[] data = _messages[i].PayloadBytes;
            // Reference PayloadBytes directly — it is immutable after LogMessage construction.
            // The _messages list holds the LogMessage alive until ClearInternal(),
            // and downstream BufferData/BufferRemainder make their own copies if needed.
            _sendBuffers[i] = new ArraySegment<byte>(data, 0, data.Length);
        }
        _sendBufferCount = _messages.Count;
        return _sendBuffers;
    }

    /// <summary>
    /// UDP: Single concatenated buffer (all-or-nothing send).
    /// </summary>
    private ArraySegment<byte>[] PrepareForUdp()
    {
        byte[] buffer = new byte[_currentSize];
        int offset = 0;
        foreach (var msg in _messages)
        {
            Buffer.BlockCopy(msg.PayloadBytes, 0, buffer, offset, msg.PayloadBytes.Length);
            offset += msg.PayloadBytes.Length;
        }
        _sendBuffers = new[] { new ArraySegment<byte>(buffer, 0, _currentSize) };
        _sendBufferCount = 1;
        return _sendBuffers;
    }

    /// <summary>
    /// Calculates total bytes across all send buffers.
    /// </summary>
    public int GetTotalSendBytes()
    {
        if (_sendBuffers == null) return _currentSize;
        int total = 0;
        for (int i = 0; i < _sendBufferCount; i++)
            total += _sendBuffers[i].Count;
        return total;
    }

    /// <summary>
    /// Notifies success for the first sentCount messages and removes them from the batch.
    /// Used for partial TCP sends.
    /// </summary>
    public void NotifyAndRemove(int sentCount)
    {
        if (sentCount <= 0) return;
        int toRemove = Math.Min(sentCount, _messages.Count);

        for (int i = 0; i < toRemove; i++)
        {
            var msg = _messages[i];
            _currentSize -= msg.PayloadSize;
            InvokeCallbackSafe(msg, null);
        }

        _messages.RemoveRange(0, toRemove);

        // Invalidate cached send buffers (partial send means buffers are stale)
        if (_messages.Count == 0)
        {
            ClearInternal();
        }
        else
        {
            // Trim send buffer cache for remaining messages
            _sendBuffers = null;
            _sendBufferCount = 0;
        }
    }

    /// <summary>
    /// Notifies success for all messages in the batch.
    /// Metrics should be updated by the caller (worker) to avoid double-counting.
    /// </summary>
    public void NotifyAllSucceeded()
    {
        for (int i = 0; i < _messages.Count; i++)
        {
            InvokeCallbackSafe(_messages[i], null);
        }
        ClearInternal();
    }

    /// <summary>
    /// Notifies failure for all messages in the batch.
    /// Metrics should be updated by the caller (worker) to avoid double-counting.
    /// </summary>
    public void NotifyAllFailed(TglogErrorCode errorCode)
    {
        var exception = new TglogException(errorCode);
        int count = _messages.Count;

        for (int i = 0; i < count; i++)
        {
            InvokeCallbackSafe(_messages[i], exception);
        }

        ClearInternal();
    }

    /// <summary>
    /// Calculates how many complete messages fit within the given byte count.
    /// Used to determine which messages were successfully sent in a partial TCP write.
    /// </summary>
    public int CalculateSentMessages(long bytesSent)
    {
        if (_config.TransportProtocol == TglogConfig.Protocol.Udp)
        {
            // UDP is all-or-nothing
            return bytesSent > 0 ? _messages.Count : 0;
        }

        // TCP: count complete messages
        long remaining = bytesSent;
        int count = 0;
        foreach (var msg in _messages)
        {
            if (remaining >= msg.PayloadSize)
            {
                remaining -= msg.PayloadSize;
                count++;
            }
            else
            {
                break;
            }
        }
        return count;
    }

    /// <summary>Mark the batch for retry. Increments retry count and records timestamps.</summary>
    public void MarkForRetry()
    {
        if (_retryStartTicks == 0)
        {
            _retryStartTicks = Environment.TickCount64;
        }
        _lastRetryAttemptTicks = Environment.TickCount64;
        _retryCount++;
    }

    /// <summary>Returns true if the backoff period has elapsed and retry should be attempted.</summary>
    public bool ShouldRetryNow(Backoff backoff)
    {
        return backoff.ShouldRetry(_lastRetryAttemptTicks, _retryCount);
    }

    /// <summary>Returns true if the total retry duration has exceeded the given timeout.</summary>
    public bool IsRetryTimeout(int timeoutMs)
    {
        if (_retryStartTicks == 0) return false;
        return (Environment.TickCount64 - _retryStartTicks) > timeoutMs;
    }

    /// <summary>Returns true if the batch age (since first message) exceeds the send timeout.</summary>
    public bool IsSendTimeout(int sendTimeoutMs)
    {
        if (_messages.Count == 0) return false;
        return GetAge() >= sendTimeoutMs;
    }

    /// <summary>Age of the batch in milliseconds (since first message was added).</summary>
    public long GetAge()
    {
        if (_createTimeTicks == 0) return 0;
        return Environment.TickCount64 - _createTimeTicks;
    }

    /// <summary>Clears all batch state.</summary>
    public void Clear()
    {
        ClearInternal();
    }

    private void ClearInternal()
    {
        _messages.Clear();
        _currentSize = 0;
        _createTimeTicks = 0;
        _retryCount = 0;
        _retryStartTicks = 0;
        _lastRetryAttemptTicks = 0;
        _sendBuffers = null;
        _sendBufferCount = 0;
    }

    /// <summary>
    /// Invokes message callback safely — catches and swallows exceptions
    /// to prevent one bad callback from affecting other messages.
    /// </summary>
    private void InvokeCallbackSafe(LogMessage message, TglogException? exception)
    {
        try
        {
            message.Callback?.Invoke(message.Payload, exception);
        }
        catch (Exception ex)
        {
            _logger.LogWarning($"Exception in user callback: {ex.Message}");
        }
    }
}
