using System.Text;

namespace Tencent.Tglog;

/// <summary>
/// Wraps a log message payload with cached UTF-8 encoding and creation timestamp.
/// </summary>
internal sealed class LogMessage
{
    /// <summary>The original string payload.</summary>
    public string Payload { get; }

    /// <summary>Callback to invoke on completion (success or failure).</summary>
    public TglogCallback? Callback { get; }

    /// <summary>Monotonic timestamp when this message was created (Environment.TickCount64).</summary>
    public long CreateTimeTicks { get; }

    /// <summary>Cached UTF-8 encoding of the payload (computed once at construction).</summary>
    public byte[] PayloadBytes { get; }

    /// <summary>Size of the UTF-8 encoded payload in bytes.</summary>
    public int PayloadSize => PayloadBytes.Length;

    public LogMessage(string payload, TglogCallback? callback)
    {
        Payload = payload;
        Callback = callback;
        CreateTimeTicks = Environment.TickCount64;
        PayloadBytes = Encoding.UTF8.GetBytes(payload);
    }

    /// <summary>
    /// Returns true if this message has exceeded the given timeout since creation.
    /// </summary>
    public bool IsTimeout(int timeoutMs)
    {
        return (Environment.TickCount64 - CreateTimeTicks) >= timeoutMs;
    }
}
