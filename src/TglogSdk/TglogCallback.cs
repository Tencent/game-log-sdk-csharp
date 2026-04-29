namespace Tencent.Tglog;

/// <summary>
/// Callback delegate for asynchronous send completion notification.
/// </summary>
/// <param name="payload">The original log payload that was sent.</param>
/// <param name="exception">null on success; contains error details on failure.</param>
public delegate void TglogCallback(string payload, TglogException? exception);
