namespace Tencent.Tglog;

/// <summary>
/// Error codes for TGLog operations.
/// </summary>
public enum TglogErrorCode
{
    /// <summary>Success</summary>
    Ok = 0,

    /// <summary>Send operation timed out</summary>
    SendTimeout = 10001,

    /// <summary>Send operation failed</summary>
    SendFailed = 10002,

    /// <summary>Client has been closed</summary>
    ProducerClosed = 10003,

    /// <summary>Message queue is full</summary>
    SendQueueIsFull = 10004,

    /// <summary>No available connections</summary>
    NoAvailableConn = 10005,

    /// <summary>Failed to create new connection</summary>
    NewConnFailed = 10006,

    /// <summary>Failed to write to connection</summary>
    ConnWriteFailed = 10007,

    /// <summary>Failed to read from connection</summary>
    ConnReadFailed = 10008,

    /// <summary>Log payload exceeds size limit</summary>
    LogTooLong = 10009,

    /// <summary>Invalid log payload (e.g. missing newline)</summary>
    BadLog = 10010,

    /// <summary>Batch size exceeded protocol limit</summary>
    BatchSizeExceeded = 10011,

    /// <summary>Memory allocation failed</summary>
    MemoryAllocFailed = 10012,

    /// <summary>Worker thread is busy</summary>
    WorkerBusy = 10013,

    /// <summary>No matching request for response</summary>
    NoMatchReqForRsp = 10014,

    /// <summary>Connection closed by peer</summary>
    ConnClosedByPeer = 10015,

    /// <summary>Socket buffer full (EAGAIN)</summary>
    ConnEagain = 10016,

    /// <summary>Invalid flag</summary>
    BadFlag = 10017,

    /// <summary>Unknown error</summary>
    Unknown = 20001
}

/// <summary>
/// Extension methods for <see cref="TglogErrorCode"/>.
/// </summary>
public static class TglogErrorCodeExtensions
{
    /// <summary>Returns true if this error code represents success.</summary>
    public static bool IsOk(this TglogErrorCode code) => code == TglogErrorCode.Ok;

    /// <summary>Returns true if this error code represents an error.</summary>
    public static bool IsError(this TglogErrorCode code) => code != TglogErrorCode.Ok;

    /// <summary>Returns the numeric code value.</summary>
    public static int GetCode(this TglogErrorCode code) => (int)code;

    /// <summary>Returns a human-readable error message.</summary>
    public static string GetMessage(this TglogErrorCode code) => code switch
    {
        TglogErrorCode.Ok => "Success",
        TglogErrorCode.SendTimeout => "Send timeout",
        TglogErrorCode.SendFailed => "Send failed",
        TglogErrorCode.ProducerClosed => "Producer closed",
        TglogErrorCode.SendQueueIsFull => "Send queue is full",
        TglogErrorCode.NoAvailableConn => "No available connection",
        TglogErrorCode.NewConnFailed => "New connection failed",
        TglogErrorCode.ConnWriteFailed => "Connection write failed",
        TglogErrorCode.ConnReadFailed => "Connection read failed",
        TglogErrorCode.LogTooLong => "Log too long",
        TglogErrorCode.BadLog => "Bad log",
        TglogErrorCode.BatchSizeExceeded => "Batch size exceeded",
        TglogErrorCode.MemoryAllocFailed => "Memory allocation failed",
        TglogErrorCode.WorkerBusy => "Worker busy",
        TglogErrorCode.NoMatchReqForRsp => "No matching request for response",
        TglogErrorCode.ConnClosedByPeer => "Connection closed by peer",
        TglogErrorCode.ConnEagain => "Socket buffer full (EAGAIN)",
        TglogErrorCode.BadFlag => "Bad flag",
        TglogErrorCode.Unknown => "Unknown error",
        _ => "Unknown error"
    };
}
