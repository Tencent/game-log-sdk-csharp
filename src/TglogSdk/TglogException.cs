namespace Tencent.Tglog;

/// <summary>
/// Exception type for TGLog errors. Contains an <see cref="TglogErrorCode"/>
/// that identifies the specific failure.
/// </summary>
public class TglogException : Exception
{
    /// <summary>The error code identifying this failure.</summary>
    public TglogErrorCode ErrorCode { get; }

    /// <summary>The numeric value of the error code.</summary>
    public int ErrorCodeValue => (int)ErrorCode;

    public TglogException(TglogErrorCode errorCode)
        : base(errorCode.GetMessage())
    {
        ErrorCode = errorCode;
    }

    public TglogException(TglogErrorCode errorCode, string message)
        : base(message)
    {
        ErrorCode = errorCode;
    }

    public TglogException(TglogErrorCode errorCode, Exception innerException)
        : base(errorCode.GetMessage(), innerException)
    {
        ErrorCode = errorCode;
    }

    public TglogException(TglogErrorCode errorCode, string message, Exception innerException)
        : base(message, innerException)
    {
        ErrorCode = errorCode;
    }

    public TglogException(string message)
        : base(message)
    {
        ErrorCode = TglogErrorCode.Unknown;
    }

    public TglogException(string message, Exception innerException)
        : base(message, innerException)
    {
        ErrorCode = TglogErrorCode.Unknown;
    }
}
