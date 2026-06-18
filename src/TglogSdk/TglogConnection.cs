using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;

namespace Tencent.Tglog;

/// <summary>
/// Manages a single non-blocking TCP or UDP connection with output buffering.
///
/// Key features:
/// - Non-blocking socket I/O (Blocking = false)
/// - Output buffer queue for partial writes (similar to libevent's bufferevent)
/// - High watermark check to prevent memory exhaustion
/// - Connection lifetime management with jitter to avoid thundering herd
/// - Data rescue support: transfer pending data to another connection on close
/// </summary>
internal sealed class TglogConnection
{
    /// <summary>Connection state machine.</summary>
    public enum ConnectionState
    {
        Disconnected,
        Connecting,
        Connected,
        Error
    }

    /// <summary>Segment of a byte buffer with tracked position for partial writes.</summary>
    internal struct BufferSegment
    {
        public byte[] Data;
        public int Offset;
        public int Count;

        public int Remaining => Count - Offset;

        public BufferSegment(byte[] data, int offset, int count)
        {
            Data = data;
            Offset = offset;
            Count = count;
        }

        public void Advance(int bytes) => Offset += bytes;
    }

    // 32MB max output buffer size (matches Java MAX_OUTPUT_BUFFER_SIZE)
    internal const int MaxOutputBufferSize = 32 * 1024 * 1024;

    private readonly TglogConfig _config;
    private readonly TglogMetrics _metrics;
    private readonly int _workerId;
    private readonly ILogger _logger;

    // Socket
    private Socket? _socket;
    private readonly IPEndPoint _endpoint;
    private readonly TglogConfig.Protocol _protocol;

    // State
    private volatile ConnectionState _state = ConnectionState.Disconnected;
    private long _connectTimeTicks;
    private long _lastActiveTimeTicks;
    private long _lifetimeWithJitterMs;

    // Retry
    private int _retryCount;
    private long _lastRetryAttemptTicks;

    // Output buffer (FIFO queue of unsent data, LinkedList for O(1) front operations)
    private readonly LinkedList<BufferSegment> _outputBuffer = new();
    private int _outputBufferSize;

    // Read buffer for detecting peer close
    private readonly byte[] _readBuffer = new byte[64];

    public TglogConnection(IPEndPoint endpoint, TglogConfig config, TglogMetrics metrics, int workerId, ILogger logger)
    {
        _endpoint = endpoint;
        _config = config;
        _metrics = metrics;
        _workerId = workerId;
        _protocol = config.TransportProtocol;
        _logger = logger;
    }

    // --- Properties ---

    public ConnectionState State => _state;
    public Socket? Socket => _socket;
    public IPEndPoint Endpoint => _endpoint;
    public bool IsConnected => _state == ConnectionState.Connected;
    public bool IsConnecting => _state == ConnectionState.Connecting;
    public bool HasBufferedData => _outputBufferSize > 0;
    public int OutputBufferSize => _outputBufferSize;
    public int RetryCount => _retryCount;

    /// <summary>
    /// When this connection was created to replace an expired connection, holds a
    /// reference to the old connection it should retire once this one reaches
    /// <see cref="ConnectionState.Connected"/>. The old connection keeps serving
    /// throughout the handshake window and is only scheduled for delayed close once
    /// the replacement is actually usable. Cleared after retirement, or dropped if
    /// the replacement fails to connect (so the old connection keeps serving).
    /// </summary>
    public TglogConnection? ReplacingConn { get; set; }

    /// <summary>
    /// Initiates a non-blocking connection.
    /// For TCP: enters Connecting state, connection completes asynchronously.
    /// For UDP: enters Connected state immediately (UDP is connectionless).
    /// </summary>
    public bool Connect()
    {
        try
        {
            if (_protocol == TglogConfig.Protocol.Tcp)
            {
                _logger.LogInformation("Connecting to {Endpoint}", _endpoint);

                _socket = new Socket(_endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                _socket.Blocking = false;
                _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                ConfigureSocketBuffers(_socket);

                try
                {
                    _socket.Connect(_endpoint);
                    // Connected immediately (loopback or fast connection)
                    return FinishConnect();
                }
                catch (SocketException ex) when (
                    ex.SocketErrorCode == SocketError.WouldBlock ||
                    ex.SocketErrorCode == SocketError.InProgress ||
                    ex.SocketErrorCode == SocketError.AlreadyInProgress)
                {
                    // Connection pending — will complete via Select(checkWrite)
                    _state = ConnectionState.Connecting;
                    return true;
                }
            }
            else // UDP
            {
                _socket = new Socket(_endpoint.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
                _socket.Blocking = false;
                ConfigureSocketBuffers(_socket);
                _socket.Connect(_endpoint); // UDP connect just sets the default destination
                _logger.LogInformation("Connected to {Endpoint} (UDP)", _endpoint);
                return FinishConnect();
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Connect failed to {Endpoint}: {Message}", _endpoint, ex.Message);
            _state = ConnectionState.Error;
            _retryCount++;
            _lastRetryAttemptTicks = Environment.TickCount64;
            CloseSocket();
            return false;
        }
    }

    /// <summary>
    /// Completes a pending TCP connection after Socket.Select reports it writable.
    /// Do not call this as a polling check or from timeout cleanup.
    /// Returns true if connected successfully.
    /// </summary>
    public bool FinishConnect()
    {
        try
        {
            if (_protocol == TglogConfig.Protocol.Tcp && _socket != null)
            {
                // Check if the socket is actually connected by attempting to get the remote endpoint
                // If the connect failed, this will throw
                if (!_socket.Connected)
                {
                    // Try to check if there's a socket error
                    int errorCode = (int)_socket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Error)!;
                    if (errorCode != 0)
                    {
                        _logger.LogWarning("FinishConnect error for {Endpoint}: socket error {ErrorCode}", _endpoint, errorCode);
                        MarkConnectFailed();
                        return false;
                    }
                }
            }

            long now = Environment.TickCount64;
            _state = ConnectionState.Connected;
            _connectTimeTicks = now;
            _lastActiveTimeTicks = now;
            _retryCount = 0;
            _lifetimeWithJitterMs = CalculateLifetimeWithJitter();
            _metrics.IncrementOpenedConn(_workerId);

            _logger.LogInformation("Connected to {Endpoint} (lifetime: {LifetimeMs}ms)", _endpoint, _lifetimeWithJitterMs);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to finish connection to {Endpoint}", _endpoint);
            MarkConnectFailed();
            return false;
        }
    }

    internal void MarkConnectFailed()
    {
        _state = ConnectionState.Error;
        _retryCount++;
        _lastRetryAttemptTicks = Environment.TickCount64;
    }

    /// <summary>
    /// Sends data through this connection. Handles direct send and output buffering.
    ///
    /// Returns:
    ///   -1: Connection error (not connected)
    ///    0: High watermark exceeded (try another connection)
    ///   >0: Bytes accepted (sent directly and/or buffered)
    /// </summary>
    public int Send(ArraySegment<byte> data)
    {
        if (_state != ConnectionState.Connected || _socket == null)
            return -1;

        try
        {
            int originalSize = data.Count;

            // If output buffer has pending data, try to flush first
            if (_outputBufferSize > 0)
            {
                FlushOutputBuffer();

                // FlushOutputBuffer may have failed and marked the connection as Error.
                // Do NOT accept new data on a failed connection — return -1 so the worker
                // can fail over / rescue instead of reporting a false success.
                if (_state != ConnectionState.Connected)
                    return -1;
            }

            // If still has buffered data, check watermark and buffer the new data
            if (_outputBufferSize > 0)
            {
                int highWatermark = _config.WriteHighWatermark > 0
                    ? _config.WriteHighWatermark
                    : MaxOutputBufferSize;

                if (_outputBufferSize > highWatermark)
                {
                    _metrics.IncrementEagainErr(_workerId);
                    return 0; // High watermark exceeded
                }

                // Buffer the data
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug($"Buffering {data.Count} bytes for {_endpoint} (output buffer: {_outputBufferSize})");
                BufferData(data);
                return originalSize;
            }

            // Try direct send
            int sent = _socket.Send(data.Array!, data.Offset, data.Count, SocketFlags.None, out SocketError error);

            if (error != SocketError.Success && error != SocketError.WouldBlock)
            {
                _state = ConnectionState.Error;
                return -1;
            }

            if (sent > 0)
            {
                _lastActiveTimeTicks = Environment.TickCount64;
            }

            if (sent < data.Count)
            {
                // Partial write or EAGAIN (sent == 0) — buffer remainder
                if (sent > 0)
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug($"Partial write for {_endpoint}: sent {sent}/{data.Count}, buffering remainder");
                    BufferData(new ArraySegment<byte>(data.Array!, data.Offset + sent, data.Count - sent));
                }
                else
                {
                    // EAGAIN — buffer all
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug($"EAGAIN for {_endpoint}: buffering all {data.Count} bytes");
                    BufferData(data);
                }
                return originalSize; // All data accepted (sent + buffered)
            }

            return sent;
        }
        catch (SocketException ex)
        {
            if (ex.SocketErrorCode == SocketError.WouldBlock)
            {
                BufferData(data);
                return data.Count;
            }
            _state = ConnectionState.Error;
            return -1;
        }
        catch (ObjectDisposedException)
        {
            _state = ConnectionState.Error;
            return -1;
        }
    }

    /// <summary>
    /// Sends multiple buffers (gathering write for TCP).
    ///
    /// Returns:
    ///   -1: Connection error
    ///    0: High watermark exceeded
    ///   >0: Total bytes accepted
    /// </summary>
    public long Send(ArraySegment<byte>[] buffers) => Send(buffers, out _);

    /// <summary>
    /// Sends multiple buffers (gathering write for TCP), reporting bytes committed
    /// before a TCP write error when the platform exposes that count.
    ///
    /// Returns:
    ///   -1: Connection error
    ///    0: High watermark exceeded
    ///   >0: Total bytes accepted
    /// </summary>
    public long Send(ArraySegment<byte>[] buffers, out long committedBytes)
    {
        committedBytes = 0;

        if (_state != ConnectionState.Connected || _socket == null)
            return -1;

        try
        {
            // Calculate total size
            int totalSize = 0;
            for (int i = 0; i < buffers.Length; i++)
                totalSize += buffers[i].Count;

            // Flush existing buffer first
            if (_outputBufferSize > 0)
            {
                FlushOutputBuffer();

                // FlushOutputBuffer may have failed and marked the connection as Error.
                // Do NOT accept new data on a failed connection — return -1 so the worker
                // can fail over / rescue instead of reporting a false success.
                if (_state != ConnectionState.Connected)
                {
                    committedBytes = 0;
                    return -1;
                }
            }

            // Check watermark if still have buffered data
            if (_outputBufferSize > 0)
            {
                int highWatermark = _config.WriteHighWatermark > 0
                    ? _config.WriteHighWatermark
                    : MaxOutputBufferSize;

                if (_outputBufferSize > highWatermark)
                {
                    _metrics.IncrementEagainErr(_workerId);
                    return 0;
                }

                // Buffer all
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug($"Buffering {totalSize} bytes ({buffers.Length} segments) for {_endpoint} (output buffer: {_outputBufferSize})");
                BufferMultiple(buffers);
                return totalSize;
            }

            // For UDP, send first buffer only (single datagram)
            if (_protocol == TglogConfig.Protocol.Udp)
            {
                var buf = buffers[0];
                int sent = _socket.Send(buf.Array!, buf.Offset, buf.Count, SocketFlags.None, out SocketError error);

                if (error != SocketError.Success && error != SocketError.WouldBlock)
                {
                    _state = ConnectionState.Error;
                    return -1;
                }

                if (sent > 0)
                {
                    _lastActiveTimeTicks = Environment.TickCount64;
                    return sent;
                }

                // EAGAIN
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug($"EAGAIN for {_endpoint} (UDP): dropped datagram of {buf.Count} bytes");
                _metrics.IncrementEagainErr(_workerId);
                return 0;
            }

            // TCP: gathering write
            var segments = new List<ArraySegment<byte>>(buffers.Length);
            for (int i = 0; i < buffers.Length; i++)
                segments.Add(buffers[i]);

            int totalSent = _socket.Send(segments, SocketFlags.None, out SocketError err);

            if (err != SocketError.Success && err != SocketError.WouldBlock)
            {
                committedBytes = totalSent;
                _state = ConnectionState.Error;
                return -1;
            }

            if (totalSent > 0)
            {
                _lastActiveTimeTicks = Environment.TickCount64;
            }

            if (totalSent < totalSize)
            {
                // Partial write — buffer the unsent portions
                BufferRemainder(buffers, totalSent);
                return totalSize; // All accepted
            }

            return totalSent;
        }
        catch (SocketException ex)
        {
            if (ex.SocketErrorCode == SocketError.WouldBlock)
            {
                // UDP is all-or-nothing: a datagram either goes out whole or not at all.
                // Buffering it (TCP-style) would let the worker treat a dropped datagram as
                // success. Match the normal UDP EAGAIN path: drop and return 0 so the worker
                // retries on another connection.
                if (_protocol == TglogConfig.Protocol.Udp)
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                        _logger.LogDebug($"EAGAIN for {_endpoint} (UDP): dropped datagram of {buffers[0].Count} bytes");
                    _metrics.IncrementEagainErr(_workerId);
                    return 0;
                }

                int total = 0;
                for (int i = 0; i < buffers.Length; i++) total += buffers[i].Count;
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug($"EAGAIN for {_endpoint}: buffering all {total} bytes ({buffers.Length} segments)");
                BufferMultiple(buffers);
                return total;
            }
            _state = ConnectionState.Error;
            committedBytes = 0;
            return -1;
        }
        catch (ObjectDisposedException)
        {
            _state = ConnectionState.Error;
            committedBytes = 0;
            return -1;
        }
    }

    /// <summary>
    /// Attempts to flush the output buffer. Called when the socket becomes writable.
    /// </summary>
    public void TryFlush()
    {
        try
        {
            FlushOutputBuffer();
        }
        catch (SocketException ex)
        {
            _logger.LogError("Failed to flush output buffer for {Endpoint}: {Message}", _endpoint, ex.Message);
            _state = ConnectionState.Error;
        }
        catch (ObjectDisposedException)
        {
            _state = ConnectionState.Error;
        }
    }

    /// <summary>
    /// Reads from the socket to detect peer close (TCP only).
    /// Returns true if connection is still alive.
    /// </summary>
    public bool HandleRead()
    {
        if (_protocol != TglogConfig.Protocol.Tcp || _socket == null)
            return true;

        try
        {
            int read = _socket.Receive(_readBuffer, SocketFlags.None);
            if (read == 0)
            {
                // Peer closed connection
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug($"Connection closed by peer: {_endpoint}");
                _state = ConnectionState.Error;
                return false;
            }
            // Unexpected data — ignore
            return true;
        }
        catch (SocketException ex)
        {
            if (ex.SocketErrorCode == SocketError.WouldBlock)
                return true; // No data available, that's fine
            _state = ConnectionState.Error;
            return false;
        }
        catch (ObjectDisposedException)
        {
            _state = ConnectionState.Error;
            return false;
        }
    }

    /// <summary>Returns true if the connection has exceeded its lifetime (with jitter).</summary>
    public bool IsExpired()
    {
        if (_config.MaxConnectionLifetimeSec <= 0) return false; // Persistent connection
        if (_connectTimeTicks == 0) return false;
        return (Environment.TickCount64 - _connectTimeTicks) >= _lifetimeWithJitterMs;
    }

    /// <summary>
    /// Gets the pending output data for rescue to another connection.
    /// Does NOT clear the buffer (caller should use RemovePendingData for that).
    /// </summary>
    public List<BufferSegment>? GetPendingData()
    {
        if (_outputBufferSize == 0) return null;
        return new List<BufferSegment>(_outputBuffer);
    }

    /// <summary>
    /// Removes and returns all pending output data (for rescue transfer).
    /// </summary>
    public List<BufferSegment>? RemovePendingData()
    {
        if (_outputBufferSize == 0) return null;
        var data = new List<BufferSegment>(_outputBuffer);
        _outputBuffer.Clear();
        _outputBufferSize = 0;
        return data;
    }

    /// <summary>
    /// Writes rescued data from another connection into this connection's output buffer.
    /// Does NOT check high watermark (rescue is best-effort).
    /// </summary>
    public bool WritePendingData(List<BufferSegment> pendingData)
    {
        if (_state != ConnectionState.Connected || _socket == null)
            return false;

        int totalSize = 0;
        foreach (var segment in pendingData)
        {
            if (segment.Remaining > 0)
            {
                _outputBuffer.AddLast(segment);
                _outputBufferSize += segment.Remaining;
                totalSize += segment.Remaining;
            }
        }

        if (totalSize > 0)
            _logger.LogInformation("Rescued {TotalSize} bytes of pending data from another connection", totalSize);

        return true;
    }

    /// <summary>Returns the total size of pending output data in bytes.</summary>
    public int GetPendingDataSize()
    {
        return _outputBufferSize;
    }

    /// <summary>Closes the connection and releases the socket.</summary>
    public void Close()
    {
        _state = ConnectionState.Disconnected;
        CloseSocket();
        _outputBuffer.Clear();
        _outputBufferSize = 0;

        _logger.LogInformation("Closed connection to {Endpoint}", _endpoint);
    }

    // --- Private Methods ---

    private void FlushOutputBuffer()
    {
        if (_socket == null) return;

        while (_outputBuffer.Count > 0)
        {
            var node = _outputBuffer.First!;
            var segment = node.Value;

            if (segment.Remaining <= 0)
            {
                _outputBuffer.RemoveFirst();
                continue;
            }

            int sent = _socket.Send(segment.Data, segment.Offset, segment.Remaining,
                SocketFlags.None, out SocketError error);

            if (error != SocketError.Success && error != SocketError.WouldBlock)
            {
                _state = ConnectionState.Error;
                return;
            }

            if (sent > 0)
            {
                _lastActiveTimeTicks = Environment.TickCount64;
                _outputBufferSize -= sent;
                segment.Advance(sent);
                node.Value = segment; // Update struct in node
            }

            if (segment.Remaining > 0)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                    _logger.LogDebug($"EAGAIN during flush: socket buffer full for {_endpoint}");
                break; // Socket buffer full, try again later
            }
            else
            {
                _outputBuffer.RemoveFirst();
            }
        }
    }

    private void BufferData(ArraySegment<byte> data)
    {
        if (data.Count <= 0) return;
        byte[] copy = new byte[data.Count];
        Buffer.BlockCopy(data.Array!, data.Offset, copy, 0, data.Count);
        _outputBuffer.AddLast(new BufferSegment(copy, 0, copy.Length));
        _outputBufferSize += copy.Length;
    }

    private void BufferMultiple(ArraySegment<byte>[] buffers)
    {
        for (int i = 0; i < buffers.Length; i++)
        {
            if (buffers[i].Count > 0)
                BufferData(buffers[i]);
        }
    }

    private void BufferRemainder(ArraySegment<byte>[] buffers, int totalSent)
    {
        long remaining = totalSent;
        for (int i = 0; i < buffers.Length; i++)
        {
            if (remaining >= buffers[i].Count)
            {
                remaining -= buffers[i].Count;
                continue;
            }

            // This buffer was partially or not sent
            int unsent = buffers[i].Count - (int)remaining;
            int offset = buffers[i].Offset + (int)remaining;
            byte[] copy = new byte[unsent];
            Buffer.BlockCopy(buffers[i].Array!, offset, copy, 0, unsent);
            _outputBuffer.AddLast(new BufferSegment(copy, 0, unsent));
            _outputBufferSize += unsent;
            remaining = 0;

            // Buffer remaining complete buffers
            for (int j = i + 1; j < buffers.Length; j++)
            {
                if (buffers[j].Count > 0)
                    BufferData(buffers[j]);
            }
            break;
        }
    }

    /// <summary>
    /// Calculates connection lifetime with random jitter to prevent thundering herd.
    /// Jitter range: [0, clamp(base/10, 20, 30)] seconds.
    /// </summary>
    private long CalculateLifetimeWithJitter()
    {
        int maxLifetimeSec = _config.MaxConnectionLifetimeSec;
        if (maxLifetimeSec <= 0) return long.MaxValue;

        int maxJitterSec = Math.Clamp(maxLifetimeSec / 10, 20, 30);
        int jitterSec = Random.Shared.Next(maxJitterSec);
        long totalMs = (maxLifetimeSec + jitterSec) * 1000L;

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug($"Connection lifetime for {_endpoint}: base={maxLifetimeSec}s, jitter={jitterSec}s, total={totalMs}ms");

        return totalMs;
    }

    private void ConfigureSocketBuffers(Socket socket)
    {
        try
        {
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendBuffer,
                _config.SocketSendBufferSize);
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveBuffer,
                _config.SocketRecvBufferSize);
        }
        catch (SocketException ex)
        {
            _logger.LogWarning("Failed to set socket buffer sizes: {Message}", ex.Message);
        }
    }

    private void CloseSocket()
    {
        try
        {
            _socket?.Close();
        }
        catch { /* ignore */ }
        _socket = null;
    }
}
