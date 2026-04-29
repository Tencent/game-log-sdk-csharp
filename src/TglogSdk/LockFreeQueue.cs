using System.Runtime.InteropServices;

namespace Tencent.Tglog;

/// <summary>
/// Padded to 128 bytes to occupy a full cache line on both x86 (64B) and ARM (128B).
/// Defined outside the generic class to avoid .NET restriction:
/// "generic types cannot have explicit layout".
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 128)]
internal struct PaddedLong
{
    [FieldOffset(0)]
    public long Value;
}

/// <summary>
/// High-performance, lock-free, bounded MPSC (Multi-Producer Single-Consumer) queue.
///
/// Thread safety:
/// - Multiple threads may call <see cref="Offer"/> concurrently (producers).
/// - Only ONE thread may call <see cref="Poll"/> (the consumer/worker thread).
///
/// Implementation uses a power-of-2 ring buffer with atomic CAS for producers
/// and simple volatile writes for the single consumer. Cache-line padding prevents
/// false sharing between producer and consumer indices.
/// </summary>
internal sealed class LockFreeQueue<T> where T : class
{
    private readonly int _capacity;
    private readonly int _mask;
    private readonly T?[] _buffer;

    // Cache-line padded indices to prevent false sharing.
    // Producers CAS on _writeIndex; the single consumer writes _readIndex.
    // Without padding, these would share a cache line, causing constant invalidation.
    private PaddedLong _writeIndex;
    private PaddedLong _readIndex;

    /// <summary>
    /// Creates a new lock-free queue with the given capacity.
    /// Capacity is rounded up to the next power of 2 for efficient modulo via bitwise AND.
    /// </summary>
    public LockFreeQueue(int capacity)
    {
        if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be >= 1");

        // Round up to next power of 2
        _capacity = NextPowerOf2(capacity);
        _mask = _capacity - 1;
        _buffer = new T?[_capacity];
        _writeIndex = new PaddedLong { Value = 0 };
        _readIndex = new PaddedLong { Value = 0 };
    }

    /// <summary>
    /// Non-blocking enqueue. Called by producer threads (multiple threads safe).
    /// Returns true if the item was enqueued, false if the queue is full.
    /// </summary>
    public bool Offer(T item)
    {
        long currentWrite, nextWrite;
        do
        {
            currentWrite = Volatile.Read(ref _writeIndex.Value);
            nextWrite = currentWrite + 1;

            // Check if queue is full
            long currentRead = Volatile.Read(ref _readIndex.Value);
            if (nextWrite - currentRead > _capacity)
            {
                return false; // Queue full
            }
        }
        while (Interlocked.CompareExchange(ref _writeIndex.Value, nextWrite, currentWrite) != currentWrite);

        // Successfully claimed slot — write the item
        int index = (int)(currentWrite & _mask);
        Volatile.Write(ref _buffer[index], item);
        return true;
    }

    /// <summary>
    /// Non-blocking dequeue. Called by the single consumer thread only.
    /// Returns the item, or null if the queue is empty or the item hasn't been written yet.
    /// </summary>
    public T? Poll()
    {
        long currentRead = Volatile.Read(ref _readIndex.Value);
        long currentWrite = Volatile.Read(ref _writeIndex.Value);

        if (currentRead >= currentWrite)
        {
            return null; // Empty
        }

        int index = (int)(currentRead & _mask);
        T? item = Volatile.Read(ref _buffer[index]);

        if (item == null)
        {
            return null; // Producer claimed slot but hasn't written yet
        }

        // Clear slot and advance read index
        Volatile.Write(ref _buffer[index], null);
        // Use Volatile.Write (release fence) instead of Interlocked — single consumer doesn't need CAS
        Volatile.Write(ref _readIndex.Value, currentRead + 1);
        return item;
    }

    /// <summary>Current number of items in the queue (approximate).</summary>
    public int Size
    {
        get
        {
            long write = Volatile.Read(ref _writeIndex.Value);
            long read = Volatile.Read(ref _readIndex.Value);
            return (int)Math.Max(0, write - read);
        }
    }

    /// <summary>True if the queue appears empty (approximate).</summary>
    public bool IsEmpty
    {
        get
        {
            long read = Volatile.Read(ref _readIndex.Value);
            long write = Volatile.Read(ref _writeIndex.Value);
            return read >= write;
        }
    }

    /// <summary>True if the queue appears full (approximate).</summary>
    public bool IsFull
    {
        get
        {
            long write = Volatile.Read(ref _writeIndex.Value);
            long read = Volatile.Read(ref _readIndex.Value);
            return (write - read) >= _capacity;
        }
    }

    /// <summary>The actual capacity (power of 2).</summary>
    public int Capacity => _capacity;

    private static int NextPowerOf2(int value)
    {
        if (value <= 0) return 1;
        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value + 1;
    }
}
