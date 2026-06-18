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
/// This implementation uses the Disruptor-style per-slot sequence pattern so that
/// the consumer never has to guess whether a slot is "empty" or merely "claimed
/// but not yet filled". Each slot carries a sequence number that the producer
/// publishes (with release semantics) only AFTER the item has been stored. The
/// consumer reads the sequence (with acquire semantics) and only consumes a slot
/// whose sequence matches the expected value.
///
/// Why the previous CAS-claim-then-write design was unsafe:
/// - A producer would CAS _writeIndex first and write the item afterwards. That
///   left a window where _writeIndex had advanced but the slot was still null.
/// - The consumer used "slot == null" to decide the queue was empty, which is
///   indistinguishable from a slot that is genuinely empty. A single slow
///   producer could therefore stall delivery of every message behind it
///   (head-of-line blocking), and the size/full checks relied on cross-thread
///   index visibility that does not hold without the sequence handshake.
///
/// Cache-line padding prevents false sharing between _writeIndex and _readIndex.
/// </summary>
internal sealed class LockFreeQueue<T> where T : class
{
    private readonly int _capacity;
    private readonly int _mask;
    private readonly T?[] _buffer;

    // Per-slot publication sequence. A slot at array index i is "ready to be
    // consumed for ticket number n" iff _sequence[i] == n + 1.
    // Initialized so that slot i is ready to be CLAIMED (written) for ticket i.
    private readonly long[] _sequence;

    // Cache-line padded indices to prevent false sharing.
    // Producers CAS on _writeIndex; the single consumer writes _readIndex.
    // Without padding, these would share a cache line, causing constant invalidation.
    private PaddedLong _writeIndex;
    private PaddedLong _readIndex;

    /// <summary>
    /// Creates a new lock-free queue with the given capacity.
    /// Capacity is rounded up to the next power of 2 (minimum 2) for efficient modulo via bitwise AND.
    /// </summary>
    public LockFreeQueue(int capacity)
    {
        if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be >= 1");

        // Round up to next power of 2 (minimum 2)
        _capacity = NextPowerOf2(capacity);
        _mask = _capacity - 1;
        _buffer = new T?[_capacity];
        _sequence = new long[_capacity];
        // Slot i starts available for the producer holding ticket i.
        for (int i = 0; i < _capacity; i++)
            _sequence[i] = i;
        _writeIndex = new PaddedLong { Value = 0 };
        _readIndex = new PaddedLong { Value = 0 };
    }

    /// <summary>
    /// Non-blocking enqueue. Called by producer threads (multiple threads safe).
    /// Returns true if the item was enqueued, false if the queue is full.
    /// </summary>
    public bool Offer(T item)
    {
        if (item == null) throw new ArgumentNullException(nameof(item));

        while (true)
        {
            long ticket = Volatile.Read(ref _writeIndex.Value);
            int index = (int)(ticket & _mask);

            // A slot is writable for this ticket only when its published
            // sequence equals the ticket value. Volatile.Read provides
            // acquire semantics.
            long seq = Volatile.Read(ref _sequence[index]);
            long diff = seq - ticket;

            if (diff == 0)
            {
                // Slot is free for us; try to claim the ticket.
                if (Interlocked.CompareExchange(ref _writeIndex.Value, ticket + 1, ticket) == ticket)
                {
                    // We own this slot. Store the item, then publish the slot by
                    // bumping its sequence to ticket + 1. The Volatile.Write below
                    // has release semantics, so the consumer that observes the new
                    // sequence is guaranteed to also observe the item.
                    Volatile.Write(ref _buffer[index], item);
                    Volatile.Write(ref _sequence[index], ticket + 1);
                    return true;
                }
                // Lost the race; retry with a fresh ticket.
            }
            else if (diff < 0)
            {
                // The slot has not been consumed yet for the previous lap, i.e.
                // sequence < ticket means the consumer is still behind: full.
                return false;
            }
            // diff > 0: another producer already advanced past this ticket;
            // retry to pick up the latest _writeIndex.
        }
    }

    /// <summary>
    /// Non-blocking dequeue. Called by the single consumer thread only.
    /// Returns the item, or null if the queue is empty or the item hasn't been published yet.
    /// </summary>
    public T? Poll()
    {
        long ticket = Volatile.Read(ref _readIndex.Value);
        int index = (int)(ticket & _mask);

        // The slot is ready to be consumed only once its sequence has been
        // published to ticket + 1 by the producer. Any other value means the
        // slot is empty or not yet filled -> treat as empty.
        long seq = Volatile.Read(ref _sequence[index]);
        if (seq - (ticket + 1) != 0)
        {
            return null;
        }

        T? item = Volatile.Read(ref _buffer[index]);
        Volatile.Write(ref _buffer[index], null);

        // Mark the slot free for the producer that will use it one lap later
        // (ticket + capacity). Release semantics publish the item-cleared state.
        Volatile.Write(ref _sequence[index], ticket + _capacity);

        // Single consumer, so a plain volatile write on _readIndex is enough;
        // producers gate exclusively on per-slot sequence, not on _readIndex.
        Volatile.Write(ref _readIndex.Value, ticket + 1);
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

    /// <summary>
    /// Round up to the next power of 2, with a minimum capacity of 2.
    /// A capacity of 1 is rejected because the per-slot sequence state would
    /// overlap (ticket and ticket+capacity collapse onto the same slot in a way
    /// that can let a producer overwrite an unconsumed element).
    /// </summary>
    private static int NextPowerOf2(int value)
    {
        if (value <= 2) return 2;
        if (value > (1 << 30))
            throw new ArgumentOutOfRangeException(nameof(value), "Capacity too large");

        value--;
        value |= value >> 1;
        value |= value >> 2;
        value |= value >> 4;
        value |= value >> 8;
        value |= value >> 16;
        return value + 1;
    }
}
