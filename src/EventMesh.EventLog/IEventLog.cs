using EventMesh.MessageContracts;

namespace EventMesh.EventLog;

public interface IEventLog : IDisposable
{
    /// <summary>
    /// Appends a new event to the log.
    /// The Offset will be assigned by the log and set on the returned record.
    /// </summary>
    Task<IEventRecord> AppendAsync(IEventRecord record, CancellationToken ct = default);

    /// <summary>
    /// Reads events starting at a given offset, up to a max count.
    /// </summary>
    Task<IReadOnlyList<IEventRecord>> ReadFromAsync(long startOffset, int maxCount, CancellationToken ct = default);

    /// <summary>
    /// Gets the current end offset (next append position).
    /// </summary>
    Task<long> GetEndOffsetAsync(CancellationToken ct = default);

    /// <summary>
    /// Replays events from a given offset via an async stream.
    /// </summary>
    IAsyncEnumerable<IEventRecord> ReplayAsync(long startOffset, CancellationToken ct = default);

    /// <summary>
    /// Performs log compaction or cleanup (future use).
    /// </summary>
    Task CompactAsync(CancellationToken ct = default);
}