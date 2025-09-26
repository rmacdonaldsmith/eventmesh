using EventMesh.MessageContracts;
using EventMesh.Messages;

namespace EventMesh.EventLog;

public class InMemoryEventLog : IEventLog
{
    private readonly List<EventRecord> _events = new();
    private long _nextOffset = 0;

    public Task<IEventRecord> AppendAsync(IEventRecord record, CancellationToken ct = default)
    {
        var eventRecord = record as EventRecord ?? new EventRecord
        {
            Topic = record.Topic,
            Payload = record.Payload,
            Timestamp = record.Timestamp,
            Headers = record.Headers
        };

        var storedRecord = eventRecord with { Offset = _nextOffset++ };
        _events.Add(storedRecord);
        return Task.FromResult<IEventRecord>(storedRecord);
    }

    public Task<IReadOnlyList<IEventRecord>> ReadFromAsync(long startOffset, int maxCount, CancellationToken ct = default)
    {
        var results = _events
            .Where(e => e.Offset >= startOffset)
            .Take(maxCount)
            .Cast<IEventRecord>()
            .ToList();
        return Task.FromResult<IReadOnlyList<IEventRecord>>(results);
    }

    public Task<long> GetEndOffsetAsync(CancellationToken ct = default)
    {
        return Task.FromResult(_nextOffset);
    }

    public async IAsyncEnumerable<IEventRecord> ReplayAsync(long startOffset, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var events = _events.Where(e => e.Offset >= startOffset);
        foreach (var eventRecord in events)
        {
            if (ct.IsCancellationRequested)
                yield break;

            yield return eventRecord;
        }
    }

    public Task CompactAsync(CancellationToken ct = default)
    {
        // No-op for in-memory implementation
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _events.Clear();
    }
}