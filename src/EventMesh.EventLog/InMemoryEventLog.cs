using EventMesh.MessageContracts;
using EventMesh.Messages;

namespace EventMesh.EventLog;

public class InMemoryEventLog : IEventLog
{
    private readonly List<EventRecord> _events = new();
    private long _nextOffset = 0;

    public Task<IEventRecord> AppendAsync(IEventRecord record, CancellationToken ct = default)
    {
        if (record == null)
            throw new ArgumentNullException(nameof(record));

        var eventRecord = record as EventRecord ?? new EventRecord
        {
            Topic = record.Topic,
            Payload = record.Payload,
            Timestamp = record.Timestamp,
            Headers = record.Headers ?? new Dictionary<string, string>()
        };

        // Ensure headers are never null
        if (eventRecord.Headers == null)
        {
            eventRecord = eventRecord with { Headers = new Dictionary<string, string>() };
        }

        var storedRecord = eventRecord with { Offset = _nextOffset++ };
        _events.Add(storedRecord);
        return Task.FromResult<IEventRecord>(storedRecord);
    }

    public Task<IReadOnlyList<IEventRecord>> ReadFromAsync(long startOffset, int maxCount, CancellationToken ct = default)
    {
        if (startOffset < 0)
            throw new ArgumentException("Start offset cannot be negative", nameof(startOffset));
        if (maxCount < 0)
            throw new ArgumentException("Max count cannot be negative", nameof(maxCount));

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
        if (startOffset < 0)
            throw new ArgumentException("Start offset cannot be negative", nameof(startOffset));

        var events = _events.Where(e => e.Offset >= startOffset);
        foreach (var eventRecord in events)
        {
            if (ct.IsCancellationRequested)
                yield break;

            yield return eventRecord;
            await Task.Yield(); // Make it properly async to avoid warning
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
        _nextOffset = 0;
    }
}