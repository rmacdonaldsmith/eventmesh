using EventMesh.MessageContracts;
using EventMesh.Messages;

namespace EventMesh.EventLog.Tests;

public class EventLogTests
{
    [Fact]
    public async Task AppendAsync_ShouldReturnSequentialOffsets()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        var eventData = "test event"u8.ToArray();
        IEventRecord record1 = new EventRecord { Payload = eventData, Topic = "test-topic" };
        IEventRecord record2 = new EventRecord { Payload = eventData, Topic = "test-topic" };

        // Act
        var result1 = await eventLog.AppendAsync(record1);
        var result2 = await eventLog.AppendAsync(record2);

        // Assert
        Assert.Equal(0L, result1.Offset);
        Assert.Equal(1L, result2.Offset);
        Assert.Equal(eventData, result1.Payload);
        Assert.Equal(eventData, result2.Payload);
        Assert.Equal("test-topic", result1.Topic);
        Assert.Equal("test-topic", result2.Topic);
    }

    [Fact]
    public async Task ReadFromAsync_ShouldReturnEventsFromOffset()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        var record1 = new EventRecord { Payload = "event1"u8.ToArray(), Topic = "topic1" };
        var record2 = new EventRecord { Payload = "event2"u8.ToArray(), Topic = "topic2" };
        var record3 = new EventRecord { Payload = "event3"u8.ToArray(), Topic = "topic3" };

        await eventLog.AppendAsync(record1);
        await eventLog.AppendAsync(record2);
        await eventLog.AppendAsync(record3);

        // Act
        var results = await eventLog.ReadFromAsync(1, 10);

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal(1L, results[0].Offset);
        Assert.Equal(2L, results[1].Offset);
        Assert.Equal("topic2", results[0].Topic);
        Assert.Equal("topic3", results[1].Topic);
    }

    [Fact]
    public async Task ReadFromAsync_ShouldRespectMaxCount()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        await eventLog.AppendAsync(new EventRecord { Payload = "event1"u8.ToArray(), Topic = "topic1" });
        await eventLog.AppendAsync(new EventRecord { Payload = "event2"u8.ToArray(), Topic = "topic2" });
        await eventLog.AppendAsync(new EventRecord { Payload = "event3"u8.ToArray(), Topic = "topic3" });

        // Act
        var results = await eventLog.ReadFromAsync(0, 2);

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Equal(0L, results[0].Offset);
        Assert.Equal(1L, results[1].Offset);
    }

    [Fact]
    public async Task ReadFromAsync_ShouldReturnEmptyWhenOffsetTooHigh()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        await eventLog.AppendAsync(new EventRecord { Payload = "event1"u8.ToArray(), Topic = "topic1" });

        // Act
        var results = await eventLog.ReadFromAsync(10, 5);

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public async Task GetEndOffsetAsync_ShouldReturnZeroForEmptyLog()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();

        // Act
        var endOffset = await eventLog.GetEndOffsetAsync();

        // Assert
        Assert.Equal(0L, endOffset);
    }

    [Fact]
    public async Task GetEndOffsetAsync_ShouldReturnNextAppendPosition()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        await eventLog.AppendAsync(new EventRecord { Payload = "event1"u8.ToArray(), Topic = "topic1" });
        await eventLog.AppendAsync(new EventRecord { Payload = "event2"u8.ToArray(), Topic = "topic2" });

        // Act
        var endOffset = await eventLog.GetEndOffsetAsync();

        // Assert
        Assert.Equal(2L, endOffset); // Next append position after offsets 0 and 1
    }

    [Fact]
    public async Task ReplayAsync_ShouldReturnAllEventsFromOffset()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        await eventLog.AppendAsync(new EventRecord { Payload = "event1"u8.ToArray(), Topic = "topic1" });
        await eventLog.AppendAsync(new EventRecord { Payload = "event2"u8.ToArray(), Topic = "topic2" });
        await eventLog.AppendAsync(new EventRecord { Payload = "event3"u8.ToArray(), Topic = "topic3" });

        // Act
        var replayedEvents = new List<IEventRecord>();
        await foreach (var eventRecord in eventLog.ReplayAsync(1))
        {
            replayedEvents.Add(eventRecord);
        }

        // Assert
        Assert.Equal(2, replayedEvents.Count);
        Assert.Equal(1L, replayedEvents[0].Offset);
        Assert.Equal(2L, replayedEvents[1].Offset);
        Assert.Equal("topic2", replayedEvents[0].Topic);
        Assert.Equal("topic3", replayedEvents[1].Topic);
    }

    [Fact]
    public async Task ReplayAsync_ShouldReturnEmptyWhenOffsetTooHigh()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        await eventLog.AppendAsync(new EventRecord { Payload = "event1"u8.ToArray(), Topic = "topic1" });

        // Act
        var replayedEvents = new List<IEventRecord>();
        await foreach (var eventRecord in eventLog.ReplayAsync(10))
        {
            replayedEvents.Add(eventRecord);
        }

        // Assert
        Assert.Empty(replayedEvents);
    }

    [Fact]
    public async Task ReplayAsync_ShouldSupportCancellation()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        await eventLog.AppendAsync(new EventRecord { Payload = "event1"u8.ToArray(), Topic = "topic1" });
        await eventLog.AppendAsync(new EventRecord { Payload = "event2"u8.ToArray(), Topic = "topic2" });

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act & Assert
        var replayedEvents = new List<IEventRecord>();
        await foreach (var eventRecord in eventLog.ReplayAsync(0, cts.Token))
        {
            replayedEvents.Add(eventRecord);
        }

        // Should not get any events due to cancellation
        Assert.Empty(replayedEvents);
    }

    [Fact]
    public async Task CompactAsync_ShouldCompleteSuccessfully()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        await eventLog.AppendAsync(new EventRecord { Payload = "event1"u8.ToArray(), Topic = "topic1" });

        // Act & Assert - Should not throw
        await eventLog.CompactAsync();

        // Verify log still works after compaction
        var endOffset = await eventLog.GetEndOffsetAsync();
        Assert.Equal(1L, endOffset);
    }

    [Fact]
    public async Task Dispose_ShouldClearEvents()
    {
        // Arrange
        var eventLog = new InMemoryEventLog();
        await eventLog.AppendAsync(new EventRecord { Payload = "event1"u8.ToArray(), Topic = "topic1" });

        // Verify we have events before disposal
        var endOffsetBefore = await eventLog.GetEndOffsetAsync();
        Assert.Equal(1L, endOffsetBefore);

        // Act
        eventLog.Dispose();

        // Assert - After disposal, should act like empty log
        var endOffsetAfter = await eventLog.GetEndOffsetAsync();
        Assert.Equal(0L, endOffsetAfter);

        var results = await eventLog.ReadFromAsync(0, 10);
        Assert.Empty(results);
    }

    [Fact]
    public void Dispose_ShouldBeIdempotent()
    {
        // Arrange
        var eventLog = new InMemoryEventLog();

        // Act & Assert - Multiple disposals should not throw
        eventLog.Dispose();
        eventLog.Dispose();
        eventLog.Dispose();
    }

    [Fact]
    public async Task AppendAsync_WithNullHeaders_ShouldWork()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        var record = new EventRecord
        {
            Payload = "test"u8.ToArray(),
            Topic = "test-topic",
            Headers = null! // Test null handling
        };

        // Act
        var result = await eventLog.AppendAsync(record);

        // Assert
        Assert.Equal(0L, result.Offset);
        Assert.Equal("test-topic", result.Topic);
        Assert.NotNull(result.Headers); // Should be converted to empty dictionary
    }
}
