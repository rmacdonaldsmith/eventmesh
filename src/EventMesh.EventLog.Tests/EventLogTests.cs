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

    [Fact]
    public async Task AppendAsync_WithNullRecord_ShouldThrowArgumentNullException()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => eventLog.AppendAsync(null!));
    }

    [Fact]
    public async Task ReadFromAsync_WithNegativeOffset_ShouldThrowArgumentException()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => eventLog.ReadFromAsync(-1, 10));
    }

    [Fact]
    public async Task ReadFromAsync_WithNegativeMaxCount_ShouldThrowArgumentException()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => eventLog.ReadFromAsync(0, -1));
    }

    [Fact]
    public async Task ReadFromAsync_WithZeroMaxCount_ShouldReturnEmpty()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        await eventLog.AppendAsync(new EventRecord { Payload = "test"u8.ToArray(), Topic = "test" });

        // Act
        var results = await eventLog.ReadFromAsync(0, 0);

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public async Task ReplayAsync_WithNegativeOffset_ShouldThrowArgumentException()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await foreach (var _ in eventLog.ReplayAsync(-1))
            {
                // Should not reach here
            }
        });
    }

    [Fact]
    public async Task AppendAsync_WithEmptyTopic_ShouldWork()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        var record = new EventRecord
        {
            Payload = "test"u8.ToArray(),
            Topic = string.Empty
        };

        // Act
        var result = await eventLog.AppendAsync(record);

        // Assert
        Assert.Equal(0L, result.Offset);
        Assert.Equal(string.Empty, result.Topic);
    }

    [Fact]
    public async Task AppendAsync_WithEmptyPayload_ShouldWork()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        var record = new EventRecord
        {
            Payload = Array.Empty<byte>(),
            Topic = "test-topic"
        };

        // Act
        var result = await eventLog.AppendAsync(record);

        // Assert
        Assert.Equal(0L, result.Offset);
        Assert.Equal("test-topic", result.Topic);
        Assert.Empty(result.Payload);
    }

    [Fact]
    public async Task Operations_AfterDispose_ShouldStillWork()
    {
        // Note: For InMemoryEventLog, operations after dispose should still work
        // but return empty/default results since we cleared the data

        // Arrange
        var eventLog = new InMemoryEventLog();
        await eventLog.AppendAsync(new EventRecord { Payload = "test"u8.ToArray(), Topic = "test" });

        // Act
        eventLog.Dispose();

        // Assert - Should work but return empty results
        var endOffset = await eventLog.GetEndOffsetAsync();
        Assert.Equal(0L, endOffset);

        var readResults = await eventLog.ReadFromAsync(0, 10);
        Assert.Empty(readResults);

        var replayResults = new List<IEventRecord>();
        await foreach (var record in eventLog.ReplayAsync(0))
        {
            replayResults.Add(record);
        }
        Assert.Empty(replayResults);

        // Should be able to append new events after dispose
        var newRecord = await eventLog.AppendAsync(new EventRecord { Payload = "new"u8.ToArray(), Topic = "new" });
        Assert.Equal(0L, newRecord.Offset); // Starts fresh
    }

    [Fact]
    public async Task ConcurrentAppends_ShouldHaveUniqueOffsets()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        const int taskCount = 10;

        // Act - Run concurrent appends
        var tasks = Enumerable.Range(0, taskCount)
            .Select(i => eventLog.AppendAsync(new EventRecord
            {
                Payload = System.Text.Encoding.UTF8.GetBytes($"event{i}"),
                Topic = $"topic{i}"
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All offsets should be unique
        var offsets = results.Select(r => r.Offset).ToList();
        Assert.Equal(taskCount, offsets.Count);
        Assert.Equal(taskCount, offsets.Distinct().Count()); // No duplicates
        Assert.True(offsets.All(o => o >= 0 && o < taskCount)); // All within expected range
    }

    [Fact]
    public async Task ConcurrentReads_ShouldReturnConsistentResults()
    {
        // Arrange
        using var eventLog = new InMemoryEventLog();
        await eventLog.AppendAsync(new EventRecord { Payload = "event1"u8.ToArray(), Topic = "topic1" });
        await eventLog.AppendAsync(new EventRecord { Payload = "event2"u8.ToArray(), Topic = "topic2" });
        await eventLog.AppendAsync(new EventRecord { Payload = "event3"u8.ToArray(), Topic = "topic3" });

        // Act - Run concurrent reads
        var readTasks = Enumerable.Range(0, 5)
            .Select(_ => eventLog.ReadFromAsync(0, 10))
            .ToArray();

        var results = await Task.WhenAll(readTasks);

        // Assert - All reads should return the same data
        Assert.All(results, result => Assert.Equal(3, result.Count));
        var firstResult = results[0];
        foreach (var result in results.Skip(1))
        {
            Assert.Equal(firstResult.Count, result.Count);
            for (int i = 0; i < firstResult.Count; i++)
            {
                Assert.Equal(firstResult[i].Offset, result[i].Offset);
                Assert.Equal(firstResult[i].Topic, result[i].Topic);
                Assert.Equal(firstResult[i].Payload, result[i].Payload);
            }
        }
    }
}
