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
}
