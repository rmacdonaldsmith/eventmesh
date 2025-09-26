using EventMesh.MessageContracts;

namespace EventMesh.Messages;

public record EventRecord : IEventRecord
{
    public long Offset { get; init; }
    public string Topic { get; init; } = string.Empty;
    public byte[] Payload { get; init; } = Array.Empty<byte>();
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public IDictionary<string, string> Headers { get; init; } = new Dictionary<string, string>();
}