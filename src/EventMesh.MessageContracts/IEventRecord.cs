namespace EventMesh.MessageContracts;

public interface IEventRecord
{
    long Offset { get; }
    string Topic { get; }
    byte[] Payload { get; }
    DateTime Timestamp { get; }
    IDictionary<string, string> Headers { get; }
}