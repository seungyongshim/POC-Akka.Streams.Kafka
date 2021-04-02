using System;
using System.Collections.Immutable;

namespace Core.Messages
{
    public record KafkaHeader(string Key, byte[] Value);
    public record KafkaMessage(ImmutableList<KafkaHeader> Headers, byte[] Value, Action Commit);
}
