using System;
using System.Collections.Immutable;

namespace SeungYongShim.Kafka.DependencyInjection.Internal
{
    public record KafkaHeader(string Key, byte[] Value);
    public record KafkaMessage(ImmutableList<KafkaHeader> Headers, byte[] Value, Action Commit);
}
