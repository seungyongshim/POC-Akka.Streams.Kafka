using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;

namespace Common
{
    public class KafkaConsumerActor : ReceiveActor
    {
        public KafkaConsumerActor(string topic, string groupId, IActorRef parserActor)
        {
            var consumerSettings = ConsumerSettings<string, string>.Create(Context.System, Deserializers.Utf8, Deserializers.Utf8)
                                                                   .WithBootstrapServers("127.0.0.1:9092")
                                                                   .WithGroupId(groupId)
                                                                   .WithProperty("auto.offset.reset", "earliest");

            var source = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Topics(topic));

            var graph = GraphDsl.Create(source, (builder, start) =>
            {
                var flow = builder.Add(from result in Flow.Create<CommittableMessage<string, string>>()
                                       let commit = result.CommitableOffset
                                       let value = result.Record.Message.Value
                                       select (value, commit));

                var sink = builder.Add(Sink.ActorRef<(string, ICommittableOffset)>(Self, new CompleteMessage()));

                builder.From(start)
                       .Via(flow)
                       .To(sink);

                return ClosedShape.Instance;
            });

            Context.Materializer().Materialize(graph);

            Receive<(string Value, ICommittableOffset Commit)>(m =>
            {
                parserActor.Forward(m.Value);
            });
        }

        private record CompleteMessage;
    }
}
