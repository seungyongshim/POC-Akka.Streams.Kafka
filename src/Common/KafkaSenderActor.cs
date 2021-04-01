using System;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;

namespace Common
{
    public class KafkaSenderActor : ReceiveActor
    {
        public KafkaSenderActor(string topic)
        {


            var producerSettings = ProducerSettings<string, string>.Create(Context.System, Serializers.Utf8, Serializers.Utf8)
                                                                   .WithBootstrapServers("127.0.0.1:9092");

            var source = Source.ActorRef<(string, object)>(5000, OverflowStrategy.DropTail);

            var graph = GraphDsl.Create(source, (builder, start) =>
            {
                var sink = builder.Add(RestartSink.WithBackoff(() => KafkaProducer.PlainSink(producerSettings),
                                                               TimeSpan.FromSeconds(1),
                                                               TimeSpan.FromSeconds(10),
                                                               0.1));

                var flow = builder.Add(Flow.Create<(string Key, object Value)>()
                                           .Select(msg => new ProducerRecord<string, string>(topic, msg.Key, msg.Value.ToString()))
                                           );

                builder.From(start)
                       .Via(flow)
                       .To(sink);

                return ClosedShape.Instance;
            });

            var sourceActor = Context.Materializer().Materialize(graph);

            Receive<string>(m => sourceActor.Forward((string.Empty, (object)m)));
        }
    }
}
