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
            var producerSettings = ProducerSettings<Null, string>.Create(Context.System, null, Serializers.Utf8)
                                                                 .WithBootstrapServers("127.0.0.1:9092");

            var source = Source.ActorRef<object>(5000, OverflowStrategy.DropTail);

            var graph = GraphDsl.Create(source, (builder, start) =>
            {
                var sink = builder.Add(KafkaProducer.PlainSink(producerSettings));
                var flow = builder.Add(from msg in Flow.Create<object>()
                                       select new ProducerRecord<Null, string>(topic, msg.ToString()));

                builder.From(start)
                       .Via(flow)
                       .To(sink);

                return ClosedShape.Instance;
            });

            var sourceActor = Context.Materializer().Materialize(graph);

            Receive<string>(m => sourceActor.Forward(m));
        }
    }
}
