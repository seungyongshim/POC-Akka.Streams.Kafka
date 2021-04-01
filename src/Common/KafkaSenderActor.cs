using System;
using Akka;
using Akka.Actor;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Util;
using Confluent.Kafka;

namespace Common
{
    public class KafkaSenderActor : ReceiveActor
    {
        private readonly ILoggingAdapter _log = Logging.GetLogger(Context);

        private record CompleteMessage;
        private record KafkaMessage(string Key, string Value);
        public KafkaSenderActor(string topic)
        {
            

            var producerSettings = ProducerSettings<string, string>.Create(Context.System, Serializers.Utf8, Serializers.Utf8)
                                                                   .WithBootstrapServers("127.0.0.1:9092");


            var source = Source.Queue<KafkaMessage>(0, OverflowStrategy.Backpressure)
                               .Throttle(1, TimeSpan.FromSeconds(100), 1, ThrottleMode.Shaping);

            var graph = GraphDsl.Create(source, (builder, start) =>
            {
                var sink = builder.Add(Sink.Ignore<bool>());


                var flow = builder.Add(Flow.Create<KafkaMessage>()
                                           .Select(msg => new ProducerRecord<string, string>(topic, msg.Key, msg.Value))
                                           .Select(ProducerMessage.Single)
                                           .Log("here1", log: _log)
                                           .Via(RestartFlow.WithBackoff(() => KafkaProducer.FlexiFlow<string, string, NotUsed>(producerSettings),
                                                                        TimeSpan.FromSeconds(1),
                                                                        TimeSpan.FromSeconds(10),
                                                                        0.2))
                                           .Log("here2")
                                           .Select(m => true));

                builder.From(start)
                       .Via(flow)
                        
                       .To(sink);

                return ClosedShape.Instance;
            });

            var sourceQueue = Context.Materializer(ActorMaterializerSettings.Create(Context.System)
                                                                            .WithSyncProcessingLimit(1))
                                     .Materialize(graph);

            //Self.Tell(string.Empty);

            ReceiveAsync<string>(async m =>
            {
                try
                {
                    var ret = await sourceQueue.OfferAsync(new KafkaMessage(null, m));
                    _log.Info(ret.ToString());
                }
                catch (TimeoutException ex)
                {
                    _log.Warning(ex, "");
                }
            });
        }
    }
}
