using System;
using Akka;
using Akka.Actor;
using Akka.Event;
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
        private readonly ILoggingAdapter _log = Logging.GetLogger(Context);

        private record CompleteMessage;
        private record KafkaMessage(string Key, string Value);
        public KafkaSenderActor(string topic)
        {

            var config = new ProducerConfig {
                BootstrapServers = "127.0.0.1:9092",
                Acks = Acks.All,
            };

            var p = new ProducerBuilder<string, string>(config).Build();

            ReceiveAsync<string>(async m =>
            {
                _log.Info(m);
                try
                {
                    var ret = await p.ProduceAsync(topic, new Message<string, string>
                    {
                        Key = "1",
                        Value = m
                    });
                }
                catch (Exception ex)
                {
                    _log.Warning(ex, "");
                }
            });
        }
    }
}
