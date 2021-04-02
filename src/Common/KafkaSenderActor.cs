using System;
using System.Text;
using Akka.Actor;
using Akka.Event;
using Confluent.Kafka;
using Google.Protobuf;

namespace Core
{
    public class KafkaSenderActor : ReceiveActor
    {
        private readonly ILoggingAdapter _log = Logging.GetLogger(Context);

        private record CompleteMessage;
        private record KafkaMessage(string Key, string Value);

        public KafkaSenderActor(string topic)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "127.0.0.1:9092",
                Acks = Acks.All,
            };

            var p = new ProducerBuilder<string, byte[]>(config).Build();

            ReceiveAsync<IMessage>(async m =>
            {
                try
                {
                    var ret = await p.ProduceAsync(topic, new Message<string, byte[]>
                    {
                        Key = "1",
                        Headers = new Headers { new Header("ClrType",
                                                           Encoding.UTF8.GetBytes(m.Descriptor.ClrType.ToString()))},
                        Value = m.ToByteArray()
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
