using System;
using System.Threading;
using Akka.Actor;
using Core.Messages;
using System.Text.Json;
using System.Collections.Generic;
using Google.Protobuf;
using System.Text;

namespace Core
{
    public class KafkaConsumerActor : ReceiveActor
    {
        KafkaConsumer KafkaConsumer { get; }
        public KafkaConsumerActor(KafkaConsumer kafkaConsumer,
                                  Dictionary<string, MessageParser> typeLookup,
                                  string topic,
                                  string groupId,
                                  IActorRef parserActor)
        {
            

            var captureSelf = Context.Self;
            KafkaConsumer = kafkaConsumer;

            KafkaConsumer.Run("127.0.0.1:9092", groupId, new []{ topic }, m =>
            {
                captureSelf.Tell(m);
            });

            Receive<KafkaMessage>(m =>
            {
                var clrType = m.Headers.FindLast(x => x.Key == "ClrType");
                if (clrType is not null)
                {
                    var typeName = Encoding.Default.GetString(clrType.Value);
                    var parser = typeLookup[typeName];
                    var o = parser.ParseFrom(m.Value);

                    parserActor.Forward(o);
                    Thread.Sleep(1000);
                    m.Commit();
                }
            });
        }

        protected override void PostStop()
        {
            KafkaConsumer.Stop();
            base.PostStop();
        }

        protected override void PreRestart(Exception reason, object message)
        {
            KafkaConsumer.Stop();
            base.PreRestart(reason, message);
        }
    }
}
