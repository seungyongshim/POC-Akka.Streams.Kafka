using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Logging;

namespace SeungYongShim.Kafka.DependencyInjection
{
    public class KafkaProducer : IDisposable
    {
        private bool disposedValue;

        public KafkaProducer(ILogger<KafkaProducer> logger)
        {
            Logger = logger;
            var config = new ProducerConfig
            {
                BootstrapServers = "127.0.0.1:9092",
                Acks = Acks.All,
            };

            Producer = new ProducerBuilder<string, byte[]>(config).Build();
        }

        public ILogger<KafkaProducer> Logger { get; }

        IProducer<string, byte[]> Producer { get; }

        public async Task ProduceAsync(string topic, string key, IMessage message, CancellationToken ct)
        {
            var ret = await Producer.ProduceAsync(topic, new Message<string, byte[]>
            {
                Key = key,
                Headers = new Headers { new Header("clr-type", Encoding.UTF8.GetBytes(message.Descriptor.ClrType.ToString())) },
                Value = message.ToByteArray()
            });
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Producer.Flush();
                    Producer.Dispose();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
