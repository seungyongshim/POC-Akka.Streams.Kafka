using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Core.Messages;
using Microsoft.Extensions.Logging;

namespace Core
{
    public class KafkaConsumer
    {
        public KafkaConsumer(ILogger<KafkaConsumer> logger)
        {
            Logger = logger;
            
        }

        public ILogger<KafkaConsumer> Logger { get; }
        CancellationTokenSource CancellationTokenSource = new CancellationTokenSource();
        Thread KafkaConsumerThread { get; set; }

        public void Run(string brokerList,
                        string groupId,
                        IList<string> topics,
                        Action<KafkaMessage> callback)
        {
            if (KafkaConsumerThread is not null) return;

            var cancellationToken = CancellationTokenSource.Token;
            var timeout = TimeSpan.FromMinutes(10);

            var config = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = groupId,
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            KafkaConsumerThread = new Thread(() =>
            {
                var slim = new ManualResetEventSlim();

                using (var consumer = new ConsumerBuilder<string, byte[]>(config)
                    .SetPartitionsAssignedHandler((c, partitions) =>
                    {
                        Logger.LogDebug($"Incremental partition assignment: [{string.Join(", ", partitions)}]");
                    })
                    .SetPartitionsRevokedHandler((c, partitions) =>
                    {
                        Logger.LogDebug($"Incremental partition revokation: [{string.Join(", ", partitions)}]");
                    })
                    .SetPartitionsLostHandler((c, partitions) =>
                    {
                        Logger.LogDebug($"Partitions were lost: [{string.Join(", ", partitions)}]");
                    })
                    .Build())
                {
                    consumer.Subscribe(topics);

                    try
                    {
                        while (true)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(cancellationToken);

                                if (consumeResult.IsPartitionEOF) continue;

                                var message = new KafkaMessage(consumeResult.Message.Headers
                                                                            .Select(x => new KafkaHeader(x.Key, x.GetValueBytes()))
                                                                            .ToImmutableList(),
                                                               consumeResult.Message.Value,
                                                               () =>
                                                               {
                                                                   try
                                                                   {
                                                                       consumer.Commit(consumeResult);
                                                                   }
                                                                   catch (KafkaException e)
                                                                   {
                                                                       Console.WriteLine($"Commit error: {e.Error.Reason}");
                                                                   }
                                                                   finally
                                                                   {
                                                                       slim.Set();
                                                                   }
                                                               });

                                callback?.Invoke(message);

                                slim.Wait(timeout, cancellationToken);
                                slim.Reset();

                            }
                            catch (ConsumeException e)
                            {
                                Logger.LogError(e, "Consume Error");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        Logger.LogDebug("Closing consumer.");
                    }
                    finally
                    {
                        consumer.Close();
                    }
                }
            });

            KafkaConsumerThread.Start();
        }

        public void Stop()
        {
            CancellationTokenSource.Cancel();
        }
    }
}
