using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using SeungYongShim.Kafka.DependencyInjection.Internal;

namespace SeungYongShim.Kafka.DependencyInjection
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
                using var slim = new ManualResetEventSlim();

                using var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
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
                                                                   Logger.LogError(e, "");
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
                            Logger.LogError(e, "");
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
            });

            KafkaConsumerThread.Start();
        }

        public void Stop()
        {
            CancellationTokenSource.Cancel();
        }
    }
}
