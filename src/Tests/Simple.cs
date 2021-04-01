using System.Threading.Tasks;
using Akka.Actor;
using Akka.TestKit.Xunit2;
using Common;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using FluentAssertions.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SeungYongShim.Akka.DependencyInjection;
using Xunit;

namespace Tests
{
    public class KafkaSpec
    {
        [Fact]
        public async void Simple()
        {
            var bootstrapServers = "localhost:9092";
            var topicName = "kafka.spec.simple.test";
            var groupId = "unittest";
            // arrange
            using var host =
                Host.CreateDefaultBuilder()
                    .UseAkka("test", @"
                        akka.kafka.producer {
                           parallelism = 100
                           flush-timeout = 10s
                           use-dispatcher = ""akka.kafka.default-dispatcher""
                        }
                        akka.kafka.default-dispatcher {
                           type = ""Dispatcher""
                           executor = ""default-executor""
                        }
                        akka.kafka.consumer {
                            poll-interval = 50ms
                            poll-timeout = 50ms
                            stop-timeout = 30s
                            close-timeout = 20s
                            commit-timeout = 15s
                            commit-time-warning = 1s
                            commit-refresh-interval = infinite
                            use-dispatcher = ""akka.kafka.default-dispatcher""
                            wait-close-partition = 500ms
                            position-timeout = 5s
                            partition-handler-warning = 5s
                        }
                    ", (sp, sys) =>
                    {
                        var testActor = sp.GetService<TestKit>().TestActor;

                        sys.ActorOf(sys.PropsFactory<KafkaConsumerActor>()
                                       .Create(topicName,
                                               groupId,
                                               testActor), "KafkaConsumer");
                        sys.ActorOf(sys.PropsFactory<KafkaSenderActor>()
                                       .Create(topicName), "KafkaSender");
                    })
                    .UseAkkaWithXUnit2()
                    .Build();

            await host.StartAsync();

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            }).Build())
            {
                try
                {
                    await adminClient.DeleteTopicsAsync(new[]
                    {
                        topicName
                    });
                    await Task.Delay(1000);
                }
                catch (DeleteTopicsException ex)
                {
                }

                await adminClient.CreateTopicsAsync(new TopicSpecification[]
                {
                    new TopicSpecification
                    {
                        Name = topicName,
                        ReplicationFactor = 1,
                        NumPartitions = 16
                    }
                });
            }

            var sys = host.Services.GetService<ActorSystem>();
            var testKit = host.Services.GetService<TestKit>();

            var kafkaSenderActor = await sys.ActorSelection("/user/KafkaSender")
                                            .ResolveOne(10.Seconds());

            var kafkaConsumerActor = await sys.ActorSelection("/user/KafkaConsumer")
                                              .ResolveOne(10.Seconds());

            kafkaSenderActor.Tell("Hello, Test1");
            kafkaSenderActor.Tell("Hello, Test2");
            kafkaSenderActor.Tell("Hello, Test3");

            testKit.ExpectMsg<string>(5.Seconds()).Should().Be("Hello, Test1");
            testKit.ExpectMsg<string>(5.Seconds()).Should().Be("Hello, Test2");
            testKit.ExpectMsg<string>(5.Seconds()).Should().Be("Hello, Test3");

            await host.StopAsync();
        }
    }
}
