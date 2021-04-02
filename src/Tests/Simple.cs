using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.TestKit.Xunit2;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Core;
using FluentAssertions;
using FluentAssertions.Extensions;
using Google.Protobuf;
using Message.Commands;
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
            static IEnumerable<Type> SafeGetTypes(Assembly assembly)
            {
                var types = Array.Empty<Type>();
                try
                {
                    types = assembly.GetTypes();
                }
                catch (Exception)
                {

                }

                foreach (var type in types)
                {
                    yield return type;
                }
            }

            var mkmkmskdf = (from assembly in AppDomain.CurrentDomain.GetAssemblies()
                             from type in SafeGetTypes(assembly)
                             where typeof(IMessage).IsAssignableFrom(type)
                             where type.IsInterface is false
                             select type).ToList();

            var klklk = (from type in mkmkmskdf
                         select (type.FullName, type.GetProperty("Parser").GetGetMethod()?.Invoke(null, null) as MessageParser))
                        .ToDictionary(x => x.FullName, x => x.Item2);

            var bootstrapServers = "localhost:9092";
            var topicName = "kafka.spec.simple.test";
            var groupId = "unittest";
            // arrange
            using var host =
                Host.CreateDefaultBuilder()
                    .ConfigureServices(services =>
                    {
                        services.AddTransient<KafkaConsumer>();
                        services.AddSingleton<Dictionary<string, MessageParser>>(klklk);
                    })
                    .UseAkka("test", @"", (sp, sys) =>
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
                catch (DeleteTopicsException)
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

            kafkaSenderActor.Tell(new Simple { Body = "Hello, Test1", Number = 1 });
            kafkaSenderActor.Tell(new Simple { Body = "Hello, Test2", Number = 2 });
            kafkaSenderActor.Tell(new Simple { Body = "Hello, Test3", Number = 3 });

            testKit.ExpectMsg<Simple>(15.Seconds()).Should().Be(new Simple { Body = "Hello, Test1", Number = 1 });
            testKit.ExpectMsg<Simple>(15.Seconds()).Should().Be(new Simple { Body = "Hello, Test2", Number = 2 });
            testKit.ExpectMsg<Simple>(15.Seconds()).Should().Be(new Simple { Body = "Hello, Test3", Number = 3 });

            await host.StopAsync();

            
        }
    }
}
