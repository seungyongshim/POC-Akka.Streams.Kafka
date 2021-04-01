using System;
using System.Threading.Tasks;
using Akka.Actor;
using Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SeungYongShim.Akka.DependencyInjection;

namespace App
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var bootstrapServers = "localhost:9092";
            var topicName = "kafka.spec.app";
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
                    ", (sp, sys) => sys.ActorOf(sys.PropsFactory<KafkaSenderActor>()
                                       .Create(topicName), "KafkaSender"))
                    .Build();

            await host.StartAsync();

            var sys = host.Services.GetService<ActorSystem>();

            var kafkaSenderActor = await sys.ActorSelection("/user/KafkaSender")
                                            .ResolveOne(TimeSpan.FromSeconds(10));

            kafkaSenderActor.Tell("Hello, Test1");

            await host.WaitForShutdownAsync();
        }
    }
}
