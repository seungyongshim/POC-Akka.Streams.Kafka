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
                        akka {
                            stdout-loglevel = DEBUG
                            loglevel = DEBUG
                            log-config-on-start = on        
                            actor {                
                                debug {  
                                      receive = on 
                                      autoreceive = on
                                      lifecycle = on
                                      event-stream = on
                                      unhandled = on
                                }
                            }
                        }
                        akka.stream {
                            debug-logging = on
                            initial-input-buffer-size = 1
                            max-input-buffer-size = 1
                        }
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
            kafkaSenderActor.Tell("Hello, Test2");
            kafkaSenderActor.Tell("Hello, Test3");
            kafkaSenderActor.Tell("Hello, Test4");
            kafkaSenderActor.Tell("Hello, Test5");
            kafkaSenderActor.Tell("Hello, Test6");
            kafkaSenderActor.Tell("Hello, Test7");
            kafkaSenderActor.Tell("Hello, Test8");
            kafkaSenderActor.Tell("Hello, Test9");
            kafkaSenderActor.Tell("Hello, Test10");

            await host.WaitForShutdownAsync();
        }
    }
}
