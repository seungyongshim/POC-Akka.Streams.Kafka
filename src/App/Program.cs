using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Akka.Actor;
using Core;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SeungYongShim.Akka.DependencyInjection;

namespace App
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var mkmkmskdf = Assembly.GetExecutingAssembly().GetTypes().Where(x => x.GetInterface("IMessage") is not null).ToList();

            var klklk= mkmkmskdf.Select(x => (x.FullName, x.GetProperty("Parser").GetGetMethod().Invoke(null, null))).ToList();

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
                       
                    ", (sp, sys) =>
                    {
                        var kafkaSender = sys.ActorOf(sys.PropsFactory<KafkaSenderActor>()
                                       .Create(topicName), "KafkaSender");
                        sys.ActorOf(sys.PropsFactory<KafkaConsumerActor>()
                                       .Create(topicName,
                                               groupId,
                                               kafkaSender), "KafkaConsumer");
                    })
                    .ConfigureServices(services =>
                    {
                    services.AddTransient<KafkaConsumer>();
                    services.AddSingleton<Dictionary<string, MessageParser>>(
                        new Dictionary<string, MessageParser> { [SampleCommand.Descriptor.ClrType.ToString()] = SampleCommand.Parser });
                    })
                    .Build();

            await host.StartAsync();

            var kafkaConsumer = host.Services.GetService<KafkaConsumer>();

            var sys = host.Services.GetService<ActorSystem>();

            var kafkaSenderActor = await sys.ActorSelection("/user/KafkaSender")
                                            .ResolveOne(TimeSpan.FromSeconds(10));

            kafkaSenderActor.Tell(new SampleCommand { Body = "Hello, Test1", Number = 1 });
            kafkaSenderActor.Tell(new SampleCommand { Body = "Hello, Test1", Number = 2 });
            kafkaSenderActor.Tell(new SampleCommand { Body = "Hello, Test1", Number = 3 });


            

            await host.WaitForShutdownAsync();
        }
    }
}
