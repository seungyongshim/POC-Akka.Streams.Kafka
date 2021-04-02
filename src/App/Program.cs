using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Akka.Actor;
using Core;
using Google.Protobuf;
using Message.Commands;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SeungYongShim.Akka.DependencyInjection;

namespace App
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var mkmkmskdf = (from assembly in AppDomain.CurrentDomain.GetAssemblies()
                             from type in assembly.GetTypes()
                             where typeof(IMessage).IsAssignableFrom(type)
                             where type.IsInterface is false
                             select type).ToList();

            var klklk = (from type in mkmkmskdf
                         select (type.FullName, type.GetProperty("Parser").GetGetMethod()?.Invoke(null, null) as MessageParser))
                        .ToDictionary(x => x.FullName, x => x.Item2);

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
                        services.AddSingleton<Dictionary<string, MessageParser>>(klklk);
                    })
                    .Build();

            await host.StartAsync();

            var kafkaConsumer = host.Services.GetService<KafkaConsumer>();

            var sys = host.Services.GetService<ActorSystem>();

            var kafkaSenderActor = await sys.ActorSelection("/user/KafkaSender")
                                            .ResolveOne(TimeSpan.FromSeconds(10));

            kafkaSenderActor.Tell(new Simple { Body = "Hello, Test1", Number = 1 });
            kafkaSenderActor.Tell(new Simple { Body = "Hello, Test1", Number = 2 });
            kafkaSenderActor.Tell(new Simple { Body = "Hello, Test1", Number = 3 });


            

            await host.WaitForShutdownAsync();
        }
    }
}
