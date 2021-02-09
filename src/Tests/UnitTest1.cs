using System;
using Akka.Actor;
using Common;
using FluentAssertions.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Nexon.Akka.DependencyInjection;
using Xunit;

namespace Tests
{
    public class KafkaSpec
    {
        [Fact]
        public async void Simple()
        {
            string topic = "kafka.spec.simple.test";
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
                    ", sys =>
                    {
                        sys.ActorOf(sys.PropsFactory<KafkaSenderActor>()
                                       .Create(topic), "KafkaSender");
                    })
                    .UseAkkaWithXUnit2()
                    .Build();

            await host.StartAsync();

            var sys = host.Services.GetService<ActorSystem>();

            var kafkaSenderActor = await sys.ActorSelection("/user/KafkaSender")
                                            .ResolveOne(10.Seconds());

            kafkaSenderActor.Tell("Hello, Test!!!");

            await host.StopAsync();
        }
    }
}
