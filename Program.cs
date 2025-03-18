namespace AmazonSqsReceiveEndpointExample;

using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

public class Program
{
    public static async Task Main()
    {
        HostApplicationBuilder builder = Host.CreateApplicationBuilder();

        var services = builder.Services;
        services.AddMassTransit(x =>
            {
                x.AddConsumer<TestConsumer>();
                x.UsingAmazonSqs((context, cfg) =>
                {
                    cfg.Host("us-east-1", h =>
                    {                        });
                    cfg.ReceiveEndpoint("masstransit-test-q", endpointConfigurator =>
                    {
                        endpointConfigurator.RethrowFaultedMessages();
                        endpointConfigurator.ThrowOnSkippedMessages();
                        endpointConfigurator.ConfigureConsumer<TestConsumer>(context);
                    });

                });
            });

        var host = builder.Build();

        await host.StartAsync();

        var provider = host.Services;
        var bus = provider.GetService<IBus>() as IBusControl;

        var queue = new Uri("queue:masstransit-test-q");
        var sendEndpoint = await bus.GetSendEndpoint(queue);

        await sendEndpoint.Send(new TestMessage { Id = "message 1" });
        await sendEndpoint.Send(new TestMessage { Id = "message 2" });

        await host.WaitForShutdownAsync();
    }
}

public class TestConsumer : IConsumer<TestMessage>
{
    private static int _count = 0;

    public async Task Consume(ConsumeContext<TestMessage> context)
    {
        if (Interlocked.Increment(ref _count) == 2)
        {
            // wait more than 30 seconds at this breakpoint before continuing execution
            // if done correctly you should see a R-DUPE warning in the logs
            Debugger.Break();
        }

        await Task.Delay(TimeSpan.FromSeconds(5));

        if (!context.ReceiveContext.Redelivered) { 
            throw new Exception();
        }
    }
}

public class TestMessage
{
    public string Id { get; set; }
}