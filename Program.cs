namespace AmazonSqsReceiveEndpointExample;

using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Threading.Tasks;

public class Program
{
    public static async Task Main()
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();

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

        await bus.StartAsync();
        await bus.Publish(new TestMessage { Id = "1" });

        Console.ReadLine();
        await host.StopAsync();
    }
}

public class TestConsumer : IConsumer<TestMessage>
{
    private readonly ILogger<TestConsumer> logger;

    public TestConsumer(ILogger<TestConsumer> logger)
    {
        this.logger = logger;
    }
    public async Task Consume(ConsumeContext<TestMessage> context)
    {
        await Task.Delay(TimeSpan.FromSeconds(1));

        var redelivered = context.ReceiveContext.Redelivered ? "redelivered" : "";
        logger.LogInformation($"Got Message {context.Message.Id} {redelivered}");

        throw new Exception();
    }
}

public class TestMessage
{
    public string Id { get; set; }
}