using LoadSimulator.Sender.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Logging;


var builder = Host.CreateApplicationBuilder(args);

builder.Logging.AddFilter("System.Net.Http.HttpClient", LogLevel.Warning);


builder.Services.Configure<HttpClientFactoryOptions>(options =>
{
    options.HttpMessageHandlerBuilderActions.Add(b =>
    {
        b.AdditionalHandlers.Clear();
    });
});

builder.Services.AddHttpClient<ILoadGenerator, HttpLoadGenerator>(client =>
{
    client.BaseAddress = new Uri("http://receiver-service:5001/");
    client.Timeout = TimeSpan.FromSeconds(10);
})

.ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
{
    PooledConnectionLifetime = TimeSpan.FromMinutes(15),
    PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
    MaxConnectionsPerServer = 10000
});

builder.Services.AddTransient<AppRunner>();

using var host = builder.Build();

var app = host.Services.GetRequiredService<AppRunner>();
await app.RunAsync();
