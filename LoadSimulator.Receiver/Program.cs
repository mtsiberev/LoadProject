using LoadSimulator.Receiver.Services;

var builder = WebApplication.CreateBuilder(args);


builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.AspNetCore.Hosting.Diagnostics", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.AspNetCore.Routing.EndpointMiddleware", LogLevel.Warning);

builder.Services.AddSingleton<IMetricsService, MetricsService>();

builder.Services.AddControllers();

var app = builder.Build();

app.MapControllers();

Console.WriteLine("=== Запуск REST-сервера на порту 5001 ===");
//app.Run("http://localhost:5001");
app.Run("http://+:5001");