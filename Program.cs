using Serilog;
using Infrastructure.SmbSyncService;
using Services.SmbSyncService;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.File(
        path: Path.Combine(AppContext.BaseDirectory, "logs\\sync-log-.txt"), 
        rollingInterval: RollingInterval.Day,
        retainedFileCountLimit: 7) // Keep the last 7 days of logs
    .CreateLogger();

try
{
    var builder = Host.CreateApplicationBuilder(args);

    builder.Services.AddSerilog();

    builder.Services.AddWindowsService(options =>
    {
        options.ServiceName = "SmbTwoWaySync";
    });

    builder.Services.Configure<SyncConfig>(builder.Configuration.GetSection("SyncConfig"));
    builder.Services.AddHostedService<SyncWorker>();

    var host = builder.Build();
    host.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "The service failed to start correctly.");
}
finally
{
    Log.CloseAndFlush();
}