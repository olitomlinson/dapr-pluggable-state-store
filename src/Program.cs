using DaprComponents.Services;
using Helpers;
using Dapr.PluggableComponents;
using System.Text.Json;

var app = DaprPluggableComponentsApplication.Create();

app.Logging.AddJsonConsole(options =>
        {
            options.IncludeScopes = true;
            options.TimestampFormat = "HH:mm:ss ";
            options.JsonWriterOptions = new JsonWriterOptions
            {
                Indented = true
            };
        });

app.RegisterService(
    "postgresql-tenant",
    serviceBuilder =>
    {
        serviceBuilder.RegisterStateStore(
            context =>
            {
                var logger = context.ServiceProvider.GetRequiredService<ILogger<StateStoreService>>();
                return new StateStoreService(context.InstanceId, logger, new StateStoreInitHelper(new PgsqlFactory()));
            });
    });
app.Run();