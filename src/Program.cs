using DaprComponents.Services;
using Helpers;
using Dapr.PluggableComponents;

var app = DaprPluggableComponentsApplication.Create();

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