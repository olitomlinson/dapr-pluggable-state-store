using DaprComponents.Services;
using Helpers;
using Dapr.PluggableComponents;

var app = DaprPluggableComponentsApplication.Create();

app.Services.AddSingleton<PluggableStateStoreHelpers>();

app.Services.AddHostedService<ExpiredDataCleanUpService>();

app.RegisterService(
    "postgresql-tenant",
    serviceBuilder =>
    {
        serviceBuilder.RegisterStateStore(
            context =>
            {   
                var logger = context.ServiceProvider.GetRequiredService<ILogger<StateStoreService>>();
                var helpers = context.ServiceProvider.GetService<PluggableStateStoreHelpers>();
                var helper = new StateStoreInitHelper(new PgsqlFactory(), logger);
                helpers.Add(context.InstanceId, helper);
                     
                return new StateStoreService(context.InstanceId, logger, helper);
            });
    });
app.Run();
