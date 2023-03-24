using DaprComponents.Services;
using Helpers;
using Dapr.PluggableComponents;

var app = DaprPluggableComponentsApplication.Create();

app.Services.AddSingleton<StateStoreInitHelper>(new StateStoreInitHelper(new PgsqlFactory()));

app.RegisterService(
    "postgresql-tenant",
    serviceBuilder =>
    {
        serviceBuilder.RegisterStateStore<StateStoreService>();
        // Register one or more components with this service.
    }); 

app.Run();