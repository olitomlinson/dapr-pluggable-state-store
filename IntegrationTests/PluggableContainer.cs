using System.Net;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using DotNet.Testcontainers.Volumes;
using Testcontainers.PostgreSql;
using JetBrains.Annotations;
using System.Text.Json.Serialization;
using Dapr.Client;

namespace IntegrationTests;

[UsedImplicitly]
public sealed class PluggableContainer : HttpClient, IAsyncLifetime
{
    private static readonly PluggableImage Image = new();
    private readonly IVolume _socketVolume;
    private readonly INetwork _network;
    private readonly IContainer _postgresContainer;
    private readonly IContainer _pluggableContainer;
    private readonly IContainer _daprContainer;

    private readonly ushort _dapr_http_port = 3501;
    private readonly ushort _dapr_grpc_port = 50002;
    private DaprClient _daprClient;
    private string _dapr_app_id;

    public PluggableContainer() : base(new HttpClientHandler())
    {
        var daprComponentsDirectory = $"{Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location)}/DaprComponents";

        var containerSuffix = Guid.NewGuid().ToString("N").Substring(23);
        _dapr_app_id = $"pluggableapp-{containerSuffix}";
        
        _network = new NetworkBuilder()
            .WithName($"network-{containerSuffix}")
            .Build();

        _postgresContainer = new PostgreSqlBuilder()
            .WithNetwork(_network)
            .WithName($"postgres-{containerSuffix}")
            .WithCommand("-c", "log_statement=all")
            .WithNetworkAliases("db")
            .Build();

        _socketVolume = new VolumeBuilder().Build();

        _daprContainer = new ContainerBuilder()
            .WithImage("daprio/daprd:1.10.4-mariner-linux-arm64")
            .WithName($"dapr-{containerSuffix}")
            .WithNetwork(_network)
            .WithNetworkAliases("dapr")
            .WithExposedPort(_dapr_http_port)
            .WithPortBinding(_dapr_http_port, true)
            .WithExposedPort(_dapr_grpc_port)
            .WithPortBinding(_dapr_grpc_port, true)
            .WithVolumeMount(_socketVolume, "/tmp/dapr-components-sockets")
            .WithResourceMapping($"{daprComponentsDirectory}/pluggablePostgres.yaml", "/DaprComponents/pluggablePostgres.yaml")
            .WithResourceMapping($"{daprComponentsDirectory}/standardPostgres.yaml", "/DaprComponents/standardPostgres.yaml")
            .WithCommand("./daprd", "-app-id", _dapr_app_id, "-dapr-http-port", _dapr_http_port.ToString(), "-dapr-grpc-port", _dapr_grpc_port.ToString(), "-components-path", "/DaprComponents", "-log-level", "debug")
            .WithWaitStrategy(
                Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(request =>  
                request
                    .ForPort(_dapr_http_port)
                    .ForPath("/v1.0/healthz")
                    .ForStatusCode(HttpStatusCode.NoContent)))
            .Build();

        _pluggableContainer = new ContainerBuilder()
            .WithImage(Image)
            .WithName($"pluggable-component-{containerSuffix}")
            .WithVolumeMount(_socketVolume, "/tmp/dapr-components-sockets")
            .WithNetwork(_network)
            .Build();
    }

    public async Task InitializeAsync()
    {
        await Image.InitializeAsync().ConfigureAwait(false);
        await _network.CreateAsync().ConfigureAwait(false);
        await _socketVolume.CreateAsync().ConfigureAwait(false);
        await _pluggableContainer.StartAsync().ConfigureAwait(false);
        await _postgresContainer.StartAsync().ConfigureAwait(false);
        await _daprContainer.StartAsync().ConfigureAwait(false);

        Environment.SetEnvironmentVariable("DAPR_HTTP_PORT", _daprContainer.GetMappedPublicPort(_dapr_http_port).ToString());
        Environment.SetEnvironmentVariable("DAPR_GRPC_PORT", _daprContainer.GetMappedPublicPort(_dapr_grpc_port).ToString());
        _daprClient = new DaprClientBuilder().Build();
    }

    public async Task DisposeAsync()
    {
        await Image.DisposeAsync().ConfigureAwait(false);
        await _pluggableContainer.DisposeAsync().ConfigureAwait(false);
        await _postgresContainer.DisposeAsync().ConfigureAwait(false);
        await _daprContainer.DisposeAsync().ConfigureAwait(false);
        await _network.DeleteAsync().ConfigureAwait(false);
        await _socketVolume.DeleteAsync().ConfigureAwait(false);
    }

    public void SetBaseAddress()
    {
        try
        {
            var uriBuilder = new UriBuilder("http", _daprContainer.Hostname, _daprContainer.GetMappedPublicPort(_dapr_http_port));
            BaseAddress = uriBuilder.Uri;
        }
        catch
        {
            // Set the base address only once.
        }
    }

    public async Task<ExecResult> GetStateViaSQL(string key, string tenantId){
        return await ((PostgreSqlContainer)_postgresContainer).ExecScriptAsync($"SELECT value FROM \"public\".\"{tenantId}-state\" WHERE key = '{_dapr_app_id}'");
    }

    public DaprClient GetDaprClient(){
        return _daprClient;
    }
}