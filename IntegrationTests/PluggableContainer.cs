using System;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.PostgreSql;
using JetBrains.Annotations;
using Xunit;

namespace IntegrationTests;

[UsedImplicitly]
public sealed class PluggableContainer : HttpClient, IAsyncLifetime
{
    //private static readonly X509Certificate Certificate = new X509Certificate2(PluggableImage.CertificateFilePath, PluggableImage.CertificatePassword);

    private static readonly PluggableImage Image = new();

    private readonly INetwork _network;

    private readonly IContainer _postgresContainer;

    private readonly IContainer _pluggableContainer;

    private readonly IContainer _daprContainer;

    public PluggableContainer()
      : base(new HttpClientHandler
      {
        // Trust the development certificate.
      // ServerCertificateCustomValidationCallback = (_, certificate, _, _) => Certificate.Equals(certificate)
      })
    {
        //const string weatherForecastStorage = "weatherForecastStorage";

        //const string connectionString = $"server={weatherForecastStorage};user id={SqlEdgeBuilder.DefaultUsername};password={SqlEdgeBuilder.DefaultPassword};database={SqlEdgeBuilder.DefaultDatabase}";

         _network = new NetworkBuilder()
          .WithName(Guid.NewGuid().ToString("D"))
           .Build();

        _postgresContainer = new PostgreSqlBuilder()
          .WithNetwork(_network)
          .WithNetworkAliases("db")
          .Build();

         _daprContainer = new ContainerBuilder()
          .WithImage("daprio/daprd:edge")
          .WithNetwork(_network)
          .WithNetworkAliases("dapr")
          .WithExposedPort(3501)
          .WithPortBinding(3501, true)
          .WithCommand("./daprd", "-app-id", "pluggableapp", "-dapr-http-port", "3501", "-components-path", "DaprComponents", "-log-level", "debug")
          .WithWaitStrategy(
             Wait.ForUnixContainer()
             .UntilHttpRequestIsSucceeded(request => 
                request
                  .ForPort(3501)
                  .ForPath("/v1.0/healthz")
                  .ForStatusCode(HttpStatusCode.NoContent)))
          .Build();


        // TODO : Implement 

        _pluggableContainer = new ContainerBuilder()
          .WithImage(Image)
          .WithNetwork(_network)
          .Build();
    }

    public async Task InitializeAsync()
    {
        // It is not necessary to clean up resources immediately (still good practice). The Resource Reaper will take care of orphaned resources.
        // await Image.InitializeAsync()
        //   .ConfigureAwait(false);

        await _network.CreateAsync()
          .ConfigureAwait(false);

        // await _pluggableContainer.StartAsync()
        //   .ConfigureAwait(false);

        await _postgresContainer.StartAsync()
          .ConfigureAwait(false);

        await _daprContainer.StartAsync()
          .ConfigureAwait(false);
    }

    public async Task DisposeAsync()
    {

        // await Image.DisposeAsync()
        //   .ConfigureAwait(false);

        // await _pluggableContainer.DisposeAsync()
        //   .ConfigureAwait(false);

        await _postgresContainer.DisposeAsync()
          .ConfigureAwait(false);
        
        await _daprContainer.DisposeAsync()
          .ConfigureAwait(false);

        await _network.DeleteAsync()
          .ConfigureAwait(false);
    }

    public void SetBaseAddress()
    {
        try
        {
            var uriBuilder = new UriBuilder("http", _daprContainer.Hostname, _daprContainer.GetMappedPublicPort(3501));
            BaseAddress = uriBuilder.Uri;
        }
        catch
        {
          // Set the base address only once.
        }
    }
}