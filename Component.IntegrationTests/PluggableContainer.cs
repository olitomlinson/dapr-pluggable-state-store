using System;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using JetBrains.Annotations;
using Xunit;

namespace IntegrationTests;

[UsedImplicitly]
public sealed class PluggableContainer : HttpClient, IAsyncLifetime
{
    private static readonly X509Certificate Certificate = new X509Certificate2(PluggableImage.CertificateFilePath, PluggableImage.CertificatePassword);

    private static readonly PluggableImage Image = new();

    private readonly INetwork _weatherForecastNetwork;

//   private readonly IContainer _sqlEdgeContainer;

    private readonly IContainer _pluggableContainer;

    public PluggableContainer()
      : base(new HttpClientHandler
      {
        // Trust the development certificate.
      // ServerCertificateCustomValidationCallback = (_, certificate, _, _) => Certificate.Equals(certificate)
      })
    {
        const string weatherForecastStorage = "weatherForecastStorage";

        //const string connectionString = $"server={weatherForecastStorage};user id={SqlEdgeBuilder.DefaultUsername};password={SqlEdgeBuilder.DefaultPassword};database={SqlEdgeBuilder.DefaultDatabase}";

        _weatherForecastNetwork = new NetworkBuilder()
          .Build();

        // _sqlEdgeContainer = new SqlEdgeBuilder()
        //   .WithNetwork(_weatherForecastNetwork)
        //   .WithNetworkAliases(weatherForecastStorage)
        //   .Build();

        // TODO : Implement Postgres Container

        // TODO : Implement Dapr Container

        // TODO : Implement 

        _pluggableContainer = new ContainerBuilder()
          .WithImage(Image)
          .WithNetwork(_weatherForecastNetwork)
        //.WithPortBinding(PluggableImage.HttpsPort, true)
          .WithEnvironment("ASPNETCORE_URLS", "https://+")
          .WithEnvironment("ASPNETCORE_Kestrel__Certificates__Default__Path", PluggableImage.CertificateFilePath)
          .WithEnvironment("ASPNETCORE_Kestrel__Certificates__Default__Password", PluggableImage.CertificatePassword)
        // .WithEnvironment("ConnectionStrings__DefaultConnection", connectionString)
          .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(PluggableImage.HttpsPort))
          .Build();
    }

    public async Task InitializeAsync()
    {
        // It is not necessary to clean up resources immediately (still good practice). The Resource Reaper will take care of orphaned resources.
        await Image.InitializeAsync()
          .ConfigureAwait(false);

        await _weatherForecastNetwork.CreateAsync()
          .ConfigureAwait(false);

        // await _sqlEdgeContainer.StartAsync()
        //   .ConfigureAwait(false);

        await _pluggableContainer.StartAsync()
          .ConfigureAwait(false);
    }

    public async Task DisposeAsync()
    {
        await Image.DisposeAsync()
          .ConfigureAwait(false);

        await _pluggableContainer.DisposeAsync()
          .ConfigureAwait(false);

        // await _sqlEdgeContainer.DisposeAsync()
        //   .ConfigureAwait(false);

        await _weatherForecastNetwork.DeleteAsync()
          .ConfigureAwait(false);
    }

    public void SetBaseAddress()
    {
        try
        {
            var uriBuilder = new UriBuilder("https", _pluggableContainer.Hostname, _pluggableContainer.GetMappedPublicPort(PluggableImage.HttpsPort));
            BaseAddress = uriBuilder.Uri;
        }
        catch
        {
          // Set the base address only once.
        }
    }
}