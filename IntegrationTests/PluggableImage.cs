using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;
using JetBrains.Annotations;

namespace IntegrationTests;

[UsedImplicitly]
public sealed class PluggableImage : IImage, IAsyncLifetime
{
    private readonly SemaphoreSlim _semaphoreSlim = new(1, 1);
    private readonly IImage _image = new DockerImage(string.Empty, "pluggable-postgres", DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString());
    public async Task InitializeAsync()
    {
        await _semaphoreSlim.WaitAsync()
          .ConfigureAwait(false);

        try
        {
            await new ImageFromDockerfileBuilder()
                .WithName(this)
                .WithDockerfileDirectory(CommonDirectoryPath.GetGitDirectory(), string.Empty)
                .WithDockerfile("dockerfile")
                .WithBuildArgument("RESOURCE_REAPER_SESSION_ID", ResourceReaper.DefaultSessionId.ToString("D")) // https://github.com/testcontainers/testcontainers-dotnet/issues/602.
                .WithDeleteIfExists(false)
                .Build()
                .CreateAsync()
                .ConfigureAwait(false);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }

    public string Repository => _image.Repository;

    public string Name => _image.Name;

    public string Tag => _image.Tag;

    public string FullName => _image.FullName;

    public string GetHostname()
    {
        return _image.GetHostname();
    }
}