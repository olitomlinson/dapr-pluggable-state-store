using System.Net;
using System.Text;
using System.Text.Json;

namespace IntegrationTests;

public class Api : IClassFixture<PluggableContainer>
{
    private readonly PluggableContainer _pluggableContainer;

    public Api(PluggableContainer pluggableContainer)
    {
        _pluggableContainer = pluggableContainer;
        _pluggableContainer.SetBaseAddress();
    }

    [Fact]
    public async Task DaprHealthCheck()
    {
        const string path = "v1.0/healthz";

        var response = await _pluggableContainer.GetAsync(path)
        .ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    [Fact]
    public async Task CreateStandardStateItem()
    {
        const string standardStateApi = "v1.0/state/standard-postgres";

        var state =  new List<State>() { new State { 
        Key = Guid.NewGuid().ToString(), 
        Value = Guid.NewGuid().ToString() }};
        var jsonContent = JsonSerializer.Serialize(state);
        using var httpContent = new StringContent(jsonContent, Encoding.UTF8, "application/json");

        var response = await _pluggableContainer.PostAsync(standardStateApi, httpContent);
        response.EnsureSuccessStatusCode();

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    [Fact]
    public async Task CreatePluggableStateItem()
    {
        const string pluggableStateApi = "v1.0/state/pluggable-postgres";

        var state =  new List<State>() { new State { 
            Key = Guid.NewGuid().ToString(), 
            Value = Guid.NewGuid().ToString(),
            Metadata = new Dictionary<string, string> {
            { "tenantId", "5" }
            }}};

        var jsonContent = JsonSerializer.Serialize(state);
        using var httpContent = new StringContent(jsonContent, Encoding.UTF8, "application/json");

        var response = await _pluggableContainer.PostAsync(pluggableStateApi, httpContent);
        response.EnsureSuccessStatusCode();

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }
}