using System.Net;
using Dapr.Client;

namespace IntegrationTests;

public class Tests : IClassFixture<PluggableContainer>
{
    private readonly PluggableContainer _pluggableContainer;
    private readonly DaprClient _daprClient;

    public Tests(PluggableContainer pluggableContainer)
    {
        _pluggableContainer = pluggableContainer;
        _pluggableContainer.SetBaseAddress();
        _daprClient = _pluggableContainer.GetDaprClient();
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

        var tokenSource = new CancellationTokenSource();

        const string standardStateApi = "v1.0/state/standard-postgres";

        var state =  new List<State>() { 
            new State { 
                Key = Guid.NewGuid().ToString(), 
                Value = Guid.NewGuid().ToString() }};


        await _daprClient.SaveStateAsync("standard-postgres", state[0].Key, state[0].Value, cancellationToken: tokenSource.Token);
        var retrievedState = await _daprClient.GetStateAsync<string>("standard-postgres", state[0].Key, cancellationToken: tokenSource.Token);

        Assert.Equal(retrievedState, state[0].Value);
    }

    [Fact]
    public async Task CreatePluggableStateItem()
    {
        var tokenSource = new CancellationTokenSource();

        const string pluggableStateApi = "v1.0/state/pluggable-postgres";

        var state =  new List<State>() { 
            new State { 
                Key = Guid.NewGuid().ToString(), 
                Value = Guid.NewGuid().ToString(),
                Metadata = new Dictionary<string, string> {
                    { "tenantId", "5" }
                }}};

        await _daprClient.SaveStateAsync("pluggable-postgres", state[0].Key, state[0].Value, metadata: state[0].Metadata, cancellationToken: tokenSource.Token);
        var retrievedState = await _daprClient.GetStateAsync<string>("pluggable-postgres", state[0].Key, metadata: state[0].Metadata, cancellationToken: tokenSource.Token);

        Assert.Equal(retrievedState, state[0].Value);
    }
}