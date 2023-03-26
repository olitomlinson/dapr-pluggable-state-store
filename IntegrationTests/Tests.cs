using System.Net;
using Dapr.Client;

namespace IntegrationTests;

public class StateIsolationTests : IClassFixture<PluggableContainer>
{
    private readonly PluggableContainer _pluggableContainer;
    private readonly DaprClient _daprClient;

    public StateIsolationTests(PluggableContainer pluggableContainer)
    {
        _pluggableContainer = pluggableContainer;
        _daprClient = _pluggableContainer.GetDaprClient();
        _pluggableContainer.SetBaseAddress();
    }

    [Fact]
    public async Task DaprHealthCheck()
    {
        const string path = "/v1.0/healthz";

        var response = await _pluggableContainer.GetAsync(path)
        .ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
    }

    [Fact]
    public async Task StateIsSharedWithinTenant()
    {
        var ct = new CancellationTokenSource(5000).Token;

        var key = "Foo";
        var value = "Bar";
        var tenantId = "123";

        await _daprClient.SaveStateAsync("pluggable-postgres", key, value, metadata: tenantId.AsMetaData(), cancellationToken: ct);
        
        var retrievedState = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: ct);

        Assert.Equal(value, retrievedState);
    }

    [Fact]
    public async Task StateIsNotSharedAcrossTenants()
    {
        var ct = new CancellationTokenSource(5000).Token;

        var key = "Foo";
        var value = "Bar";
        var tenantId = "123";
        var illegalTenantId = "567";

        await _daprClient.SaveStateAsync("pluggable-postgres", key, value, metadata: tenantId.AsMetaData(), cancellationToken: ct);
        
        var retrievedState = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: illegalTenantId.AsMetaData(), cancellationToken: ct);

        Assert.Null(retrievedState);
    }


    [Fact]
    public async Task SequentialUpdatesWithoutEtag()
    {
        var key = "What-Comes-First";
        var seedValue = "Chicken";
        var tenantId = "101";

        await _daprClient.SaveStateAsync("pluggable-postgres", key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        await _daprClient.SaveStateAsync("pluggable-postgres", key, updatedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Multiple(
            () => Assert.Equal(seedValue,firstGet),
            () => Assert.Equal(updatedValue, secondGet)
        );
    }

    [Fact]
    public async Task SequentialUpdatesWithEtag()
    {
        var key = "What-Comes-First";
        var seedValue = "Chicken";
        var tenantId = "101";

        await _daprClient.SaveStateAsync<string>("pluggable-postgres", key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";

        var success = await _daprClient.TrySaveStateAsync<string>("pluggable-postgres", key, updatedValue, etag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
  
        Assert.Multiple(
            () => Assert.Equal(seedValue,firstGet),
            () => Assert.Equal(updatedValue, secondGet)
        );
    }

    [Fact]
    public async Task SequentialUpdatesAndEtagMismatchIsThrown()
    {
        var key = "What-Comes-First";
        var seedValue = "Chicken";
        var tenantId = "101";

        await _daprClient.SaveStateAsync<string>("pluggable-postgres", key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        var modifiedEtag = $"{etag}-is-now-mismatched";

        var success = await _daprClient.TrySaveStateAsync<string>("pluggable-postgres", key, updatedValue, modifiedEtag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
       
        Assert.False(success);
    }

    [Fact]
    public async Task SequentialUpdatesCantUseOldEtags()
    {
        var key = "What-Comes-First";
        var seedValue = "Chicken";
        var tenantId = "101";

        await _daprClient.SaveStateAsync<string>("pluggable-postgres", key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        await _daprClient.SaveStateAsync<string>("pluggable-postgres", key, updatedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
  
        updatedValue = "Goose";
        var success = await _daprClient.TrySaveStateAsync<string>("pluggable-postgres", key, updatedValue, etag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var thirdGet = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
  
        Assert.Multiple(
            () => Assert.False(success),
            () => Assert.Equal(secondGet, thirdGet)
        );
    }

     [Fact]
    public async Task DeleteWithoutEtag()
    {
        var key = "What-Comes-First";
        var seedValue = "Chicken";
        var tenantId = "102";

        await _daprClient.SaveStateAsync<string>("pluggable-postgres", key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        await _daprClient.DeleteStateAsync("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);


        Assert.Multiple(
            () => Assert.Equal(seedValue, firstGet),
            () => Assert.Null(secondGet)
        );
    }

    public async Task ScanEntireDatabaseForStateBleedAcrossTenants()
    {
        // TODO : Write a SQL query that scans through all tables in all schemas,
        // Looking for a uniqely generated value (value being state stored against a dapr key).
        // The value should only appear once, and it should be stored against the 
        // correct key, in the correct table, in the correct schema.

        // If the key appears more than once, or in an unexpected location, there 
        // has been a catastrophic error.
    }
}

public static class StringExtensions
{
    public static IReadOnlyDictionary<string,string> AsMetaData(this string tenantId){
        return new Dictionary<string, string> {{ "tenantId", tenantId}};
    }
}