using Dapr.Client;
using Xunit.Abstractions;

namespace IntegrationTests;

[Collection("Sequence")]  
public class StateIsolationTests : IClassFixture<PluggableContainer>
{
    private readonly ITestOutputHelper output;
    private readonly PluggableContainer _pluggableContainer;
    private readonly DaprClient _daprClient;

    public StateIsolationTests(PluggableContainer pluggableContainer, ITestOutputHelper output)
    {
        _pluggableContainer = pluggableContainer;
        _daprClient = _pluggableContainer.GetDaprClient();
        this.output = output;
    }

    [Fact]
    public async Task CheckDaprSideCarIsHealthy()
    {
        var healthy = await _daprClient.CheckHealthAsync();
        Assert.True(healthy);
    }

    [Fact]
    public async Task StateIsSharedWithinTenant()
    {
        var ct = new CancellationTokenSource(5000).Token;

        var key = "Foo";
        var value = "Bar";
        var tenantId = Guid.NewGuid().ToString();

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
        var tenantId = Guid.NewGuid().ToString();
        var illegalTenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync("pluggable-postgres", key, value, metadata: tenantId.AsMetaData(), cancellationToken: ct);
        
        var retrievedState = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: illegalTenantId.AsMetaData(), cancellationToken: ct);

        Assert.Null(retrievedState);
    }


    [Fact]
    public async Task SequentialUpdatesWithoutEtag()
    {
        var key = "What-Comes-First";
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

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
        var tenantId = Guid.NewGuid().ToString();

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
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>("pluggable-postgres", key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        var wrongEtag = $"{etag}-is-now-mismatched";

        var success = await _daprClient.TrySaveStateAsync<string>("pluggable-postgres", key, updatedValue, wrongEtag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
       
        Assert.False(success);
    }

    [Fact]
    public async Task SequentialUpdatesCantUseOldEtags()
    {
        var key = "What-Comes-First";
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

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
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>("pluggable-postgres", key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        await _daprClient.DeleteStateAsync("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Multiple(
            () => Assert.Equal(seedValue, firstGet),
            () => Assert.Null(secondGet)
        );
    }

    [Fact]
    public async Task DeleteWithEtag()
    {
        var key = "What-Comes-First";
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>("pluggable-postgres", key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var success = await _daprClient.TryDeleteStateAsync("pluggable-postgres", key, etag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Multiple(
            () => Assert.True(success),
            () => Assert.Null(secondGet)
        );
    }

    [Fact]
    public async Task DeleteWithWrongEtagDoesNotDelete()
    {
        var key = "What-Comes-First";
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>("pluggable-postgres", key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>("pluggable-postgres", key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var wrongEtag = $"{etag}-is-now-mismatched";
        var success = await _daprClient.TryDeleteStateAsync("pluggable-postgres", key, wrongEtag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
       
        Assert.Multiple(
            () => Assert.Equal(seedValue, firstGet),
            () => Assert.False(success)
        );
    }

    [Fact]
    public async Task ParallelUpsertOperationsAcrossUniqueTenants()
    {
        IEnumerable<(string,string)> produceTestData(int upto)
        {
            for (int i = 0; i < upto; i ++)
            {
                yield return (i.ToString(), $"{i} some data to save");
            }
        }
    
        var cts = new CancellationTokenSource();
        var options = new ParallelOptions() { MaxDegreeOfParallelism = 50, CancellationToken = cts.Token };
        await Parallel.ForEachAsync(produceTestData(1000), options, async (input, token) =>
        {
            string tenantId = Guid.NewGuid().ToString();
            var metadata = tenantId.AsMetaData();
            await _daprClient.SaveStateAsync<string>("pluggable-postgres", input.Item1, input.Item2, metadata: metadata, cancellationToken: token);
            var get = await _daprClient.GetStateAsync<string>("pluggable-postgres", input.Item1, metadata: metadata);
            if (input.Item2 != get)
                Assert.Fail("get value did not match what was persisted");
        });
           
        Assert.True(true);
    }

    [Fact]
    public async Task ParallelUpsertOperationsOnSingleTenant()
    {
        var tenantId = Guid.NewGuid().ToString();
        var metadata = tenantId.AsMetaData();
       
        /*  PROBE
                This is a probe to warm up the tenant, otherwise 
                the table won't exist in some requests due to READ COMMITED transaction isolation.
                The quick fix is to warm up the tenant before hitting it with lots of parallelism.
                Maybe a better long term solution is to move to SERIALIZABLE isolation, but this
                will come with a performance degredation. */

        await _daprClient.SaveStateAsync<string>("pluggable-postgres", "probe", "probe", metadata: metadata);
        await Task.Delay(500);

        /*  END PROBE */

        var input = new List<(string, string)>();        
        foreach(var i in Enumerable.Range(0, 1000))
        {
            input.Add((i.ToString(), $"{i} some data to save"));
        }
    
        var cts = new CancellationTokenSource();
        var options = new ParallelOptions() { MaxDegreeOfParallelism = 50, CancellationToken = cts.Token };
        await Parallel.ForEachAsync(input, options, async (input, token) =>
        {
            await _daprClient.SaveStateAsync<string>("pluggable-postgres", input.Item1, input.Item2, metadata: metadata, cancellationToken: token);
            var get = await _daprClient.GetStateAsync<string>("pluggable-postgres", input.Item1, metadata: metadata);
            if (input.Item2 != get)
                Assert.Fail("get value did not match what was persisted");
        });
              
        Assert.True(true);
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