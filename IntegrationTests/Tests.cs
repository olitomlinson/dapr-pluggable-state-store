using Dapr.Client;
using Xunit.Abstractions;

namespace IntegrationTests;

[Collection("Sequence")]  
public class StateIsolationTests : IClassFixture<PluggableContainer>
{
    private readonly ITestOutputHelper output;
    private readonly PluggableContainer _pluggableContainer;
    private readonly DaprClient _daprClient;
    private Func<string> GetRandomKey;
    private static string _pluggableStore = "pluggable-postgres";
    private static string _InTreeStore = "standard-postgres";
    private Random _random = new Random();

    public static IEnumerable<object[]> StateStoresToTestAgainst(){
        yield return new object[] { _pluggableStore };
        yield return new object[] { _InTreeStore };
    }

    public StateIsolationTests(PluggableContainer pluggableContainer, ITestOutputHelper output)
    {
        _pluggableContainer = pluggableContainer;
        _daprClient = _pluggableContainer.GetDaprClient();
        this.output = output;
        GetRandomKey = () => {  return $"key-{_random.Next(1000000, 9999999)}"; };
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
        var key = GetRandomKey();
        var value = "Bar";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync(_pluggableStore, key, value, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        
        var retrievedState = await _daprClient.GetStateAsync<string>(_pluggableStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Equal(value, retrievedState);
    }

    [Fact]
    public async Task StateIsNotSharedAcrossTenants()
    {
        var key = GetRandomKey();
        var value = "Bar";
        var tenantId = Guid.NewGuid().ToString();
        var illegalTenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync(_pluggableStore, key, value, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var retrievedState = await _daprClient.GetStateAsync<string>(_pluggableStore, key, metadata: illegalTenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Null(retrievedState);
    }

    [Theory]
    [MemberData(nameof(StateStoresToTestAgainst))]
    public async Task ObjectsCanBeStoredAndRetrieved(string stateStoreName)
    {
        var key = GetRandomKey();
        var tenantId = Guid.NewGuid().ToString();
        var metadata = tenantId.AsMetaData();

        var seedValue = new TestClass() {
            TestStr = "foo",
            TestInt = 99999
        };

        await _daprClient.SaveStateAsync<TestClass>(stateStoreName, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var get = await _daprClient.GetStateAsync<TestClass>(stateStoreName, key, metadata: metadata);     

        Assert.Multiple(
            () => Assert.Equal(seedValue.TestInt, get.TestInt),
            () => Assert.Equal(seedValue.TestStr, get.TestStr)
        );
    }

    [Theory]
    [MemberData(nameof(StateStoresToTestAgainst))]
    public async Task ObjectUpdatesWithoutEtag(string stateStoreName)
    {
        var key = GetRandomKey();
        var tenantId = Guid.NewGuid().ToString();
        var metadata = tenantId.AsMetaData();

        var seedValue = new TestClass() {
            TestStr = "Chicken",
            TestInt = 99999
        };

        await _daprClient.SaveStateAsync<TestClass>(stateStoreName, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<TestClass>(stateStoreName, key, metadata: metadata);   

        var updatedValue = new TestClass {
            TestStr = seedValue.TestStr,
            TestInt = seedValue.TestInt
        };

        await _daprClient.SaveStateAsync<TestClass>(stateStoreName, key, updatedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<TestClass>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);  

        Assert.Multiple(
            () => Assert.Equal(seedValue.TestInt, firstGet.TestInt),
            () => Assert.Equal(seedValue.TestStr, firstGet.TestStr),
            () => Assert.Equal(updatedValue.TestStr, secondGet.TestStr),
            () => Assert.Equal(updatedValue.TestInt, secondGet.TestInt)
        );
    }

    [Theory]
    [MemberData(nameof(StateStoresToTestAgainst))]
    public async Task UpdatesWithoutEtag(string stateStoreName)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync(stateStoreName, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<string>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        await _daprClient.SaveStateAsync(stateStoreName, key, updatedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Multiple(
            () => Assert.Equal(seedValue,firstGet),
            () => Assert.Equal(updatedValue, secondGet)
        );
    }

    [Theory]
    [MemberData(nameof(StateStoresToTestAgainst))]
    public async Task UpdatesWithEtag(string stateStoreName)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(stateStoreName, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";

        var success = await _daprClient.TrySaveStateAsync<string>(stateStoreName, key, updatedValue, etag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
  
        Assert.Multiple(
            () => Assert.Equal(seedValue,firstGet),
            () => Assert.Equal(updatedValue, secondGet)
        );
    }

    [Theory]
    [MemberData(nameof(StateStoresToTestAgainst))]
    public async Task UpdatesAndEtagInvalidIsThrown(string stateStoreName)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(stateStoreName, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        var malformedEtag = $"not-a-valid-etag";

        await Assert.ThrowsAsync<Dapr.DaprException>(async () => { 
            await _daprClient.TrySaveStateAsync<string>(stateStoreName, key, updatedValue, malformedEtag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token); });
    }

    [Theory(Skip = "blocked by https://github.com/dapr/components-contrib/issues/2773")]
    [MemberData(nameof(StateStoresToTestAgainst))]
    public async Task UpdatesCantUseOldEtags(string stateStoreName)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(stateStoreName, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        await _daprClient.SaveStateAsync<string>(stateStoreName, key, updatedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
  
        updatedValue = "Goose";
        var success = await _daprClient.TrySaveStateAsync<string>(stateStoreName, key, updatedValue, etag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var thirdGet = await _daprClient.GetStateAsync<string>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
  
        Assert.Multiple(
            () => Assert.False(success),
            () => Assert.Equal(secondGet, thirdGet)
        );
    }

    [Theory]
    [MemberData(nameof(StateStoresToTestAgainst))]
    public async Task DeleteWithoutEtag(string stateStoreName)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(stateStoreName, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<string>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        await _daprClient.DeleteStateAsync(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Multiple(
            () => Assert.Equal(seedValue, firstGet),
            () => Assert.Null(secondGet)
        );
    }

    [Theory]
    [MemberData(nameof(StateStoresToTestAgainst))]
    public async Task DeleteWithEtag(string stateStoreName)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(stateStoreName, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var success = await _daprClient.TryDeleteStateAsync(stateStoreName, key, etag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Multiple(
            () => Assert.True(success),
            () => Assert.Null(secondGet)
        );
    }

    [Theory]
    [MemberData(nameof(StateStoresToTestAgainst))]
    public async Task DeleteWithWrongEtagDoesNotDelete(string stateStoreName)
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(stateStoreName, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(stateStoreName, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var wrongEtag = $"123";
        var success = await _daprClient.TryDeleteStateAsync(stateStoreName, key, wrongEtag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
       
        Assert.Multiple(
            () => Assert.Equal(seedValue, firstGet),
            () => Assert.False(success)
        );
    }

    [Theory]
    [MemberData(nameof(StateStoresToTestAgainst))]
    public async Task ParallelUpdatesAcrossUniqueTenants(string stateStoreName)
    {

        IEnumerable<(string,string)> produceTestData(int upto)
        {
            string randomSuffix = GetRandomKey();
            for (int i = 0; i < upto; i ++)
            {
                yield return ($" {i}+{randomSuffix}", $"{i} some data to save");
            }
        }
    
        var cts = new CancellationTokenSource();
        var options = new ParallelOptions() { MaxDegreeOfParallelism = 50, CancellationToken = cts.Token };
        await Parallel.ForEachAsync(produceTestData(1000), options, async (input, token) =>
        {
            string tenantId = Guid.NewGuid().ToString();
            var metadata = tenantId.AsMetaData();
            await _daprClient.SaveStateAsync<string>(stateStoreName, input.Item1, input.Item2, metadata: metadata, cancellationToken: token);
            var get = await _daprClient.GetStateAsync<string>(stateStoreName, input.Item1, metadata: metadata);
            if (input.Item2 != get)
                Assert.Fail("get value did not match what was persisted");
        });
           
        Assert.True(true);
    }

    [Theory]
    [MemberData(nameof(StateStoresToTestAgainst))]
    public async Task ParallelUpdatesOnSingleTenant(string stateStoreName)
    {
        var tenantId = Guid.NewGuid().ToString();
        var metadata = tenantId.AsMetaData();
       
        /*  PROBE
                This is a probe to warm up the tenant, otherwise 
                the table won't exist in some requests due to READ COMMITED transaction isolation.
                The quick fix is to warm up the tenant before hitting it with lots of parallelism.
                Maybe a better long term solution is to move to SERIALIZABLE isolation, but this
                will come with a performance degredation. */

        await _daprClient.SaveStateAsync<string>(stateStoreName, "probe", "probe", metadata: metadata);
        await Task.Delay(500);
        /*  END PROBE */

        IEnumerable<(string,string)> produceTestData(int upto)
        {
            string randomSuffix = GetRandomKey();
            for (int i = 0; i < upto; i ++)
            {
                yield return ($" {i}+{randomSuffix}", $"{i} some data to save");
            }
        }
    
        var cts = new CancellationTokenSource();
        var options = new ParallelOptions() { MaxDegreeOfParallelism = 50, CancellationToken = cts.Token };
        await Parallel.ForEachAsync(produceTestData(1000), options, async (input, token) =>
        {
            await _daprClient.SaveStateAsync<string>(stateStoreName, input.Item1, input.Item2, metadata: metadata, cancellationToken: token);
            var get = await _daprClient.GetStateAsync<string>(stateStoreName, input.Item1, metadata: metadata);
            if (input.Item2 != get)
                Assert.Fail("get value did not match what was persisted");
        });
              
        Assert.True(true);
    }

    
    [Fact(Skip = "Not yet implemented")]
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
public class TestClass 
{
    public string TestStr { get; set; }

    public int TestInt { get; set; }

}