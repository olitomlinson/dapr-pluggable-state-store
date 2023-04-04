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

    private string _stateStore;
    private Random _random = new Random();

    public StateIsolationTests(PluggableContainer pluggableContainer, ITestOutputHelper output)
    {
        _pluggableContainer = pluggableContainer;
        _daprClient = _pluggableContainer.GetDaprClient();
        this.output = output;
        _stateStore = "pluggable-postgres";
        //_stateStore = "standard-postgres";
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
        var ct = new CancellationTokenSource(5000).Token;

        var key = GetRandomKey();
        var value = "Bar";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync(_stateStore, key, value, metadata: tenantId.AsMetaData(), cancellationToken: ct);
        
        var retrievedState = await _daprClient.GetStateAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: ct);

        Assert.Equal(value, retrievedState);
    }

    [Fact]
    public async Task StateIsNotSharedAcrossTenants()
    {
        var ct = new CancellationTokenSource(5000).Token;

        var key = GetRandomKey();
        var value = "Bar";
        var tenantId = Guid.NewGuid().ToString();
        var illegalTenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync(_stateStore, key, value, metadata: tenantId.AsMetaData(), cancellationToken: ct);
        var retrievedState = await _daprClient.GetStateAsync<string>(_stateStore, key, metadata: illegalTenantId.AsMetaData(), cancellationToken: ct);

        Assert.Null(retrievedState);
    }

    [Fact]
    public async Task ObjectsCanBeStoredAndRetrieved()
    {
        var key = GetRandomKey();
        var tenantId = Guid.NewGuid().ToString();
        var metadata = tenantId.AsMetaData();

        var seedValue = new TestClass() {
            TestStr = "foo",
            TestInt = 99999
        };

        await _daprClient.SaveStateAsync<TestClass>(_stateStore, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var get = await _daprClient.GetStateAsync<TestClass>(_stateStore, key, metadata: metadata);     

        Assert.Multiple(
            () => Assert.Equal(seedValue.TestInt, get.TestInt),
            () => Assert.Equal(seedValue.TestStr, get.TestStr)
        );

        Assert.True(true);
    }

    [Fact]
    public async Task SequentialObjectUpdatesWithoutEtag()
    {
        var key = GetRandomKey();
        var tenantId = Guid.NewGuid().ToString();
        var metadata = tenantId.AsMetaData();

        var seedValue = new TestClass() {
            TestStr = "Chicken",
            TestInt = 99999
        };

        await _daprClient.SaveStateAsync<TestClass>(_stateStore, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<TestClass>(_stateStore, key, metadata: metadata);   

        var updatedValue = new TestClass {
            TestStr = seedValue.TestStr,
            TestInt = seedValue.TestInt
        };

        await _daprClient.SaveStateAsync<TestClass>(_stateStore, key, updatedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<TestClass>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);  

        Assert.Multiple(
            () => Assert.Equal(seedValue.TestInt, firstGet.TestInt),
            () => Assert.Equal(seedValue.TestStr, firstGet.TestStr),
            () => Assert.Equal(updatedValue.TestStr, secondGet.TestStr),
            () => Assert.Equal(updatedValue.TestInt, secondGet.TestInt)
        );

        Assert.True(true);
    }

    [Fact]
    public async Task SequentialUpdatesWithoutEtag()
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync(_stateStore, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        await _daprClient.SaveStateAsync(_stateStore, key, updatedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Multiple(
            () => Assert.Equal(seedValue,firstGet),
            () => Assert.Equal(updatedValue, secondGet)
        );
    }

    [Fact]
    public async Task SequentialUpdatesWithEtag()
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(_stateStore, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";

        var success = await _daprClient.TrySaveStateAsync<string>(_stateStore, key, updatedValue, etag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
  
        Assert.Multiple(
            () => Assert.Equal(seedValue,firstGet),
            () => Assert.Equal(updatedValue, secondGet)
        );
    }

    [Fact]
    public async Task SequentialUpdatesAndEtagMismatchIsThrown()
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(_stateStore, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        var wrongEtag = $"98765";

        var success = await _daprClient.TrySaveStateAsync<string>(_stateStore, key, updatedValue, wrongEtag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
       
        Assert.False(success);
    }

    [Fact(Skip = "throws a daprException which is likely wrong behaviour")]
    public async Task SequentialUpdatesAndEtagInvalidIsThrown()
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(_stateStore, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        var wrongEtag = $"not-a-valid-etag";

        var success = await _daprClient.TrySaveStateAsync<string>(_stateStore, key, updatedValue, wrongEtag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.False(success);
    }

    [Fact]
    public async Task SequentialUpdatesCantUseOldEtags()
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(_stateStore, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var updatedValue = "Egg";
        await _daprClient.SaveStateAsync<string>(_stateStore, key, updatedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
  
        updatedValue = "Goose";
        var success = await _daprClient.TrySaveStateAsync<string>(_stateStore, key, updatedValue, etag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var thirdGet = await _daprClient.GetStateAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
  
        Assert.Multiple(
            () => Assert.False(success),
            () => Assert.Equal(secondGet, thirdGet)
        );
    }

     [Fact]
    public async Task DeleteWithoutEtag()
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(_stateStore, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var firstGet = await _daprClient.GetStateAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        await _daprClient.DeleteStateAsync(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Multiple(
            () => Assert.Equal(seedValue, firstGet),
            () => Assert.Null(secondGet)
        );
    }

    [Fact]
    public async Task DeleteWithEtag()
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(_stateStore, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var success = await _daprClient.TryDeleteStateAsync(_stateStore, key, etag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var secondGet = await _daprClient.GetStateAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        Assert.Multiple(
            () => Assert.True(success),
            () => Assert.Null(secondGet)
        );
    }

    [Fact]
    public async Task DeleteWithWrongEtagDoesNotDelete()
    {
        var key = GetRandomKey();
        var seedValue = "Chicken";
        var tenantId = Guid.NewGuid().ToString();

        await _daprClient.SaveStateAsync<string>(_stateStore, key, seedValue, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
        var (firstGet, etag) = await _daprClient.GetStateAndETagAsync<string>(_stateStore, key, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);

        var wrongEtag = $"123456";
        var success = await _daprClient.TryDeleteStateAsync(_stateStore, key, wrongEtag, metadata: tenantId.AsMetaData(), cancellationToken: new CancellationTokenSource(5000).Token);
       
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
            await _daprClient.SaveStateAsync<string>(_stateStore, input.Item1, input.Item2, metadata: metadata, cancellationToken: token);
            var get = await _daprClient.GetStateAsync<string>(_stateStore, input.Item1, metadata: metadata);
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

        await _daprClient.SaveStateAsync<string>(_stateStore, "probe", "probe", metadata: metadata);
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
            await _daprClient.SaveStateAsync<string>(_stateStore, input.Item1, input.Item2, metadata: metadata, cancellationToken: token);
            var get = await _daprClient.GetStateAsync<string>(_stateStore, input.Item1, metadata: metadata);
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
public class TestClass 
{
    public string TestStr { get; set; }

    public int TestInt { get; set; }

}