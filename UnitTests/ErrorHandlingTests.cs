using Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NSubstitute;
using Google.Protobuf.Collections;
using Dapr.Client.Autogen.Grpc.v1;
using Npgsql;

namespace Tests;

[TestClass]
public class ErrorHandlingTests
{
    [TestMethod]
    [ExpectedException(typeof(InvalidOperationException),
    "Call 'InitAsync' firs")]
    public async Task MustCallInitBeforeUsingTheDatabaseHelper()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(pgsqlFactory, Substitute.For<ILogger>());

        var operationMetadata = new Dictionary<string,string>();
        
        h.TenantAwareDatabaseFactory?.Invoke(operationMetadata, null);
        Assert.Fail();
    }

    [TestMethod]
    [ExpectedException(typeof(StateStoreInitHelperException),
    "Mandatory component metadata property 'connectionString' is not set")]
    public async Task ConnectionStringIsNotSpecified()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(pgsqlFactory, Substitute.For<ILogger>());

        var componentMetadata = new Dictionary<string,string>();
        await h.InitAsync(componentMetadata);
    }

    [TestMethod]
    [ExpectedException(typeof(StateStoreInitHelperException),
    "Mandatory component metadata property 'tenant' is not set")]
    public async Task TenantModeIsNotSpecified()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(pgsqlFactory, Substitute.For<ILogger>());

        var componentMetadata = new Dictionary<string,string>(){
            {"connectionString",    "some-c-string"}
        };
        await h.InitAsync(componentMetadata);
    }

    [TestMethod]
    [ExpectedException(typeof(StateStoreInitHelperException),
    "'metadata.tenantId' value is not specified")]
    public async Task RequestFailsWhenNoTenantIdIsSpecified()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(pgsqlFactory, Substitute.For<ILogger>());

        var componentMetadata = new Dictionary<string,string>(){
            {"connectionString",    "some-c-string"},
            {"tenant",              "schema"}
        };
        await h.InitAsync(componentMetadata);

        var operationMetadata = new Dictionary<string, string>();
        h.TenantAwareDatabaseFactory?.Invoke(operationMetadata, null);
    }
}