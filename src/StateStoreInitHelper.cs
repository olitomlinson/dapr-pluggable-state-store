using Dapr.PluggableComponents.Components;
using Npgsql;

namespace Helpers
{
    public class StateStoreInitHelper
    {
        private const string TABLE_KEYWORD = "table";
        private const string SCHEMA_KEYWORD = "schema";
        private const string TENANT_KEYWORD = "tenant";
        private const string CONNECTION_STRING_KEYWORD = "connectionString";
        private const string DEFAULT_TABLE_NAME = "state";
        private const string DEFAULT_SCHEMA_NAME = "public";
        private IPgsqlFactory _pgsqlFactory;
        private ILogger _logger;
        public Func<IReadOnlyDictionary<string, string>, NpgsqlConnection, Pgsql>? TenantAwareDatabaseFactory { get; private set; }

        private string _connectionString;
        
        public StateStoreInitHelper(IPgsqlFactory pgsqlFactory, ILogger logger){
            _pgsqlFactory = pgsqlFactory;
            _logger = logger;
            TenantAwareDatabaseFactory = (_,_) => { throw new InvalidOperationException("Call 'InitAsync' first"); };
        }

        public string GetDatabaseConnectionString()
        {
            if (_connectionString == null)
                return "";
            else return _connectionString;
        }

        public async Task PerformDatabaseProbeAsync()
        {
            var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();   
            await using (var cmd = new NpgsqlCommand("SELECT 1", connection))
            {
                await using (var reader = await cmd.ExecuteReaderAsync())
                { }
            }
            await connection.CloseAsync();
        }

        public async Task<(Func<IReadOnlyDictionary<string,string>, Pgsql>, NpgsqlConnection)> GetDbFactory()
        {
            var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();
            Func<IReadOnlyDictionary<string,string>,Pgsql> factory = (metadata) => {
                return TenantAwareDatabaseFactory(metadata, connection);
            };   
            return (factory, connection);
        }

        public async Task InitAsync(IReadOnlyDictionary<string,string> componentMetadataProperties){
            
            (var isTenantAware, var tenantTarget) = IsTenantAware(componentMetadataProperties);        
            
            _connectionString = GetConnectionString(componentMetadataProperties);

            var defaultSchema = GetDefaultSchemaName(componentMetadataProperties);

            string defaultTable = GetDefaultTableName(componentMetadataProperties);  

            TenantAwareDatabaseFactory = 
                (operationMetadata, connection) => {
                    /* 
                        Why is this a func? 
                        Schema and Table are not known until a state operation is requested, 
                        as we rely on a combination on the component metadata and operation metadata,
                    */

                    if (!isTenantAware)
                        return _pgsqlFactory.Create(
                            defaultSchema, 
                            defaultTable, 
                            connection);
                    
                    var tenantId = GetTenantIdFromMetadata(operationMetadata);

                    
                    switch(tenantTarget){
                        case SCHEMA_KEYWORD :
                            return _pgsqlFactory.Create(
                                schema:             $"{tenantId}-{defaultSchema}", 
                                table:              defaultTable, 
                                connection); 
                        case TABLE_KEYWORD : 
                            return _pgsqlFactory.Create(
                                schema:             defaultSchema, 
                                table:              $"{tenantId}-{defaultTable}",
                                connection);
                        default:
                            throw new Exception("Couldn't instanciate the correct tenant-aware Pgsql wrapper");
                    }
                };
        }

        private (bool,string) IsTenantAware(IReadOnlyDictionary<string,string> properties){
            bool isTenantAware = (properties.TryGetValue(TENANT_KEYWORD, out string tenantTarget));
            if (isTenantAware && !(new string[]{ SCHEMA_KEYWORD, TABLE_KEYWORD }.Contains(tenantTarget)))
                throw new Exception($"Unsupported 'tenant' property value of '{tenantTarget}'. Use 'schema' or 'table' instead");
            
            return (isTenantAware, tenantTarget);
        }

        private string GetDefaultSchemaName(IReadOnlyDictionary<string,string> properties){
            if (!properties.TryGetValue(SCHEMA_KEYWORD, out string defaultSchema))
                defaultSchema = DEFAULT_SCHEMA_NAME;
            return defaultSchema;
        }

        private string GetDefaultTableName(IReadOnlyDictionary<string,string> properties){
           if (!properties.TryGetValue(TABLE_KEYWORD, out string defaultTable))
                defaultTable = DEFAULT_TABLE_NAME;
            return defaultTable;
        }

        private string GetConnectionString(IReadOnlyDictionary<string,string> properties){
            if (!properties.TryGetValue(CONNECTION_STRING_KEYWORD, out string connectionString))
                throw new StateStoreInitHelperException($"Missing connection string - 'metadata.{CONNECTION_STRING_KEYWORD} is a mandatory property'");
            return connectionString;
        }

        private string GetTenantIdFromMetadata(IReadOnlyDictionary<string, string> operationMetadata){
            operationMetadata.TryGetValue("tenantId", out string tenantId);   
            if (String.IsNullOrEmpty(tenantId))
                throw new StateStoreInitHelperException("Missing Tenant Id - 'metadata.tenantId' is a mandatory property");
            return tenantId;
        }
    }

    [System.Serializable]
    public class StateStoreInitHelperException : System.Exception
    {
        public StateStoreInitHelperException() { }
        public StateStoreInitHelperException(string message) : base(message) { }
        public StateStoreInitHelperException(string message, System.Exception inner) : base(message, inner) { }
        protected StateStoreInitHelperException(
            System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
}
