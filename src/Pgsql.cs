using Npgsql;
using System.Collections.Concurrent;

namespace Helpers
{
    public class Pgsql
    {
        private readonly string _SafeSchema;
        private readonly string _SafeTable;
        private readonly string _schema;
        private readonly string _table;
        private ILogger _logger;
        private readonly NpgsqlConnection _connection;

        /*  DATABASE RESOURCE LOCKS 
                These locks are important to prevent a storm of resource creation 
                on a highly parallel cold start. Yes, this does cause a performance 
                degredation but this provides a more predictable & stable experience,
                without having to run all database transactions in SERIALIZABLE isolation mode.
                
                Risk : The dictionaries will grow due to the unbounded nature of tenants.
                To mitigate this, the dictionarys could potentially be replaced with MemoryCache
                objects. (Assuming modern MemoryCache in .NET 6 is good to go)
        */
        static ConcurrentDictionary<string, object> _locks = new ConcurrentDictionary<string, object>();
        static ConcurrentDictionary<string, string> _resources = new ConcurrentDictionary<string, string>();

        public Pgsql(string schema, string table, NpgsqlConnection connection, ILogger logger)
        {
            if (string.IsNullOrEmpty(schema))
                throw new ArgumentException("'schema' is not set");
            _SafeSchema = Safe(schema);
            _schema = schema;

            if (string.IsNullOrEmpty(table))
                throw new ArgumentException("'table' is not set");
            _SafeTable = Safe(table);
            _table = table;

            _logger = logger;

            _connection = connection;
        }

        public async Task CreateSchemaIfNotExistsAsync(NpgsqlTransaction transaction = null)
        {
            var sql = 
                @$"CREATE SCHEMA IF NOT EXISTS {_SafeSchema} 
                AUTHORIZATION postgres;
                
                CREATE OR REPLACE FUNCTION {_SafeSchema}.delete_key_with_etag_v1(
                    tbl regclass,
                    keyvalue text,
                    etagvalue text,
                    OUT success boolean)
                    RETURNS boolean
                    LANGUAGE 'plpgsql'
                    COST 100
                    VOLATILE PARALLEL UNSAFE
                AS $BODY$     
                BEGIN
                EXECUTE format('
                    DELETE FROM %s
                    WHERE  key = $1 AND etag = $2
                    RETURNING TRUE', tbl)
                USING   keyvalue, etagvalue
                INTO    success;
                RETURN;  -- optional in this case
                END
                $BODY$;

                ALTER FUNCTION {_SafeSchema}.delete_key_with_etag_v1(regclass, text, text)
                    OWNER TO postgres;

                CREATE OR REPLACE FUNCTION {_SafeSchema}.delete_key_v1(
                    tbl regclass,
                    keyvalue text,
                    OUT success boolean)
                    RETURNS boolean
                    LANGUAGE 'plpgsql'
                    COST 100
                    VOLATILE PARALLEL UNSAFE
                AS $BODY$     
                BEGIN
                EXECUTE format('
                    DELETE FROM %s
                    WHERE  key = $1
                    RETURNING TRUE', tbl)
                USING   keyvalue
                INTO    success;
                RETURN;  -- optional in this case
                END
                $BODY$;

                ALTER FUNCTION {_SafeSchema}.delete_key_v1(regclass, text)
                    OWNER TO postgres;
                ";
            
            _logger.LogDebug($"{nameof(CreateSchemaIfNotExistsAsync)} - {sql}");
            
            await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            await cmd.ExecuteNonQueryAsync();

            _logger.LogDebug($"{nameof(CreateSchemaIfNotExistsAsync)} - Schema Created : {_SafeSchema}");
        }
        
        public async Task CreateTableIfNotExistsAsync(NpgsqlTransaction transaction = null)
        {
            var sql = 
                @$"CREATE TABLE IF NOT EXISTS {SchemaAndTable} 
                ( 
                    key text NOT NULL PRIMARY KEY COLLATE pg_catalog.""default"" 
                    ,value text
                    ,etag text
                    ,insertdate TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                    ,updatedate TIMESTAMP WITH TIME ZONE NULL
                ) 
                TABLESPACE pg_default; 
                ALTER TABLE IF EXISTS {SchemaAndTable} OWNER to postgres;";

            _logger.LogDebug($"{nameof(CreateTableIfNotExistsAsync)} - SQL : [{sql}]");

            await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            await cmd.ExecuteNonQueryAsync();

            _logger.LogDebug($"{nameof(CreateTableIfNotExistsAsync)} - Table Created : {SchemaAndTable}");
        }


        private string Safe(string input)
        {
            return $"\"{input}\"";
        }

        public string SchemaAndTable 
        { 
            get {
                return $"{_SafeSchema}.{_SafeTable}";
            }
        }

        public async Task<Tuple<string,string>> GetAsync(string key, NpgsqlTransaction transaction = null)
        {
            string value = "";
            string etag = "";
            string sql = 
                @$"SELECT 
                    key
                    ,value
                    ,etag
                    
                FROM {SchemaAndTable} 
                WHERE 
                    key = (@key)";

            _logger.LogInformation($"{nameof(GetAsync)} - key: [{key}], value: [{value}], sql: [{sql}]");

            await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            {
                cmd.Parameters.AddWithValue("key", key);
                await using (var reader = await cmd.ExecuteReaderAsync())
                while (await reader.ReadAsync())
                {
                    value = reader.GetString(1);
                    etag = reader.GetString(2);
                    _logger.LogDebug($"{nameof(GetAsync)} - Result - key: {reader.GetString(0)}, value: {value}, etag : {etag}");
                    return new Tuple<string,string>(value, etag);
                }
            }
            return new Tuple<string,string>(null,null);
        }

        public async Task UpsertAsync(string key, string value, string etag, NpgsqlTransaction transaction = null)
        {
            EnsureDatabaseResourcesExist(transaction);
            await InsertOrUpdateAsync(key, value, etag, transaction);
        }

        private void EnsureDatabaseResourcesExist(NpgsqlTransaction transaction = null)
        {
            GateAccessToResourceCreation($"S:{_schema}", () => CreateSchemaIfNotExistsAsync(transaction).Wait());
            GateAccessToResourceCreation($"T:{_table}", () => CreateTableIfNotExistsAsync(transaction).Wait());
        }

        private void GateAccessToResourceCreation(string resourceName, Action resourceFactory)
        {
            if (!_resources.TryGetValue(resourceName, out string _))
            {   
                var _lock = _locks.GetOrAdd(resourceName, (x) => { return new Object(); });
                lock (_lock) 
                {
                    if (!_resources.TryGetValue(resourceName, out string _))
                    { 
                        resourceFactory();
                        _resources.TryAdd(resourceName, DateTime.UtcNow.ToString());
                    }
                }
            }
        }

        public async Task InsertOrUpdateAsync(string key, string value, string etag, NpgsqlTransaction transaction = null)
        {
            int rowsAffected = 0;  
            var correlationId = Guid.NewGuid().ToString("N").Substring(23);   

            if (String.IsNullOrEmpty(etag))
            {
                var query = @$"INSERT INTO {SchemaAndTable} 
                (
                    key
                    ,value
                    ,etag
                ) 
                VALUES 
                (
                    @1 
                    ,@2
                    ,gen_random_uuid()::text
                )
                ON CONFLICT (key)
                DO
                UPDATE SET 
                    value = @2
                    ,etag = gen_random_uuid()::text
                    ,updatedate = NOW()
                ;";

                _logger.LogDebug($"{nameof(InsertOrUpdateAsync)} ({correlationId}) - Etag not present - key: [{key}], value: [{value}], sql: [{query}]");

                await using (var cmd = new NpgsqlCommand(query, _connection, transaction))
                {
                    cmd.Parameters.AddWithValue("1", NpgsqlTypes.NpgsqlDbType.Text, key);
                    cmd.Parameters.AddWithValue("2", NpgsqlTypes.NpgsqlDbType.Jsonb, value);

                    rowsAffected = await cmd.ExecuteNonQueryAsync();
                    _logger.LogDebug($"{nameof(InsertOrUpdateAsync)} ({correlationId}) - Row inserted/updated: {rowsAffected} ");
                }
            }
            else
            {
                var query = @$"
                UPDATE {SchemaAndTable} 
                SET 
                    value = @2
                    ,updatedate = NOW()
                    ,etag = gen_random_uuid()::text
                WHERE 
                    key = (@1)
                    AND 
                    etag = (@3)
                ;";
                
                _logger.LogDebug($"{nameof(InsertOrUpdateAsync)} ({correlationId}) - Etag present - key: [{key}], value: [{value}], etag: [{etag}], sql: [{query}]");

                await using (var cmd = new NpgsqlCommand(query, _connection, transaction))
                {
                    cmd.Parameters.AddWithValue("1", NpgsqlTypes.NpgsqlDbType.Text, key);
                    cmd.Parameters.AddWithValue("2", NpgsqlTypes.NpgsqlDbType.Jsonb, value);
                    cmd.Parameters.AddWithValue("3", NpgsqlTypes.NpgsqlDbType.Text, etag);

                    rowsAffected = await cmd.ExecuteNonQueryAsync();
                    _logger.LogDebug($"{nameof(InsertOrUpdateAsync)} ({correlationId}) - Row updated: {rowsAffected}");
                }
            }

            if (rowsAffected == 0 && !string.IsNullOrEmpty(etag))
            {
                _logger.LogDebug($"{nameof(InsertOrUpdateAsync)} ({correlationId}) - Etag present but no rows modified, throwing EtagMismatchException");
                throw new Dapr.PluggableComponents.Components.StateStore.ETagMismatchException();
            }
        }

        public async Task DeleteAsync(string key, string etag, NpgsqlTransaction transaction = null)
        {      
            var sql = "";
            if (string.IsNullOrEmpty(etag))
                sql = $"SELECT * FROM {_schema}.delete_key_v1(tbl := '{_table}', keyvalue := '{key}')";
            else
                sql = $"SELECT * FROM {_schema}.delete_key_with_etag_v1(tbl := '{_table}', keyvalue := '{key}', etagvalue := '{etag}')";
            _logger.LogDebug($"{nameof(DeleteAsync)} - Sql : [{sql}]");

            using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            {
                cmd.Parameters.Add(new NpgsqlParameter("success", System.Data.DbType.Boolean) { Direction = System.Data.ParameterDirection.Output });
                var result = await cmd.ExecuteScalarAsync();

                if (!string.IsNullOrEmpty(etag) && result is System.DBNull){
                    _logger.LogDebug($"{nameof(DeleteAsync)} - Etag present but no rows deleted, throwing EtagMismatchException");
                    throw new Dapr.PluggableComponents.Components.StateStore.ETagMismatchException();
                }
                else if (result is System.DBNull)
                    _logger.LogDebug($"{nameof(DeleteAsync)} - Result : DBnull");
                else if (result is true)
                    _logger.LogDebug($"{nameof(DeleteAsync)} - Result : {(bool)result}");
            }
        }
    }
}


public static class PostgresExtensions{
    public static bool TableDoesNotExist(this PostgresException ex){
        return (ex.SqlState == "42P01");
    }

    public static bool FunctionDoesNotExist( this PostgresException ex){
        return (ex.SqlState == "42883");
    }
}
