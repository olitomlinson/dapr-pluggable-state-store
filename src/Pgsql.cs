using Npgsql;

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
                CREATE EXTENSION IF NOT EXISTS ""uuid-ossp"";";
            
            _logger.LogDebug($"CreateSchemaAsync - {sql}");
            
            await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            await cmd.ExecuteNonQueryAsync();

            _logger.LogDebug($"Schema Created : [{_SafeSchema}]");
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

            _logger.LogDebug($"CreateTableAsync - {sql}");

            await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            await cmd.ExecuteNonQueryAsync();

            _logger.LogDebug($"Table Created : [{SchemaAndTable}]");
        }


        private string Safe(string input)
        {
            // Postgres needs any object starting with a non-alpha to be wrapped in double-qoutes
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

            _logger.LogInformation($"GetAsync:  key: [{key}], value: [{value}], sql: [{sql}]");

            await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            {
                cmd.Parameters.AddWithValue("key", key);
                await using (var reader = await cmd.ExecuteReaderAsync())
                while (await reader.ReadAsync())
                {
                    value = reader.GetString(1);
                    etag = reader.GetString(2);
                    _logger.LogDebug("key: {0}, value: {1}, etag : {2}", reader.GetString(0), value, etag);
                    return new Tuple<string,string>(value, etag);
                }
            }
            return new Tuple<string,string>(null,null);
        }

        public async Task UpsertAsync(string key, string value, string etag, NpgsqlTransaction transaction = null)
        {
    
            // this is an optimisation, which I will probably remove when I eventually support First-Write-Wins.
            if (string.IsNullOrEmpty(etag))
            {
                await CreateSchemaIfNotExistsAsync(transaction); 
                await CreateTableIfNotExistsAsync(transaction); 
            }

            await InsertOrUpdateAsync(key, value, etag, transaction);
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
                    ,uuid_generate_v4()::text
                )
                ON CONFLICT (key)
                DO
                UPDATE SET 
                    value = @2
                    ,etag = uuid_generate_v4()::text
                    ,updatedate = NOW()
                ;";

                _logger.LogDebug($"({correlationId}) Etag not present - InsertOrUpdateAsync : key: [{key}], value: [{value}], sql: [{query}]");

                await using (var cmd = new NpgsqlCommand(query, _connection, transaction))
                {
                    cmd.Parameters.AddWithValue("1", NpgsqlTypes.NpgsqlDbType.Text, key);
                    cmd.Parameters.AddWithValue("2", NpgsqlTypes.NpgsqlDbType.Jsonb, value);

                    rowsAffected = await cmd.ExecuteNonQueryAsync();
                    _logger.LogDebug($"({correlationId}) {rowsAffected} Row inserted/updated");
                }
            }
            else
            {
                var query = @$"
                UPDATE {SchemaAndTable} 
                SET 
                    value = @2
                    ,updatedate = NOW()
                    ,etag = uuid_generate_v4()::text
                WHERE 
                    key = (@1)
                    AND 
                    etag = (@3)
                ;";
                
                _logger.LogDebug($"({correlationId}) Etag present - InsertOrUpdateAsync : key: [{key}], value: [{value}], etag: [{etag}], sql: [{query}]");

                await using (var cmd = new NpgsqlCommand(query, _connection, transaction))
                {
                    cmd.Parameters.AddWithValue("1", NpgsqlTypes.NpgsqlDbType.Text, key);
                    cmd.Parameters.AddWithValue("2", NpgsqlTypes.NpgsqlDbType.Jsonb, value);
                    cmd.Parameters.AddWithValue("3", NpgsqlTypes.NpgsqlDbType.Text, etag);

                    rowsAffected = await cmd.ExecuteNonQueryAsync();
                    _logger.LogDebug($"({correlationId}) {rowsAffected} Row updated");
                }
            }

             if (rowsAffected == 0 && !string.IsNullOrEmpty(etag))
             {
                _logger.LogDebug($"({correlationId}) Etag present but no rows modified, throwing EtagMismatchException");
                throw new Dapr.PluggableComponents.Components.StateStore.ETagMismatchException();
             }
        }

        public async Task DeleteRowAsync(string key, string etag, NpgsqlTransaction transaction = null)
        {
            // TODO this is vulenerable to sql-injection as-is, need to try converting to a proc because
            // you can't use parameters in code blocks like below.
            var correlationId = Guid.NewGuid().ToString("N").Substring(23);   
            var rowsDeleted = 0;

            if (string.IsNullOrEmpty(etag))
            {
                var sql = @$"
                DO $$
                BEGIN 
                    IF EXISTS
                        ( SELECT 1
                        FROM   information_schema.tables 
                        WHERE  table_schema = '{_schema}'
                        AND    table_name = '{_table}'
                        )
                    THEN
                        DELETE FROM {SchemaAndTable}
                        WHERE 
                            key = '{key}';
                    END IF;
                END $$;";

                _logger.LogDebug($"({correlationId}) Etag not present - DeleteRowAsync: key: [{key}], sql: [{sql}]");

                await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
                {
                    rowsDeleted = await cmd.ExecuteNonQueryAsync();
                    _logger.LogDebug($"({correlationId}) {rowsDeleted} Row/s deleted");
                }

            }
            else
            {

                // this method incorrectly reports the rows affected as always -1. 
                // This is due to the code executing as an anonymous block.
                // This needs changing from an anonymous block to a real named function,
                // so that return value can be used to return ROW_COUNT;
                var sql = @$"
                DO $$
                BEGIN 
                    IF EXISTS
                        ( SELECT 1
                        FROM   information_schema.tables 
                        WHERE  table_schema = '{_schema}'
                        AND    table_name = '{_table}'
                        )
                    THEN
                        DELETE FROM {SchemaAndTable}
                        WHERE 
                            key = '{key}'
                            AND
                            etag = '{etag}';
                            --GET DIAGNOSTICS del_count = ROW_COUNT;
                            --RETURN count;
                    END IF;
                END $$;";

                _logger.LogDebug($"({correlationId}) Etag present - DeleteRowAsync: key: [{key}], etag: [{etag}], sql: [{sql}]");

                await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
                {
                    rowsDeleted = await cmd.ExecuteNonQueryAsync();
                    _logger.LogDebug($"({correlationId}) {rowsDeleted} Row/s deleted");
                }
            }

            if (rowsDeleted < 0 && !string.IsNullOrEmpty(etag))
            {
                _logger.LogDebug($"({correlationId}) Etag present but no rows deleted, throwing EtagMismatchException");
                throw new Dapr.PluggableComponents.Components.StateStore.ETagMismatchException();
            }
        }
    }
}


public static class PostgresExtensions{
    public static bool TableDoesNotExist(this PostgresException ex){
        return (ex.SqlState == "42P01");
    }
}
