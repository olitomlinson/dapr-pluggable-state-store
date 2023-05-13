using Npgsql;

public class ExpiredDataCleanUpService : IHostedService, IDisposable
{
    private int _sequence = 0;
    private readonly ILogger<ExpiredDataCleanUpService> _logger;
    private Timer? _timer = null;

    private PluggableStateStoreHelpers _helpers = null;

    public ExpiredDataCleanUpService(ILogger<ExpiredDataCleanUpService> logger, PluggableStateStoreHelpers helpers)
    {
        _logger = logger;
        _helpers = helpers;
    }

    public Task StartAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Expired Data Clean Up Service running.");

        _timer = new Timer(DoWork, null, TimeSpan.Zero,
            TimeSpan.FromSeconds(5));

        return Task.CompletedTask;
    }

    private void DoWork(object? state)
    {
        var seq = Interlocked.Increment(ref _sequence);

        if (_helpers.Count == 0)
            _logger.LogInformation("Expired Data Clean Up is working. No registered State Stores. Seq: {seq}", seq);
        else
            foreach (var helper in _helpers) {
                _logger.LogInformation("Expired Data Clean Up is working. Store Found: {Key}, Seq: {seq}", helper.Key, seq);
            }

        var store = _helpers.FirstOrDefault();

        string sql = 
            @$"
            SELECT 
                tenant_id, schema_id, table_id
            FROM 
                ""pluggable_metadata"".""tenant"" 
            ORDER BY 
                last_expired_at ASC NULLS FIRST
            LIMIT 1;
            ";

        var cs = store.Value?.GetDatabaseConnectionString();
        if (!string.IsNullOrEmpty(cs))
        {
            var connection = new NpgsqlConnection(cs);
            connection.Open();  

            List<string> tenantIdsToDelete = new List<string>();
            using (var cmd = new NpgsqlCommand(sql, connection, null))
            {
                using (var reader = cmd.ExecuteReader())
                while (reader.Read())
                {
                    var schemaAndTenant = reader.GetString(0);
                    var schemaId = reader.GetString(1);
                    var tableId = reader.GetString(2);
                   
                    _logger.LogInformation($"tenant : {schemaAndTenant}, schema: {schemaId}, table: {tableId}");
                    tenantIdsToDelete.Add(schemaAndTenant);
                }
            }

            foreach(var tenantId in tenantIdsToDelete)
            {
                var rowsAffected = DeleteFromTable(tenantId, connection);
                rowsAffected = UpdateLastDelete(tenantId, connection);
            }

            connection.Close();
        }
    }

    private int UpdateLastDelete(string schemaAndTable, NpgsqlConnection connection)
    {
        var query = @$"
            UPDATE ""pluggable_metadata"".""tenant"" 
            SET 
                last_expired_at = CURRENT_TIMESTAMP
            WHERE 
                tenant_id = '{schemaAndTable}'
            ;";

            using (var cmd = new NpgsqlCommand(query, connection, null))
            {
                return cmd.ExecuteNonQuery();       
            }
    }

    private int DeleteFromTable(string schemaAndTable, NpgsqlConnection connection)
    {
        var sql = $"DELETE FROM {schemaAndTable} WHERE expiredate IS NOT NULL AND expiredate < CURRENT_TIMESTAMP";
        using (var cmd = new NpgsqlCommand(sql, connection, null))
        {
            var rowsAffected = cmd.ExecuteNonQuery();
            _logger.LogInformation($"rows deleted from '{schemaAndTable}': {rowsAffected}");
            return rowsAffected;
        }
    }
    public Task StopAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Expired Data Clean Up Service is stopping.");

        _timer?.Change(Timeout.Infinite, 0);

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _timer?.Dispose();
    }
}
