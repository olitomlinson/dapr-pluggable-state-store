using Npgsql;

namespace Helpers
{
    public interface IPgsqlFactory
    {
        Pgsql Create(string schema, string table, NpgsqlConnection connection);
    }
    public class PgsqlFactory : IPgsqlFactory
    {
        private ILogger _logger;
        public PgsqlFactory(ILogger logger)
        {
            _logger = logger;
        }
        public Pgsql Create(string schema, string table, NpgsqlConnection connection)
        {
            return new Pgsql(schema, table, connection, _logger);
        }
    }
}
