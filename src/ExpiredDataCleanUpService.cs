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
