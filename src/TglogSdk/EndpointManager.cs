using System.Linq;
using System.Net;
using Microsoft.Extensions.Logging;

namespace Tencent.Tglog;

/// <summary>
/// Manages DNS resolution and endpoint change detection.
/// Periodically resolves the configured hostname and notifies listeners when endpoints change.
/// </summary>
internal sealed class EndpointManager : IDisposable
{
    private readonly string _host;
    private readonly int _port;
    private readonly TglogConfig _config;
    private readonly ILogger _logger;

    private Timer? _timer;
    private volatile List<IPEndPoint> _currentEndpoints = new();
    private readonly List<Action<IReadOnlyList<IPEndPoint>>> _changeListeners = new();
    private volatile bool _stopped;
    private int _updating;

    public EndpointManager(TglogConfig config, ILoggerFactory loggerFactory)
    {
        _host = config.Host;
        _port = config.Port;
        _config = config;
        _logger = loggerFactory.CreateLogger("TGLog.EndpointManager");
    }

    /// <summary>Current resolved endpoints (read-only snapshot).</summary>
    public IReadOnlyList<IPEndPoint> CurrentEndpoints => _currentEndpoints;

    /// <summary>Registers a listener to be called when endpoints change.</summary>
    public void AddChangeListener(Action<IReadOnlyList<IPEndPoint>> listener)
    {
        _changeListeners.Add(listener);
    }

    /// <summary>Starts the endpoint manager: performs initial resolution and begins periodic updates.</summary>
    public void Start()
    {
        // Initial resolution (synchronous) — failures propagate so startup reports the real cause.
        _currentEndpoints = ResolveHost();

        _logger.LogInformation("Initial DNS resolution for {Host} -> {Count} endpoint(s)", _host, _currentEndpoints.Count);

        // Start periodic DNS updates
        var interval = TimeSpan.FromSeconds(_config.DnsUpdateIntervalSec);
        _timer = new Timer(_ => UpdateEndpoints(), null, interval, interval);

        _logger.LogInformation("Started DNS resolver for {Host} (interval: {IntervalSec}s)", _host, _config.DnsUpdateIntervalSec);
    }

    /// <summary>Stops the endpoint manager.</summary>
    public void Stop()
    {
        if (_stopped)
            return;

        _stopped = true;
        _timer?.Dispose();
        _timer = null;
        _logger.LogInformation("Stopped DNS resolver");
    }

    public void Dispose()
    {
        Stop();
    }

    private void UpdateEndpoints()
    {
        if (_stopped || Interlocked.Exchange(ref _updating, 1) != 0)
            return;

        try
        {
            var newEndpoints = ResolveHost();
            if (_stopped)
                return;

            var currentEndpoints = _currentEndpoints;
            if (!EndpointsEqual(currentEndpoints, newEndpoints))
            {
                _logger.LogInformation($"Endpoints changed for {_host}: {string.Join(", ", currentEndpoints)} -> {string.Join(", ", newEndpoints)}");

                _currentEndpoints = newEndpoints;
                NotifyListeners(newEndpoints);
            }
        }
        catch (Exception ex)
        {
            if (!_stopped)
                _logger.LogError("DNS resolution failed for {Host}: {Message}", _host, ex.Message);
        }
        finally
        {
            Volatile.Write(ref _updating, 0);
        }
    }

    private List<IPEndPoint> ResolveHost()
    {
        var addresses = Dns.GetHostAddresses(_host);
        var endpoints = new List<IPEndPoint>(addresses.Length);

        foreach (var addr in addresses)
            endpoints.Add(new IPEndPoint(addr, _port));

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug($"Resolved {_host} to {endpoints.Count} endpoint(s): {string.Join(", ", endpoints)}");

        return endpoints;
    }

    private static bool EndpointsEqual(List<IPEndPoint> a, List<IPEndPoint> b)
    {
        if (a.Count != b.Count) return false;
        var setA = new HashSet<string>(a.Select(ep => ep.ToString()));
        var setB = new HashSet<string>(b.Select(ep => ep.ToString()));
        return setA.SetEquals(setB);
    }

    private void NotifyListeners(List<IPEndPoint> newEndpoints)
    {
        var readOnly = newEndpoints.AsReadOnly();
        foreach (var listener in _changeListeners)
        {
            try
            {
                listener(readOnly);
            }
            catch (Exception ex)
            {
                _logger.LogError("Listener exception: {Message}", ex.Message);
            }
        }
    }
}
