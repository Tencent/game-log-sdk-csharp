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
    private bool _disposed;

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
        // Initial resolution (synchronous)
        UpdateEndpoints();

        if (_currentEndpoints.Count == 0)
        {
            throw new TglogException(TglogErrorCode.NewConnFailed,
                $"Failed to resolve host: {_host}");
        }

        // Start periodic DNS updates
        var interval = TimeSpan.FromSeconds(_config.DnsUpdateIntervalSec);
        _timer = new Timer(_ => UpdateEndpoints(), null, interval, interval);

        _logger.LogInformation("Started DNS resolver for {Host} (interval: {IntervalSec}s)", _host, _config.DnsUpdateIntervalSec);
    }

    /// <summary>Stops the endpoint manager.</summary>
    public void Stop()
    {
        _timer?.Dispose();
        _timer = null;
        _logger.LogInformation("Stopped DNS resolver");
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        Stop();
    }

    private void UpdateEndpoints()
    {
        try
        {
            var newEndpoints = ResolveHost();

            if (!EndpointsEqual(_currentEndpoints, newEndpoints))
            {
                _logger.LogInformation($"Endpoints changed for {_host}: {string.Join(", ", _currentEndpoints)} -> {string.Join(", ", newEndpoints)}");

                _currentEndpoints = newEndpoints;
                NotifyListeners(newEndpoints);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError("DNS resolution failed for {Host}: {Message}", _host, ex.Message);
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
