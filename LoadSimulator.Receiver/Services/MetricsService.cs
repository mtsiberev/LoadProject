using System.Diagnostics;

namespace LoadSimulator.Receiver.Services;

public class MetricsService : IMetricsService
{
    private long _totalProcessed = 0;
    private long _totalErrors = 0;

    private long _lastSecondProcessed = 0;
    private long _currentRps = 0;

    private long _totalTicksElapsed = 0;
    private long _lastSecondTicksElapsed = 0;
    private long _currentAverageLatencyMs = 0;
    private long _currentHeapMemoryMb = 0;

    private readonly Timer _rpsTimer;
    private const long MaxRpsThreshold = 100_000;

    public MetricsService()
    {
        _rpsTimer = new Timer(_ =>
        {
            long processedInLastSecond = Interlocked.Exchange(ref _lastSecondProcessed, 0);
            Interlocked.Exchange(ref _currentRps, processedInLastSecond);

            long ticksInLastSecond = Interlocked.Exchange(ref _lastSecondTicksElapsed, 0);
            if (processedInLastSecond > 0)
            {
                double averageMs = (double)ticksInLastSecond / processedInLastSecond / Stopwatch.Frequency * 1000;
                Interlocked.Exchange(ref _currentAverageLatencyMs, (long)averageMs);
            }
            else
            {
                Interlocked.Exchange(ref _currentAverageLatencyMs, 0);
            }

            long heapBytes = GC.GetTotalMemory(forceFullCollection: false);
            Interlocked.Exchange(ref _currentHeapMemoryMb, heapBytes / (1024 * 1024));

            if (processedInLastSecond > 0)
            {
                Console.WriteLine($"[Сервер] Нагрузка: {processedInLastSecond} RPS | " +
                    $"Latency: {Interlocked.Read(ref _currentAverageLatencyMs)} ms | " +
                    $"Память кучи: {Interlocked.Read(ref _currentHeapMemoryMb)} MB | " +
                    $"Ошибок: {Interlocked.Read(ref _totalErrors)}");
            }
        }, null, 1000, 1000);
    }

    public bool IsRateLimitExceeded()
    {
        return Interlocked.Read(ref _lastSecondProcessed) > MaxRpsThreshold;
    }

    public void TrackRequest(long elapsedTicks, bool isSuccess)
    {
        Interlocked.Increment(ref _lastSecondProcessed);
        Interlocked.Add(ref _lastSecondTicksElapsed, elapsedTicks);

        if (isSuccess)
        {
            Interlocked.Increment(ref _totalProcessed);
            Interlocked.Add(ref _totalTicksElapsed, elapsedTicks);
        }
        else
        {
            Interlocked.Increment(ref _totalErrors);
        }
    }

    public void Reset()
    {
        Interlocked.Exchange(ref _totalProcessed, 0);
        Interlocked.Exchange(ref _totalErrors, 0);
        Interlocked.Exchange(ref _lastSecondProcessed, 0);
        Interlocked.Exchange(ref _currentRps, 0);
        Interlocked.Exchange(ref _totalTicksElapsed, 0);
        Interlocked.Exchange(ref _lastSecondTicksElapsed, 0);
        Interlocked.Exchange(ref _currentAverageLatencyMs, 0);
        Interlocked.Exchange(ref _currentHeapMemoryMb, 0);
    }
}