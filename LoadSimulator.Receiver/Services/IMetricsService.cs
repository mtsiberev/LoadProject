namespace LoadSimulator.Receiver.Services;

public interface IMetricsService
{
    void TrackRequest(long elapsedTicks, bool isSuccess);
    bool IsRateLimitExceeded();
    void Reset();
}
