namespace LoadSimulator.Sender.Services;

public interface ILoadGenerator
{
    Task GenerateLoadByConcurrencyAsync(
        int totalRequests,
        int maxParallelThreads,
        int startThreads,
        int rampUpDurationSeconds,
        CancellationToken ct = default);

    Task GenerateLoadByRpsAsync(
    int totalRequests,
    int maxParallelThreads,
    int startThreads,
    int rampUpDurationSeconds,
    CancellationToken ct = default);
}
