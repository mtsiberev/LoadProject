using LoadSimulator.Shared;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Channels;

namespace LoadSimulator.Sender.Services;

public class HttpLoadGenerator : ILoadGenerator
{
    private readonly HttpClient _httpClient;
    private SemaphoreSlim _currentSemaphore;

    public HttpLoadGenerator(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    /// <summary>
    /// Режим 1 (Concurrency-based)
    /// </summary>
    public async Task GenerateLoadByConcurrencyAsync(
        int totalRequests,
        int maxParallelThreads,
        int startThreads = 100,
        int rampUpDurationSeconds = 30,
        CancellationToken ct = default)
    {
        Console.WriteLine("[Генератор] Режим: Concurrency. Разгон от " + startThreads + " до " + maxParallelThreads + " потоков за " + rampUpDurationSeconds + "с.");

        long successCount = 0;
        long errorCount = 0;
        long lastSuccessCount = 0;
        int allowedThreads = startThreads;

        _currentSemaphore = new SemaphoreSlim(startThreads, maxParallelThreads);

        var jsonBytes = PreparePayload();
        var watch = Stopwatch.StartNew();

        using var ctsForTimer = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var controlTask = Task.Run(async () =>
        {
            using var periodicTimer = new PeriodicTimer(TimeSpan.FromSeconds(1));
            try
            {
                while (await periodicTimer.WaitForNextTickAsync(ctsForTimer.Token).ConfigureAwait(false))
                {
                    double elapsed = watch.Elapsed.TotalSeconds;
                    long currentSuccess = Interlocked.Read(ref successCount);

                    if (elapsed < rampUpDurationSeconds)
                    {
                        double progressPercent = elapsed / rampUpDurationSeconds;
                        int targetThreads = startThreads + (int)((maxParallelThreads - startThreads) * progressPercent);

                        if (targetThreads > allowedThreads)
                        {
                            int diff = targetThreads - allowedThreads;
                            allowedThreads = targetThreads;
                            _currentSemaphore.Release(diff);
                        }
                    }
                    else if (allowedThreads < maxParallelThreads)
                    {
                        int diff = maxParallelThreads - allowedThreads;
                        allowedThreads = maxParallelThreads;
                        _currentSemaphore.Release(diff);
                    }

                    long rpsThisSecond = currentSuccess - lastSuccessCount;
                    lastSuccessCount = currentSuccess;

                    PrintMetrics(elapsed, allowedThreads, rpsThisSecond, Interlocked.Read(ref errorCount), false);
                }
            }
            catch (OperationCanceledException) { }
        });

        try
        {
            await Parallel.ForEachAsync(Enumerable.Range(0, totalRequests), new ParallelOptions
            {
                MaxDegreeOfParallelism = maxParallelThreads,
                CancellationToken = ct
            },
            async (index, token) =>
            {
                try
                {
                    var sem = _currentSemaphore;
                    if (sem == null) return;

                    await sem.WaitAsync(token).ConfigureAwait(false);
                    try
                    {
                        bool isSuccess = await SendRequestAsync(jsonBytes, token).ConfigureAwait(false);
                        if (isSuccess) Interlocked.Increment(ref successCount);
                        else Interlocked.Increment(ref errorCount);
                    }
                    finally
                    {
                        sem.Release();
                    }
                }
                catch (OperationCanceledException) { }
                catch { Interlocked.Increment(ref errorCount); }
            }).ConfigureAwait(false);
        }
        finally
        {
            watch.Stop();
            ctsForTimer.Cancel();
            try { await controlTask.ConfigureAwait(false); } catch { }
            _currentSemaphore?.Dispose();
        }

        double totalSec = watch.Elapsed.TotalSeconds;
        double avgRps = totalSec > 0 ? (successCount / totalSec) : 0;
        Console.WriteLine("\n[Генератор] Тест Concurrency завершен! Всего успешных: " + successCount.ToString() + " | Средний RPS: " + avgRps.ToString("F0"));
    }

    /// <summary>
    /// Режим 2 (Rate-based)
    /// </summary>
    public async Task GenerateLoadByRpsAsync(
        int totalRequests,
        int maxRps,
        int startRps = 1000,
        int rampUpDurationSeconds = 30,
        CancellationToken ct = default)
    {
        Console.WriteLine("[Генератор] Режим: Канальный Rate-Limiting. Разгон от " + startRps + " до " + maxRps + " RPS за " + rampUpDurationSeconds + "с.");

        long successCount = 0;
        long errorCount = 0;
        long lastSuccessCount = 0;
        long totalDispatchedTokens = 0;

        var tokenChannel = Channel.CreateBounded<bool>(new BoundedChannelOptions(10000)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true
        });

        var jsonBytes = PreparePayload();
        var watch = Stopwatch.StartNew();

        using var periodicTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(10));
        using var ctsForTimer = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var producerTask = Task.Run(async () =>
        {
            int currentAllowedRps = startRps;
            var writer = tokenChannel.Writer;
            double tokenRemainder = 0;
            double lastMetricReportTime = 0;

            try
            {
                while (await periodicTimer.WaitForNextTickAsync(ctsForTimer.Token).ConfigureAwait(false))
                {
                    double elapsed = watch.Elapsed.TotalSeconds;

                    if (Interlocked.Read(ref totalDispatchedTokens) >= totalRequests)
                    {
                        break;
                    }

                    if (elapsed < rampUpDurationSeconds)
                    {
                        double progressPercent = elapsed / rampUpDurationSeconds;
                        currentAllowedRps = startRps + (int)((maxRps - startRps) * progressPercent);
                    }
                    else
                    {
                        currentAllowedRps = maxRps;
                    }

                    double tokensNeeded = (currentAllowedRps / 100.0) + tokenRemainder;
                    int tokensToSend = (int)tokensNeeded;
                    tokenRemainder = tokensNeeded - tokensToSend;

                    for (int i = 0; i < tokensToSend; i++)
                    {
                        if (Interlocked.Read(ref totalDispatchedTokens) >= totalRequests) break;

                        await writer.WriteAsync(true, ctsForTimer.Token).ConfigureAwait(false);
                        Interlocked.Increment(ref totalDispatchedTokens);
                    }

                    if (elapsed - lastMetricReportTime >= 1.0)
                    {
                        long currentSuccess = Interlocked.Read(ref successCount);
                        long rpsThisSecond = currentSuccess - lastSuccessCount;
                        lastSuccessCount = currentSuccess;

                        PrintMetrics(elapsed, currentAllowedRps, rpsThisSecond, Interlocked.Read(ref errorCount), true);
                        lastMetricReportTime = elapsed;
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                writer.TryComplete();
            }
        });

        try
        {
            await Parallel.ForEachAsync(tokenChannel.Reader.ReadAllAsync(ct), new ParallelOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount * 512,
                CancellationToken = ct
            },
            async (tokenSpawned, token) =>
            {
                try
                {
                    bool isSuccess = await SendRequestAsync(jsonBytes, token).ConfigureAwait(false);
                    if (isSuccess) Interlocked.Increment(ref successCount);
                    else Interlocked.Increment(ref errorCount);
                }
                catch (OperationCanceledException) { }
                catch { Interlocked.Increment(ref errorCount); }
            }).ConfigureAwait(false);
        }
        finally
        {
            watch.Stop();
            tokenChannel.Writer.TryComplete();
            ctsForTimer.Cancel();
            try { await producerTask.ConfigureAwait(false); } catch { }
        }

        double totalSec = watch.Elapsed.TotalSeconds;
        double avgRps = totalSec > 0 ? (successCount / totalSec) : 0;
        Console.WriteLine("\n[Генератор] Тест RPS завершен! Всего успешных: " + successCount.ToString() + " | Средний RPS: " + avgRps.ToString("F0"));
    }

    private async Task<bool> SendRequestAsync(byte[] bytes, CancellationToken token)
    {
        try
        {
            using var content = new ReadOnlyMemoryContent(bytes);

            content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

            using var response = await _httpClient.PostAsync("api/load", content, token).ConfigureAwait(false); return response.IsSuccessStatusCode;
        }
        catch(Exception e) 
        {
            Console.WriteLine(e.Message);
            return false; 
        }
    }

    private byte[] PreparePayload()
    {
        long deviceId = Random.Shared.Next(1, 50);

        var samplePayload = new DataPayload
        {
            Id = Guid.NewGuid(),
            Content = "Content",
            CreatedAt = DateTime.UtcNow
        };
        byte[] jsonBytes = JsonSerializer.SerializeToUtf8Bytes(samplePayload);
        byte[] finalPayload = new byte[4 + 8 + jsonBytes.Length];

        Span<byte> headerSpan = finalPayload.AsSpan(0, 12);

        headerSpan[0] = 0xAA;
        headerSpan[1] = 0xBB;
        headerSpan[2] = 0xCC;
        headerSpan[3] = 0xDD;

        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(headerSpan.Slice(4, 8), deviceId);
        jsonBytes.CopyTo(finalPayload.AsSpan(12));

        return finalPayload;
    }


    private void PrintMetrics(double elapsed, int currentLimit, long success, long errors, bool isRpsMode)
    {
        string limitType = isRpsMode ? "Целевой RPS" : "Активных потоков"; string successType = "Текущий RPS";
        Console.WriteLine("[" + elapsed.ToString("F1") + "с] " + limitType + ": " + currentLimit.ToString() + " | " + successType + ": " + success.ToString() + " | Ошибки: " + errors.ToString());
    }
}
