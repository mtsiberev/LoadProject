using LoadSimulator.Receiver.Services;
using Microsoft.AspNetCore.Mvc;
using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Text.Json;

namespace LoadSimulator.Receiver.Controllers;

[ApiController]
[Route("api/load")]
public class LoadController : ControllerBase
{
    private readonly IMetricsService _metricsService;

    private static readonly OkResult CachedOk = new OkResult();
    private static readonly StatusCodeResult CachedTooManyRequests = new StatusCodeResult(429);
    private static readonly StatusCodeResult CachedInternalError = new StatusCodeResult(500);
    private static readonly BadRequestObjectResult CachedInvalidMarker = new BadRequestObjectResult("Неверный маркер протокола");
    private static readonly BadRequestObjectResult CachedShortPacket = new BadRequestObjectResult("Пакет слишком короткий");
    private static readonly BadRequestObjectResult CachedJsonError = new BadRequestObjectResult("Ошибка десериализации JSON");

    public LoadController(IMetricsService metricsService)
    {
        _metricsService = metricsService;
    }

    [HttpPost]
    public async Task<IActionResult> ProcessPayload()
    {
        if (_metricsService.IsRateLimitExceeded())
        {
            _metricsService.TrackRequest(0, isSuccess: false);
            return CachedTooManyRequests;
        }

        long startTime = Stopwatch.GetTimestamp();

        try
        {
            var reader = Request.BodyReader;

            ReadResult readResult;
            while (true)
            {
                readResult = await reader.ReadAsync();
                var buffer = readResult.Buffer;

                if (buffer.Length >= 12 || readResult.IsCompleted)
                {
                    break;
                }
                reader.AdvanceTo(buffer.Start, buffer.End);
            }

            var finalBuffer = readResult.Buffer;
            IActionResult result;

            if (finalBuffer.Length >= 12)
            {
                if (finalBuffer.IsSingleSegment)
                {
                    result = ReceiveData(finalBuffer.FirstSpan);
                }
                else
                {
                    byte[] sharedArray = ArrayPool<byte>.Shared.Rent((int)finalBuffer.Length);
                    try
                    {
                        finalBuffer.CopyTo(sharedArray);
                        result = ReceiveData(sharedArray.AsSpan(0, (int)finalBuffer.Length));
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(sharedArray);
                    }
                }
            }
            else
            {
                result = CachedShortPacket;
            }

            reader.AdvanceTo(finalBuffer.End);

            long stopTime = Stopwatch.GetTimestamp();
            _metricsService.TrackRequest(stopTime - startTime, isSuccess: true);

            return result;
        }
        catch
        {
            long stopTime = Stopwatch.GetTimestamp();
            _metricsService.TrackRequest(stopTime - startTime, isSuccess: false);
            return CachedInternalError;
        }
    }

    private IActionResult ReceiveData(ReadOnlySpan<byte> buffer)
    {
        if (buffer[0] != 0xAA || buffer[1] != 0xBB || buffer[2] != 0xCC || buffer[3] != 0xDD)
            return CachedInvalidMarker;

        long deviceId = System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(4, 8));
        ReadOnlySpan<byte> jsonSpan = buffer.Slice(12);

        try
        {
            //some logic
            return CachedOk;
        }
        catch (JsonException)
        {
            return CachedJsonError;
        }
    }
}