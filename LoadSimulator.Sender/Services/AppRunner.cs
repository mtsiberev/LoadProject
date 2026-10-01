namespace LoadSimulator.Sender.Services;

public class AppRunner
{
    private readonly ILoadGenerator _loadGenerator;

    public AppRunner(ILoadGenerator loadGenerator)
    {
        _loadGenerator = loadGenerator;
    }

    public async Task RunAsync()
    {
        Console.WriteLine("=== Конфигурация генератора нагрузки ===");
        Console.WriteLine("1 - Плавный разгон по количеству потоков (Concurrency-based)");
        Console.WriteLine("2 - Плавный разгон по частоте запросов (Rate-based)");

        string choice = "1";
        //choice = "2";

        Console.WriteLine("\n=== Запуск генератора нагрузки ===");

        int totalRequests = 1_000_000;
        int rampUpDurationSeconds = 30;
        
        if (choice == "1")
        {
            int maxParallelThreads = 10000;
            int startThreads = 1000;

            await _loadGenerator.GenerateLoadByConcurrencyAsync(
                totalRequests,
                maxParallelThreads,
                startThreads,
                rampUpDurationSeconds);
        }
        else if (choice == "2")
        {

            int maxRps = 40000;
            int startRps = 5000;

            await _loadGenerator.GenerateLoadByRpsAsync(
                totalRequests,
                maxRps,
                startRps,
                rampUpDurationSeconds);
        }
        else
        {
            Console.WriteLine("Тестирование отменено.");
            return;
        }

        Console.WriteLine("=== Тестирование успешно завершено ===");
        Console.ReadLine();
    }
}