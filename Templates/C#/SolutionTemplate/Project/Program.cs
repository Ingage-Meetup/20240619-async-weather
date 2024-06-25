using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace Project
{
    class Program
    {
        //public const string BaseUrl = "http://localhost:7000";
        public const string BaseUrl = "https://c3weatherapi.azurewebsites.net";
        public const int MaxRetries = 10;

        public static List<List<string>> CreateUrlBlocks(List<string> urls, int blockSize)
        {
            var totalBlocks = decimal.Round(decimal.Divide(urls.Count, blockSize), MidpointRounding.ToPositiveInfinity);
            var blocks = new List<List<string>>();

            for (var index = 0; index < totalBlocks; index++)
            {
                var blockUrls = urls.Skip(index * blockSize).Take(blockSize).ToList();
                blocks.Add(blockUrls);
            }

            return blocks;
        }

        public static async Task<List<HttpResponseMessage>> RetryFailedAsync(HttpClient client, List<HttpResponseMessage> failedTasks, int retries = 0)
        {
            var results = new List<HttpResponseMessage>();

            if (!failedTasks.Any()) return results;

            var retriedTasks = new List<Task<HttpResponseMessage>>();
            var retriedUrls = new List<string>();
            foreach (var failedTask in failedTasks)
            {
                retriedUrls.Add(failedTask.RequestMessage.RequestUri.LocalPath);
                retriedTasks.Add(client.GetAsync(failedTask.RequestMessage.RequestUri));
            }
            Console.WriteLine($"Retrying URLs: {string.Join(", ", retriedUrls)}");

            var taskResults = await Task.WhenAll(retriedTasks);

            var successfulRetriedTasks = taskResults.Where(x => x.IsSuccessStatusCode).ToList();
            var failedRetriedTasks = taskResults.Where(x => !x.IsSuccessStatusCode).ToList();

            if (!failedRetriedTasks.Any())
            {
                return successfulRetriedTasks;
            }

            if (retries >= MaxRetries)
            {
                // We have both success and failures
                return taskResults.ToList();
            }

            var delay = TimeSpan.FromSeconds(Math.Pow(2, retries));
            Console.WriteLine($"Delaying for {delay.TotalSeconds} seconds before retrying");
            await Task.Delay(delay);

            return await RetryFailedAsync(client, failedRetriedTasks, retries + 1);
        }

        public static async Task<List<HttpResponseMessage>> ProcessParallelWithBlocksAndRetriesAsync(HttpClient client, DateTime startDate, int daysToLoad)
        {
            var urls = new List<string>();
            for (var index = 0; index < daysToLoad; index++)
            {
                var date = startDate.AddDays(index).ToString("yyyy-MM-dd");
                urls.Add($"{BaseUrl}/{date}");
            }

            var blocks = CreateUrlBlocks(urls, 10);

            var allResults = new List<HttpResponseMessage>();
            foreach (var blockUrls in blocks)
            {
                var watch = new Stopwatch();
                watch.Start();

                var tasks = new List<Task<HttpResponseMessage>>();
                foreach (var blockUrl in blockUrls)
                {
                    tasks.Add(client.GetAsync(blockUrl));
                }

                var taskResults = await Task.WhenAll(tasks);

                var successfulTasks = taskResults.Where(x => x.IsSuccessStatusCode).ToList();
                var failedTasks = taskResults.Where(x => !x.IsSuccessStatusCode).ToList();
                var retriedTasks = await RetryFailedAsync(client, failedTasks);

                // TODO: Should we be logging failed tasks too?
                allResults.AddRange(successfulTasks);
                allResults.AddRange(retriedTasks);

                watch.Stop();

                // Approximate, doesn't account for multiple retires
                var successful = successfulTasks.Count() + retriedTasks.Count(x => x.IsSuccessStatusCode);
                var failures = failedTasks.Count() + retriedTasks.Count(x => !x.IsSuccessStatusCode);

                Console.WriteLine($"Took {watch.ElapsedMilliseconds / 1000d} seconds to execute with {taskResults.Count()} results. Average time is {watch.ElapsedMilliseconds / (taskResults.Count() * 1000d)}. Success {successful} Failures {failures}.");
            }

            return allResults;
        }


        public static async Task Main(string[] args)
        {
            var client = new HttpClient();
            client.DefaultRequestHeaders.Add("api-key", "charlie");
            client.DefaultRequestHeaders.Add("random-errors", "true");

            var watch = new Stopwatch();
            watch.Start();
            var results = await ProcessParallelWithBlocksAndRetriesAsync(client, new DateTime(2023, 01, 01), 365);
            watch.Stop();

            int successful = 0;
            int failures = 0;
            foreach (var result in results)
            {
                if (result.IsSuccessStatusCode)
                {
                    successful += 1;
                }
                else
                {
                    failures += 1;
                }

                var body = await result.Content.ReadAsStringAsync();
                Console.WriteLine($"Status Code: {result.StatusCode} Content: {body}");
            }
            Console.WriteLine();
            Console.WriteLine($"Took {watch.ElapsedMilliseconds / 1000d} seconds to execute with {results.Count} results. Average time is {watch.ElapsedMilliseconds / (results.Count * 1000d) }. Success {successful} Failures {failures}.");
        }
    }
}
