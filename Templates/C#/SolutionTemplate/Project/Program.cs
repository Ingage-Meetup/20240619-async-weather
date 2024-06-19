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

        public static async Task<IList<HttpResponseMessage>> ProcessOneDayAsync(HttpClient client)
        {
            var result = await client.GetAsync($"{BaseUrl}/2020-01-01");
            return new List<HttpResponseMessage>() {result};
        }

        public static async Task<IList<HttpResponseMessage>> ProcessOneWeekSerialAsync(HttpClient client)
        {
            var result1 = await client.GetAsync($"{BaseUrl}/2020-01-01");
            var result2 = await client.GetAsync($"{BaseUrl}/2020-01-02");
            var result3 = await client.GetAsync($"{BaseUrl}/2020-01-03");
            var result4 = await client.GetAsync($"{BaseUrl}/2020-01-04");
            var result5 = await client.GetAsync($"{BaseUrl}/2020-01-05");
            var result6 = await client.GetAsync($"{BaseUrl}/2020-01-06");
            var result7 = await client.GetAsync($"{BaseUrl}/2020-01-07");

            return new List<HttpResponseMessage>() {result1, result2, result3, result4, result5, result6, result7};
        }

        public static async Task<IList<HttpResponseMessage>> ProcessOneWeekParallelAsync(HttpClient client)
        {
            var result1 = client.GetAsync($"{BaseUrl}/2020-01-01");
            var result2 = client.GetAsync($"{BaseUrl}/2020-01-02");
            var result3 = client.GetAsync($"{BaseUrl}/2020-01-03");
            var result4 = client.GetAsync($"{BaseUrl}/2020-01-04");
            var result5 = client.GetAsync($"{BaseUrl}/2020-01-05");
            var result6 = client.GetAsync($"{BaseUrl}/2020-01-06");
            var result7 = client.GetAsync($"{BaseUrl}/2020-01-07");
            
            Task.WaitAll(result1, result2, result3, result4, result5, result6, result7);

            return new List<HttpResponseMessage>() { result1.Result, result2.Result, result3.Result, result4.Result, result5.Result, result6.Result, result7.Result };
        }

        public static async Task<IList<HttpResponseMessage>> ProcessOneWeekParallelInLoopAsync(HttpClient client)
        {
            var tasks = new List<Task<HttpResponseMessage>>();
            var startDate = new DateTime(2020, 01, 01);
            for (var i = 0; i < 7; i++)
            {
                var date = startDate.AddDays(i).ToString("yyyy-MM-dd");
                tasks.Add(client.GetAsync($"{BaseUrl}/{date}"));
            }

            var taskResults = await Task.WhenAll(tasks);
            return new List<HttpResponseMessage>(taskResults);
        }

        public static async Task<IList<HttpResponseMessage>> ProcessTwoWeekParallelInLoopWithoutBlocksAsync(HttpClient client)
        {
            var tasks = new List<Task<HttpResponseMessage>>();
            var startDate = new DateTime(2020, 01, 01);
            for (var index = 0; index < 14; index++)
            {
                var date = startDate.AddDays(index).ToString("yyyy-MM-dd");
                tasks.Add(client.GetAsync($"{BaseUrl}/{date}"));
            }

            var taskResults = await Task.WhenAll(tasks);
            return new List<HttpResponseMessage>(taskResults);
        }

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

        public static async Task<IList<HttpResponseMessage>> ProcessTwoWeekParallelInLoopWithBlocksAsync(HttpClient client)
        {
            var urls = new List<string>();
            var startDate = new DateTime(2020, 01, 01);
            var daysToLoad = 14;
            for (var index = 0; index < daysToLoad; index++) 
            {
                var date = startDate.AddDays(index).ToString("yyyy-MM-dd");
                urls.Add($"{BaseUrl}/{date}");
            }

            var blocks = CreateUrlBlocks(urls, 10);

            var allResults = new List<HttpResponseMessage>();
            foreach(var blockUrls in blocks) {
                var tasks = new List<Task<HttpResponseMessage>>();
                foreach (var blockUrl in blockUrls)
                {
                    tasks.Add(client.GetAsync(blockUrl));
                }

                var taskResults = await Task.WhenAll(tasks);
                allResults.AddRange(taskResults);
            }

            return allResults;
        }

        public static async Task<IList<HttpResponseMessage>> ProcessParallelWithBlocksAsync(HttpClient client, DateTime startDate, int daysToLoad)
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
                allResults.AddRange(taskResults);

                watch.Stop();
                var successful = taskResults.Count(x => x.IsSuccessStatusCode);
                var failures  = taskResults.Count(x => !x.IsSuccessStatusCode);
                Console.WriteLine($"Took {watch.ElapsedMilliseconds / 1000d} seconds to execute with {taskResults.Count()} results. Average time is {watch.ElapsedMilliseconds / (taskResults.Count() * 1000d) }. Success {successful} Failures {failures}.");
            }

            return allResults;
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

            var delay = TimeSpan.FromSeconds(Math.Pow(retries, 2));
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

                // TODO: Should this include all successful / failures and retries?
                //var successful = taskResults.Count(x => x.IsSuccessStatusCode);
                //var initialFailures = failedTasks.Count();
                //var failures = taskResults.Count(x => !x.IsSuccessStatusCode);

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

            var watch = new Stopwatch();
            watch.Start();

            //var results = await ProcessOneDayAsync(client);
            //var results = await ProcessOneWeekSerialAsync(client);
            //var results = await ProcessOneWeekParallelAsync(client);
            //var results = await ProcessOneWeekParallelInLoopAsync(client);
            //var results = await ProcessTwoWeekParallelInLoopWithoutBlocksAsync(client);
            //var results = await ProcessTwoWeekParallelInLoopWithBlocksAsync(client);
            //var results = await ProcessParallelWithBlocksAsync(client, new DateTime(2020, 01, 01), 21);
            //var results = await ProcessParallelWithBlocksAsync(client, new DateTime(2023, 01, 01), 365);
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
