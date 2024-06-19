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
        public const string BaseUrl = "http://localhost:7000";

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
            var results = await ProcessParallelWithBlocksAsync(client, new DateTime(2020, 01, 01), 21);
            //var results = await ProcessParallelWithBlocksAsync(client, new DateTime(2023, 01, 01), 365);


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
