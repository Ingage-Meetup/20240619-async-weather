using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;

namespace Project
{
    class Program
    {

        static async Task<IList<HttpResponseMessage>> ProcessOneDay(HttpClient client)
        {
            var result = await client.GetAsync("http://localhost:7000/2020-01-01");
            return new List<HttpResponseMessage>() {result};
        }

        static async Task<IList<HttpResponseMessage>> ProcessOneWeekSerial(HttpClient client)
        {
            var result1 = await client.GetAsync("http://localhost:7000/2020-01-01");
            var result2 = await client.GetAsync("http://localhost:7000/2020-01-02");
            var result3 = await client.GetAsync("http://localhost:7000/2020-01-03");
            var result4 = await client.GetAsync("http://localhost:7000/2020-01-04");
            var result5 = await client.GetAsync("http://localhost:7000/2020-01-05");
            var result6 = await client.GetAsync("http://localhost:7000/2020-01-06");
            var result7 = await client.GetAsync("http://localhost:7000/2020-01-07");

            return new List<HttpResponseMessage>() {result1, result2, result3, result4, result5, result6, result7};
        }

        static async Task<IList<HttpResponseMessage>> ProcessOneWeekParallel(HttpClient client)
        {
            var result1 = client.GetAsync("http://localhost:7000/2020-01-01");
            var result2 = client.GetAsync("http://localhost:7000/2020-01-02");
            var result3 = client.GetAsync("http://localhost:7000/2020-01-03");
            var result4 = client.GetAsync("http://localhost:7000/2020-01-04");
            var result5 = client.GetAsync("http://localhost:7000/2020-01-05");
            var result6 = client.GetAsync("http://localhost:7000/2020-01-06");
            var result7 = client.GetAsync("http://localhost:7000/2020-01-07");
            
            Task.WaitAll(result1, result2, result3, result4, result5, result6, result7);

            return new List<HttpResponseMessage>() { result1.Result, result2.Result, result3.Result, result4.Result, result5.Result, result6.Result, result7.Result };
        }

        static async Task<IList<HttpResponseMessage>> ProcessOneWeekParallelInLoop(HttpClient client)
        {
            var tasks = new List<Task<HttpResponseMessage>>();
            var startDate = new DateTime(2020, 01, 01);
            for (var i = 0; i < 7; i++)
            {
                var date = startDate.AddDays(i).ToString("yyyy-MM-dd");
                tasks.Add(client.GetAsync($"http://localhost:7000/{date}"));
            }

            var taskResults = await Task.WhenAll(tasks);
            return new List<HttpResponseMessage>(taskResults);
        }

        static async Task<IList<HttpResponseMessage>> ProcessTwoWeekParallelInLoopWithoutBlocks(HttpClient client)
        {
            var tasks = new List<Task<HttpResponseMessage>>();
            var startDate = new DateTime(2020, 01, 01);
            for (var index = 0; index < 14; index++)
            {
                var date = startDate.AddDays(index).ToString("yyyy-MM-dd");
                tasks.Add(client.GetAsync($"http://localhost:7000/{date}"));
            }

            var taskResults = await Task.WhenAll(tasks);
            return new List<HttpResponseMessage>(taskResults);
        }

        static List<List<string>> CreateUrlBlocks(List<string> urls, int blockSize)
        {
            var totalBlocks = decimal.Round(decimal.Divide(urls.Count, blockSize), MidpointRounding.ToPositiveInfinity);
            var blocks = new List<List<string>>();

            for (var block = 0; block < totalBlocks; block++)
            {
                var currentBlockSize = urls.Count > (block + 1) * blockSize ? blockSize : urls.Count % blockSize;
                var blockUrls = urls.GetRange(block * blockSize, currentBlockSize);
                blocks.Add(blockUrls);
            }

            return blocks;
        }

        static async Task<IList<HttpResponseMessage>> ProcessTwoWeekParallelInLoopWithBlocks(HttpClient client)
        {
            var urls = new List<string>();
            var startDate = new DateTime(2020, 01, 01);
            var daysToLoad = 14;
            for (var index = 0; index < daysToLoad; index++) 
            {
                var date = startDate.AddDays(index).ToString("yyyy-MM-dd");
                urls.Add($"http://localhost:7000/{date}");
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


        static async Task Main(string[] args)
        {
            var client = new HttpClient();
            client.DefaultRequestHeaders.Add("api-key", "charlie");

            var watch = new Stopwatch();
            watch.Start();

            //var results = await ProcessOneDay(client);
            //var results = await ProcessOneWeekSerial(client);
            //var results = await ProcessOneWeekParallel(client);
            //var results = await ProcessOneWeekParallelInLoop(client);
            //var results = await ProcessTwoWeekParallelInLoopWithoutBlocks(client);
            var results = await ProcessTwoWeekParallelInLoopWithBlocks(client);
            

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

                Console.WriteLine(result);
            }
            Console.WriteLine();
            Console.WriteLine($"Took {watch.ElapsedMilliseconds / 1000d} seconds to execute with {results.Count} results. Average time is {watch.ElapsedMilliseconds / (decimal) results.Count }. Success {successful} Failures {failures}.");
        }
    }
}
