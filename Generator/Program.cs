using CCFPerformanceTester.Helper;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using System.IO;
using System.Net.Http;
using System.Text.Json;

namespace CCFPerformanceTester.Generator
{
    class Program
    {
        static int Main(string[] args)
        {
            Option<string> methodOption, targetOption, outputOption;
            Option<int> entriesOption;

            CommandLineHelper.CreateGeneratorCommandOptions(out methodOption, out targetOption, out entriesOption, out outputOption);

            var rootCommand = new RootCommand
            {
                methodOption,
                targetOption,
                entriesOption,
                outputOption
            };
            
            rootCommand.Name = "run.sh";
            rootCommand.Description = "Generates a parquet file with CCF pre-serialized requests.";
 
            rootCommand.Handler = CommandHandler.Create<string, string, int, string>(GenerateRequestsParquet);
            return rootCommand.InvokeAsync(args).Result;
        }


        private static void GenerateRequestsParquet(string method, string target, int entries, string requestsfile)
        {
            List<int> requestIds;
            List<string> requestBodies;

            Console.WriteLine("Starting pre-serialized requests");

            GenerateRequestStrings(method, target, entries, out requestIds, out requestBodies);

            string path = Path.GetFullPath(Directory.GetCurrentDirectory() + "/" + requestsfile);

            ParquetHelper.CreateRawRequestParquetFile(requestIds, requestBodies, path);

            Console.WriteLine($"Requests were successfully generated: {path}");
        }

        private static void GenerateRequestStrings(string method, string target, int entries, out List<int> requestIds, out List<string> requestBodies)
        {
            requestIds = new List<int> { };
            requestBodies = new List<string> { };
            for (int id = 0; id < entries; id++)
            {
                requestIds.Add(id);

                var requestContent = $"{{\"id\": {id}, \"msg\": \"MESSAGE {id}\"}}";

                var requestString =
                    $"{method} {target} HTTP/1.1{Environment.NewLine}" +
                    $"Content-type: application/json{Environment.NewLine}" +
                    $"Content-Length: {requestContent.Length}{Environment.NewLine}" +
                    $"{Environment.NewLine}" +
                    requestContent;

                requestBodies.Add(requestString);
            }

            Console.WriteLine("Generation of the request strings done");
        }
    }
}
