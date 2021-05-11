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
            var methodOption = new Option<string>(
                                   alias: "--method",
                                   getDefaultValue: () => "POST",
                                   description: "HTTP request method, set of options: [POST, GET, DELETE]");

            methodOption.AddValidator(
                cmd =>
                {
                    List<string> availableMethods = new List<string> { "POST", "GET", "DELETE" };

                    var methodValue = (string)cmd.GetValueOrDefault();

                    if (!availableMethods.Exists(m => m.Equals(methodValue)))
                    {
                        return "Invalid value to method option, type --help to see the options";
                    }
                    else
                    {
                        return null;
                    }

                }
            );

            var targetOption = new Option<string>(
                                   alias: "--target",
                                   getDefaultValue: () => "/app/log/private",
                                   description: "Target which will receive the requests");

            targetOption.AddValidator(
                cmd =>
                {
                    var targetValue = (string)cmd.GetValueOrDefault();

                    if (!targetValue.StartsWith("/"))
                    {
                        return "Invalid value to target argument, check the written path";
                    }
                    else
                    {
                        return null;
                    }

                }
            );

            var entriesOption = new Option<int>(
                                   alias: "--entries",
                                   getDefaultValue: () => 100,
                                   description: "How many requests will be generated");

            entriesOption.AddValidator(
                cmd =>
                {
                    var entriesValue = (int)cmd.GetValueOrDefault();

                    if (entriesValue < 1)
                    {
                        return "Invalid value to entries argument, check if inserted value is correct";
                    }
                    else
                    {
                        return null;
                    }

                }
            );


            var rootCommand = new RootCommand
            {
                methodOption,
                targetOption,
                entriesOption,
            };

            rootCommand.Name = "run.sh";
            rootCommand.Description = "Generates a parquet file with CCF pre-serialized requests.";

            rootCommand.Handler = CommandHandler.Create<string, string, int>(GenerateRequestsParquet);

            return rootCommand.InvokeAsync(args).Result;
        }

        private static void GenerateRequestsParquet(string method, string target, int entries)
        {
            List<int> requestIds;
            List<string> requestBodies;

            Console.WriteLine("Starting pre-serialized requests");

            GenerateRequestStrings(method, target, entries, out requestIds, out requestBodies);

            string path = Directory.GetCurrentDirectory() + "/../results/requests.parquet";

            ParquetHelper.CreateRawRequestParquetFile(requestIds, requestBodies, path);

            Console.WriteLine("Requests were successfully generated");
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
