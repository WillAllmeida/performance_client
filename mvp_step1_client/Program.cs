using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using System.IO;
using System.Linq;

namespace mvp_step1_client
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

            rootCommand.Handler = CommandHandler.Create<string, string, int>(WriteParquet);

            return rootCommand.InvokeAsync(args).Result;
        }

        private static void WriteParquet(string method, string target, int entries)
        {
            List<int> requestIds = new List<int> { };
            List<string> requestBodies = new List<string> { };

            for (int id = 0; id < entries; id++)
            {
                requestIds.Add(id);

                var requestContent = $"{{\"id\": {id}, \"msg\": \"MESSAGE {id}\"}}";

                var requestText =
                    $"{method} {target} HTTP/1.1{Environment.NewLine}" +
                    $"Content-type: application/json{Environment.NewLine}" +
                    $"Content-Length: {requestContent.Length}{Environment.NewLine}" +
                    $"{Environment.NewLine}" +
                    requestContent;

                requestBodies.Add(requestText);
            }

            var indexColumn = new DataColumn(
                new DataField<int>("Message ID"),
                requestIds.ToArray());

            var requestColumn = new DataColumn(
               new DataField<string>("Serialized Request"),
               requestBodies.ToArray());

            
            var schema = new Schema(indexColumn.Field, requestColumn.Field);

            string path = Directory.GetCurrentDirectory();


            using (Stream fileStream = File.Create(path + "/test.parquet"))
            {
                using (var parquetWriter = new ParquetWriter(schema, fileStream))
                {
                    
                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        groupWriter.WriteColumn(indexColumn);
                        groupWriter.WriteColumn(requestColumn);
                    }
                }
            }

            Console.WriteLine("Requests generated successfully");
        }
    }
}
