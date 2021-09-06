using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CCFPerformanceTester.Helper
{
    public class CommandLineHelper
    {
        public static void CreateGeneratorCommandOptions(out Option<string> methodOption, out Option<string> targetOption, out Option<int> entriesOption, out Option<string> outputOption)
        {
            methodOption = new Option<string>(
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

            targetOption = new Option<string>(
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

            entriesOption = new Option<int>(
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

            outputOption = new Option<string>(
                                   alias: "--requestsfile",
                                   getDefaultValue: () => "/../results/requests.parquet",
                                   description: "Path to Parquet with raw requests");

            ValidatePath(outputOption, "Invalid output path, verify if the entered path is correct");
        }

        public static void CreateSenderCommandOptions(out Option<string> hostOption, out Option<string> userOption, out Option<string> userPkOption, out Option<string> caCertOption, out Option<string> infileOption, out Option<string> requestsFileOption, out Option<string> responsesFileOption)
        {
            hostOption = new Option<string>(
                                               alias: "--host",
                                               getDefaultValue: () => "127.0.0.1:8000",
                                               description: "Address of CCF node host");

            hostOption.AddValidator(
                cmd =>
                {
                    var hostValue = (string)cmd.GetValueOrDefault();
                    try
                    {
                        if (!Uri.IsWellFormedUriString(hostValue, UriKind.Absolute))
                        {
                            var tryCreateUri = new UriBuilder(hostValue);
                        }
                        return null;
                    } catch
                    {
                        return "Invalid Host, verify if the entered host is well formed";
                    }

                }
            );
            userOption = new Option<string>(
                                   alias: "--user",
                                   getDefaultValue: () => "user0_cert.pem",
                                   description: "Path to User certificate file");

            ValidateFile(userOption, "Invalid user certificate file, verify if the entered path is correct");
            
            userPkOption = new Option<string>(
                                   alias: "--pk",
                                   getDefaultValue: () => "user0_privk.pem",
                                   description: "Path to User private key file");

            ValidateFile(userPkOption, "Invalid user private key file, verify if the entered path is correct");
            
            caCertOption = new Option<string>(
                                   alias: "--cacert",
                                   getDefaultValue: () => "networkcert.pem",
                                   description: "Path to Certificate Authority file");
            ValidateFile(caCertOption, "Invalid Certificate Authority file, verify if the entered path is correct");
            
            infileOption = new Option<string>(
                                   alias: "--infile",
                                   getDefaultValue: () => "/../results/requests.parquet",
                                   description: "Path to Parquet with raw requests");
            ValidateFile(infileOption, "Invalid input file, verify if the entered path is correct");
            
            requestsFileOption = new Option<string>(
                                   alias: "--requestsfile",
                                   getDefaultValue: () => "/../results/sentrequests.parquet",
                                   description: "Path to Parquet with sent requests data");
            
            ValidatePath(requestsFileOption, "Invalid sent requests output path, verify if the entered path is correct");
            
            responsesFileOption = new Option<string>(
                                   alias: "--responsesfile",
                                   getDefaultValue: () => "/../results/responses.parquet",
                                   description: "Path to Parquet with responses data");

            ValidatePath(responsesFileOption, "Invalid responses output path, verify if the entered path is correct");
        }

        private static void ValidatePath(Option<string> pathOption, string errorMessage)
        {
            pathOption.AddValidator(
                            cmd =>
                            {
                                var pathValue = (string)cmd.GetValueOrDefault();

                                var fullPath = Path.GetFullPath(Directory.GetCurrentDirectory() + "/" + pathValue);

                                var directory = Path.GetDirectoryName(fullPath);


                                if (!Directory.Exists(directory))
                                {
                                    return errorMessage;
                                }
                                else
                                {
                                    return null;
                                }

                            }
                        );
        }

        private static void ValidateFile(Option<string> fileOption, string errorMessage)
        {
            fileOption.AddValidator(
                            cmd =>
                            {
                                var fileValue = (string)cmd.GetValueOrDefault();

                                var fullPath = Path.GetFullPath(Directory.GetCurrentDirectory() + "/" + fileValue);


                                if (!File.Exists(fullPath))
                                {
                                    return errorMessage;
                                }
                                else
                                {
                                    return null;
                                }

                            }
                        );
        }
    }
}
