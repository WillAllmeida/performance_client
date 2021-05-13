using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CCFPerformanceTester.Helper
{
    public class CommandLineHelper
    {
        public static void CreateGeneratorCommandOptions(out Option<string> methodOption, out Option<string> targetOption, out Option<int> entriesOption)
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
        }

        public static void CreateSenderCommandOptions(out Option<string> hostOption, out Option<string> userOption, out Option<string> userPkOption, out Option<string> caCertOption, out Option<string> infileOption, out Option<string> requestsFileOption, out Option<string> responsesFileOption)
        {
            hostOption = new Option<string>(
                                               alias: "--host",
                                               getDefaultValue: () => "127.0.0.1:8000",
                                               description: "Address of CCF node host");
            userOption = new Option<string>(
                                   alias: "--user",
                                   getDefaultValue: () => "user0_cert.pem",
                                   description: "Path to User certificate file");
            userPkOption = new Option<string>(
                                   alias: "--pk",
                                   getDefaultValue: () => "user0_privk.pem",
                                   description: "Path to User private key file");
            caCertOption = new Option<string>(
                                   alias: "--cacert",
                                   getDefaultValue: () => "networkcert.pem",
                                   description: "Path to Certificate Authority file");
            infileOption = new Option<string>(
                                   alias: "--infile",
                                   getDefaultValue: () => "/../results/requests.parquet",
                                   description: "Path to Parquet with raw requests");
            requestsFileOption = new Option<string>(
                                   alias: "--requestsfile",
                                   getDefaultValue: () => "/../results/sentrequests.parquet",
                                   description: "Path to Parquet with sent requests data");
            responsesFileOption = new Option<string>(
                                   alias: "--responsesfile",
                                   getDefaultValue: () => "/../results/responses.parquet",
                                   description: "Path to Parquet with responses data");
        }
    }
}
