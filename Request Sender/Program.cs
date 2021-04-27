using CCFPerformanceTester.Helper;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Request_Sender
{
    class Program
    {

        public static string url = "https://localhost:5001/Test/test";
        //public static HttpClient client = new HttpClient();
        public static TcpClient client = new TcpClient();

        static async Task Main(string[] args)
        {
            var a = GetStringRequests();

            await SendAllRequests(a);
            //string postDataAsString = requestString;

            //byte[] postDataBinary = Encoding.UTF8.GetBytes(postDataAsString);

            //// make post request
            //client.Client.Send(postDataBinary);

            //// get response
            //byte[] bytes = new byte[2048];
            //int lengthOfResponse = client.Client.Receive(bytes);

            //var resp = System.Text.Encoding.UTF8.GetString(bytes, 0, lengthOfResponse);
            //client.BaseAddress = new Uri("https://localhost:44392/Test/test");
            //var serializedRequests = ParquetHelper.ReadParquetFile();

            //var requestMessages = ConvertToHTTPRequestMessage(serializedRequests, args[0]);
            //await SendAllRequests(requestMessages);
        }

        private static Dictionary<int, string> GetStringRequests()
        {
            var requestContent = $@"{{""id"": {1}, ""msg"": ""MESSAGE {1}""}}";

            var requestString =
                $"POST /app/log/private HTTP/1.1{Environment.NewLine}" +
                //$"Host: localhost{Environment.NewLine}" +
                $"Content-type: application/json{Environment.NewLine}" +
                $"Content-Length: {requestContent.Length}{Environment.NewLine}" +
                $"{Environment.NewLine}" +
                requestContent;

            var dic = new Dictionary<int, string> { };
            for (int i = 0; i <= 1000; i++)
            {
                dic.Add(i, requestString);
            }
            return dic;
        }

        private static async Task<SslStream> AuthenticateClient()
        {
            var client = new TcpClient();

            //var store =  new X509Store(StoreName.Root, StoreLocation.CurrentUser);
            //store.Open(OpenFlags.ReadWrite);

            await client.ConnectAsync("127.0.0.1", 8000);

            var serverCertificate = X509Certificate2.CreateFromPemFile("user0_cert.pem", "user0_privk.pem");
            //var newtorkCertificate = new X509Certificate2("networkcert.pem");

            X509Certificate2Collection certificateCollection = new X509Certificate2Collection();

            certificateCollection.Add(serverCertificate);
            //certificateCollection.Add(newtorkCertificate);

            //store.Add(newtorkCertificate);

            SslStream sslStream = new SslStream(
                client.GetStream(),
                false,
                new RemoteCertificateValidationCallback(App_CertificateValidation),
                null
            );

            await sslStream.AuthenticateAsClientAsync(
            "127.0.0.1",
            certificateCollection,
            SslProtocols.Tls12,
            true);


            return sslStream;
        }

        static bool App_CertificateValidation(Object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None) { return true; }
            if (sslPolicyErrors == SslPolicyErrors.RemoteCertificateChainErrors) { return true; }
            return false;
        }

        private static async Task<Dictionary<int, string>> SendStreamAsync(Dictionary<int, string> requestsDictionary, SslStream sslStream)
        {
            var sentRequestsDictionary = new Dictionary<int, string>();


            foreach (var k in requestsDictionary.Keys)
            {
                byte[] request = Encoding.UTF8.GetBytes(requestsDictionary[k]);
                await sslStream.WriteAsync(request, 0, request.Length);
                var responseTimestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                sentRequestsDictionary.Add(k, responseTimestamp.ToString());

                await sslStream.FlushAsync();
            }

            return sentRequestsDictionary;
        }

        private static async Task<Dictionary<int, ResponseOutput>> ReadResponsesContinuosly(SslStream sslStream, int expectedResponses)
        {
            var responsesDictionary = new Dictionary<int, ResponseOutput> { };
            Byte[] data = new Byte[2048];
            Int32 bytes;
            var i = 0;

            while (true)
            {

                bytes = await sslStream.ReadAsync(data, 0, data.Length);
                var responseTimestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds();

                if (bytes > 0)
                {
                    string responseData = Encoding.ASCII.GetString(data, 0, bytes);


                    if (responseData.Length > 100)
                    {
                        var requestOutput = new ResponseOutput()
                        {
                            ReceiveTime = responseTimestamp.ToString(),
                            RawResponse = responseData
                        };

                        responsesDictionary.Add(i, requestOutput);
                        i += 1;
                    }
                    if (i == expectedResponses)
                    {
                        break;
                    }
                }

            }

            return responsesDictionary;
        }

        private static async Task SendAllRequests(Dictionary<int, string> requestsDictionary)
        {
            var sslStream = await AuthenticateClient();

            string path = Directory.GetCurrentDirectory();

            var sw = Stopwatch.StartNew();

            var readResponsesTask = ReadResponsesContinuosly(sslStream, requestsDictionary.Count);
            var sendRequestsTask = SendStreamAsync(requestsDictionary, sslStream);

            Console.WriteLine("Sent all the requests");
            await Task.WhenAll(readResponsesTask, sendRequestsTask);

            var responsesDictionary = readResponsesTask.Result;
            var sentRequestsDictionary = sendRequestsTask.Result;

            sw.Stop();
            Console.WriteLine(sw.ElapsedMilliseconds);

            Console.WriteLine("Creating parquet files");
            ParquetHelper.CreateSentRequestsParquetFile(sentRequestsDictionary, path);
            ParquetHelper.CreateRequestsResponseParquetFile(responsesDictionary, path);
        }

    }
}