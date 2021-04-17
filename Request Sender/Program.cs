using CCFPerformanceTester.Helper;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Request_Sender
{
    class Program
    {

        public static string url = "https://localhost:5001/Test/test";
        public static HttpClient client = new HttpClient();

        static async Task Main(string[] args)
        {

            //var requestContent = $@"{{""id"": {1}, ""msg"": ""MESSAGE {1}""}}";

            //var requestString =
            //    $"POST /Test/test HTTP/1.1{Environment.NewLine}" +
            //    $"Host: localhost{Environment.NewLine}" +
            //    $"Content-type: application/json{Environment.NewLine}" +
            //    $"Content-Length: {requestContent.Length}{Environment.NewLine}" +
            //    $"{Environment.NewLine}" +
            //    requestContent;

            //TcpClient client = new TcpClient();
            //var ipAddress = Dns.GetHostEntry("localhost").AddressList[0];

            //client.Connect("localhost", 44392);
            //SslStream sslStream = new SslStream(
            //    client.GetStream(),
            //    false,
            //    null
            //);
            //sslStream.AuthenticateAsClient("localhost");

            //byte[] buffer = new byte[2048];
            //int bytes;
            //byte[] request = Encoding.UTF8.GetBytes(requestString);
            //sslStream.Write(request, 0, request.Length);
            //sslStream.Flush();

            //// Read response
            //do
            //{
            //    bytes = sslStream.Read(buffer, 0, buffer.Length);
            //    var a = Encoding.UTF8.GetString(buffer, 0, bytes);
            //    Console.Write(Encoding.UTF8.GetString(buffer, 0, bytes));
            //} while (bytes == 2048);
            //string postDataAsString = requestString;

            //byte[] postDataBinary = Encoding.UTF8.GetBytes(postDataAsString);

            //// make post request
            //client.Client.Send(postDataBinary);

            //// get response
            //byte[] bytes = new byte[2048];
            //int lengthOfResponse = client.Client.Receive(bytes);

            //var resp = System.Text.Encoding.UTF8.GetString(bytes, 0, lengthOfResponse);
            //client.BaseAddress = new Uri("https://localhost:44392/Test/test");
            var serializedRequests = ParquetHelper.ReadParquetFile();

            var requestMessages = ConvertToHTTPRequestMessage(serializedRequests, args[0]);
            await SendAllRequests(requestMessages);
        }

        public static Dictionary<int, HttpRequestMessage> ConvertToHTTPRequestMessage(Dictionary<int, RequestFormat> stringDictionary, string baseURL)
        {
            Dictionary<int, HttpRequestMessage> formattedRequests = new Dictionary<int, HttpRequestMessage>();

            foreach (var k in stringDictionary.Keys)
            {
                var requestMessage = new HttpRequestMessage
                {
                    Version = HttpVersion.Version11,
                    Method = stringDictionary[k].Method,
                    RequestUri = new Uri(baseURL + stringDictionary[k].Path),
                    Content = new StringContent(JsonSerializer.Serialize(stringDictionary[k].Content), Encoding.UTF8, "application/json")
                };

                formattedRequests.Add(k, requestMessage);
            }

            return formattedRequests;

        }
        private static async Task SendAllRequests(Dictionary<int, HttpRequestMessage> requestsDictionary)
        {
            string path = Directory.GetCurrentDirectory();
            var tasks = new List<Task<string>>();

            var sw = Stopwatch.StartNew();
            foreach (var k in requestsDictionary.Keys)
            {

                tasks.Add(SendRequest(k, requestsDictionary[k]));
            }

            sw.Stop();
            Console.WriteLine("Enviei todas");
            var requestsInfo = await Task.WhenAll(tasks);
            Console.WriteLine(sw.ElapsedMilliseconds);
            ParquetHelper.CreateRequestsParquetFile(requestsInfo, path);
        }

        private static async Task<string> SendRequest(int messageId, HttpRequestMessage requestMessage)
        {
            var response = await client.SendAsync(requestMessage);
            var sendingTimestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds();

            var responseContent = await response.Content.ReadAsStringAsync();
            var responseTimestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds();

            return $"{messageId}/{sendingTimestamp}/{responseTimestamp}/{responseContent}";
        }
    }
}