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
                $"POST /Test/test HTTP/1.1{Environment.NewLine}" +
                $"Host: localhost{Environment.NewLine}" +
                $"Content-type: application/json{Environment.NewLine}" +
                $"Content-Length: {requestContent.Length}{Environment.NewLine}" +
                $"{Environment.NewLine}" +
                requestContent;

            var dic = new Dictionary<int, string> { };
            for(int i = 0; i < 5000; i++)
            {
                dic.Add(i, requestString);
            }
            return dic;
        }

        private static async Task<SslStream> AuthenticateClient()
        {
            var client = new TcpClient();
            var ipAddress = Dns.GetHostEntry("localhost").AddressList[0];

            await client.ConnectAsync("localhost", 44392);
            SslStream sslStream = new SslStream(
                client.GetStream(),
                false,
                null
            );
            await sslStream.AuthenticateAsClientAsync("localhost");
            return sslStream;
        }

        private static async Task<KeyValuePair<int, SslStream>> SendStreamAsync(string requestString, int id, SslStream sslStream)
        {
            
            var x = new KeyValuePair<int, SslStream>(id, sslStream);
            var b = new StringBuilder();
            byte[] buffer = new byte[2048];
            int bytes;

            

            byte[] request = Encoding.UTF8.GetBytes(requestString);
            sslStream.Write(request, 0, request.Length);
            Console.WriteLine($"Sent request {id}");
            //sslStream.EndWrite(request);
            await sslStream.FlushAsync();

            

            var sb = new StringBuilder();
            do
            {
                bytes = sslStream.Read(buffer, 0, buffer.Length);
                //Console.WriteLine(Encoding.UTF8.GetString(buffer, 0, bytes));
                Console.WriteLine($"Received response {id}");
            } while (bytes == 2048);

            

            return x;
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
        private static async Task SendAllRequests(Dictionary<int, string> requestsDictionary)
        {
            var sslStream = await AuthenticateClient();

            string path = Directory.GetCurrentDirectory();
            var tasks = new List<Task<KeyValuePair<int, SslStream>>>();

            var sw = Stopwatch.StartNew();
            foreach (var k in requestsDictionary.Keys)
            {

                tasks.Add(SendStreamAsync(requestsDictionary[k], k, sslStream));
            }

            sw.Stop();
            Console.WriteLine("Enviei todas");
            Console.WriteLine(sw.ElapsedMilliseconds);
            var requestsInfo = await Task.WhenAll(tasks);

            //var dict = new Dictionary<int, SslStream> { };

            //foreach (var (k, v) in requestsInfo)
            //{
            //    dict.Add(k, v);
            //}

            //var readStreams = await ReadAllStreamsAsync(dict);
            //var a = readStreams.Values;
            //Console.WriteLine(string.Join("\n", a));
            //ParquetHelper.CreateRequestsParquetFile(requestsInfo, path);
        }

        private static async Task<Dictionary<int, string>> ReadAllStreamsAsync(Dictionary<int, SslStream> requestsDict)
        {
            var taskLists = new List<Task<KeyValuePair<int, string>>> { };
            foreach(var (k, v) in requestsDict)
            {
                taskLists.Add(ReadSingleStreamAsync(k, v));
            }

            var result = await Task.WhenAll(taskLists);

            var dict = new Dictionary<int, string> { };

            foreach(var (k, v) in result)
            {
                dict.Add(k, v);
            }

            return dict;
        }

        private static async Task<KeyValuePair<int, string>> ReadSingleStreamAsync(int id, SslStream stream)
        {
            byte[] buffer = new byte[2048];
            int bytes;
            var sb = new StringBuilder();
            do
            {
                bytes = await stream.ReadAsync(buffer, 0, buffer.Length);
                sb.Append(Encoding.UTF8.GetString(buffer, 0, bytes));
            } while (bytes == 2048);

            return new KeyValuePair<int, string>(id, sb.ToString());
        }
        //private static async Task<string> SendRequest(int messageId, HttpRequestMessage requestMessage)
        //{
        //    var response = await client.SendAsync(requestMessage);
        //    var sendingTimestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds();

        //    var responseContent = await response.Content.ReadAsStringAsync();
        //    var responseTimestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds();

        //    return $"{messageId}/{sendingTimestamp}/{responseTimestamp}/{responseContent}";
        //}
    }
}