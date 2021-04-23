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
                $"Host: localhost{Environment.NewLine}" +
                $"Content-type: application/json{Environment.NewLine}" +
                $"Content-Length: {requestContent.Length}{Environment.NewLine}" +
                $"{Environment.NewLine}" +
                requestContent;

            var dic = new Dictionary<int, string> { };
            for (int i = 0; i < 1000; i++)
            {
                dic.Add(i, requestString);
            }
            return dic;
        }

        private static async Task<SslStream> AuthenticateClient()
        {
            var client = new TcpClient();

            
            await client.ConnectAsync("127.0.0.1", 8000);

            var serverCertificate = X509Certificate2.CreateFromPemFile("user0_cert.pem", "user0_privk.pem");
            X509Certificate2Collection certificateCollection = new X509Certificate2Collection();
            
            certificateCollection.Add(serverCertificate);



            SslStream sslStream = new SslStream(
                client.GetStream(),
                false,
                new RemoteCertificateValidationCallback (App_CertificateValidation),
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

        private static async Task<KeyValuePair<int, SslStream>> SendStreamAsync(Dictionary<int, string> requestsDictionary, SslStream sslStream)
        {
            var id = 1;
            var x = new KeyValuePair<int, SslStream>(id, sslStream);
            var b = new StringBuilder();


            foreach (var k in requestsDictionary.Keys)
            {
                byte[] buffer = new byte[2048];
                byte[] request = Encoding.UTF8.GetBytes(requestsDictionary[k]);
                await sslStream.WriteAsync(request, 0, request.Length);


                await sslStream.FlushAsync();
            }

            return x;
        }

        private static async Task ReadResponsesContinuosly(SslStream sslStream, int expectedResponses)
        {
            Byte[] data = new Byte[2048];
            String responseData = String.Empty;
            Int32 bytes;
            var i = 0;

            while (true)
            {

                bytes = await sslStream.ReadAsync(data, 0, data.Length);
                if (bytes > 0)
                {
                    responseData = Encoding.ASCII.GetString(data, 0, bytes);


                    if (responseData.Length > 100)
                        Console.WriteLine(responseData);
                        i += 1;

                    if (i == expectedResponses)
                        break;

                }

            }
        }

        private static async Task SendAllRequests(Dictionary<int, string> requestsDictionary)
        {
            var sslStream = await AuthenticateClient();

            string path = Directory.GetCurrentDirectory();
            var tasks = new List<Task<KeyValuePair<int, SslStream>>>();

            var sw = Stopwatch.StartNew();

            var a = ReadResponsesContinuosly(sslStream, requestsDictionary.Count);
            var b = SendStreamAsync(requestsDictionary, sslStream);

            Console.WriteLine("Enviei todas");
            await Task.WhenAll(a, b);

            sw.Stop();
            Console.WriteLine(sw.ElapsedMilliseconds);


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
    }
}