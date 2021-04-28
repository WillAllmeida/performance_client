﻿using CCFPerformanceTester.Helper;
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

        public static TcpClient client = new TcpClient();

        static async Task Main(string[] args)
        {
            var requestMessages = ParquetHelper.ReadParquetFile();

            await SendAllRequests(requestMessages, args[1], args[3], args[5], args[7]);

        }

        private static async Task<SslStream> AuthenticateClient(string host, string userCert, string userPK, string CAcert)
        {
            var client = new TcpClient();

            var hostData = host.Split(':', 2);


            var store =  new X509Store(StoreName.My, StoreLocation.CurrentUser);
            store.Open(OpenFlags.ReadWrite);

            await client.ConnectAsync(hostData[0], Convert.ToInt32(hostData[1]));

            var serverCertificate = X509Certificate2.CreateFromPemFile(userCert, userPK);
            var newtorkCertificate = new X509Certificate2(CAcert);

            X509Certificate2Collection certificateCollection = new X509Certificate2Collection();

            certificateCollection.Add(serverCertificate);
            certificateCollection.Add(newtorkCertificate);

            store.Add(newtorkCertificate);
            store.Close();

            SslStream sslStream = new SslStream(
                client.GetStream(),
                false,
                new RemoteCertificateValidationCallback(App_CertificateValidation),
                null
            );

            await sslStream.AuthenticateAsClientAsync(
            hostData[0],
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

        private static async Task SendAllRequests(Dictionary<int, string> requestsDictionary, string host, string userCert, string userPK, string CAcert)
        {
            var sslStream = await AuthenticateClient(host, userCert, userPK, CAcert);

            string path = Directory.GetCurrentDirectory();

            var readResponsesTask = ReadResponsesContinuosly(sslStream, requestsDictionary.Count);
            var sendRequestsTask = SendStreamAsync(requestsDictionary, sslStream);

            Console.WriteLine("Sent all the requests");
            await Task.WhenAll(readResponsesTask, sendRequestsTask);

            var responsesDictionary = readResponsesTask.Result;
            var sentRequestsDictionary = sendRequestsTask.Result;


            Console.WriteLine("Creating parquet files");
            ParquetHelper.CreateSentRequestsParquetFile(sentRequestsDictionary, path);
            ParquetHelper.CreateRequestsResponseParquetFile(responsesDictionary, path);
        }

    }
}