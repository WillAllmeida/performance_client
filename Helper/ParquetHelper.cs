﻿using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace CCFPerformanceTester.Helper
{
    public class ParquetHelper
    {
        public static void CreateRawRequestParquetFile(List<int> requestIds, List<string> requestBodies, string path)
        {

            var indexColumn = new DataColumn(
                new DataField<int>("Message ID"),
                requestIds.ToArray());

            var requestColumn = new DataColumn(
               new DataField<string>("Serialized Request"),
               requestBodies.ToArray());

            List<DataColumn> schemaColumns = new List<DataColumn>() { indexColumn, requestColumn };

            var schema = new Schema(schemaColumns.ConvertAll(col => col.Field));

            using (Stream fileStream = File.Create(path))
            {
                using (var parquetWriter = new ParquetWriter(schema, fileStream))
                {

                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        foreach (var column in schemaColumns)
                        {
                            groupWriter.WriteColumn(column);
                        }
                    }
                }
            }
        }

        public static void CreateSentRequestsParquetFile(Dictionary<int, string> requestsDictionary, string path)
        {
            var arrayLength = requestsDictionary.Count;

            int[] messageIDs = new int[arrayLength];
            string[] sendTime = new string[arrayLength];
            foreach (var k in requestsDictionary.Keys)
            {
                messageIDs[k] = k;
                sendTime[k] = requestsDictionary[k];
            }

            path = path + "/sentrequests.parquet";

            var indexColumn = new DataColumn(
                            new DataField<int>("Message ID"),
                            messageIDs);

            var sendTimeColumn = new DataColumn(
               new DataField<string>("Send Time"),
               sendTime);

            List<DataColumn> schemaColumns = new List<DataColumn>() { indexColumn, sendTimeColumn };

            var schema = new Schema(schemaColumns.ConvertAll(col => col.Field));

            using (Stream fileStream = File.Create(path))
            {
                using (var parquetWriter = new ParquetWriter(schema, fileStream))
                {

                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        foreach (var column in schemaColumns)
                        {
                            groupWriter.WriteColumn(column);
                        }
                    }
                }
            }
        }

        public static void CreateRequestsResponseParquetFile(Dictionary<int, ResponseOutput> responsesDictionary, string path)
        {
            var arrayLength = responsesDictionary.Count;

            int[] messageIDs = new int[arrayLength];
            string[] responseTime = new string[arrayLength];
            string[] rawResponse = new string[arrayLength];

            foreach (var k in responsesDictionary.Keys)
            {
                messageIDs[k] = k;
                responseTime[k] = responsesDictionary[k].ReceiveTime;
                rawResponse[k] = responsesDictionary[k].RawResponse;
            }

            path = path + "/responserequests.parquet";

            var indexColumn = new DataColumn(
                            new DataField<int>("Message ID"),
                            messageIDs);

            var responseTimeColumn = new DataColumn(
               new DataField<string>("Receive Time"),
               responseTime);

            var responseContentColumn = new DataColumn(
               new DataField<string>("Raw Response"),
               rawResponse);

            List<DataColumn> schemaColumns = new List<DataColumn>() { indexColumn, responseTimeColumn, responseContentColumn };

            var schema = new Schema(schemaColumns.ConvertAll(col => col.Field));

            using (Stream fileStream = File.Create(path))
            {
                using (var parquetWriter = new ParquetWriter(schema, fileStream))
                {

                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        foreach (var column in schemaColumns)
                        {
                            groupWriter.WriteColumn(column);
                        }
                    }
                }
            }
        }

        public static Dictionary<int, ResponseOutput> ReadParquetFile()
        {
            Dictionary<int, ResponseOutput> serializedRequests = new Dictionary<int, ResponseOutput>();

            string path = Directory.GetCurrentDirectory() + "/../../../../mvp_step1_client/requests.parquet";

            using (Stream fileStream = File.OpenRead(path))
            {
                using (var parquetReader = new ParquetReader(fileStream))
                {
                    DataField[] dataFields = parquetReader.Schema.GetDataFields();

                    using (ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(0))
                    {
                        DataColumn[] columns = dataFields.Select(groupReader.ReadColumn).ToArray();
                        DataColumn firstColumn = columns[0];
                        DataColumn secondColumn = columns[1];

                        Array idData = firstColumn.Data;
                        Array requestData = secondColumn.Data;

                        for (var j = 0; j < firstColumn.Data.Length; j++)
                        {
                            var convertedRequestData = (string)requestData.GetValue(j);
                            var convertedIdData = (int)idData.GetValue(j);
                            serializedRequests.Add(convertedIdData, JsonSerializer.Deserialize<ResponseOutput>(convertedRequestData));
                        }

                    }

                    return serializedRequests;
                }
            }
        }
    }
}
