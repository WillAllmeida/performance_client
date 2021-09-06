using Parquet;
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

            WriteDataInFile(path, schemaColumns);
        }

        public static void CreateSentRequestsParquetFile(Dictionary<int, string> requestsDictionary, string path, string relativePath)
        {
            var arrayLength = requestsDictionary.Count;

            int[] messageIDs = new int[arrayLength];
            string[] sendTime = new string[arrayLength];
            foreach (var k in requestsDictionary.Keys)
            {
                messageIDs[k] = k;
                sendTime[k] = requestsDictionary[k];
            }

            path = Path.GetFullPath(path + relativePath);

            var indexColumn = new DataColumn(
                            new DataField<int>("Message ID"),
                            messageIDs);

            var sendTimeColumn = new DataColumn(
               new DataField<string>("Send Time"),
               sendTime);

            List<DataColumn> schemaColumns = new List<DataColumn>() { indexColumn, sendTimeColumn };

            WriteDataInFile(path, schemaColumns);
        }

        private static void WriteDataInFile(string path, List<DataColumn> schemaColumns)
        {
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

        public static void CreateRequestsResponseParquetFile(Dictionary<int, ResponseOutput> responsesDictionary, string path, string relativePath)
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

            path = Path.GetFullPath(path + relativePath);

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

            WriteDataInFile(path, schemaColumns);
        }

        public static Dictionary<int, string> ReadParquetFile(string infile)
        {
            Dictionary<int, string> serializedRequests = new Dictionary<int, string>();

            string path = Path.GetFullPath(Directory.GetCurrentDirectory() + "/" + infile);

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
                            serializedRequests.Add(convertedIdData, convertedRequestData);
                        }

                    }

                    return serializedRequests;
                }
            }
        }
    }
}
