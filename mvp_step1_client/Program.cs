using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace mvp_step1_client
{
    class Program
    {
        static void Main(string[] args)
        {
            WriteParquet();
        }

        private static void WriteParquet()
        {
            List<int> requestIds = new List<int> { };
            List<string> requestBodies = new List<string> { };

            for (int id = 0; id < 1001; id++)
            {
                requestIds.Add(id);
                var requestContent = $"{{\"id\": {id}, \"msg\": \"MESSAGE {id}\"}}";
                var requestText = $"POST /app/log/private HTTP/1.1{Environment.NewLine}" +
                    $"Content-type: application/json{Environment.NewLine}" +
                    $"Content-Length: {requestContent.Length}{Environment.NewLine}" +
                    requestContent;
                requestBodies.Add(requestText);
            }

            var indexColumn = new DataColumn(
                new DataField<int>("Message ID"),
                requestIds.ToArray());

            var requestColumn = new DataColumn(
               new DataField<string>("Serialized Request"),
               requestBodies.ToArray());

            // create file schema
            var schema = new Schema(indexColumn.Field, requestColumn.Field);

            string path = Directory.GetCurrentDirectory();

            path += "/../../../";
            Console.WriteLine(path);

            using (Stream fileStream = File.Create(path + "test.parquet"))
            {
                using (var parquetWriter = new ParquetWriter(schema, fileStream))
                {
                    // create a new row group in the file
                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        groupWriter.WriteColumn(indexColumn);
                        groupWriter.WriteColumn(requestColumn);
                    }
                }
            }
        }
    }
}
