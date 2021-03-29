using Parquet;
using Parquet.Data;
using System.Collections.Generic;
using System.IO;

namespace CCFPerformanceTester.Helper
{
    public class ParquetHelper
    {
        public static void CreateParquetFile(List<int> requestIds, List<string> requestBodies, string path)
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
                        foreach(var column in schemaColumns)
                        {
                            groupWriter.WriteColumn(column);
                        }
                    }
                }
            }
        }
    }
}
