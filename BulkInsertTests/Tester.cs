using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Dapper;

namespace BulkInsertTests
{
    class Tester
    {
        SqlConnection connection;
        int count;
        public Tester(SqlConnection connection, int count)
        {
            this.connection = connection;
            connection.Open();
            this.count = count;
        }

        public void OneByOne()
        {
            const string sql = "INSERT INTO [Test] ([Value]) Values (@Value)";
            for (var i = 0; i < count; i++)
            {
                connection.Execute(sql, new { Value = Guid.NewGuid().ToString()});
            }
        }

        public void BatchOf1000()
        {
            foreach (var batch in Enumerable.Range(0, count).Chunk(1000))
            {
                if (batch.Length == 0) continue;
                var sql = "INSERT INTO [Test] ([Value]) VALUES \r\n" + string.Join(",\r\n", batch.Select(x => $"('{Guid.NewGuid().ToString()}')"));
                connection.Execute(sql);
            }
        }

        public void BulkCopy()
        {
            var table = new DataTable();
            table.Columns.Add("Value", typeof(string));

            for (var i = 0; i < count; i++)
            {
                table.Rows.Add(Guid.NewGuid().ToString());
            }

            using (var bulk = new SqlBulkCopy(this.connection))
            {
                bulk.DestinationTableName = "test";
                bulk.WriteToServer(table);
            }
        }

    }


    public static class Extensions
    {

        public static IEnumerable<T[]> Chunk<T>(this IEnumerable<T> values, int chunkSize)
        {
            var buffer = new List<T>(chunkSize);

            foreach (var item in values)
            {
                buffer.Add(item);
                if (buffer.Count >= chunkSize)
                {
                    yield return buffer.ToArray();
                    buffer.Clear();
                }
            }

            yield return buffer.ToArray();

        }

    }
}
