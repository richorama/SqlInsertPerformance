using System;
using System.Data.SqlClient;
using System.Diagnostics;
using Dapper;

namespace BulkInsertTests
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var connection = new SqlConnection("Data Source=localhost;Initial Catalog=Test;Trusted_Connection=Yes;"))
            {
                var tester = new Tester(connection, 2_000);

                var run = new Action<string,Action>((name, test) => { 

                    connection.Execute("truncate table [test]");
                    var stopwatch = Stopwatch.StartNew();

                    test();

                    stopwatch.Stop();

                    Console.WriteLine($"{name} - elapsed time: {stopwatch.ElapsedMilliseconds}ms");
                });

                run(nameof(tester.OneByOne), tester.OneByOne);
                run(nameof(tester.BatchOf1000), tester.BatchOf1000);
                run(nameof(tester.BulkCopy), tester.BulkCopy);
            }

            Console.WriteLine("Press [Enter]...");
            Console.ReadLine();
        }
    }
}


/*
OneByOne    - elapsed time: 54533ms (10,000 records) 
BulkInsert  - elapsed time:  9315ms (1,000,000 records)
BatchOf1000 - elapsed time: 22256ms (1,000,000 records)
*/

