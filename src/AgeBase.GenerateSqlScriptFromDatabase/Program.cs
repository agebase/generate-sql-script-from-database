using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;

namespace AgeBase.GenerateSqlScriptFromDatabase
{
    public class Program
    {
        public static void Main(string[] args)
        {
            if (args == null || args.Length != 2)
            {
                Console.WriteLine("Missing arguments:");
                Console.WriteLine("generatescript \"server=.;database=test_db;user=sa;password=password\" \"c:\\temp\\script.sql\"");
                Environment.Exit(1);
            }

            var serverConnection = new ServerConnection(new SqlConnection(args[0]));
            var server = new Server(serverConnection);
            var database = server.Databases[server.ConnectionContext.DatabaseName];

            var urns = GetTables(database);

            urns.AddRange(GetIndexes(database));
            urns.AddRange(GetForeignKeys(database));

            var scripter = CreateScripter(server);
            var script = GenerateScript(scripter, urns);

            SaveScript(args[1], script);

            serverConnection.Disconnect();
        }

        private static Scripter CreateScripter(Server server)
        {
            Console.WriteLine("Setting script options");

            return new Scripter(server)
            {
                Options =
                {
                    ScriptSchema = true,
                    ScriptData = true,
                    TargetServerVersion = SqlServerVersion.Version110,
                    Default = true,
                    Indexes = true,
                    ClusteredIndexes = true,
                    FullTextIndexes = true,
                    NonClusteredIndexes = true,
                    DriAll = true,
                    IncludeDatabaseContext = false,
                    NoFileGroup = true,
                    NoTablePartitioningSchemes = true,
                    NoIndexPartitioningSchemes = true
                },
                PrefetchObjects = true
            };
        }

        private static StringBuilder GenerateScript(Scripter scripter, List<Urn> urns)
        {
            Console.WriteLine("Building script");

            var retval = new StringBuilder();

            foreach (var str in scripter.EnumScript(urns.ToArray()))
            {
                if (str.StartsWith("INSERT [dbo].[umbracoServer]"))
                {
                    Console.WriteLine("Ignoring umbracoServer insert");
                }
                else
                {
                    retval.AppendLine(str);
                    retval.AppendLine("GO");
                }
            }

            return retval;
        }

        private static List<Urn> GetTables(Database database)
        {
            Console.WriteLine("Getting tables");

            return database?.Tables?
                .Cast<Table>()
                .Where(tb => !tb.IsSystemObject)
                .Select(tb => tb.Urn)
                .ToList();
        }

        private static IEnumerable<Urn> GetIndexes(Database database)
        {
            Console.WriteLine("Getting indexes");

            var retval = new List<Urn>();

            foreach (Table table in database.Tables)
            {
                if (table.Name.Equals("sysdiagrams", StringComparison.CurrentCultureIgnoreCase))
                {
                    continue;
                }

                foreach (Index index in table.Indexes)
                {
                    if (index.IndexedColumns.Count > 0)
                    {
                        retval.Add(index.Urn);
                    }
                    else
                    {
                        Console.WriteLine($"Failed to add index for Table {table.Name}, Index {index.Name}");
                    }
                }
            }

            return retval;
        }

        private static IEnumerable<Urn> GetForeignKeys(Database database)
        {
            Console.WriteLine("Getting foreign keys");

            var retval = new List<Urn>();

            foreach (Table table in database.Tables)
            {
                if (table.Name.Equals("sysdiagrams", StringComparison.CurrentCultureIgnoreCase))
                {
                    continue;
                }

                foreach (ForeignKey foreignKey in table.ForeignKeys)
                {
                    if (foreignKey.Columns.Count > 0)
                    {
                        retval.Add(foreignKey.Urn);
                    }
                    else
                    {
                        Console.WriteLine($"Failed to add Foreign Key for Table {table.Name}, Foreign Key {foreignKey.Name}");
                    }
                }
            }

            return retval;
        }

        private static void SaveScript(string path, StringBuilder script)
        {
            Console.WriteLine("Writing script to disk");

            using (var binaryWriter = new BinaryWriter(new FileStream(path, FileMode.Create)))
            {
                binaryWriter.Write(Encoding.UTF8.GetBytes(script.ToString()));
                binaryWriter.Close();
            }
        }
    }
}