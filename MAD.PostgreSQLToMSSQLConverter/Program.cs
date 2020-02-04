using System;

namespace MAD.PostgreSQLToMSSQLConverter
{
    class Program
    {
        static void Main(string[] args)
        {
            const string postgreSqlConnectionString = "Server=127.0.0.1;Database=InherentSolutions_SysPro;User ID=postgres;password=ILikeCheese;timeout=1000;";
            const string mssqlConnectionString = @"Data Source=(local)\DEVSQL2019;Initial Catalog=InherentSolutions_SysPro;User id=sa;Password=ILikeCheese;";

            new ConverterService().ConvertPostgreToMSSQL(postgreSqlConnectionString, mssqlConnectionString).Wait();
        }
    }
}
