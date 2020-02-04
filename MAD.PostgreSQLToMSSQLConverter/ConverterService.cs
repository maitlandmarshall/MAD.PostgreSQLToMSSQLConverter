using Dapper;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MAD.PostgreSQLToMSSQLConverter
{
    public class ConverterService
    {
        public async Task ConvertPostgreToMSSQL(string postgreConnectionString, string sqlConnectionString)
        {
            using NpgsqlConnection postgreConnection = new NpgsqlConnection(postgreConnectionString);
            using SqlConnection mssqlConnection = new SqlConnection(sqlConnectionString);

            IEnumerable<SqlColumn> postgreTables = await this.GetAllTableColumns(postgreConnection);

            // Group the SqlColumns by TableName and build & execute CREATE TABLE statements
            // Then import data into mssqlConnection from postgreConnection
            var groupedByTableName = postgreTables.GroupBy(y => y.TableName);

            foreach (var group in groupedByTableName)
            {
                string tableName = group.Key;

                // Drop the table if it already exists
                await mssqlConnection.ExecuteAsync(@$"DROP TABLE IF EXISTS dbo.{tableName}");

                // Create the schema in MSSQL
                string createTableSql = this.GetMSSQLCreateTableStatementForArrayOfSqlColumn(tableName, group.ToArray());
                await mssqlConnection.ExecuteAsync(createTableSql);

                // Insert the data into MSSQL
                IEnumerable<dynamic> postgreData = await postgreConnection.QueryAsync($"SELECT * FROM {tableName}");

                if (!postgreData.Any())
                    continue;

                foreach (string insertIntoTableSql in this.PostgreDataToMSSQLInsertStatement(tableName, group.ToArray(), postgreData))
                {
                    await mssqlConnection.ExecuteAsync(insertIntoTableSql);
                }
            }
        }

        private IEnumerable<string> PostgreDataToMSSQLInsertStatement(string tableName, SqlColumn[] sqlColumns, IEnumerable<dynamic> postgreData)
        {
            do
            {
                IEnumerable<dynamic> batch = postgreData.Take(1000);

                StringBuilder sb = new StringBuilder();
                sb.AppendLine($"INSERT INTO {tableName} ( {String.Join(",", sqlColumns.Select(y => $"[{y.ColumnName}]"))} )");
                sb.AppendLine("VALUES");

                IEnumerable<string> insertValueArrays = batch
                    .Select(y => this.DictionaryToInsertValueArray(y as IDictionary<string, object>, sqlColumns))
                    .ToArray();

                sb.AppendLine(String.Join(",", insertValueArrays));

                yield return sb.ToString();

                postgreData = postgreData.Skip(1000);

            } while (postgreData.Any());
        }

        private string DictionaryToInsertValueArray(IDictionary<string, object> insertValueDict, SqlColumn[] sqlColumns)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("(");

            sb.AppendLine(String.Join(",", sqlColumns.Select(y =>
            {
                string dataType = this.PostgreSqlDataTypeToMSSQLDataType(y.DataType);
                object value = insertValueDict[y.ColumnName];

                if (value is null)
                    return "null";

                if (value is string valueStr)
                {
                    return $"'{valueStr.Replace("'", "''")}'";
                } 
                else if (value is DateTime valueDate)
                {
                    return $"'{valueDate.ToString("yyyy-MM-dd HH:mm:ss.fff")}'";
                }
                else if (value is NpgsqlPoint valuePoint)
                {
                    return $"'{valuePoint.ToString()}'";
                }
                else if (value is bool valueBool)
                {
                    return valueBool ? 1 : 0;
                } 
                else if (value is byte[] valueByteArray)
                {
                    return $"0x{BitConverter.ToString(valueByteArray).Replace("-", "")}";
                }
                else if (value is IEnumerable valueArray)
                {
                    return $"'{String.Join(",", valueArray.Cast<object>())}'";
                }
                else if (value is TimeSpan valueTimeSpan)
                {
                    return $"'{valueTimeSpan.ToString()}'";
                }
                else if (value is DateTimeOffset valueDateTimeOffset)
                {
                    return $"'{valueDateTimeOffset.ToString("hh:mm:ss")}'";
                }

                if (dataType == "nvarchar")
                {
                    return $"'{insertValueDict[y.ColumnName]}'";
                }

                return insertValueDict[y.ColumnName];
            })));

            sb.AppendLine(")");

            return sb.ToString();
        }

        private string GetMSSQLCreateTableStatementForArrayOfSqlColumn(string tableName, SqlColumn[] sqlColumns)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"CREATE TABLE {tableName} (");
            sb.Append(
                String.Join($",{Environment.NewLine}", sqlColumns.Select(this.SqlColumnToMSSQLCreateTableColumn))
            );

            sb.Append(")");

            return sb.ToString();
        }

        private string SqlColumnToMSSQLCreateTableColumn(SqlColumn column)
        {
            StringBuilder sb = new StringBuilder();

            string columnName = column.ColumnName;
            string dataType = this.PostgreSqlDataTypeToMSSQLDataType(column.DataType);

            sb.Append($"[{columnName}] {dataType}");

            if (dataType == "nvarchar")
            {
                sb.Append("(max)");
            }
            else if (dataType == "numeric")
            {
                sb.Append("(18, 7)");
            }
            else if (dataType == "varbinary")
            {
                sb.Append("(max)");
            }

            if (!column.IsNullable)
            {
                sb.Append(" NOT NULL");
            }
            else
            {
                sb.Append(" NULL");
            }

            return sb.ToString();
        }

        private string PostgreSqlDataTypeToMSSQLDataType(string postgreDataType)
        {
            switch (postgreDataType.ToLower())
            {
                case "char":
                case "text":
                case "varchar":
                case "xml":
                case "character varying":
                case "point":
                case "character":
                case "array":
                case "inet":
                case "interval":
                    return "nvarchar";
                case "integer":
                case "smallint":
                    return "int";
                case "bigint":
                    return "bigint";
                case "boolean":
                    return "bit";
                case "uuid":
                    return "uniqueidentifier";
                case "bytea":
                    return "varbinary";
                case "float":
                    return "float";
                case "real":
                    return "real";
                case "numeric":
                    return "numeric";
                case "date":
                    return "date";
                case "timestamptz":
                case "timestamp with time zone":
                case "timestamp without time zone":
                    return "datetime";
                case "time without time zone":
                case "time with time zone":
                    return "time";
                case "double precision":
                    return "double precision";
                default:
                    throw new NotImplementedException();
            }
        }

        private async Task<IEnumerable<SqlColumn>> GetAllTableColumns(IDbConnection dbConnection)
        {
            IEnumerable<SqlColumn> sqlColumns = await dbConnection.QueryAsync<SqlColumn>(@"
SELECT table_catalog as TableCatalog, table_schema as TableSchema, table_name as TableName, 
column_name as ColumnName, ordinal_position as OrdinalPosition, CASE WHEN is_nullable = 'NO' THEN 0 ELSE 1 END as IsNullable, data_type as DataType FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_catalog, table_schema, table_name, ordinal_position");

            return sqlColumns;
        }
    }
}
