using System;
using System.Collections.Generic;
using System.Text;

namespace MAD.PostgreSQLToMSSQLConverter
{
    public class SqlColumn
    {
        public string TableCatalog { get; set; }
        public string TableSchema { get; set; }
        public string TableName { get; set; }
        public string ColumnName { get; set; }
        public string DataType { get; set; }
        public bool IsNullable { get; set; }
        public int OrdinalPosition { get; set; }
    }
}
