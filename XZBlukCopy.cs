using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using System.Diagnostics;
using System.Collections;
using System.Globalization;
using System.Data;

namespace ShapefileToMySQL2
{
    public class XZBlukCopy
    {
        private ColumnMapItemColl _columnMapItems;//column mappings
        private MySqlConnection _destinationDbConnection;
        private String _destinationTableName;//table name in DB 
        private int _batchSize;//batch size

        public ColumnMapItemColl ColumnMapItems
        {
            get { return _columnMapItems; }
            set { _columnMapItems = value; }
        }
        public MySqlConnection DestinationDbConnection
        {
            get { return _destinationDbConnection; }
            set { _destinationDbConnection = value; }
        }
        public string DestinationTableName
        {
            get { return _destinationTableName; }
            set { _destinationTableName = value; }
        }
        public int BatchSize
        {
            get { return _batchSize; }
            set { _batchSize = value; }
        }

        public void Upload(DataTable table)
        {
            bool includeBlob = false;
            foreach (XZColumnMapItem columnMapItem in ColumnMapItems)
            {
                if (columnMapItem.DataType.ToLower().CompareTo("byte[]") == 0 || columnMapItem.DataType.ToLower().CompareTo("blob") == 0)
                {
                    includeBlob = true;
                    break;
                }
            }
            if (includeBlob)
            {
                UploadWithBlob(table);
            }
            else {
                UploadWithoutBlob(table);
            }

        }
        
        public void UploadWithoutBlob(DataTable table)
        {
            int count = table.Rows.Count;

            CommonFunctions functions = new CommonFunctions();
            string sql = functions.ConstructBaseSql(DestinationTableName, ColumnMapItems);
            int baseLength = sql.Length;

            StringBuilder buffer = new StringBuilder();
            MySqlCommand cmd = new MySqlCommand();
            cmd.Connection = DestinationDbConnection;
            for (int rowIndex = 0; rowIndex < count; rowIndex++)
            {
                int flag = (rowIndex + 1) % BatchSize;    //标记当前模运算的结果 注意i+1
                buffer.Append(functions.ConstructIndividualRowValueWithoutBlob(table.Rows[rowIndex], ColumnMapItems));
                if (flag == 0 || rowIndex == count - 1)
                {
                    sql += buffer.ToString().Substring(0, buffer.Length - 1);
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                    buffer.Clear();
                    sql = sql.Substring(0, baseLength);
                }
            }
        }

        // if the ColumnMapItems has a XZColumnMapItem with a blob type, this method will be invoked!
        public void UploadWithBlob(DataTable table)
        {
            int count = table.Rows.Count;

            CommonFunctions functions = new CommonFunctions();
            string sql = functions.ConstructBaseSql(DestinationTableName, ColumnMapItems);
            int baseLength = sql.Length;

            StringBuilder buffer = new StringBuilder();
            MySqlCommand cmd = new MySqlCommand();
            //cmd.Parameters.Add("a", MySqlDbType.VarChar,255).Value = "";
            cmd.Connection = DestinationDbConnection;
            for (int rowIndex = 0; rowIndex < count; rowIndex++)
            {
                int flag = (rowIndex + 1) % BatchSize;    //标记当前模运算的结果 注意rowIndex+1
                //TODO change the string 
                buffer.Append(functions.ConstIndiRowParaStrWithoutBlob(rowIndex, ColumnMapItems));
                functions.ConstIndiRowParameterValue(rowIndex, cmd, table.Rows[rowIndex], ColumnMapItems);
                
                if (flag == 0 || rowIndex == count - 1)
                {
                    sql += buffer.ToString().Substring(0, buffer.Length - 1);
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                    buffer.Clear();
                    sql = sql.Substring(0, baseLength);
                }
            }
        }

    }

    internal class CommonFunctions
    {
        public string GetColumnNames(ColumnMapItemColl mapItemCollection, ColumnProperty propertyToFetch)
        {
            if (mapItemCollection == null) { throw new ArgumentNullException("mapItemCollection"); }
            if (mapItemCollection.Count <= 0) { throw new ArgumentOutOfRangeException("mapItemCollection"); }

            StringBuilder builder = new StringBuilder();
            switch (propertyToFetch)
            {
                case ColumnProperty.Source:
                    foreach (XZColumnMapItem columnMapItem in mapItemCollection)
                    {
                        builder.AppendFormat("`{0}`,", columnMapItem.SourceColumn);
                    }
                    break;
                case ColumnProperty.Destination:
                    foreach (XZColumnMapItem columnMapItem in mapItemCollection)
                    {
                        builder.AppendFormat("`{0}`,", columnMapItem.DestinationColumn);
                    }
                    break;
                default:
                    builder.Append(",");
                    break;
            }
            return builder.ToString().Substring(0, builder.Length - 1);
        }

        //get string "insert into tableName(A,B,C...) values "
        public string ConstructBaseSql(string tableName, ColumnMapItemColl mapItemCollection)
        {
            string columnNames = GetColumnNames(mapItemCollection, ColumnProperty.Destination);
            string baseSql = string.Format("insert into {0}({1}) values ", tableName, columnNames);
            return baseSql;
        }

        //get string "(value1,value2,value3,value4,value5....),"
        public string ConstructIndividualRowValueWithoutBlob(DataRow row, ColumnMapItemColl mapItemCollection)
        {
            StringBuilder builder = new StringBuilder();
            foreach (XZColumnMapItem columnMapItem in mapItemCollection)
            {
                string value = row[columnMapItem.SourceColumn].ToString();
                string constructedValue = ConstructIndividualValue(columnMapItem.DataType, value);
                builder.Append(constructedValue);
            }
            return "(" + builder.ToString().Substring(0, builder.ToString().Length - 1) + "),";
        }

        //"(?rootName1,?pid1,?pName1,?tid1,?name1,?did1),"
        public string ConstIndiRowParaStrWithoutBlob(int rowIndex, ColumnMapItemColl mapItemCollection)
        {
            StringBuilder builder = new StringBuilder();
            foreach (XZColumnMapItem columnMapItem in mapItemCollection)
            {
                string columnName = columnMapItem.DestinationColumn.Replace("~", "_");
                builder.Append("?").Append(columnName).Append("note").Append(rowIndex).Append(",");
            }
            return "(" + builder.ToString().Substring(0, builder.ToString().Length - 1) + "),";
        }


        public void ConstIndiRowParameterValue(int rowIndex, MySqlCommand cmd, DataRow row, ColumnMapItemColl mapItemCollection) {
            foreach (XZColumnMapItem columnMapItem in mapItemCollection)
            {
                StringBuilder builder = new StringBuilder();
                string columnName = columnMapItem.DestinationColumn.Replace("~", "_");
                builder.Append("?").Append(columnName).Append("note").Append(rowIndex);
                string parameterName = builder.ToString();
                cmd.Parameters.Add(parameterName, GetMySQLDataTypeFromCSharp(columnMapItem.DataType)).Value = row[columnMapItem.SourceColumn];
            }
        }

        public MySqlDbType GetMySQLDataTypeFromCSharp(string typeName)
        {
            switch (typeName.ToLower())
            {
                case "uint16":
                    return MySqlDbType.UInt16;
                case "uint32":
                    return MySqlDbType.UInt32;
                case "uint64":
                    return MySqlDbType.UInt64;
                case "byte":
                    return MySqlDbType.Byte;
                case "byte[]":
                    return MySqlDbType.LongBlob;
                case "bool":
                    return MySqlDbType.Bit;
                case "int16":
                    return MySqlDbType.Int16;
                case "int32":
                    return MySqlDbType.Int32;
                case "int64":
                    return MySqlDbType.Int64;
                case "single":
                    return MySqlDbType.Float;
                case "double":
                    return MySqlDbType.Double;
                case "string":
                    return MySqlDbType.VarChar;
                case "datetime":
                    return MySqlDbType.DateTime;
                case "object":
                    return MySqlDbType.Blob;
                default:
                    throw new InvalidOperationException("CSharp data type '" + typeName + "' has no matched by MySQL.");
            }
        }

        private string ConstructIndividualValue(string dataType, string value)
        {
            string returnValue = "";
            switch (dataType.ToUpper())
            {
                case "INT":
                case "TINYINT":
                case "SMALLINT":
                case "MEDIUMINT":
                case "BIGINT":
                case "FLOAT":
                case "DOUBLE":
                case "DECIMAL":
                    returnValue = string.Format("{0},", value);
                    break;
                case "CHAR":
                case "VARCHAR":
                case "BLOB":
                case "TEXT":
                case "TINYBLOB":
                case "TINYTEXT":
                case "MEDIUMBLOB":
                case "MEDIUMTEXT":
                case "LONGBLOB":
                case "LONGTEXT":
                case "ENUM":
                    returnValue = string.Format("'{0}',", MySql.Data.MySqlClient.MySqlHelper.EscapeString(value));
                    break;
                case "DATE":
                    returnValue = String.Format("'{0:yyyy-MM-dd}',", value);
                    //returnValue = string.Format(CultureInfo.InvariantCulture, "{0:dd-MM-yyyy}", value);
                    break;
                case "TIMESTAMP":
                case "DATETIME":
                    DateTime date = DateTime.Parse(value);
                    returnValue = String.Format("'{0:yyyy-MM-dd HH:mm:ss}',", date);
                    break;
                case "TIME":
                    returnValue = String.Format("'{0:HH:mm:ss}',", value);
                    break;

                case "YEAR2":
                    returnValue = String.Format("'{0:yy}',", value);
                    break;
                case "YEAR4":
                    returnValue = String.Format("'{0:yyyy}',", value);
                    break;
                default:
                    // we don't understand the format. to safegaurd the code, just enclose with ''
                    returnValue = string.Format("'{0}',", MySql.Data.MySqlClient.MySqlHelper.EscapeString(value));
                    break;
            }
            return returnValue;
        }

       
    }

    public enum ColumnProperty
    {
        Source,
        Destination,
    }

    public class ColumnMapItemColl : CollectionBase
    {
        public XZColumnMapItem Item(int index)
        {
            if (index < 0) { throw new IndexOutOfRangeException("index"); }
            if (index > Count - 1) { throw new IndexOutOfRangeException("index"); }
            return (XZColumnMapItem)List[index];
        }

        public bool Add(XZColumnMapItem item)
        {
            if (item == null) { throw new ArgumentNullException("item"); }
            XZColumnMapItem existing = Find(item.DestinationColumn);
            if (existing != null)
            {
                if (existing.DestinationColumn.ToUpper() == item.DestinationColumn.ToUpper())
                {
                    return false;
                }
            }

            List.Add(item);
            return true;
        }

        public void Remove(int index)
        {
            if (index < 0) { throw new IndexOutOfRangeException("index"); }
            if (index > Count - 1) { throw new IndexOutOfRangeException("index"); }
            List.RemoveAt(index);
        }

        public XZColumnMapItem Find(string destinationColumnName)
        {
            // Find a columnMap in the collection
            return List.Cast<XZColumnMapItem>().FirstOrDefault(item => item.DestinationColumn.ToUpper() == destinationColumnName.ToUpper());
        }
    }

    public class XZColumnMapItem
    {
        private String _sourceColumn;
        private String _destinationColumn;
        private String _dataType;//C# DataType.ToString()

        public XZColumnMapItem() { }
        public XZColumnMapItem(String s, String d, String dt)
        {
            _sourceColumn = s;
            _destinationColumn = d;
            _dataType = dt;
        }

        public string SourceColumn
        {
            get { return _sourceColumn; }
            set { _sourceColumn = value; }
        }
        public string DestinationColumn
        {
            get { return _destinationColumn; }
            set { _destinationColumn = value; }
        }
        public string DataType
        {
            get { return _dataType; }
            set { _dataType = value; }
        }
    }

}
