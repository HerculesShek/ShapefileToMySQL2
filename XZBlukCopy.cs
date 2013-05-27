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
            int count = table.Rows.Count;
            
            CommonFunctions functions = new CommonFunctions();
            string sql = functions.ConstructBaseSql(DestinationTableName, ColumnMapItems);
            int baseLength = sql.Length;
            
            StringBuilder buffer = new StringBuilder();
            MySqlCommand cmd = new MySqlCommand();
            cmd.Connection = DestinationDbConnection;
            for (int i = 0; i < count; i++)
            {
                int ii = (i + 1) % BatchSize;    //标记当前模运算的结果 注意i+1
                buffer.Append(functions.ConstructIndividualRowValue(table.Rows[i], ColumnMapItems));
                if (ii == 0 || i == count - 1)
                {
                    sql += buffer.ToString().Substring(0,buffer.Length-1);
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

        // insert into tableName(A,B,C...) values 
        public string ConstructBaseSql(string tableName, ColumnMapItemColl mapItemCollection)
        {
            string columnNames = GetColumnNames(mapItemCollection, ColumnProperty.Destination);
            string baseSql = string.Format("insert into {0}({1}) values ", tableName, columnNames);
            return baseSql;
        }

        //  (value1,value2,value3,value4,value5....),
        public string ConstructIndividualRowValue(DataRow row, ColumnMapItemColl mapItemCollection)
        {
            StringBuilder builder = new StringBuilder();
            foreach (XZColumnMapItem columnMapItem in mapItemCollection)
            {
                //TODO 当columnMapItem的DataType是"System.Byte[]"的时候问题很棘手
                string value = row[columnMapItem.SourceColumn].ToString();
                if (columnMapItem.DataType == "System.Byte[]")
                {
                    byte[] bytesData = (byte[])row[columnMapItem.SourceColumn];
                    value = System.Text.Encoding.UTF8.GetString(bytesData);
                }

                string constructedValue = ConstructIndividualValue(columnMapItem.DataType, value);
                builder.Append(constructedValue);
            }
            return "(" + builder.ToString().Substring(0, builder.ToString().Length - 1) + "),";
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
        private String _dataType;

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
