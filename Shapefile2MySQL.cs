using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Data;

namespace ShapefileToMySQL2
{
    public static class Shapefile2MySQL
    {
        public static TimeSpan Progress(String filePath, String tableName)
        {
            ShapeFile sf = new ShapeFile(filePath, tableName, true);
            sf.createTable();
            Dictionary<uint, FeatureDataRow> d = sf.GetFeatures();
            
            Stopwatch sw = new Stopwatch();
            sw.Start();
            sf.InsertIntoMySQL(d);
            //DataTable ds = sf.InsertIntoDataTable(d);
            //sf.InsertWithMySqlBulkCopy2(ds);
            sw.Stop();
            return sw.Elapsed;
        }
    }
}
