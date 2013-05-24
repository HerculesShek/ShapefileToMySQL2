using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Data;

namespace ShapefileToMySQL2
{
    public static class Shapefile2MySQL
    {
        private static int batchSize = 15000;

        public static int BatchSize
        {
            get { return batchSize; }
            set { batchSize = value; }
        }

        public static TimeSpan ExportToDB(String filePath, String tableName)
        {
            ShapeFile sf = new ShapeFile(filePath, tableName, true);
            sf.createTable();
            Stopwatch sw = new Stopwatch();
            sw.Start();

            DataTable bufferTable;
            int quotient = sf.FeatureCount / batchSize;
            int remainder = sf.FeatureCount % batchSize;

            for (int i = 0; i < quotient; i++)
            {
                uint start = (uint)(i * batchSize);
                int end = (i + 1) * batchSize - 1;
                bufferTable = sf.InsertIntoBufferTable(start, end);
                sf.InsertWithMySqlBulkCopy(bufferTable);
                sf.ShapefileAndDBaseTable.Clear();
            }
            if (remainder != 0)
            {
                uint start = (uint)(quotient * batchSize);
                int end = sf.FeatureCount - 1;
                bufferTable = sf.InsertIntoBufferTable(start, end);
                sf.InsertWithMySqlBulkCopy(bufferTable);
                sf.ShapefileAndDBaseTable.Clear();
            }
            sw.Stop();
            return sw.Elapsed;
        }

        public static TimeSpan Progress(String filePath, String tableName)
        {
            ShapeFile sf = new ShapeFile(filePath, tableName, true);
            sf.createTable();

            Dictionary<uint, FeatureDataRow> d = sf.GetFeatures();
            
            Stopwatch sw = new Stopwatch();
            sw.Start();
            //sf.InsertIntoMySQL(d);

            DataTable ds = sf.InsertIntoShapefileAndDBaseTable(d);
            sf.InsertWithMySqlBulkCopy2(ds);
            sw.Stop();
            return sw.Elapsed;
        }
    }
}
