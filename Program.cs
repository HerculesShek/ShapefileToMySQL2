﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Data;
using MySql.Data.MySqlClient;
using GeoAPI.Geometries;

namespace ShapefileToMySQL2
{
    class Program
    {
        static void Main(string[] args)
        {
             Progress();
             //Progress3();

            //Console.WriteLine(typeof(byte[]).ToString());
            //Console.ReadKey();
            // Progress2(3000000,1536);  //用时21秒左右
        }

        public static void Progress()
        {
            //String path = @"C:\Users\Hersules\Downloads\县级行政界线\BOUNT_poly.shp";
            String path = @"C:\Users\Hersules\Downloads\hyd4_4m\River4_polyline.shp";
            //String path = @"D:\Dropbox\SharpMap\Trunk\SharpMap-MySQL\Examples\DemoWebSite\App_Data\countries.shp";
            //String path = @"D:\Dropbox\SharpMap\Trunk\SharpMap-MySQL\Examples\DemoWebSite\App_Data\cities.shp";

            ShapeFile sf = new ShapeFile(path, "", true);
            String sql = sf.GetCreateTableDDL();
            Dictionary<uint, FeatureDataRow> d = sf.GetFeatures();
            sf.createTable();
            Console.WriteLine("SRID:" + sf.SRID);
            Console.WriteLine("行数：" + d.Count);
            Console.WriteLine("DbaseFile 文件编码： " + sf.DbaseFile.FileEncoding);
            Console.WriteLine("Create table 语句： " + sql);

            //Console.WriteLine("insert sql 语句： " + sf.getInsertDML());

            Stopwatch sw = new Stopwatch();
            sw.Start();
            //计时代码
            Console.WriteLine("\r\n 开始导入数据。。。。");
            //Console.Write(sf.InsertIntoMySQL(d) + "行数据");
            DataTable ds = sf.InsertIntoShapefileAndDBaseTable(d);
            sf.InsertWithMySqlBulkCopy(ds);

            Console.Write("导入结束！\r\n");
            sw.Stop();
            Console.WriteLine("总运行时间：" + sw.Elapsed);
            Console.WriteLine("测量实例得出的总运行时间（毫秒为单位）：" + sw.ElapsedMilliseconds);

            Console.ReadKey();
        }

        public static void Progress2(int c, int batchSize)
        {
            string connStr = String.Format("server={0};uid={1};pwd={2};database={3}",
              "localhost", "root", "xrt512", "shapefiles");
            MySqlConnection conn = new MySqlConnection(connStr);
            conn.Open();

            String sql = "insert into m values(";
            int l = sql.Length;
            StringBuilder buffer = new StringBuilder();
            Random r = new Random();


            Stopwatch sw = new Stopwatch();
            sw.Start();
            //计时代码
            Console.WriteLine("\r\n 开始导入数据。。。。");
            MySqlCommand cmd = new MySqlCommand();
            cmd.Connection = conn;


            int cc = 0;
            for (int i = 0; i < c; i++)
            {
                int ii = (i + 1) % batchSize;    //标记当前模运算的结果 注意i+1
                buffer.Append("@t" + i + "),(");
                if (ii == 0 || i == c - 1)
                {
                    sql += buffer.Replace(",(", "", buffer.Length - 2, 2).ToString();
                    //Console.WriteLine(sql);
                    cmd.CommandText = sql;
                    cmd.Parameters.Clear(); //清除上一次传入的参数！
                    for (int j = 0; j < (ii == 0 ? batchSize : (i + 1) % batchSize); j++)
                    {
                        //Console.WriteLine(j);
                        int m = j + batchSize * cc;
                        cmd.Parameters.Add(new MySqlParameter("@t" + m, new byte[] { 22, 42, 52, 6, 1, 3, 25, 123, 196, 45, 6, 7 }));
                    }
                    cmd.ExecuteNonQuery();
                    buffer.Clear();
                    sql = sql.Substring(0, l);
                    ++cc;
                }
            }
            conn.Clone();

            Console.Write("导入结束！\r\n");
            sw.Stop();
            Console.WriteLine("总运行时间：" + sw.Elapsed);
            Console.WriteLine("测量实例得出的总运行时间（毫秒为单位）：" + sw.ElapsedMilliseconds);
            Console.ReadKey();
        }

        public static void Progress3()
        {
            string filePath = @"D:\Data\shape\wh\Link11现状_WGS84_7_北京地方坐标系\Link11现状_WGS84_7_北京地方坐标系.shp";
            ShapeFile sf = new ShapeFile(filePath, null, true);
            sf.createTable();
            DataTable bufferTable;
            int batchSize = 1000;

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

        }
    }

}

