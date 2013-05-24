using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Text;
using ProjNet.Converters.WellKnownText;
using GeoAPI.CoordinateSystems;
using System.Data;
using MySql.Data.MySqlClient;
using NetTopologySuite;
using GeoAPI;
using GeoAPI.Geometries;


namespace ShapefileToMySQL2
{
    /// <summary>
    /// Shapefile geometry type.
    /// </summary>
    public enum ShapeType
    {
        /// <summary>
        /// Null shape with no geometric data
        /// </summary>
        Null = 0,
        /// <summary>
        /// A point consists of a pair of double-precision coordinates.
        /// SharpMap interprets this as <see cref="GeoAPI.Geometries.IPoint"/>
        /// </summary>
        Point = 1,
        /// <summary>
        /// PolyLine is an ordered set of vertices that consists of one or more parts. A part is a
        /// connected sequence of two or more points. Parts may or may not be connected to one
        ///	another. Parts may or may not intersect one another.
        /// SharpMap interprets this as either <see cref="GeoAPI.Geometries.ILineString"/> or <see cref="GeoAPI.Geometries.IMultiLineString"/>
        /// </summary>
        PolyLine = 3,
        /// <summary>
        /// A polygon consists of one or more rings. A ring is a connected sequence of four or more
        /// points that form a closed, non-self-intersecting loop. A polygon may contain multiple
        /// outer rings. The order of vertices or orientation for a ring indicates which side of the ring
        /// is the interior of the polygon. The neighborhood to the right of an observer walking along
        /// the ring in vertex order is the neighborhood inside the polygon. Vertices of rings defining
        /// holes in polygons are in a counterclockwise direction. Vertices for a single, ringed
        /// polygon are, therefore, always in clockwise order. The rings of a polygon are referred to
        /// as its parts.
        /// SharpMap interprets this as either <see cref="GeoAPI.Geometries.IPolygon"/> or <see cref="GeoAPI.Geometries.IMultiPolygon"/>
        /// </summary>
        Polygon = 5,
        /// <summary>
        /// A MultiPoint represents a set of points.
        /// SharpMap interprets this as <see cref="GeoAPI.Geometries.IMultiPoint"/>
        /// </summary>
        Multipoint = 8,
        /// <summary>
        /// A PointZ consists of a triplet of double-precision coordinates plus a measure.
        /// SharpMap interprets this as <see cref="GeoAPI.Geometries.IPoint"/>
        /// </summary>
        PointZ = 11,
        /// <summary>
        /// A PolyLineZ consists of one or more parts. A part is a connected sequence of two or
        /// more points. Parts may or may not be connected to one another. Parts may or may not
        /// intersect one another.
        /// SharpMap interprets this as <see cref="GeoAPI.Geometries.ILineString"/> or <see cref="GeoAPI.Geometries.IMultiLineString"/>
        /// </summary>
        PolyLineZ = 13,
        /// <summary>
        /// A PolygonZ consists of a number of rings. A ring is a closed, non-self-intersecting loop.
        /// A PolygonZ may contain multiple outer rings. The rings of a PolygonZ are referred to as
        /// its parts.
        /// SharpMap interprets this as either <see cref="GeoAPI.Geometries.IPolygon"/> or <see cref="GeoAPI.Geometries.IMultiPolygon"/>
        /// </summary>
        PolygonZ = 15,
        /// <summary>
        /// A MultiPointZ represents a set of <see cref="PointZ"/>s.
        /// SharpMap interprets this as <see cref="GeoAPI.Geometries.IMultiPoint"/>
        /// </summary>
        MultiPointZ = 18,
        /// <summary>
        /// A PointM consists of a pair of double-precision coordinates in the order X, Y, plus a measure M.
        /// SharpMap interprets this as <see cref="GeoAPI.Geometries.IPoint"/>
        /// </summary>
        PointM = 21,
        /// <summary>
        /// A shapefile PolyLineM consists of one or more parts. A part is a connected sequence of
        /// two or more points. Parts may or may not be connected to one another. Parts may or may
        /// not intersect one another.
        /// SharpMap interprets this as <see cref="GeoAPI.Geometries.ILineString"/> or <see cref="GeoAPI.Geometries.IMultiLineString"/>
        /// </summary>
        PolyLineM = 23,
        /// <summary>
        /// A PolygonM consists of a number of rings. A ring is a closed, non-self-intersecting loop.
        /// SharpMap interprets this as either <see cref="GeoAPI.Geometries.IPolygon"/> or <see cref="GeoAPI.Geometries.IMultiPolygon"/>
        /// </summary>
        PolygonM = 25,
        /// <summary>
        /// A MultiPointM represents a set of <see cref="PointM"/>s.
        /// SharpMap interprets this as <see cref="GeoAPI.Geometries.IMultiPoint"/>
        /// </summary>
        MultiPointM = 28,
        /// <summary>
        /// A MultiPatch consists of a number of surface patches. Each surface patch describes a
        /// surface. The surface patches of a MultiPatch are referred to as its parts, and the type of
        /// part controls how the order of vertices of an MultiPatch part is interpreted.
        /// SharpMap doesn't support this feature type.
        /// </summary>
        MultiPatch = 31
    } ;

    public class ShapeFile //: FilterProvider, IProvider
    {

        private ICoordinateSystem _coordinateSystem;
        private bool _coordsysReadFromFile;  //坐标系统是否已经从文件中获得

        private int _fileSize;
        private Envelope _envelope;
        private int _featureCount;
        public int FeatureCount
        {
            get { return _featureCount; }
            set { _featureCount = value; }
        }
        private bool _fileBasedIndex;   //基于文件的索引 表示有没有shx文件
        private readonly bool _fileBasedIndexWanted;
        private string _filename;
        private bool _isOpen;
        private ShapeType _shapeType;
        private int _srid = -1;
        private BinaryReader _brShapeFile;
        private BinaryReader _brShapeIndex;
        /// <summary>
        /// The Dbase-III File for attribute data
        /// </summary>
        public DbaseReader DbaseFile;
        private Stream _fsShapeFile;
        private IGeometryFactory _factory;

        //private static int _memoryCacheLimit = 50000;


        private static readonly object GspLock = new object();


        private Stream _fsShapeIndex;
        private readonly bool _useMemoryCache;
        private DateTime _lastCleanTimestamp = DateTime.Now;
        private readonly TimeSpan _cacheExpireTimeout = TimeSpan.FromMinutes(1);
        private readonly Dictionary<uint, FeatureDataRow> _cacheDataTable = new Dictionary<uint, FeatureDataRow>();

        private int[] _offsetOfRecord;

        /// <summary>
        /// Initializes a ShapeFile DataProvider without a file-based spatial index.
        /// </summary>
        /// <param name="filename">Path to shape file</param>
        public ShapeFile(string filename)
            : this(filename, "",true)
        {
        }


        /// <summary>
        /// Initializes a ShapeFile DataProvider.
        /// </summary>
        /// <remarks>
        /// <para>If FileBasedIndex is true, the spatial index will be read from a local copy. If it doesn't exist,
        /// it will be generated and saved to [filename] + '.sidx'.</para>
        /// <para>Using a file-based index is especially recommended for ASP.NET applications which will speed up
        /// start-up time when the cache has been emptied.
        /// </para>
        /// </remarks>
        /// <param name="filename">Path to shape file</param>
        /// <param name="fileBasedIndex">Use file-based spatial index</param>
        public ShapeFile(string filename, String tableName, bool fileBasedIndex)
        {
            _filename = filename;
            _fileBasedIndexWanted = fileBasedIndex;
            _fileBasedIndex = (_fileBasedIndexWanted) && File.Exists(Path.ChangeExtension(filename, ".shx"));

            TableName = tableName;

            //Initialize DBF
            //获取包含修改的路径信息的字符串
            var dbffile = Path.ChangeExtension(filename, ".dbf");
            if (File.Exists(dbffile))
            {
                DbaseFile = new DbaseReader(dbffile);   //这里是仅仅指明了一下，没有解析
            }
            //Parse shape header
            ParseHeader();
            //Read projection file
            //投影文件，这里一定要执行的，因为SRID的信息在prj文件当中，没有SRID就没有Factory
            ParseProjection();
            //By default, don't enable _MemoryCache if there are a lot of features
            //_useMemoryCache = GetFeatureCount() <= MemoryCacheLimit;
        }

        /// <summary>
        /// Initializes a ShapeFile DataProvider.
        /// </summary>
        /// <remarks>
        /// <para>If FileBasedIndex is true, the spatial index will be read from a local copy. If it doesn't exist,
        /// it will be generated and saved to [filename] + '.sidx'.</para>
        /// <para>Using a file-based index is especially recommended for ASP.NET applications which will speed up
        /// start-up time when the cache has been emptied.
        /// </para>
        /// </remarks>
        /// <param name="filename">Path to shape file</param>
        /// <param name="fileBasedIndex">Use file-based spatial index</param>
        /// <param name="useMemoryCache">Use the memory cache. BEWARE in case of large shapefiles</param>
        public ShapeFile(string filename, bool fileBasedIndex, bool useMemoryCache)
            : this(filename, fileBasedIndex, useMemoryCache, 0)
        {
        }

        /// <summary>
        /// Initializes a ShapeFile DataProvider.
        /// </summary>
        /// <remarks>
        /// <para>If FileBasedIndex is true, the spatial index will be read from a local copy. If it doesn't exist,
        /// it will be generated and saved to [filename] + '.sidx'.</para>
        /// <para>Using a file-based index is especially recommended for ASP.NET applications which will speed up
        /// start-up time when the cache has been emptied.
        /// </para>
        /// </remarks>
        /// <param name="filename">Path to shape file</param>
        /// <param name="fileBasedIndex">Use file-based spatial index</param>
        /// <param name="useMemoryCache">Use the memory cache. BEWARE in case of large shapefiles</param>
        /// <param name="SRID">The spatial reference id</param>
        public ShapeFile(string filename, bool fileBasedIndex, bool useMemoryCache, int SRID)
            : this(filename, "",fileBasedIndex)
        {
            _useMemoryCache = useMemoryCache;
            this.SRID = SRID;
        }

        /// <summary>
        /// Gets or sets the coordinate system of the ShapeFile. If a shapefile has 
        /// a corresponding [filename].prj file containing a Well-Known Text 
        /// description of the coordinate system this will automatically be read.
        /// If this is not the case, the coordinate system will default to null.
        /// </summary>
        /// <exception cref="ApplicationException">An exception is thrown if the coordinate system is read from file.</exception>
        public ICoordinateSystem CoordinateSystem
        {
            get { return _coordinateSystem; }
            set
            {
                if (_coordsysReadFromFile)
                    throw new ApplicationException("Coordinate system is specified in projection file and is read only");
                _coordinateSystem = value;
            }
        }

        /// <summary>
        /// Gets the <see cref="SharpMap.Data.Providers.ShapeType">shape geometry type</see> in this shapefile.
        /// </summary>
        /// <remarks>
        /// The property isn't set until the first time the datasource has been opened,
        /// and will throw an exception if this property has been called since initialization. 
        /// <para>All the non-Null shapes in a shapefile are required to be of the same shape
        /// type.</para>
        /// </remarks>
        public ShapeType ShapeType
        {
            get { return _shapeType; }
        }

        /// <summary>
        /// Gets or sets the filename of the shapefile
        /// </summary>
        /// <remarks>If the filename changes, indexes will be rebuilt</remarks>
        public string Filename
        {
            get { return _filename; }
            set
            {
                if (value != _filename)
                {
                    if (IsOpen)
                        throw new ApplicationException("Cannot change filename while datasource is open");

                    _filename = value;
                    _fileBasedIndex = (_fileBasedIndexWanted) && File.Exists(Path.ChangeExtension(value, ".shx"));

                    var dbffile = Path.ChangeExtension(value, ".dbf");
                    if (File.Exists(dbffile))
                        DbaseFile = new DbaseReader(dbffile);

                    ParseHeader();
                    ParseProjection();
                }
            }
        }

        /// <summary>
        /// Gets or sets the encoding used for parsing strings from the DBase DBF file.
        /// </summary>
        /// <remarks>
        /// The DBase default encoding is <see cref="System.Text.Encoding.UTF8"/>.
        /// </remarks>
        public Encoding Encoding
        {
            get { return DbaseFile.Encoding; }
            set { DbaseFile.Encoding = value; }
        }


        #region Disposers and finalizers

        private bool _disposed;

        /// <summary>
        /// Finalizes the object
        /// </summary>
        ~ShapeFile()
        {
            _disposed = true;
            // Dispose();
        }

        #endregion

        #region IProvider Members

        /// <summary>
        /// Opens the datasource
        /// </summary>
        public void Open()  //这里面最重要的两个操作是填充_offsetOfRecord和调用DbaseFile的open函数
        {
            // Get a Connector.  The connector returned is guaranteed to be connected and ready to go.
            // Pooling.Connector connector = Pooling.ConnectorPool.ConnectorPoolManager.RequestConnector(this,true);
            if (!_isOpen)
            {
                string shxFile = Path.ChangeExtension(_filename, "shx");
                if (File.Exists(shxFile))
                {
                    _fsShapeIndex = new FileStream(shxFile, FileMode.Open, FileAccess.Read);
                    _brShapeIndex = new BinaryReader(_fsShapeIndex, Encoding.Unicode);
                }
                _fsShapeFile = new FileStream(_filename, FileMode.Open, FileAccess.Read);
                _brShapeFile = new BinaryReader(_fsShapeFile);
                // Create array to hold the index array for this open session
                _offsetOfRecord = new int[_featureCount];
                PopulateIndexes();
                //InitializeShape(_filename, _fileBasedIndex);
                if (DbaseFile != null)
                    DbaseFile.Open();
                _isOpen = true;
                CreateShapefileAndDBaseTable();
            }
        }

        /// <summary>
        /// Closes the datasource
        /// </summary>
        public void Close()
        {
            if (!_disposed)
            {
                if (_isOpen)
                {
                    _brShapeFile.Close();
                    _fsShapeFile.Close();
                    if (_brShapeIndex != null)
                    {
                        _brShapeIndex.Close();
                        _fsShapeIndex.Close();
                    }

                    // Give back the memory from the index array.
                    _offsetOfRecord = null;

                    if (DbaseFile != null)
                        DbaseFile.Close();
                    _isOpen = false;
                }
            }
        }

        /// <summary>
        /// Returns true if the datasource is currently open
        /// </summary>		
        public bool IsOpen
        {
            get { return _isOpen; }
        }

        /// <summary>
        /// Returns geometries whose bounding box intersects 'bbox'
        /// </summary>
        /// <remarks>
        /// <para>Please note that this method doesn't guarantee that the geometries returned actually intersect 'bbox', but only
        /// that their boundingbox intersects 'bbox'.</para>
        /// <para>This method is much faster than the QueryFeatures method, because intersection tests
        /// are performed on objects simplified by their boundingbox, and using the Spatial Index.</para>
        /// </remarks>
        /// <param name="bbox"></param>
        /// <returns></returns>
        public Collection<IGeometry> GetGeometriesInView(Envelope bbox)
        {
            //Use the spatial index to get a list of features whose boundingbox intersects bbox
            var objectlist = GetObjectIDsInView(bbox);
            if (objectlist.Count == 0) //no features found. Return an empty set
                return new Collection<IGeometry>();

            return GetGeometriesInViewWithoutFilter(objectlist);
        }


        private Collection<IGeometry> GetGeometriesInViewWithoutFilter(Collection<uint> oids)
        {
            var result = new Collection<IGeometry>();
            foreach (var oid in oids)
            {
                result.Add(GetGeometryByID(oid));
            }

            //CleanInternalCache(oids);
            return result;
        }

        public Collection<uint> GetObjectIDsInView(Envelope bbox)
        {
            if (!IsOpen)
                throw (new ApplicationException("An attempt was made to read from a closed datasource"));

            //TODO 使用自己的查询方法返回某个Envelop的对象的ID集合
            // return _tree.Search(bbox);
            return null;
        }

        /// <summary>
        /// Returns the geometry corresponding to the Object ID
        /// </summary>
        /// <remarks>FilterDelegate is no longer applied to this ge</remarks>
        /// <param name="oid">Object ID</param>
        /// <returns>The geometry at the Id</returns>
        public IGeometry GetGeometryByID(uint oid)
        {
            if (_useMemoryCache)
            {
                FeatureDataRow fdr;
                _cacheDataTable.TryGetValue(oid, out fdr);
                if (fdr == null)
                {
                    fdr = GetFeature(oid);
                }
                return fdr.Geometry;
            }
            return ReadGeometry(oid);
        }

        /// <summary>
        /// Gets or sets a value indicating that the provider should check if geometry belongs to a deleted record.
        /// </summary>
        /// <remarks>This really slows rendering performance down</remarks>
        public bool CheckIfRecordIsDeleted { get; set; }

        /// <summary>
        /// Returns the total number of features in the datasource (without any filter applied)
        /// </summary>
        /// <returns></returns>
        public int GetFeatureCount()
        {
            return _featureCount;
        }

        /// <summary>
        /// Gets a <see cref="FeatureDataRow"/> from the datasource at the specified index
        /// <para/>
        /// Please note well: It is not checked whether 
        /// <list type="Bullet">
        /// <item>the data record matches the <see cref="FilterProvider.FilterDelegate"/> assigned.</item>
        /// </list>
        /// </summary>
        /// <param name="rowId">The object identifier for the record</param>
        /// <returns>The feature data row</returns>
        public FeatureDataRow GetFeature(uint rowId)
        {
            return GetFeature(rowId, DbaseFile.NewTable);
        }

        /// <summary>
        /// Returns the extents of the datasource
        /// </summary>
        /// <returns></returns>
        public Envelope GetExtents()
        {
            return null;
        }

        /// <summary>
        /// Gets the connection ID of the datasource
        /// </summary>
        /// <remarks>
        /// The connection ID of a shapefile is its filename
        /// </remarks>
        public string ConnectionID
        {
            get { return _filename; }
        }

        /// <summary>
        /// Gets or sets the spatial reference ID (CRS)
        /// </summary>
        public virtual int SRID
        {
            get { return _srid; }
            set
            {
                _srid = value;
                lock (GspLock)
                {
                    //TODO 这里的工厂老是报错？？？ 解决方法是把原来的sharpmap项目中的app.config文件的东西复制到当前的项目中
                    IGeometryServices ntsFromGeoApi = new NtsGeometryServices();
                    Factory = GeometryServiceProvider.Instance.CreateGeometryFactory(_srid);
                }
            }
        }

        #endregion


        //
        private void ParseHeader()
        {
            _fsShapeFile = new FileStream(_filename, FileMode.Open, FileAccess.Read);
            _brShapeFile = new BinaryReader(_fsShapeFile, Encoding.Unicode);
            _brShapeFile.BaseStream.Seek(0, 0);
            //Check file header
            if (_brShapeFile.ReadInt32() != 170328064)
                //File Code is actually 9994, but in Little Endian Byte Order this is '170328064'
                throw (new ApplicationException("Invalid Shapefile (.shp)"));
            //Read filelength as big-endian. The length is based on 16bit words  因为是16位的，所以乘以2
            _brShapeFile.BaseStream.Seek(24, 0); //seek to File Length
            _fileSize = 2 * SwapByteOrder(_brShapeFile.ReadInt32());

            //这个Shapefile文件所记录的空间数据的几何类型
            _brShapeFile.BaseStream.Seek(32, 0); //seek to ShapeType
            _shapeType = (ShapeType)_brShapeFile.ReadInt32();

            //Read the spatial bounding box of the contents 是所有几何对象的空间范围
            _brShapeFile.BaseStream.Seek(36, 0); //seek to box 
            _envelope = new Envelope(new Coordinate(_brShapeFile.ReadDouble(), _brShapeFile.ReadDouble()),
                                     new Coordinate(_brShapeFile.ReadDouble(), _brShapeFile.ReadDouble()));

            // Work out the number of features, if we have an index file use that
            if (File.Exists(Path.ChangeExtension(_filename, ".shx")))
            {
                _fsShapeIndex = new FileStream(Path.ChangeExtension(_filename, ".shx"), FileMode.Open, FileAccess.Read);
                _brShapeIndex = new BinaryReader(_fsShapeIndex, Encoding.Unicode);

                _brShapeIndex.BaseStream.Seek(24, 0); //seek to File Length
                var indexFileSize = SwapByteOrder(_brShapeIndex.ReadInt32()); //Read filelength as big-endian. The length is based on 16bit words
                _featureCount = (2 * indexFileSize - 100) / 8; //Calculate FeatureCount. Each feature takes up 8 bytes. The header is 100 bytes
                _brShapeIndex.Close();
                _fsShapeIndex.Close();
            }
            else
            {
                // Move to the start of the data这里说明头文件占有100个字节
                _brShapeFile.BaseStream.Seek(100, 0); //Skip content length
                long offset = 100; // Start of the data records

                // Loop through the data to extablish the number of features contained within the data file
                while (offset < _fileSize)
                {
                    ++_featureCount;
                    //每个记录项目的前4个字节是记录号，从1开始 int类型
                    _brShapeFile.BaseStream.Seek(offset + 4, 0); //Skip content length 
                    //每个记录项目的第4到第8个字节表示当前的这条记录占用了多少字节 是int类型的
                    var dataLength = 2 * SwapByteOrder(_brShapeFile.ReadInt32());

                    // This is to cover the chance when the data is corupt
                    // as seen with the sample counties file, in this example the index file
                    // has been adjusted to cover the problem.
                    if ((offset + dataLength) > _fileSize)
                    {
                        --_featureCount;
                    }

                    //读取实体内容：
                    //读取记录号
                    _brShapeFile.BaseStream.Seek(offset, 0);
                    // TODO 这个地方没有乘以2 返回的结果是正确的，这个oid可以不用
                    int oid = SwapByteOrder(_brShapeFile.ReadInt32());
                    //TODO 这里拿到了对象的二进制数据 用二进制数据构建几何对象
                    _brShapeFile.BaseStream.Seek(offset + 8, 0);
                    Byte[] recordInfo = _brShapeFile.ReadBytes(dataLength);

                    offset += dataLength; // Add Record data length
                    offset += 8; //  Plus add the record header size
                }
            }
            _brShapeFile.Close();
            _fsShapeFile.Close();

        }

        /// <summary>
        /// Reads and parses the projection if a projection file exists
        /// </summary>
        private void ParseProjection()
        {
            string projfile = Path.GetDirectoryName(Filename) + "\\" + Path.GetFileNameWithoutExtension(Filename) + ".prj";
            if (File.Exists(projfile))
            {
                try
                {
                    string wkt = File.ReadAllText(projfile);
                    _coordinateSystem = (ICoordinateSystem)CoordinateSystemWktReader.Parse(wkt);
                    SRID = (int)_coordinateSystem.AuthorityCode;
                    _coordsysReadFromFile = true;
                    //GeometryServiceProvider 

                }
                catch (Exception ex)
                {
                    Trace.TraceWarning("Coordinate system file '" + projfile + "' found, but could not be parsed. WKT parser returned:" + ex.Message);
                   //TODO 这个地方有异常：
                    //Cannot use GeometryServiceProvider without an assigned IGeometryServices class
                    //throw;
                }
            }
            else
            {
                if (_coordinateSystem == null)
                    SRID = 0;
                else
                {
                    SRID = (int)_coordinateSystem.AuthorityCode;
                }
            }
        }

        //有shx文件就用它来填充_offsetOfRecord，否则，遍历shp文件填充
        private void PopulateIndexes()
        {
            if (_brShapeIndex != null)
            {
                _brShapeIndex.BaseStream.Seek(100, 0);  //skip the header
                for (int x = 0; x < _featureCount; ++x)
                {
                    _offsetOfRecord[x] = 2 * SwapByteOrder(_brShapeIndex.ReadInt32()); //Read shape data position // ibuffer);
                    _brShapeIndex.BaseStream.Seek(_brShapeIndex.BaseStream.Position + 4, 0); //Skip content length
                }
            }
            else
            {
                // we need to create an index from the shape file
                // Record the current position pointer for later
                var oldPosition = _brShapeFile.BaseStream.Position;
                // Move to the start of the data
                _brShapeFile.BaseStream.Seek(100, 0); //Skip content length
                long offset = 100; // Start of the data records

                for (int x = 0; x < _featureCount; ++x)
                {
                    _offsetOfRecord[x] = (int)offset;
                    _brShapeFile.BaseStream.Seek(offset + 4, 0); //Skip content length
                    int dataLength = 2 * SwapByteOrder(_brShapeFile.ReadInt32());
                    offset += dataLength; // Add Record data length
                    offset += 8; //  Plus add the record header size
                }
                // Return the position pointer
                _brShapeFile.BaseStream.Seek(oldPosition, 0);
            }
        }

        ///<summary>
        ///Swaps the byte order of an int32
        ///</summary>
        /// <param name="i">Integer to swap</param>
        /// <returns>Byte Order swapped int32</returns>
        private static int SwapByteOrder(int i)
        {
            var buffer = BitConverter.GetBytes(i);
            Array.Reverse(buffer, 0, buffer.Length);
            return BitConverter.ToInt32(buffer, 0);
        }


        protected IGeometryFactory Factory
        {
            get
            {
                if (_srid == -1)
                    SRID = 0;
                return _factory;
            }
            set
            {
                _factory = value;
                _srid = _factory.SRID;
            }
        }

        /// <summary>
        /// Reads and parses the geometry with ID 'oid' from the ShapeFile
        /// </summary>
        /// <param name="oid">Object ID</param>
        /// <returns>geometry</returns>
        private IGeometry ReadGeometry(uint oid)
        {
            // Do we want to receive geometries of deleted records as well?
            if (CheckIfRecordIsDeleted)
            {
                //Test if record is deleted
                lock (DbaseFile)
                {
                    if (DbaseFile.RecordDeleted(oid)) return null;
                }
            }

            lock (_brShapeFile)
            {
                _brShapeFile.BaseStream.Seek(_offsetOfRecord[oid] + 8, 0); //Skip record number and content length
                var type = (ShapeType)_brShapeFile.ReadInt32(); //Shape type
                if (type == ShapeType.Null)
                    return null;

                if (_shapeType == ShapeType.Point || _shapeType == ShapeType.PointM || _shapeType == ShapeType.PointZ)
                {
                    return Factory.CreatePoint(new Coordinate(_brShapeFile.ReadDouble(), _brShapeFile.ReadDouble()));
                }

                if (_shapeType == ShapeType.Multipoint || _shapeType == ShapeType.MultiPointM ||
                    _shapeType == ShapeType.MultiPointZ)
                {
                    _brShapeFile.BaseStream.Seek(32 + _brShapeFile.BaseStream.Position, 0); //skip min/max box
                    var nPoints = _brShapeFile.ReadInt32(); // get the number of points
                    if (nPoints == 0)
                        return null;
                    var feature = new Coordinate[nPoints];
                    for (var i = 0; i < nPoints; i++)
                        feature[i] = new Coordinate(_brShapeFile.ReadDouble(), _brShapeFile.ReadDouble());

                    return Factory.CreateMultiPoint(feature);
                }

                if (_shapeType == ShapeType.PolyLine || _shapeType == ShapeType.Polygon ||
                    _shapeType == ShapeType.PolyLineM || _shapeType == ShapeType.PolygonM ||
                    _shapeType == ShapeType.PolyLineZ || _shapeType == ShapeType.PolygonZ)
                {
                    _brShapeFile.BaseStream.Seek(32 + _brShapeFile.BaseStream.Position, 0); //skip min/max box

                    var nParts = _brShapeFile.ReadInt32(); // get number of parts (segments)
                    if (nParts == 0 || nParts < 0)
                        return null;
                    var nPoints = _brShapeFile.ReadInt32(); // get number of points
                    var segments = new int[nParts + 1];
                    //Read in the segment indexes
                    for (var b = 0; b < nParts; b++)
                        segments[b] = _brShapeFile.ReadInt32();
                    //add end point
                    segments[nParts] = nPoints;

                    if ((int)_shapeType % 10 == 3)
                    {
                        var lineStrings = new ILineString[nParts];
                        for (var lineID = 0; lineID < nParts; lineID++)
                        {
                            var line = new Coordinate[segments[lineID + 1] - segments[lineID]];
                            var offset = segments[lineID];
                            for (var i = segments[lineID]; i < segments[lineID + 1]; i++)
                                line[i - offset] = new Coordinate(_brShapeFile.ReadDouble(), _brShapeFile.ReadDouble());
                            lineStrings[lineID] = Factory.CreateLineString(line);
                        }

                        if (lineStrings.Length == 1)
                            return lineStrings[0];

                        return Factory.CreateMultiLineString(lineStrings);
                    }
                    //First read all the rings
                    var rings = new ILinearRing[nParts];
                    for (var ringID = 0; ringID < nParts; ringID++)
                    {
                        var ring = new Coordinate[segments[ringID + 1] - segments[ringID]];
                        var offset = segments[ringID];
                        for (var i = segments[ringID]; i < segments[ringID + 1]; i++)
                            ring[i - offset] = new Coordinate(_brShapeFile.ReadDouble(), _brShapeFile.ReadDouble());
                        rings[ringID] = Factory.CreateLinearRing(ring);
                    }
                    ILinearRing exteriorRing;
                    var isCounterClockWise = new bool[rings.Length];
                    var polygonCount = 0;
                    for (var i = 0; i < rings.Length; i++)
                    {
                        //TODO 把IsCCW改为IsCCW();
                        isCounterClockWise[i] = rings[i].IsCCW();
                        if (!isCounterClockWise[i])
                            polygonCount++;
                    }
                    if (polygonCount == 1) //We only have one polygon
                    {
                        exteriorRing = rings[0];
                        ILinearRing[] interiorRings = null;
                        if (rings.Length > 1)
                        {
                            interiorRings = new ILinearRing[rings.Length - 1];
                            Array.Copy(rings, 1, interiorRings, 0, interiorRings.Length);
                        }
                        return Factory.CreatePolygon(exteriorRing, interiorRings);
                    }
                    var polygons = new List<IPolygon>();
                    exteriorRing = rings[0];
                    var holes = new List<ILinearRing>();

                    for (var i = 1; i < rings.Length; i++)
                    {
                        if (!isCounterClockWise[i])
                        {
                            polygons.Add(Factory.CreatePolygon(exteriorRing, holes.ToArray()));
                            holes.Clear();
                            exteriorRing = rings[i];
                        }
                        else
                            holes.Add(rings[i]);
                    }
                    polygons.Add(Factory.CreatePolygon(exteriorRing, holes.ToArray()));

                    return Factory.CreateMultiPolygon(polygons.ToArray());
                }
                else
                    throw (new ApplicationException("Shapefile type " + _shapeType.ToString() + " not supported"));
            }
        }

        /// <summary>
        /// Gets a datarow from the datasource at the specified index belonging to the specified datatable
        /// <para/>
        /// Please note well: It is not checked whether 
        /// <list type="Bullet">
        /// <item>the data record matches the <see cref="FilterProvider.FilterDelegate"/> assigned.</item>
        /// </list>
        /// </summary>
        /// <param name="rowId">The object identifier for the record</param>
        /// <param name="dt">The datatable the feature should belong to.</param>
        /// <returns>The feature data row</returns>
        public FeatureDataRow GetFeature(uint rowId, FeatureDataTable dt)
        {
            Debug.Assert(dt != null);
            if (DbaseFile != null)
            {
                FeatureDataRow fdr = null;
                //MemoryCache
                if (_useMemoryCache)
                {
                    _cacheDataTable.TryGetValue(rowId, out fdr);
                    if (fdr == null)
                    {
                        lock (DbaseFile)
                        {
                            fdr = DbaseFile.GetFeature(rowId, dt);
                        }
                        fdr.Geometry = ReadGeometry(rowId);
                        _cacheDataTable.Add(rowId, fdr);
                    }
                    //Make a copy to return
                    var fdrNew = dt.NewRow();
                    Array.Copy(fdr.ItemArray, 0, fdrNew.ItemArray, 0, fdr.ItemArray.Length);

                    fdrNew.Geometry = fdr.Geometry;
                    return fdr;
                }
                lock (DbaseFile)
                {
                    fdr = DbaseFile.GetFeature(rowId, dt);
                }
                // GetFeature returns null if the record has deleted flag
                if (fdr == null)
                    return null;
                // Read the geometry
                fdr.Geometry = ReadGeometry(rowId);
                return fdr;
            }

            throw (new ApplicationException(
                "An attempt was made to read DBase data from a shapefile without a valid .DBF file"));
        }

        //构建创建表的DDL语句  有些地理信息比较庞大，将shapeInfo的数据类型改成了longblob
        public String GetCreateTableDDL()
        {
            this.Open();
            DataTable schemaTable = this.DbaseFile.GetSchemaTable();
            String createTable = @"drop table if exists  " + TableName +" ;\r\n"+
                                 @"create table IF NOT EXISTS " + TableName +
                                 @"(
                                    oid int unsigned unique,                   
                                    shapeType tinyint unsigned,
                                    shapeInfo MediumBlob,
                                    minX double,
                                    maxX double,
                                    minY double,
                                    maxY double,";
            StringBuilder buffer = new StringBuilder();
            for (int i = 1; i < schemaTable.Rows.Count; i++)    //从1开始，跳过oid那一列
            {   //这个地方还需要判断一下精度，唯一性等约束
                String columnName = (String)schemaTable.Rows[i]["ColumnName"];
                Type type = (Type)schemaTable.Rows[i]["DataType"];
                String typeName = GetMySQLDataTypeFromCSharp(type);
                buffer.Append("`").Append(columnName).Append("` ").Append(typeName).Append(",");
            }
            createTable += buffer.Replace(',', ')', buffer.Length - 1, 1).ToString();
            createTable += "ENGINE = MyISAM";
            return createTable;
        }

        private String _tableName;

        public String TableName
        {
            get { return _tableName; }
            set
            {
                if ("" == value || value == null)
                {
                    _tableName = Path.GetFileNameWithoutExtension(Filename);
                }
                else
                {
                    _tableName = value;
                }
            }
        }

        public void createTable()
        {
            string connStr = String.Format("server={0};uid={1};pwd={2};database={3}",
               "localhost", "root", "xrt512", "shapefiles");
            MySqlConnection conn = new MySqlConnection(connStr);
            conn.Open();

            String sql = GetCreateTableDDL();
            MySqlCommand cmd = new MySqlCommand(sql, conn);
            cmd.ExecuteNonQuery();
            conn.Close();
        }

        public String getInsertDML()
        {
            if (!this.IsOpen) this.Open();
            DataTable schemaTable = this.DbaseFile.GetSchemaTable();
            String insert = @"insert into " + TableName +
                              @"(oid,shapeType,shapeInfo,minX,maxX,minY,maxY,";
            StringBuilder buffer = new StringBuilder();
            for (int i = 1; i < schemaTable.Rows.Count; i++)//从1开始，跳过oid那一列
            {   //TODO 这个地方还需要判断一下精度，唯一性等约束
                String columnName = (String)schemaTable.Rows[i]["ColumnName"];
                buffer.Append(columnName).Append(",");
            }
            insert += buffer.Replace(",", ") values (@oid, @shapeType, @shapeInfo, @minX, @maxX, @minY, @maxY, ", buffer.Length - 1, 1).ToString();
            buffer.Clear();
            for (int i = 1; i < schemaTable.Rows.Count; i++)//从1开始，跳过oid那一列
            {   //TODO 这个地方还需要判断一下精度，唯一性等约束
                String columnName = (String)schemaTable.Rows[i]["ColumnName"];
                buffer.Append(" @").Append(columnName).Append(",");
            }
            insert += buffer.Replace(',', ')', buffer.Length - 1, 1).ToString();
            return insert;
        }

        public void InsertWithMySqlBulkCopy(DataTable d)
        {
            XZBlukCopy c = new XZBlukCopy();
            string connStr = String.Format("server={0};uid={1};pwd={2};database={3}",
               "localhost", "root", "xrt512", "shapefiles");
            MySqlConnection conn = new MySqlConnection(connStr);
            conn.Open();
            c.DestinationDbConnection = conn;
            ColumnMapItemColl cc = new ColumnMapItemColl();
            for (int i = 0; i < d.Columns.Count; i++)
            {
                XZColumnMapItem item = new XZColumnMapItem();
                item.DataType = d.Columns[i].DataType.ToString();
                item.DestinationColumn = d.Columns[i].ColumnName;
                item.SourceColumn = d.Columns[i].ColumnName;
                cc.Add(item);
            }
            c.ColumnMapItems = cc;
            c.DestinationTableName = _tableName;
            c.BatchSize = 50;
            c.Upload(d);
        }

        /// <summary>
        /// 另一种方式的bulkCopy     992行数据33秒左右 使用MyISAM之后 效率提升很大
        /// </summary>
        /// <param name="d"></param>
        public void InsertWithMySqlBulkCopy2(DataTable d)
        {
            string connStr = String.Format("server={0};uid={1};pwd={2};database={3}",
               "localhost", "root", "xrt512", "shapefiles");
            MySqlConnection conn = new MySqlConnection(connStr);
            conn.Open();

            MySqlDataAdapter adp = new MySqlDataAdapter("select * from " + _tableName /* +" where 1=0" */, conn);
            MySqlCommandBuilder cmdb = new MySqlCommandBuilder(adp);//添加该语句即可
            adp.Fill(d);
            DataRow[] rs = new DataRow[d.Rows.Count];
            d.Rows.CopyTo(rs,0);
            adp.Update(rs);
            d.AcceptChanges();
        }


        private DataTable _shapefileAndDBaseTable;

        public DataTable ShapefileAndDBaseTable
        {
            get { return _shapefileAndDBaseTable; }
            set { _shapefileAndDBaseTable = value; }
        }

        private void CreateShapefileAndDBaseTable()
        {
            _shapefileAndDBaseTable = this.DbaseFile.NewTable;//得到的是具有原来表结构的 空表
            _shapefileAndDBaseTable.Columns.Add(new DataColumn("shapeType", typeof(uint)));
            _shapefileAndDBaseTable.Columns.Add(new DataColumn("shapeInfo", typeof(byte[])));
            _shapefileAndDBaseTable.Columns.Add(new DataColumn("minX", typeof(double)));
            _shapefileAndDBaseTable.Columns.Add(new DataColumn("maxX", typeof(double)));
            _shapefileAndDBaseTable.Columns.Add(new DataColumn("minY", typeof(double)));
            _shapefileAndDBaseTable.Columns.Add(new DataColumn("maxY", typeof(double)));
        }

        public DataTable InsertIntoBufferTable(uint startIndex,int endIndex)
        {
            for (uint oid = startIndex; oid <= endIndex; oid++)
            {
                DataRow dr = _shapefileAndDBaseTable.NewRow();//创建一个具有相同架构的row

                FeatureDataRow fdr = DbaseFile.GetFeature(oid, DbaseFile.NewTable);
                fdr.Geometry = ReadGeometry(oid);
                AssignSameFeild(dr, fdr);
                IGeometry g = fdr.Geometry;
                Envelope e = g.EnvelopeInternal;
                dr["shapeType"] = _shapeType;
                dr["shapeInfo"] = GeometryToWKB.Write(g);
                dr["minX"] = e.MinX;
                dr["maxX"] = e.MaxX;
                dr["minY"] = e.MinY;
                dr["maxY"] = e.MaxY;
                _shapefileAndDBaseTable.Rows.Add(dr);
            }
            return _shapefileAndDBaseTable;
        }



        /// <summary>
        /// 使用源数据构建一个DataTable对象，用于BulkCopy，但是目发现google code上的MySqlBulkCopy项目是个失败的项目，没有性能的提升，遂自写XZBulkCopy
        /// </summary>
        /// <param name="d">字典格式的源数据</param>
        /// <returns>构造好的DataTable对象</returns>
        public DataTable InsertIntoShapefileAndDBaseTable(Dictionary<uint, FeatureDataRow> d)
        {
            for (uint oid = 0; oid < _featureCount; oid++)
            {
                DataRow dr = _shapefileAndDBaseTable.NewRow();//创建一个具有相同架构的row
                //这个地方有必要写一个方法，就是如果datarow1的字段名字与datarow2的字段名字一致，则复制前者的值到后者
                FeatureDataRow fdr = d[oid];
                AssignSameFeild(dr, fdr);
                IGeometry g = fdr.Geometry;
                Envelope e = g.EnvelopeInternal;
                dr["shapeType"] = _shapeType;
                dr["shapeInfo"] = GeometryToWKB.Write(g);
                dr["minX"] = e.MinX;
                dr["maxX"] = e.MaxX;
                dr["minY"] = e.MinY;
                dr["maxY"] = e.MaxY;
                _shapefileAndDBaseTable.Rows.Add(dr);
            }
            return _shapefileAndDBaseTable;
        }

        private void AssignSameFeild(DataRow to,FeatureDataRow from) 
        {
            DataTable schemaTable = this.DbaseFile.GetSchemaTable();
            for (int i = 0; i < schemaTable.Rows.Count; i++)
            {
                String cn = (String)schemaTable.Rows[i]["ColumnName"];
                to[cn] = from[cn];
            }
        }

        public int InsertIntoMySQL(Dictionary<uint, FeatureDataRow> d)
        {
            int count = 0;
            string connStr = String.Format("server={0};uid={1};pwd={2};database={3}",
               "localhost", "root", "xrt512", "shapefiles");
            MySqlConnection conn = new MySqlConnection(connStr);
            conn.Open();
            String sql = getInsertDML();
            MySqlCommand cmd = new MySqlCommand(sql, conn);
            for (uint oid = 0; oid < _featureCount; oid++)
            {
                FeatureDataRow fdr = d[oid];
                IGeometry g = fdr.Geometry;
                Envelope e = g.EnvelopeInternal;
                MySqlParameter[] parames = new MySqlParameter[this.DbaseFile.GetSchemaTable().Rows.Count + 6];
                parames[0] = new MySqlParameter("@oid", oid);
                parames[1] = new MySqlParameter("@shapeType", _shapeType);
                parames[2] = new MySqlParameter("@shapeInfo", GeometryToWKB.Write(g));
                parames[3] = new MySqlParameter("@minX", e.MinX);
                parames[4] = new MySqlParameter("@maxX", e.MaxX);
                parames[5] = new MySqlParameter("@minY", e.MinY);
                parames[6] = new MySqlParameter("@maxY", e.MaxY);

                DataTable schemaTable = this.DbaseFile.GetSchemaTable();
                for (int i = 1; i < schemaTable.Rows.Count; i++)//从1开始，跳过oid那一列
                {   //这个地方还需要判断一下精度，唯一性等约束
                    this.DbaseFile.CurrentRecordOid = oid;
                    String columnName = (String)schemaTable.Rows[i]["ColumnName"];
                    parames[6 + i] = new MySqlParameter("@" + columnName, this.DbaseFile.GetValueByColumnName(columnName));
                    //Console.WriteLine(this.DbaseFile.GetValueByColumnName(columnName));
                }
                cmd.Parameters.Clear(); //清除上一次传入的参数！
                cmd.Parameters.AddRange(parames);
                count += cmd.ExecuteNonQuery();
            }
            conn.Close();
            return count;
        }

        //获取所有的集合元素的信息
        public Dictionary<uint, FeatureDataRow> GetFeatures()
        {
            Dictionary<uint, FeatureDataRow> featureDictionary = new Dictionary<uint, FeatureDataRow>();
            for (uint oid = 0; oid < _featureCount; oid++)
            {
                FeatureDataRow fdr = DbaseFile.GetFeature(oid, DbaseFile.NewTable);
                fdr.Geometry = ReadGeometry(oid);
                featureDictionary.Add(oid,fdr);
            }
            return featureDictionary;
        }

        public String GetMySQLDataTypeFromCSharp(Type t)
        {
            switch (t.Name.ToLower())
            {
                case "byte":
                    return "tinyint";
                case "bool":
                    return "BOOL";
                case "int16":
                    return "smallint";
                case "int32":
                    return "int";
                case "int64":
                    return "bigint";
                case "single":
                    return "float";
                case "double":
                    return "double";
                case "string":
                    return "VARCHAR(255)";
                case "datetime":
                    return "datetime";
                case "object":
                    return "blob";
                default:
                    throw new InvalidOperationException("CSharp data type '" + t + "' has no matched by MySQL.");
            }
        }

    }
}