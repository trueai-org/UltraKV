using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Text;

namespace UltraKV;

public unsafe class UltraKVEngine : IDisposable
{
    private readonly FileStream _file;
    private readonly FreeSpaceManager _freeSpaceManager;
    private readonly DataProcessor _dataProcessor;
    private readonly ConcurrentDictionary<string, long> _keyIndex;
    private readonly DatabaseHeader _databaseHeader;
    private readonly UltraKVConfig _config;
    private readonly object _writeLock = new object();
    private readonly Timer? _flushTimer;
    private bool _disposed;
    private DateTime _lastFlushTime = DateTime.UtcNow;
    private readonly bool _enableUpdateValidation; // 新增：是否启用更新验证

    /// <summary>
    /// 加密数据头部结构
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    private struct EncryptedDataHeader
    {
        public uint OriginalSize;    // 4 bytes - 原始数据大小（压缩/加密前）
        public uint EncryptedSize;   // 4 bytes - 加密数据大小
        public byte IsDeleted;       // 1 byte - 删除标记
        public byte Reserved1;       // 1 byte - 保留
        public ushort Reserved2;     // 2 bytes - 保留
                                     // Total: 12 bytes

        public const int SIZE = 12;

        public EncryptedDataHeader(uint originalSize, uint encryptedSize, bool isDeleted = false)
        {
            OriginalSize = originalSize;
            EncryptedSize = encryptedSize;
            IsDeleted = isDeleted ? (byte)1 : (byte)0;
            Reserved1 = 0;
            Reserved2 = 0;
        }
    }

    public UltraKVEngine(string filePath, UltraKVConfig? config = null)
    {
        _config = config ?? UltraKVConfig.Default;
        _config.Validate();

        _enableUpdateValidation = _config.EnableUpdateValidation; // 保存配置

        var isNewFile = !File.Exists(filePath);
        _file = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);

        if (isNewFile)
        {
            // 新建数据库
            _databaseHeader = DatabaseHeader.Create(_config);
            WriteDatabaseHeader();

            // 传递 EnableFreeSpaceReuse 配置到 FreeSpaceManager
            var freeSpaceManager = new FreeSpaceManager(_file, _databaseHeader.FreeSpaceRegionSizeKB * 1024, _config.EnableFreeSpaceReuse);
            _freeSpaceManager = freeSpaceManager;

            // 保存空闲空间头部信息
            _freeSpaceManager.SaveFreeSpaceHeader();

            // 如果启动空闲空间重用
            if (_config.EnableFreeSpaceReuse)
            {
                // 加载或创建空闲空间块信息
                // TODO
            }
        }
        else
        {
            // 打开现有数据库
            _databaseHeader = ReadDatabaseHeader();
            if (!_databaseHeader.IsValid())
            {
                throw new InvalidDataException("Invalid database file format");
            }

            // 验证配置兼容性
            ValidateConfigCompatibility(_config);

            // 传递 EnableFreeSpaceReuse 配置到 FreeSpaceManager
            var freeSpaceManager = new FreeSpaceManager(_file, _databaseHeader.FreeSpaceRegionSizeKB * 1024, _config.EnableFreeSpaceReuse);
            _freeSpaceManager = freeSpaceManager;

            // 如果需要重建
            if (_freeSpaceManager.NeedsRebuild(_databaseHeader.FreeSpaceRegionSizeKB * 1024, _config.EnableFreeSpaceReuse))
            {
                // 则调用 PerformShrink
                var shrinkResult = PerformShrink();
                Console.WriteLine($"Rebuilt free space manager: {shrinkResult}");
            }

            // 保存空闲空间头部信息
            _freeSpaceManager.SaveFreeSpaceHeader();

            // 如果启动空闲空间重用
            if (_config.EnableFreeSpaceReuse)
            {
                // 加载或创建空闲空间块信息
                // TODO
            }
        }


        _dataProcessor = new DataProcessor(_databaseHeader, _config.EncryptionKey);
        _keyIndex = new ConcurrentDictionary<string, long>();

        // 保存空闲空间头部信息
        _freeSpaceManager.SaveFreeSpaceHeader();

        if (!isNewFile)
        {
            LoadIndex();
        }

        // 启动定时刷盘器
        if (_config.GcFlushInterval > 0)
        {
            _flushTimer = new Timer(OnFlushTimer, null,
                TimeSpan.FromSeconds(_config.GcFlushInterval),
                TimeSpan.FromSeconds(_config.GcFlushInterval));
        }
    }

    /// <summary>
    /// 验证键的长度
    /// </summary>
    private void ValidateKey(string key)
    {
        if (string.IsNullOrEmpty(key))
            throw new ArgumentException("Key cannot be null or empty");

        var keyBytes = Encoding.UTF8.GetBytes(key);
        if (keyBytes.Length > _config.MaxKeyLength)
            throw new ArgumentException($"Key length ({keyBytes.Length} bytes) exceeds maximum allowed length ({_config.MaxKeyLength} bytes)");
    }

    /// <summary>
    /// 验证写入后的数据
    /// </summary>
    private void ValidateWrittenData(string key, string expectedValue, long position)
    {
        if (!_enableUpdateValidation)
            return;

        try
        {
            // 临时从文件中读取刚写入的数据进行验证
            var actualValue = ReadValueFromPosition(key, position);
            if (actualValue != expectedValue)
            {
                throw new InvalidDataException($"Update validation failed for key '{key}': expected '{expectedValue}', but read '{actualValue}'");
            }
        }
        catch (Exception ex)
        {
            throw new InvalidDataException($"Update validation failed for key '{key}': {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 从指定位置读取数据值（用于验证）
    /// </summary>
    private string? ReadValueFromPosition(string key, long position)
    {
        var originalPosition = _file.Position; // 保存当前位置
        try
        {
            _file.Seek(position, SeekOrigin.Begin);

            if (_databaseHeader.EncryptionType != EncryptionType.None || _databaseHeader.CompressionType != CompressionType.None)
            {
                // 加密/压缩模式
                var encryptedHeaderBuffer = new byte[EncryptedDataHeader.SIZE];
                if (_file.Read(encryptedHeaderBuffer, 0, EncryptedDataHeader.SIZE) != EncryptedDataHeader.SIZE)
                    return null;

                fixed (byte* headerPtr = encryptedHeaderBuffer)
                {
                    var encryptedHeader = *(EncryptedDataHeader*)headerPtr;
                    if (encryptedHeader.IsDeleted != 0)
                        return null;

                    var encryptedData = new byte[encryptedHeader.EncryptedSize];
                    if (_file.Read(encryptedData, 0, (int)encryptedHeader.EncryptedSize) != encryptedHeader.EncryptedSize)
                        return null;

                    var processedData = _dataProcessor.UnprocessData(encryptedData);
                    fixed (byte* dataPtr = processedData)
                    {
                        var record = new Record(dataPtr, processedData.Length);
                        return record.GetValue();
                    }
                }
            }
            else
            {
                // 非加密模式
                var headerBuffer = new byte[RecordHeader.SIZE];
                if (_file.Read(headerBuffer, 0, RecordHeader.SIZE) != RecordHeader.SIZE)
                    return null;

                fixed (byte* headerPtr = headerBuffer)
                {
                    var header = *(RecordHeader*)headerPtr;
                    if (header.IsDeleted != 0)
                        return null;

                    var totalDataSize = RecordHeader.SIZE + (int)header.KeyLen + (int)header.ValueLen;
                    var allData = new byte[totalDataSize];

                    Array.Copy(headerBuffer, allData, RecordHeader.SIZE);
                    var remainingSize = totalDataSize - RecordHeader.SIZE;
                    if (remainingSize > 0)
                    {
                        _file.Read(allData, RecordHeader.SIZE, remainingSize);
                    }

                    fixed (byte* dataPtr = allData)
                    {
                        var record = new Record(dataPtr, allData.Length);
                        return record.GetValue();
                    }
                }
            }
        }
        finally
        {
            _file.Seek(originalPosition, SeekOrigin.Begin); // 恢复原始位置
        }
    }

    /// <summary>
    /// 获取验证统计信息
    /// </summary>
    public ValidationStats GetValidationStats()
    {
        return new ValidationStats
        {
            IsUpdateValidationEnabled = _enableUpdateValidation,
            MaxKeyLength = _config.MaxKeyLength,
            IsFreeSpaceReuseEnabled = _config.EnableFreeSpaceReuse,
            FreeSpaceStats = _freeSpaceManager.GetStats()
        };
    }

    /// <summary>
    /// 定时刷盘回调
    /// </summary>
    private void OnFlushTimer(object? state)
    {
        try
        {
            if (_disposed) return;

            var now = DateTime.UtcNow;
            if ((now - _lastFlushTime).TotalSeconds >= _config.GcFlushInterval)
            {
                Flush();
                _lastFlushTime = now;

                // 如果启用了自动回收，检查是否需要GC
                if (_databaseHeader.IsGcAutoRecycleEnabled)
                {
                    var stats = GetStats();
                    if (_databaseHeader.ShouldGC(stats.TotalFileSize, stats.FreeSpaceStats.TotalFreeSpace, stats.ValidRecordCount))
                    {
                        // 在后台线程执行GC，避免阻塞
                        Task.Run(() =>
                        {
                            try
                            {
                                var result = Shrink();
                                Console.WriteLine($"Auto GC completed: {result}");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Auto GC failed: {ex.Message}");
                            }
                        });
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Flush timer error: {ex.Message}");
        }
    }

    /// <summary>
    /// 写入头部信息
    /// </summary>
    private void WriteDatabaseHeader()
    {
        lock (_writeLock)
        {
            _file.Seek(0, SeekOrigin.Begin);
            var headerBytes = new byte[DatabaseHeader.SIZE];
            fixed (byte* headerPtr = headerBytes)
            {
                *(DatabaseHeader*)headerPtr = _databaseHeader;
            }
            _file.Write(headerBytes, 0, DatabaseHeader.SIZE);
            _file.Flush();
        }
    }

    private DatabaseHeader ReadDatabaseHeader()
    {
        if (_file.Length < DatabaseHeader.SIZE)
        {
            throw new InvalidDataException("File is too small to contain a valid database header");
        }

        _file.Seek(0, SeekOrigin.Begin);
        var buffer = new byte[DatabaseHeader.SIZE];
        if (_file.Read(buffer, 0, DatabaseHeader.SIZE) != DatabaseHeader.SIZE)
        {
            throw new InvalidDataException("Failed to read database header");
        }

        fixed (byte* bufferPtr = buffer)
        {
            return *(DatabaseHeader*)bufferPtr;
        }
    }

    private void ValidateConfigCompatibility(UltraKVConfig config)
    {
        if (config.CompressionType != _databaseHeader.CompressionType)
        {
            throw new InvalidOperationException(
                $"Compression type mismatch. Database: {_databaseHeader.CompressionType}, Config: {config.CompressionType}");
        }

        if (config.EncryptionType != _databaseHeader.EncryptionType)
        {
            throw new InvalidOperationException(
                $"Encryption type mismatch. Database: {_databaseHeader.EncryptionType}, Config: {config.EncryptionType}");
        }
    }

    public void Put(string key, string value)
    {
        if (string.IsNullOrEmpty(key))
            throw new ArgumentException("Key cannot be null or empty");

        // 验证 key 长度
        ValidateKey(key);

        using var record = new Record(key, value);
        var rawData = record.GetRawData();

        lock (_writeLock)
        {
            // 如果键已存在，标记旧记录为删除
            if (_keyIndex.TryGetValue(key, out var oldPosition))
            {
                MarkAsDeleted(oldPosition);
            }

            long position;

            if (_databaseHeader.EncryptionType != EncryptionType.None || _databaseHeader.CompressionType != CompressionType.None)
            {
                // 加密/压缩模式：使用新的存储格式
                var processedData = _dataProcessor.ProcessData(rawData);

                var encryptedHeader = new EncryptedDataHeader
                {
                    OriginalSize = (uint)rawData.Length,
                    EncryptedSize = (uint)processedData.Length,
                    IsDeleted = 0,
                    Reserved1 = 0,
                    Reserved2 = 0
                };

                var totalSize = EncryptedDataHeader.SIZE + processedData.Length;
                var multipliedSize = (int)(totalSize * (1 + _databaseHeader.AllocationMultiplier / 100.0));

                // 寻找合适的空闲空间（如果启用了空闲空间重用）
                if (_freeSpaceManager.TryGetFreeSpace(multipliedSize, out var freeBlock))
                {
                    position = freeBlock.Position;
                    var remainingSize = freeBlock.Size - totalSize;
                    if (remainingSize > 64)
                    {
                        _freeSpaceManager.AddFreeSpace(position + totalSize, remainingSize);
                    }
                }
                else
                {
                    position = _file.Length;
                    _file.SetLength(position + multipliedSize);
                }

                // 写入加密数据头部 + 加密数据
                _file.Seek(position, SeekOrigin.Begin);
                var headerBytes = new byte[EncryptedDataHeader.SIZE];
                fixed (byte* headerPtr = headerBytes)
                {
                    *(EncryptedDataHeader*)headerPtr = encryptedHeader;
                }
                _file.Write(headerBytes, 0, EncryptedDataHeader.SIZE);
                _file.Write(processedData);
            }
            else
            {
                // 非加密模式：使用原有格式
                var requiredSize = rawData.Length;
                var multipliedSize = (int)(requiredSize * (1 + _databaseHeader.AllocationMultiplier / 100.0));

                if (_freeSpaceManager.TryGetFreeSpace(multipliedSize, out var freeBlock))
                {
                    position = freeBlock.Position;
                    var remainingSize = freeBlock.Size - requiredSize;
                    if (remainingSize > 64)
                    {
                        _freeSpaceManager.AddFreeSpace(position + requiredSize, remainingSize);
                    }
                }
                else
                {
                    position = _file.Length;
                    _file.SetLength(position + multipliedSize);
                }

                // 直接写入原始数据
                _file.Seek(position, SeekOrigin.Begin);
                _file.Write(rawData);
            }

            _file.Flush();
            _keyIndex[key] = position;

            // 如果启用了更新验证，验证刚写入的数据
            if (_enableUpdateValidation)
            {
                ValidateWrittenData(key, value, position);
            }
        }
    }

    public string? Get(string key)
    {
        if (string.IsNullOrEmpty(key) || !_keyIndex.TryGetValue(key, out var position))
            return null;

        try
        {
            _file.Seek(position, SeekOrigin.Begin);

            if (_databaseHeader.EncryptionType != EncryptionType.None || _databaseHeader.CompressionType != CompressionType.None)
            {
                // 加密/压缩模式：读取加密数据头部
                var encryptedHeaderBuffer = new byte[EncryptedDataHeader.SIZE];
                if (_file.Read(encryptedHeaderBuffer, 0, EncryptedDataHeader.SIZE) != EncryptedDataHeader.SIZE)
                    return null;

                fixed (byte* headerPtr = encryptedHeaderBuffer)
                {
                    var encryptedHeader = *(EncryptedDataHeader*)headerPtr;
                    if (encryptedHeader.IsDeleted != 0)
                    {
                        _keyIndex.TryRemove(key, out _);
                        return null;
                    }

                    // 读取加密数据
                    var encryptedData = new byte[encryptedHeader.EncryptedSize];
                    if (_file.Read(encryptedData, 0, (int)encryptedHeader.EncryptedSize) != encryptedHeader.EncryptedSize)
                        return null;

                    // 解密+解压缩
                    var processedData = _dataProcessor.UnprocessData(encryptedData);

                    // 解析记录
                    fixed (byte* dataPtr = processedData)
                    {
                        var record = new Record(dataPtr, processedData.Length);
                        return record.GetValue();
                    }
                }
            }
            else
            {
                // 非加密模式：使用原有逻辑
                var headerBuffer = new byte[RecordHeader.SIZE];
                if (_file.Read(headerBuffer, 0, RecordHeader.SIZE) != RecordHeader.SIZE)
                    return null;

                fixed (byte* headerPtr = headerBuffer)
                {
                    var header = *(RecordHeader*)headerPtr;
                    if (header.IsDeleted != 0)
                    {
                        _keyIndex.TryRemove(key, out _);
                        return null;
                    }

                    var totalDataSize = RecordHeader.SIZE + (int)header.KeyLen + (int)header.ValueLen;
                    var allData = new byte[totalDataSize];

                    Array.Copy(headerBuffer, allData, RecordHeader.SIZE);
                    var remainingSize = totalDataSize - RecordHeader.SIZE;
                    if (remainingSize > 0)
                    {
                        _file.Read(allData, RecordHeader.SIZE, remainingSize);
                    }

                    fixed (byte* dataPtr = allData)
                    {
                        var record = new Record(dataPtr, allData.Length);
                        return record.GetValue();
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading key '{key}': {ex.Message}");
            return null;
        }
    }

    public bool Delete(string key)
    {
        if (string.IsNullOrEmpty(key) || !_keyIndex.TryGetValue(key, out var position))
            return false;

        lock (_writeLock)
        {
            MarkAsDeleted(position);
            _keyIndex.TryRemove(key, out _);
            return true;
        }
    }

    private void MarkAsDeleted(long position)
    {
        try
        {
            if (_databaseHeader.EncryptionType != EncryptionType.None || _databaseHeader.CompressionType != CompressionType.None)
            {
                // 加密模式：标记EncryptedDataHeader的IsDeleted字段
                _file.Seek(position + 8, SeekOrigin.Begin); // 跳到IsDeleted字段 (OriginalSize(4) + EncryptedSize(4))
                _file.WriteByte(1);
            }
            else
            {
                // 非加密模式：标记RecordHeader的IsDeleted字段
                _file.Seek(position + 16, SeekOrigin.Begin); // 跳到IsDeleted字段
                _file.WriteByte(1);
            }
        }
        catch
        {
            // 忽略错误
        }
    }

    private void LoadIndex()
    {
        var dataStartPosition = _freeSpaceManager.GetDataStartPosition();
        var position = dataStartPosition;

        while (position < _file.Length)
        {
            try
            {
                _file.Seek(position, SeekOrigin.Begin);

                if (_databaseHeader.EncryptionType != EncryptionType.None || _databaseHeader.CompressionType != CompressionType.None)
                {
                    // 加密模式：读取EncryptedDataHeader
                    var encryptedHeaderBuffer = new byte[EncryptedDataHeader.SIZE];
                    if (_file.Read(encryptedHeaderBuffer, 0, EncryptedDataHeader.SIZE) != EncryptedDataHeader.SIZE)
                        break;

                    fixed (byte* headerPtr = encryptedHeaderBuffer)
                    {
                        var encryptedHeader = *(EncryptedDataHeader*)headerPtr;
                        var totalSize = EncryptedDataHeader.SIZE + (int)encryptedHeader.EncryptedSize;

                        if (encryptedHeader.IsDeleted == 0)
                        {
                            // 读取加密数据
                            var encryptedData = new byte[encryptedHeader.EncryptedSize];
                            if (_file.Read(encryptedData, 0, (int)encryptedHeader.EncryptedSize) == encryptedHeader.EncryptedSize)
                            {
                                try
                                {
                                    // 解密+解压缩
                                    var processedData = _dataProcessor.UnprocessData(encryptedData);

                                    fixed (byte* dataPtr = processedData)
                                    {
                                        var record = new Record(dataPtr, processedData.Length);
                                        var key = record.GetKey();
                                        _keyIndex[key] = position;
                                    }
                                }
                                catch
                                {
                                    // 解密失败，跳过
                                }
                            }
                        }
                        else
                        {
                            // 已删除的记录，加入空闲空间
                            _freeSpaceManager.AddFreeSpace(position, totalSize);
                        }

                        position += totalSize;
                    }
                }
                else
                {
                    // 非加密模式：使用原有逻辑
                    var headerBuffer = new byte[RecordHeader.SIZE];
                    if (_file.Read(headerBuffer, 0, RecordHeader.SIZE) != RecordHeader.SIZE)
                        break;

                    fixed (byte* headerPtr = headerBuffer)
                    {
                        var header = *(RecordHeader*)headerPtr;
                        var totalSize = RecordHeader.SIZE + (int)header.KeyLen + (int)header.ValueLen;

                        if (header.IsDeleted == 0)
                        {
                            var recordData = new byte[totalSize];
                            _file.Seek(position, SeekOrigin.Begin);
                            if (_file.Read(recordData, 0, totalSize) == totalSize)
                            {
                                fixed (byte* dataPtr = recordData)
                                {
                                    var record = new Record(dataPtr, recordData.Length);
                                    var key = record.GetKey();
                                    _keyIndex[key] = position;
                                }
                            }
                        }
                        else
                        {
                            _freeSpaceManager.AddFreeSpace(position, totalSize);
                        }

                        position += totalSize;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading index at position {position}: {ex.Message}");
                break;
            }
        }
    }

    public bool ContainsKey(string key)
    {
        if (string.IsNullOrEmpty(key))
            return false;

        if (!_keyIndex.ContainsKey(key))
            return false;

        if (_keyIndex.TryGetValue(key, out var position))
        {
            try
            {
                if (_databaseHeader.EncryptionType != EncryptionType.None || _databaseHeader.CompressionType != CompressionType.None)
                {
                    _file.Seek(position + 8, SeekOrigin.Begin); // EncryptedDataHeader.IsDeleted位置
                }
                else
                {
                    _file.Seek(position + 16, SeekOrigin.Begin); // RecordHeader.IsDeleted位置
                }

                var isDeleted = _file.ReadByte();

                if (isDeleted == 1)
                {
                    _keyIndex.TryRemove(key, out _);
                    return false;
                }

                return true;
            }
            catch
            {
                _keyIndex.TryRemove(key, out _);
                return false;
            }
        }

        return false;
    }

    public int DeleteBatch(IEnumerable<string> keys)
    {
        if (keys == null)
            throw new ArgumentNullException(nameof(keys));

        var deletedCount = 0;
        var keysToDelete = keys.Where(k => !string.IsNullOrEmpty(k)).ToList();

        lock (_writeLock)
        {
            foreach (var key in keysToDelete)
            {
                if (_keyIndex.TryGetValue(key, out var position))
                {
                    MarkAsDeleted(position);
                    _keyIndex.TryRemove(key, out _);
                    deletedCount++;
                }
            }
        }

        return deletedCount;
    }

    public void Clear()
    {
        lock (_writeLock)
        {
            _keyIndex.Clear();
            var dataStartPosition = _freeSpaceManager.GetDataStartPosition();
            _file.SetLength(dataStartPosition);
            _freeSpaceManager.Clear();
            _file.Flush();
        }
    }

    public void Flush()
    {
        lock (_writeLock)
        {
            _file.Flush();
            _freeSpaceManager.SaveFreeSpaceRegion();
        }
    }

    public bool ShouldShrink()
    {
        var stats = _freeSpaceManager.GetStats();
        var dataStartPosition = _freeSpaceManager.GetDataStartPosition();
        var actualDataSize = _file.Length - dataStartPosition;

        var freeSpaceRatio = actualDataSize > 0 ? (double)stats.TotalFreeSpace / actualDataSize : 0.0;
        return freeSpaceRatio > 0.5 && _file.Length > 1024 * 1024;
    }

    /// <summary>
    /// 强制回收，允许强制重建
    /// </summary>
    /// <param name="force">是否强制回收，无论条件是否满足</param>
    /// <returns>回收结果</returns>
    public ShrinkResult Shrink(bool force = false)
    {
        if (!force)
        {
            // 检查是否满足回收条件
            var stats = GetStats();
            if (!_databaseHeader.ShouldGC(stats.TotalFileSize, stats.FreeSpaceStats.TotalFreeSpace, stats.ValidRecordCount)
                && !_config.ShouldTriggerGC(stats))
            {
                return new ShrinkResult
                {
                    OriginalFileSize = _file.Length,
                    NewFileSize = _file.Length,
                    SpaceSaved = 0,
                    SpaceSavedPercentage = 0,
                    ValidRecords = _keyIndex.Count,
                    TotalRecordsProcessed = _keyIndex.Count,
                    ElapsedMilliseconds = 0
                };
            }
        }

        return PerformShrink();
    }

    /// <summary>
    /// 收缩文件，移除碎片化空间，支持加密和非加密格式
    /// </summary>
    public ShrinkResult PerformShrink()
    {
        lock (_writeLock)
        {
            var startTime = DateTimeOffset.UtcNow;
            var originalFileSize = _file.Length;
            var tempFilePath = _file.Name + ".tmp";

            int validRecords = 0;
            int totalRecordsProcessed = 0;

            // 创建一个 _keyIndex 的副本，如果在文件移动失败时，重置索引副本
            var originalKeyIndex = new ConcurrentDictionary<string, long>(_keyIndex);

            try
            {
                using var tempFile = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write);

                // 写入数据库头部
                var headerBytes = new byte[DatabaseHeader.SIZE];
                fixed (byte* headerPtr = headerBytes)
                {
                    *(DatabaseHeader*)headerPtr = _databaseHeader;
                }
                tempFile.Write(headerBytes, 0, DatabaseHeader.SIZE);

                // 创建临时空闲空间管理器
                using var tempFreeSpaceManager = new FreeSpaceManager(tempFile, _databaseHeader.FreeSpaceRegionSizeKB * 1024, _config.EnableFreeSpaceReuse);

                var newKeyIndex = new ConcurrentDictionary<string, long>();
                var currentPosition = tempFreeSpaceManager.GetDataStartPosition();

                // 按键顺序重写所有有效记录
                foreach (var kvp in _keyIndex.OrderBy(x => x.Key))
                {
                    var key = kvp.Key;
                    var oldPosition = kvp.Value;
                    totalRecordsProcessed++;

                    try
                    {
                        _file.Seek(oldPosition, SeekOrigin.Begin);

                        if (_databaseHeader.EncryptionType != EncryptionType.None || _databaseHeader.CompressionType != CompressionType.None)
                        {
                            // 加密模式：读取 EncryptedDataHeader
                            var encryptedHeaderBuffer = new byte[EncryptedDataHeader.SIZE];
                            if (_file.Read(encryptedHeaderBuffer, 0, EncryptedDataHeader.SIZE) != EncryptedDataHeader.SIZE)
                                continue;

                            fixed (byte* headerPtr = encryptedHeaderBuffer)
                            {
                                var encryptedHeader = *(EncryptedDataHeader*)headerPtr;
                                if (encryptedHeader.IsDeleted != 0)
                                    continue; // 跳过已删除的记录

                                var totalSize = EncryptedDataHeader.SIZE + (int)encryptedHeader.EncryptedSize;

                                // 读取完整的加密数据块
                                var fullEncryptedData = new byte[totalSize];
                                _file.Seek(oldPosition, SeekOrigin.Begin);
                                if (_file.Read(fullEncryptedData, 0, totalSize) != totalSize)
                                    continue;

                                // 写入到新文件
                                tempFile.Seek(currentPosition, SeekOrigin.Begin);
                                tempFile.Write(fullEncryptedData);

                                // 更新新索引
                                newKeyIndex[key] = currentPosition;
                                currentPosition += totalSize;
                                validRecords++;
                            }
                        }
                        else
                        {
                            // 非加密模式：读取 RecordHeader
                            var headerBuffer = new byte[RecordHeader.SIZE];
                            if (_file.Read(headerBuffer, 0, RecordHeader.SIZE) != RecordHeader.SIZE)
                                continue;

                            fixed (byte* headerPtr = headerBuffer)
                            {
                                var header = *(RecordHeader*)headerPtr;
                                if (header.IsDeleted != 0)
                                    continue; // 跳过已删除的记录

                                var totalSize = RecordHeader.SIZE + (int)header.KeyLen + (int)header.ValueLen;
                                var recordData = new byte[totalSize];

                                // 读取完整记录
                                _file.Seek(oldPosition, SeekOrigin.Begin);
                                if (_file.Read(recordData, 0, totalSize) != totalSize)
                                    continue;

                                // 写入到新文件
                                tempFile.Seek(currentPosition, SeekOrigin.Begin);
                                tempFile.Write(recordData);

                                // 更新新索引
                                newKeyIndex[key] = currentPosition;
                                currentPosition += totalSize;
                                validRecords++;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Warning: Failed to process record '{key}' during shrink: {ex.Message}");
                        continue;
                    }
                }

                // 设置新文件大小
                tempFile.SetLength(currentPosition);
                tempFile.Flush();

                // 关闭原文件并替换
                var originalPath = _file.Name;
                _file.Close();

                // 在移动前需要清空索引，避免读取索引时发现文件不存在了
                _keyIndex.Clear();

                // 这里不能立即删除，而应该先备份
                if (File.Exists(originalPath))
                {
                    try { File.Delete(originalPath + ".bak"); } catch { }
                    File.Move(originalPath, originalPath + ".bak");
                }

                File.Move(tempFilePath, originalPath);

                // 重新打开文件
                var newFile = new FileStream(originalPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
                typeof(UltraKVEngine).GetField("_file", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)?.SetValue(this, newFile);

                // 更新索引
                _keyIndex.Clear();
                foreach (var kvp in newKeyIndex)
                {
                    _keyIndex[kvp.Key] = kvp.Value;
                }

                // 重新初始化空闲空间管理器
                _freeSpaceManager.Clear();

                var endTime = DateTimeOffset.UtcNow;
                var elapsedMs = (long)(endTime - startTime).TotalMilliseconds;
                var newFileSize = _file.Length;
                var spaceSaved = originalFileSize - newFileSize;
                var spaceSavedPercentage = originalFileSize > 0 ? (double)spaceSaved / originalFileSize * 100 : 0;

                return new ShrinkResult
                {
                    OriginalFileSize = originalFileSize,
                    NewFileSize = newFileSize,
                    SpaceSaved = spaceSaved,
                    SpaceSavedPercentage = spaceSavedPercentage,
                    ValidRecords = validRecords,
                    TotalRecordsProcessed = totalRecordsProcessed,
                    ElapsedMilliseconds = elapsedMs
                };
            }
            catch (Exception ex)
            {
                // 清理临时文件
                if (File.Exists(tempFilePath))
                {
                    try { File.Delete(tempFilePath); } catch { }
                }

                // 恢复原始索引
                _keyIndex.Clear();
                foreach (var kvp in originalKeyIndex)
                {
                    _keyIndex[kvp.Key] = kvp.Value;
                }

                throw new InvalidOperationException($"Shrink operation failed: {ex.Message}", ex);
            }
        }
    }

    /// <summary>
    /// 获取详细的数据库统计信息，支持加密和非加密格式
    /// </summary>
    public DatabaseStats GetStats()
    {
        var freeSpaceStats = _freeSpaceManager.GetStats();
        var dataStartPosition = _freeSpaceManager.GetDataStartPosition();
        var actualDataSize = _file.Length - dataStartPosition;

        // 计算实际使用的数据大小
        long totalRecordSize = 0;
        long totalOriginalSize = 0; // 解密前的原始大小
        int validRecordCount = 0;
        int deletedRecordCount = 0;
        var compressionSavings = new List<double>();

        foreach (var position in _keyIndex.Values)
        {
            try
            {
                _file.Seek(position, SeekOrigin.Begin);

                if (_databaseHeader.EncryptionType != EncryptionType.None || _databaseHeader.CompressionType != CompressionType.None)
                {
                    // 加密模式：读取 EncryptedDataHeader
                    var encryptedHeaderBuffer = new byte[EncryptedDataHeader.SIZE];
                    if (_file.Read(encryptedHeaderBuffer, 0, EncryptedDataHeader.SIZE) == EncryptedDataHeader.SIZE)
                    {
                        fixed (byte* headerPtr = encryptedHeaderBuffer)
                        {
                            var encryptedHeader = *(EncryptedDataHeader*)headerPtr;
                            var recordSize = EncryptedDataHeader.SIZE + (int)encryptedHeader.EncryptedSize;

                            if (encryptedHeader.IsDeleted == 0)
                            {
                                totalRecordSize += recordSize;
                                totalOriginalSize += encryptedHeader.OriginalSize;
                                validRecordCount++;

                                // 计算压缩比
                                if (encryptedHeader.OriginalSize > 0)
                                {
                                    var compressionRatio = (double)encryptedHeader.EncryptedSize / encryptedHeader.OriginalSize;
                                    compressionSavings.Add(compressionRatio);
                                }
                            }
                            else
                            {
                                deletedRecordCount++;
                            }
                        }
                    }
                }
                else
                {
                    // 非加密模式：读取 RecordHeader
                    var headerBuffer = new byte[RecordHeader.SIZE];
                    if (_file.Read(headerBuffer, 0, RecordHeader.SIZE) == RecordHeader.SIZE)
                    {
                        fixed (byte* headerPtr = headerBuffer)
                        {
                            var header = *(RecordHeader*)headerPtr;
                            var recordSize = RecordHeader.SIZE + (int)header.KeyLen + (int)header.ValueLen;

                            if (header.IsDeleted == 0)
                            {
                                totalRecordSize += recordSize;
                                totalOriginalSize += recordSize; // 非加密模式下两者相同
                                validRecordCount++;
                            }
                            else
                            {
                                deletedRecordCount++;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Failed to read record at position {position}: {ex.Message}");
                // 忽略读取错误
            }
        }

        // 计算实际压缩比
        var actualCompressionRatio = compressionSavings.Count > 0 ? compressionSavings.Average() : CalculateCompressionRatio();

        return new DatabaseStats
        {
            Header = _databaseHeader,
            KeyCount = _keyIndex.Count,
            ValidRecordCount = validRecordCount,
            DeletedRecordCount = deletedRecordCount,
            TotalFileSize = _file.Length,
            DataRegionSize = actualDataSize,
            UsedDataSize = totalRecordSize,
            OriginalDataSize = totalOriginalSize, // 新增：原始数据大小（解密前）
            FreeSpaceStats = freeSpaceStats,
            CompressionRatio = actualCompressionRatio,
            FragmentationRatio = freeSpaceStats.FragmentationRatio,
            SpaceUtilization = actualDataSize > 0 ? (double)totalRecordSize / actualDataSize : 0.0,
            EncryptionOverhead = CalculateEncryptionOverhead(totalOriginalSize, totalRecordSize),
            AverageRecordSize = validRecordCount > 0 ? (double)totalRecordSize / validRecordCount : 0.0
        };
    }

    /// <summary>
    /// 计算理论压缩比（用于无法获取实际压缩数据时的估算）
    /// </summary>
    private double CalculateCompressionRatio()
    {
        if (_databaseHeader.CompressionType == CompressionType.None)
            return 1.0;

        // 基于算法特性的理论压缩比
        return _databaseHeader.CompressionType switch
        {
            CompressionType.Gzip => 0.65,      // 一般文本压缩到65%
            CompressionType.Deflate => 0.70,   // 稍差于Gzip
            CompressionType.Brotli => 0.55,    // 最好的压缩比
            _ => 1.0
        };
    }

    /// <summary>
    /// 计算加密开销
    /// </summary>
    private double CalculateEncryptionOverhead(long originalSize, long encryptedSize)
    {
        if (_databaseHeader.EncryptionType == EncryptionType.None || originalSize == 0)
            return 0.0;

        // 加密开销 = (加密后大小 - 原始大小) / 原始大小
        var overhead = (double)(encryptedSize - originalSize) / originalSize;
        return Math.Max(0.0, overhead); // 确保不为负数
    }

    public IEnumerable<string> GetAllKeys()
    {
        return _keyIndex.Keys.ToList();
    }

    public DatabaseInfo GetDatabaseInfo()
    {
        var stats = _freeSpaceManager.GetStats();
        return new DatabaseInfo
        {
            Header = _databaseHeader,
            KeyCount = _keyIndex.Count,
            FileSize = _file.Length,
            FreeSpaceStats = stats
        };
    }

    /// <summary>
    /// 获取GC统计信息
    /// </summary>
    public GCStats GetGCStats()
    {
        var stats = GetStats();
        var shouldTriggerGC = _databaseHeader.ShouldGC(stats.TotalFileSize, stats.FreeSpaceStats.TotalFreeSpace, stats.ValidRecordCount);
        var shouldForceGC = _config.ShouldTriggerGC(stats);

        return new GCStats
        {
            IsGcAutoRecycleEnabled = _databaseHeader.IsGcAutoRecycleEnabled,
            GcMinFileSizeKB = _databaseHeader.GcMinFileSizeKB,
            GcFreeSpaceThreshold = _databaseHeader.GcFreeSpaceThreshold,
            GcMinRecordCount = _databaseHeader.GcMinRecordCount,
            GcFlushInterval = _config.GcFlushInterval,
            CurrentFileSize = stats.TotalFileSize,
            CurrentFreeSpace = stats.WastedSpace,
            CurrentFreeSpaceRatio = stats.TotalFileSize > 0 ? (double)stats.WastedSpace / stats.TotalFileSize : 0.0,
            CurrentRecordCount = stats.ValidRecordCount,
            ShouldTriggerGC = shouldTriggerGC,
            ShouldForceGC = shouldForceGC,
            LastFlushTime = _lastFlushTime
        };
    }

    /// <summary>
    /// 验证统计信息
    /// </summary>
    public struct ValidationStats
    {
        public bool IsUpdateValidationEnabled;
        public int MaxKeyLength;
        public bool IsFreeSpaceReuseEnabled;
        public FreeSpaceStats FreeSpaceStats;

        public override string ToString()
        {
            return $"Validation - UpdateValidation: {IsUpdateValidationEnabled}, " +
                   $"MaxKeyLength: {MaxKeyLength}, " +
                   $"FreeSpaceReuse: {IsFreeSpaceReuseEnabled}, " +
                   $"FreeSpace: {(FreeSpaceStats.IsEnabled ? $"{FreeSpaceStats.TotalFreeSpace / 1024.0:F1}KB" : "Disabled")}";
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _flushTimer?.Dispose();

            // 最后一次刷盘
            try
            {
                Flush();
            }
            catch { }

            // 更新最后访问时间
            var updatedHeader = _databaseHeader;
            updatedHeader.UpdateAccessTime();

            _file.Seek(0, SeekOrigin.Begin);
            var headerBytes = new ReadOnlySpan<byte>(&updatedHeader, DatabaseHeader.SIZE);
            _file.Write(headerBytes);

            _freeSpaceManager?.Dispose();
            _dataProcessor?.Dispose();
            _file?.Dispose();
            _disposed = true;
        }
    }
}

/// <summary>
/// GC统计信息
/// </summary>
public struct GCStats
{
    public bool IsGcAutoRecycleEnabled;
    public long GcMinFileSizeKB;
    public double GcFreeSpaceThreshold;
    public int GcMinRecordCount;
    public int GcFlushInterval;
    public long CurrentFileSize;
    public long CurrentFreeSpace;
    public double CurrentFreeSpaceRatio;
    public int CurrentRecordCount;
    public bool ShouldTriggerGC;
    public bool ShouldForceGC;
    public DateTime LastFlushTime;

    public override string ToString()
    {
        return $"GC Stats - AutoRecycle: {IsGcAutoRecycleEnabled}, " +
               $"Current: {CurrentFileSize / 1024}KB ({CurrentFreeSpaceRatio:P1} free), " +
               $"Records: {CurrentRecordCount}, " +
               $"ShouldGC: {ShouldTriggerGC}, ForceGC: {ShouldForceGC}, " +
               $"LastFlush: {LastFlushTime:HH:mm:ss}";
    }
}

/// <summary>
/// 详细的数据库统计信息（更新版本）
/// </summary>
public struct DatabaseStats
{
    public DatabaseHeader Header;
    public int KeyCount;
    public int ValidRecordCount;
    public int DeletedRecordCount;
    public long TotalFileSize;
    public long DataRegionSize;
    public long UsedDataSize;
    public long OriginalDataSize;        // 新增：原始数据大小（压缩/加密前）
    public FreeSpaceStats FreeSpaceStats;
    public double CompressionRatio;
    public double FragmentationRatio;
    public double SpaceUtilization;
    public double EncryptionOverhead;    // 新增：加密开销
    public double AverageRecordSize;     // 新增：平均记录大小

    public readonly long WastedSpace => FreeSpaceStats.TotalFreeSpace;
    public readonly bool IsFragmented => FragmentationRatio > 0.3;
    public readonly bool ShouldShrink => SpaceUtilization < 0.5 && TotalFileSize > 1024 * 1024;
    public readonly double CompressionSavings => OriginalDataSize > 0 ? 1.0 - CompressionRatio : 0.0;
    public readonly double TotalSpaceEfficiency => OriginalDataSize > 0 ? (double)UsedDataSize / OriginalDataSize : 1.0;

    public override readonly string ToString()
    {
        var sb = new StringBuilder();
        sb.AppendLine($"=== Database Statistics ===");
        sb.AppendLine($"Records: {KeyCount} (Valid: {ValidRecordCount}, Deleted: {DeletedRecordCount})");
        sb.AppendLine($"File Size: {TotalFileSize / 1024.0:F1} KB");
        sb.AppendLine($"Used Data: {UsedDataSize / 1024.0:F1} KB");

        if (OriginalDataSize > 0 && OriginalDataSize != UsedDataSize)
        {
            sb.AppendLine($"Original Size: {OriginalDataSize / 1024.0:F1} KB");
            sb.AppendLine($"Compression: {CompressionRatio:P1} ({CompressionSavings:P1} saved)");
        }

        if (Header.EncryptionType != EncryptionType.None)
        {
            sb.AppendLine($"Encryption Overhead: {EncryptionOverhead:P1}");
        }

        sb.AppendLine($"Free Space: {WastedSpace / 1024.0:F1} KB");
        sb.AppendLine($"Space Utilization: {SpaceUtilization:P1}");
        sb.AppendLine($"Fragmentation: {FragmentationRatio:P1}");
        sb.AppendLine($"Average Record Size: {AverageRecordSize:F1} bytes");
        sb.AppendLine($"Shrink Recommended: {ShouldShrink}");

        return sb.ToString();
    }
}

public struct DatabaseInfo
{
    public DatabaseHeader Header;
    public int KeyCount;
    public long FileSize;
    public FreeSpaceStats FreeSpaceStats;

    public override string ToString()
    {
        return $"Keys: {KeyCount}, Size: {FileSize / 1024.0:F1}KB, " +
               $"Compression: {Header.CompressionType}, Encryption: {Header.EncryptionType}, " +
               $"Created: {DateTimeOffset.FromUnixTimeMilliseconds(Header.CreatedTime):yyyy-MM-dd HH:mm:ss}";
    }
}

// 收缩结果
public readonly struct ShrinkResult
{
    public long OriginalFileSize { get; init; }
    public long NewFileSize { get; init; }
    public long SpaceSaved { get; init; }
    public double SpaceSavedPercentage { get; init; }
    public int ValidRecords { get; init; }
    public int TotalRecordsProcessed { get; init; }
    public long ElapsedMilliseconds { get; init; }

    public override string ToString()
    {
        return $"Shrink completed: {SpaceSaved / 1024.0:F1} KB saved ({SpaceSavedPercentage:F1}%), " +
               $"{ValidRecords}/{TotalRecordsProcessed} records kept, took {ElapsedMilliseconds}ms";
    }
}

// 增强的统计信息
public readonly struct EngineStats
{
    public int RecordCount { get; init; }
    public int DeletedCount { get; init; }
    public int IndexSize { get; init; }
    public long FileSize { get; init; }
    public double DeletionRatio { get; init; }
    public bool ShrinkRecommended { get; init; }

    public override string ToString()
    {
        return $"Records: {RecordCount}, Deleted: {DeletedCount}, " +
               $"File: {FileSize / 1024.0:F1} KB, Deletion ratio: {DeletionRatio:P1}, " +
               $"Shrink recommended: {ShrinkRecommended}";
    }
}