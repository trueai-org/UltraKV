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
    private readonly object _writeLock = new object();
    private bool _disposed;

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
        config ??= UltraKVConfig.Minimal;
        config.Validate();

        var isNewFile = !File.Exists(filePath);
        _file = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);

        if (isNewFile)
        {
            // 新建数据库
            _databaseHeader = DatabaseHeader.Create(config);
            WriteDatabaseHeader();
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
            ValidateConfigCompatibility(config);
        }

        _freeSpaceManager = new FreeSpaceManager(_file, (int)_databaseHeader.FreeSpaceRegionSize);
        _dataProcessor = new DataProcessor(_databaseHeader, config.EncryptionKey);
        _keyIndex = new ConcurrentDictionary<string, long>();

        if (!isNewFile)
        {
            LoadIndex();
        }
    }

    private void WriteDatabaseHeader()
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
                var multipliedSize = (int)(totalSize * _databaseHeader.AllocationMultiplier);

                // 寻找合适的空闲空间
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
                var multipliedSize = (int)(requiredSize * _databaseHeader.AllocationMultiplier);

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
    /// 收缩文件，移除碎片化空间，支持加密和非加密格式
    /// </summary>
    public ShrinkResult Shrink()
    {
        lock (_writeLock)
        {
            var startTime = DateTimeOffset.UtcNow;
            var originalFileSize = _file.Length;
            var tempFilePath = _file.Name + ".tmp";

            int validRecords = 0;
            int totalRecordsProcessed = 0;

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
                using var tempFreeSpaceManager = new FreeSpaceManager(tempFile, (int)_databaseHeader.FreeSpaceRegionSize);

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

                File.Delete(originalPath);
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

    public void Dispose()
    {
        if (!_disposed)
        {
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