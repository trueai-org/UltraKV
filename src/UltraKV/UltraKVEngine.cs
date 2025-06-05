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

        // 处理数据（压缩+加密）
        var processedData = _dataProcessor.ProcessData(rawData);

        lock (_writeLock)
        {
            // 如果键已存在，标记旧记录为删除
            if (_keyIndex.TryGetValue(key, out var oldPosition))
            {
                MarkAsDeleted(oldPosition);
            }

            // 寻找合适的空闲空间
            var requiredSize = processedData.Length;
            var multipliedSize = (int)(requiredSize * _databaseHeader.AllocationMultiplier);

            long position;
            if (_freeSpaceManager.TryGetFreeSpace(multipliedSize, out var freeBlock))
            {
                position = freeBlock.Position;

                // 如果分配的空间有剩余，加入空闲空间
                var remainingSize = freeBlock.Size - requiredSize;
                if (remainingSize > 64) // 小于64字节的碎片忽略
                {
                    _freeSpaceManager.AddFreeSpace(position + requiredSize, remainingSize);
                }
            }
            else
            {
                // 文件末尾追加
                position = _file.Length;
                _file.SetLength(position + multipliedSize);
            }

            // 写入数据
            _file.Seek(position, SeekOrigin.Begin);
            _file.Write(processedData);
            _file.Flush();

            // 更新索引
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

            // 先读取原始头部来获取数据大小
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

                // 计算总数据大小并读取
                var totalDataSize = RecordHeader.SIZE + (int)header.KeyLen + (int)header.ValueLen;
                var allData = new byte[totalDataSize];

                // 将头部拷贝到完整数据中
                Array.Copy(headerBuffer, allData, RecordHeader.SIZE);

                // 读取剩余数据
                var remainingSize = totalDataSize - RecordHeader.SIZE;
                if (remainingSize > 0)
                {
                    _file.Read(allData, RecordHeader.SIZE, remainingSize);
                }

                // 逆向处理数据（解密+解压缩）
                var processedData = _dataProcessor.UnprocessData(allData);

                // 解析记录
                fixed (byte* dataPtr = processedData)
                {
                    var record = new Record(dataPtr, processedData.Length);
                    return record.GetValue();
                }
            }
        }
        catch
        {
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

    /// <summary>
    /// 检查键是否存在
    /// </summary>
    public bool ContainsKey(string key)
    {
        if (string.IsNullOrEmpty(key))
            return false;

        // 首先检查内存索引
        if (!_keyIndex.ContainsKey(key))
            return false;

        // 验证记录是否真实存在且未被删除
        if (_keyIndex.TryGetValue(key, out var position))
        {
            try
            {
                _file.Seek(position + 16, SeekOrigin.Begin); // 跳到IsDeleted字段
                var isDeleted = _file.ReadByte();

                if (isDeleted == 1)
                {
                    // 如果记录已删除，从索引中移除
                    _keyIndex.TryRemove(key, out _);
                    return false;
                }

                return true;
            }
            catch
            {
                // 读取失败，从索引中移除
                _keyIndex.TryRemove(key, out _);
                return false;
            }
        }

        return false;
    }

    /// <summary>
    /// 批量删除键
    /// </summary>
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

    /// <summary>
    /// 清空所有数据
    /// </summary>
    public void Clear()
    {
        lock (_writeLock)
        {
            // 清空内存索引
            _keyIndex.Clear();

            // 重置文件大小到数据开始位置
            var dataStartPosition = _freeSpaceManager.GetDataStartPosition();
            _file.SetLength(dataStartPosition);

            // 重新初始化空闲空间管理器
            _freeSpaceManager.Clear();

            // 强制刷新
            _file.Flush();
        }
    }

    /// <summary>
    /// 强制刷新缓冲区到磁盘
    /// </summary>
    public void Flush()
    {
        lock (_writeLock)
        {
            _file.Flush();
            _freeSpaceManager.SaveFreeSpaceRegion();
        }
    }

    /// <summary>
    /// 检查是否应该收缩文件
    /// </summary>
    public bool ShouldShrink()
    {
        var stats = _freeSpaceManager.GetStats();
        var dataStartPosition = _freeSpaceManager.GetDataStartPosition();
        var actualDataSize = _file.Length - dataStartPosition;

        // 如果空闲空间超过50%且总文件大小超过1MB，建议收缩
        var freeSpaceRatio = (double)stats.TotalFreeSpace / actualDataSize;
        return freeSpaceRatio > 0.5 && _file.Length > 1024 * 1024;
    }

    /// <summary>
    /// 收缩文件，移除碎片化空间
    /// </summary>
    public void Shrink()
    {
        lock (_writeLock)
        {
            var tempFilePath = _file.Name + ".tmp";

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

                    try
                    {
                        // 读取原记录
                        _file.Seek(oldPosition, SeekOrigin.Begin);
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
                        }
                    }
                    catch
                    {
                        // 跳过有问题的记录
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
    /// 获取数据库统计信息
    /// </summary>
    public DatabaseStats GetStats()
    {
        var freeSpaceStats = _freeSpaceManager.GetStats();
        var dataStartPosition = _freeSpaceManager.GetDataStartPosition();
        var actualDataSize = _file.Length - dataStartPosition;

        // 计算实际使用的数据大小
        long totalRecordSize = 0;
        int validRecordCount = 0;
        int deletedRecordCount = 0;

        foreach (var position in _keyIndex.Values)
        {
            try
            {
                _file.Seek(position, SeekOrigin.Begin);
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
                            validRecordCount++;
                        }
                        else
                        {
                            deletedRecordCount++;
                        }
                    }
                }
            }
            catch
            {
                // 忽略读取错误
            }
        }

        return new DatabaseStats
        {
            Header = _databaseHeader,
            KeyCount = _keyIndex.Count,
            ValidRecordCount = validRecordCount,
            DeletedRecordCount = deletedRecordCount,
            TotalFileSize = _file.Length,
            DataRegionSize = actualDataSize,
            UsedDataSize = totalRecordSize,
            FreeSpaceStats = freeSpaceStats,
            CompressionRatio = CalculateCompressionRatio(),
            FragmentationRatio = freeSpaceStats.FragmentationRatio,
            SpaceUtilization = actualDataSize > 0 ? (double)totalRecordSize / actualDataSize : 0.0
        };
    }

    private double CalculateCompressionRatio()
    {
        if (_databaseHeader.CompressionType == CompressionType.None)
            return 1.0;

        // 简化的压缩比计算，实际应该基于压缩前后的数据大小
        return _databaseHeader.CompressionType switch
        {
            CompressionType.Gzip => 0.6,
            CompressionType.Deflate => 0.65,
            CompressionType.Brotli => 0.55,
            _ => 1.0
        };
    }

    private void MarkAsDeleted(long position)
    {
        try
        {
            _file.Seek(position + 16, SeekOrigin.Begin); // 跳到IsDeleted字段
            _file.WriteByte(1);

            // 计算记录大小并加入空闲空间
            _file.Seek(position, SeekOrigin.Begin);
            var headerBuffer = new byte[RecordHeader.SIZE];
            if (_file.Read(headerBuffer, 0, RecordHeader.SIZE) == RecordHeader.SIZE)
            {
                fixed (byte* headerPtr = headerBuffer)
                {
                    var header = *(RecordHeader*)headerPtr;
                    var recordSize = RecordHeader.SIZE + (int)header.KeyLen + (int)header.ValueLen;
                    _freeSpaceManager.AddFreeSpace(position, recordSize);
                }
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

                var headerBuffer = new byte[RecordHeader.SIZE];
                if (_file.Read(headerBuffer, 0, RecordHeader.SIZE) != RecordHeader.SIZE)
                    break;

                fixed (byte* headerPtr = headerBuffer)
                {
                    var header = *(RecordHeader*)headerPtr;
                    var totalSize = RecordHeader.SIZE + (int)header.KeyLen + (int)header.ValueLen;

                    if (header.IsDeleted == 0)
                    {
                        // 读取完整记录
                        var recordData = new byte[totalSize];
                        _file.Seek(position, SeekOrigin.Begin);
                        if (_file.Read(recordData, 0, totalSize) == totalSize)
                        {
                            // 逆向处理数据
                            var processedData = _dataProcessor.UnprocessData(recordData);

                            fixed (byte* dataPtr = processedData)
                            {
                                var record = new Record(dataPtr, processedData.Length);
                                var key = record.GetKey();
                                _keyIndex[key] = position;
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
            catch
            {
                break;
            }
        }
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
/// 详细的数据库统计信息
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
    public FreeSpaceStats FreeSpaceStats;
    public double CompressionRatio;
    public double FragmentationRatio;
    public double SpaceUtilization;

    public readonly long WastedSpace => FreeSpaceStats.TotalFreeSpace;
    public readonly bool IsFragmented => FragmentationRatio > 0.3;
    public readonly bool ShouldShrink => SpaceUtilization < 0.5 && TotalFileSize > 1024 * 1024;

    public override readonly string ToString()
    {
        return $"Keys: {KeyCount} (Valid: {ValidRecordCount}, Deleted: {DeletedRecordCount}), " +
               $"Size: {TotalFileSize / 1024.0:F1}KB, Used: {UsedDataSize / 1024.0:F1}KB, " +
               $"Free: {WastedSpace / 1024.0:F1}KB, Utilization: {SpaceUtilization:P1}, " +
               $"Compression: {CompressionRatio:P1}, Fragmentation: {FragmentationRatio:P1}";
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