using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace UltraKV;

public class UltraKVEngine : IDisposable
{
    private readonly string _filePath;
    private FastFileWriter _writer; // 移除 readonly，收缩时需要重新创建

    // 使用更快的索引结构
    private readonly ConcurrentDictionary<string, IndexEntry> _index;

    private volatile int _recordCount;
    private volatile int _deletedCount; // 跟踪删除的记录数
    private volatile bool _disposed;

    // 内联结构减少内存分配
    private readonly struct IndexEntry
    {
        public readonly long Position;
        public readonly int Size;

        public IndexEntry(long position, int size)
        {
            Position = position;
            Size = size;
        }
    }

    public UltraKVEngine(string filePath)
    {
        _filePath = filePath;
        _index = new ConcurrentDictionary<string, IndexEntry>();

        // 先加载索引，再初始化写入器
        LoadIndex();
        _writer = new FastFileWriter(filePath);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Put(string key, string value)
    {
        if (_disposed) ThrowDisposed();

        var record = new Record(key, value);
        var position = _writer.Write(record.GetRawData());

        _index[key] = new IndexEntry(position, record.TotalSize);
        Interlocked.Increment(ref _recordCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string? Get(string key)
    {
        if (_disposed) ThrowDisposed();

        if (!_index.TryGetValue(key, out var entry))
            return null;

        // 使用独立的文件流进行读取
        try
        {
            using var file = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var buffer = new byte[entry.Size];

            file.Seek(entry.Position, SeekOrigin.Begin);
            if (file.Read(buffer, 0, entry.Size) != entry.Size)
                return null;

            unsafe
            {
                fixed (byte* ptr = buffer)
                {
                    var header = *(RecordHeader*)ptr;
                    if (header.IsDeleted != 0)
                        return null;

                    return System.Text.Encoding.UTF8.GetString(
                        ptr + RecordHeader.SIZE + header.KeyLen,
                        (int)header.ValueLen);
                }
            }
        }
        catch
        {
            return null;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Delete(string key)
    {
        if (_disposed) ThrowDisposed();

        // 检查键是否存在
        if (!_index.ContainsKey(key))
            return false;

        var record = new Record(key, "", true);
        _writer.Write(record.GetRawData());

        _index.TryRemove(key, out _);
        Interlocked.Increment(ref _deletedCount);

        return true;
    }

    // 批量删除
    public int DeleteBatch(IEnumerable<string> keys)
    {
        if (_disposed) ThrowDisposed();

        var deletedCount = 0;
        foreach (var key in keys)
        {
            if (Delete(key))
                deletedCount++;
        }

        return deletedCount;
    }

    // 清空所有数据
    public void Clear()
    {
        if (_disposed) ThrowDisposed();

        var allKeys = _index.Keys.ToArray();
        DeleteBatch(allKeys);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ContainsKey(string key) => _index.ContainsKey(key);

    // 获取所有键
    public IEnumerable<string> GetAllKeys()
    {
        if (_disposed) ThrowDisposed();
        return _index.Keys.ToList();
    }

    /// <summary>
    /// 手动收缩数据库，移除已删除的记录，回收磁盘空间
    /// 修复版本：使用安全的内存操作，避免访问违规
    /// </summary>
    public ShrinkResult Shrink()
    {
        if (_disposed) ThrowDisposed();

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var originalFileSize = new FileInfo(_filePath).Length;
        var tempPath = _filePath + ".shrink";
        var validRecords = 0;
        var totalRecordsProcessed = 0;

        try
        {
            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Starting database shrink...");

            // 创建新的临时文件
            using var tempWriter = new FastFileWriter(tempPath);
            var newIndex = new Dictionary<string, IndexEntry>(); // 使用普通Dictionary避免并发问题

            // 安全地读取并重写有效记录
            using (var reader = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                var buffer = new byte[64 * 1024];
                var position = 0L;

                while (position < reader.Length)
                {
                    try
                    {
                        reader.Seek(position, SeekOrigin.Begin);

                        // 安全读取记录头
                        var headerBytes = new byte[RecordHeader.SIZE];
                        if (reader.Read(headerBytes, 0, RecordHeader.SIZE) != RecordHeader.SIZE)
                            break;

                        // 安全解析头部
                        RecordHeader header;
                        unsafe
                        {
                            fixed (byte* ptr = headerBytes)
                            {
                                header = *(RecordHeader*)ptr;
                            }
                        }

                        // 验证头部数据的合理性
                        if (header.KeyLen == 0 || header.KeyLen > 64 * 1024 ||
                            header.ValueLen > 64 * 1024 * 1024) // 64MB 最大值限制
                        {
                            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Warning: Invalid header at position {position}, skipping...");
                            position += RecordHeader.SIZE;
                            continue;
                        }

                        var totalSize = RecordHeader.SIZE + header.KeyLen + header.ValueLen;

                        // 验证记录大小的合理性
                        if (totalSize > 128 * 1024 * 1024) // 128MB 最大记录限制
                        {
                            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Warning: Record too large ({totalSize} bytes) at position {position}, skipping...");
                            position += RecordHeader.SIZE;
                            continue;
                        }

                        // 确保缓冲区足够大
                        if (totalSize > buffer.Length)
                        {
                            buffer = new byte[totalSize];
                        }

                        // 重新定位并读取完整记录
                        reader.Seek(position, SeekOrigin.Begin);
                        var bytesRead = reader.Read(buffer, 0, (int)totalSize);

                        if (bytesRead != totalSize)
                        {
                            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Warning: Incomplete record at position {position}, expected {totalSize} bytes, got {bytesRead}");
                            break;
                        }

                        totalRecordsProcessed++;

                        // 安全解析记录内容
                        string key;
                        bool isDeleted;

                        try
                        {
                            unsafe
                            {
                                fixed (byte* dataPtr = buffer)
                                {
                                    var recordHeader = *(RecordHeader*)dataPtr;
                                    isDeleted = recordHeader.IsDeleted != 0;

                                    // 安全提取键
                                    var keyBytes = new byte[recordHeader.KeyLen];
                                    Marshal.Copy((IntPtr)(dataPtr + RecordHeader.SIZE), keyBytes, 0, (int)recordHeader.KeyLen);
                                    key = System.Text.Encoding.UTF8.GetString(keyBytes);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Warning: Failed to parse record at position {position}: {ex.Message}");
                            position += totalSize;
                            continue;
                        }

                        // 只保留未删除且在当前索引中的记录
                        if (!isDeleted && _index.ContainsKey(key))
                        {
                            try
                            {
                                var recordData = new byte[totalSize];
                                Array.Copy(buffer, 0, recordData, 0, (int)totalSize);

                                var newPosition = tempWriter.Write(recordData);
                                newIndex[key] = new IndexEntry(newPosition, (int)totalSize);
                                validRecords++;
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Warning: Failed to write record for key '{key}': {ex.Message}");
                            }
                        }

                        position += totalSize;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Error processing record at position {position}: {ex.Message}");
                        // 尝试跳过当前记录
                        position += Math.Max(RecordHeader.SIZE, 1);

                        // 如果错误太多，停止处理
                        if (++totalRecordsProcessed > 1000000) // 防止无限循环
                        {
                            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Too many errors, stopping shrink operation");
                            break;
                        }
                    }
                }
            }

            tempWriter.Flush();
            tempWriter.Dispose();

            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Processed {totalRecordsProcessed} records, kept {validRecords} valid records");

            // 关闭当前写入器
            _writer.Dispose();

            // 等待文件句柄完全释放
            // 判断是否需要等待文件句柄释放
            Thread.Sleep(10);

            // 原子替换文件
            if (File.Exists(_filePath))
            {
                var backupPath = _filePath + ".backup";
                File.Move(_filePath, backupPath); // 先备份原文件

                try
                {
                    File.Move(tempPath, _filePath);
                    File.Delete(backupPath); // 删除备份
                }
                catch
                {
                    // 如果失败，恢复原文件
                    if (File.Exists(backupPath))
                        File.Move(backupPath, _filePath);
                    throw;
                }
            }
            else
            {
                File.Move(tempPath, _filePath);
            }

            // 重新初始化写入器和索引
            _writer = new FastFileWriter(_filePath);
            _index.Clear();

            foreach (var kvp in newIndex)
            {
                _index[kvp.Key] = kvp.Value;
            }

            // 重置计数器
            _recordCount = validRecords;
            _deletedCount = 0;

            sw.Stop();
            var newFileSize = new FileInfo(_filePath).Length;
            var spaceSaved = originalFileSize - newFileSize;

            var result = new ShrinkResult
            {
                OriginalFileSize = originalFileSize,
                NewFileSize = newFileSize,
                SpaceSaved = spaceSaved,
                ValidRecords = validRecords,
                TotalRecordsProcessed = totalRecordsProcessed,
                ElapsedMilliseconds = sw.ElapsedMilliseconds,
                SpaceSavedPercentage = originalFileSize > 0 ? (double)spaceSaved / originalFileSize * 100 : 0
            };

            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Shrink completed: {spaceSaved / 1024.0:F1} KB saved ({result.SpaceSavedPercentage:F1}%) in {sw.ElapsedMilliseconds}ms");

            return result;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Shrink failed: {ex.Message}");

            // 清理临时文件
            try
            {
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }
            catch { }

            // 尝试重新初始化写入器
            try
            {
                if (_writer == null)
                    _writer = new FastFileWriter(_filePath);
            }
            catch { }

            throw;
        }
    }

    /// <summary>
    /// 异步收缩数据库
    /// </summary>
    public async Task<ShrinkResult> ShrinkAsync()
    {
        return await Task.Run(Shrink);
    }

    /// <summary>
    /// 检查是否建议进行收缩
    /// </summary>
    public bool ShouldShrink(double threshold = 0.15)
    {
        if (_recordCount == 0 || _deletedCount == 0)
            return false;

        var deletionRatio = (double)_deletedCount / _recordCount;
        return deletionRatio >= threshold;
    }

    public void Flush() => _writer.Flush();

    private void LoadIndex()
    {
        if (!File.Exists(_filePath))
            return;

        try
        {
            using var file = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var buffer = new byte[64 * 1024];
            var position = 0L;

            while (position < file.Length)
            {
                file.Seek(position, SeekOrigin.Begin);

                if (file.Read(buffer, 0, RecordHeader.SIZE) != RecordHeader.SIZE)
                    break;

                unsafe
                {
                    fixed (byte* ptr = buffer)
                    {
                        var header = *(RecordHeader*)ptr;
                        var totalSize = RecordHeader.SIZE + header.KeyLen + header.ValueLen;

                        if (totalSize > buffer.Length)
                        {
                            buffer = new byte[totalSize];
                            file.Seek(position, SeekOrigin.Begin);
                        }

                        if (file.Read(buffer, 0, (int)totalSize) == totalSize)
                        {
                            fixed (byte* dataPtr = buffer)
                            {
                                var recordHeader = *(RecordHeader*)dataPtr;
                                var key = System.Text.Encoding.UTF8.GetString(
                                    dataPtr + RecordHeader.SIZE,
                                    (int)recordHeader.KeyLen);

                                if (recordHeader.IsDeleted != 0)
                                {
                                    _index.TryRemove(key, out _);
                                    _deletedCount++;
                                }
                                else
                                {
                                    _index[key] = new IndexEntry(position, (int)totalSize);
                                }
                            }
                        }

                        position += totalSize;
                        _recordCount++;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Warning: Failed to load index from {_filePath}: {ex.Message}");
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowDisposed() => throw new ObjectDisposedException(nameof(UltraKVEngine));

    public EngineStats GetStats()
    {
        var fileSize = 0L;
        try
        {
            fileSize = new FileInfo(_filePath).Length;
        }
        catch { }

        var deletionRatio = _recordCount > 0 && _deletedCount > 0 ?
            (double)_deletedCount / _recordCount : 0;

        return new EngineStats
        {
            RecordCount = _recordCount,
            DeletedCount = _deletedCount,
            IndexSize = _index.Count,
            FileSize = fileSize,
            DeletionRatio = deletionRatio,
            ShrinkRecommended = ShouldShrink()
        };
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _writer?.Dispose();
            _disposed = true;
        }
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