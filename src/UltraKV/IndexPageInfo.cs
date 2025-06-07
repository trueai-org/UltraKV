using System.Runtime.InteropServices;
using System.Text;

namespace UltraKV;

// 索引页头部结构
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct IndexPageHeader
{
    public uint Magic;              // 4 bytes - 魔法数字 "IDXP"
    public int EntryCount;          // 4 bytes - 当前条目数量
    public int MaxEntries;          // 4 bytes - 最大条目数量
    public int UsedSpace;           // 4 bytes - 已使用空间
    public int FreeSpace;           // 4 bytes - 剩余可用空间
    public long LastUpdateTime;     // 8 bytes - 最后更新时间
                                    // Total: 28 bytes

    public const uint MAGIC_NUMBER = 0x49445850; // "IDXP"
    public const int SIZE = 32; // 对齐到32字节

    public bool IsValid =>
        Magic == MAGIC_NUMBER &&
        EntryCount >= 0 &&
        MaxEntries >= 0 &&
        UsedSpace >= SIZE && // 至少有头部空间
        FreeSpace >= 0 &&
        LastUpdateTime >= 0;
}

/// <summary>
/// 索引页信息 - 管理单个索引页内的多个索引条目
/// </summary>
public unsafe class IndexPageInfo : IDisposable
{
    private readonly byte* _pageBuffer;
    private readonly int _pageSize;
    private readonly object _lock = new object();
    private readonly DataProcessor _dataProcessor;
    private bool _disposed;
    private bool _isDirty;

    public IndexPageInfo(int pageSize, DataProcessor dataProcessor)
    {
        _pageSize = pageSize;
        _dataProcessor = dataProcessor;
        _pageBuffer = (byte*)Marshal.AllocHGlobal(pageSize);

        InitializePage();
    }

    public IndexPageInfo(byte[] data, DataProcessor dataProcessor)
    {
        _pageSize = data.Length;
        _dataProcessor = dataProcessor;
        _pageBuffer = (byte*)Marshal.AllocHGlobal(_pageSize);

        // 从数据加载
        fixed (byte* srcPtr = data)
        {
            Buffer.MemoryCopy(srcPtr, _pageBuffer, _pageSize, data.Length);
        }

        ValidatePageHeader();
    }

    private void InitializePage()
    {
        // 清空页面
        new Span<byte>(_pageBuffer, _pageSize).Clear();

        // 初始化页面头部
        var header = (IndexPageHeader*)_pageBuffer;
        header->Magic = IndexPageHeader.MAGIC_NUMBER;
        header->EntryCount = 0;
        header->MaxEntries = CalculateMaxEntries();
        header->UsedSpace = IndexPageHeader.SIZE;
        header->FreeSpace = _pageSize - IndexPageHeader.SIZE;
        header->LastUpdateTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _isDirty = true;
    }

    private void ValidatePageHeader()
    {
        var header = (IndexPageHeader*)_pageBuffer;
        if (header->Magic != IndexPageHeader.MAGIC_NUMBER)
        {
            throw new InvalidDataException("Invalid index page magic number");
        }
    }

    /// <summary>
    /// TODO 这里需要精确计算页面能容纳的最大条目数
    /// </summary>
    /// <returns></returns>
    private int CalculateMaxEntries()
    {
        // 计算页面能容纳的最大条目数
        // 每个条目包括：IndexEntry(16字节) + ProcessedKey数据长度字段(4字节) + ProcessedKey数据
        // 这里预估平均ProcessedKey长度为40字节（考虑加密开销）
        var avgEntrySize = IndexEntry.SIZE + sizeof(int) + 40; // IndexEntry + KeyDataLength + 平均ProcessedKey长度
        var availableSpace = _pageSize - IndexPageHeader.SIZE;
        return availableSpace / avgEntrySize;
    }

    /// <summary>
    /// 添加或更新索引条目 - 支持处理后的key数据
    /// </summary>
    public bool AddOrUpdateEntry(string key, IndexEntry entry, byte[] processedKeyData)
    {
        if (string.IsNullOrEmpty(key) || processedKeyData == null)
            return false;

        lock (_lock)
        {
            // 更新IndexEntry中的KeyLength字段为处理后数据的长度
            entry.KeyLength = processedKeyData.Length;

            var requiredSpace = IndexEntry.SIZE + processedKeyData.Length;

            var header = (IndexPageHeader*)_pageBuffer;

            // 检查是否已存在该Key
            var existingOffset = FindEntryOffset(key);
            if (existingOffset >= 0)
            {
                // 如果 key 存在，不需要更新
                // 更新现有条目
                //UpdateExistingEntry(existingOffset, entry, processedKeyData);
                return true;
            }

            // 检查空间是否足够
            if (header->FreeSpace < requiredSpace)
            {
                return false; // 空间不足
            }

            // 添加新条目
            AddNewEntry(entry, processedKeyData);
            return true;
        }
    }

    /// <summary>
    /// 添加新条目
    /// </summary>
    private void AddNewEntry(IndexEntry entry, byte[] processedKeyData)
    {
        var header = (IndexPageHeader*)_pageBuffer;
        var writeOffset = header->UsedSpace;

        // 写入IndexEntry
        var entryPtr = (IndexEntry*)(_pageBuffer + writeOffset);
        *entryPtr = entry;
        writeOffset += IndexEntry.SIZE;

        // 写入ProcessedKey数据
        fixed (byte* keyPtr = processedKeyData)
        {
            Buffer.MemoryCopy(keyPtr, _pageBuffer + writeOffset, processedKeyData.Length, processedKeyData.Length);
        }
        writeOffset += processedKeyData.Length;

        // 更新头部信息
        header->EntryCount++;
        header->UsedSpace = writeOffset;
        header->FreeSpace = _pageSize - writeOffset;

        _isDirty = true;
        UpdateHeader();
    }

    /// <summary>
    /// 查找索引条目
    /// </summary>
    public long FindEntry(string key)
    {
        if (string.IsNullOrEmpty(key))
            return -1;

        lock (_lock)
        {
            var offset = FindEntryOffset(key);
            if (offset >= 0)
            {
                var entryPtr = (IndexEntry*)(_pageBuffer + offset);
                return entryPtr->IsValidEntry ? entryPtr->ValuePosition : -1;
            }
            return -1;
        }
    }

    /// <summary>
    /// 删除索引条目（标记为删除）
    /// </summary>
    public bool RemoveEntry(string key)
    {
        if (string.IsNullOrEmpty(key))
            return false;

        lock (_lock)
        {
            var offset = FindEntryOffset(key);
            if (offset >= 0)
            {
                var entryPtr = (IndexEntry*)(_pageBuffer + offset);
                var updatedEntry = *entryPtr;
                updatedEntry.IsDeleted = 1;
                *entryPtr = updatedEntry;

                _isDirty = true;
                UpdateHeader();

                return true;
            }
            return false;
        }
    }

    /// <summary>
    /// 查找条目在页面中的偏移位置
    /// </summary>
    private int FindEntryOffset(string key)
    {
        var header = (IndexPageHeader*)_pageBuffer;
        var currentOffset = IndexPageHeader.SIZE;

        for (int i = 0; i < header->EntryCount; i++)
        {
            // 读取IndexEntry
            var entryPtr = (IndexEntry*)(_pageBuffer + currentOffset);
            var entry = *entryPtr;
            var entryOffset = currentOffset;
            currentOffset += IndexEntry.SIZE;

            // 从IndexEntry中获取Key长度
            var keyLength = entry.KeyLength;

            // 读取ProcessedKey数据
            var processedKeyData = new byte[keyLength];
            fixed (byte* keyPtr = processedKeyData)
            {
                Buffer.MemoryCopy(_pageBuffer + currentOffset, keyPtr, keyLength, keyLength);
            }
            currentOffset += keyLength;

            var originalKey = UnprocessKey(processedKeyData);
            if (originalKey == key)
            {
                return entryOffset; // 返回IndexEntry的偏移位置
            }
        }

        return -1;
    }

    /// <summary>
    /// 反处理Key数据 - 解密和解压缩
    /// </summary>
    private string UnprocessKey(byte[] processedKeyData)
    {
        var keyBytes = _dataProcessor.UnprocessData(processedKeyData);
        return Encoding.UTF8.GetString(keyBytes);
    }

    /// <summary>
    /// 根据key查找索引条目（不考虑加密内容差异）
    /// </summary>
    public IndexEntry? FindEntryByKey(string key)
    {
        if (string.IsNullOrEmpty(key))
            return null;

        lock (_lock)
        {
            var offset = FindEntryOffset(key);
            if (offset >= 0)
            {
                var entryPtr = (IndexEntry*)(_pageBuffer + offset);
                var entry = *entryPtr;
                return entry.IsValidEntry ? entry : null;
            }
            return null;
        }
    }

    ///// <summary>
    ///// 更新现有条目
    ///// </summary>
    //private void UpdateExistingEntry(int entryOffset, IndexEntry newEntry, byte[] processedKeyData)
    //{
    //    //// 由于key数据可能长度发生变化，需要重新布局
    //    //// 简单的实现：先删除旧条目，再添加新条目
    //    //var entryPtr = (IndexEntry*)(_pageBuffer + entryOffset);
    //    //var oldEntry = *entryPtr;
    //    //oldEntry.IsDeleted = 1;
    //    //*entryPtr = oldEntry;

    //    //// 添加新条目（会被放到页面末尾）
    //    //AddNewEntry(newEntry, processedKeyData);

    //    //var entryPtr = (IndexEntry*)(_pageBuffer + entryOffset);
    //    //*entryPtr = newEntry;

    //    //_isDirty = true;
    //    //UpdateHeader();
    //}

    /// <summary>
    /// 更新页面头部
    /// </summary>
    private void UpdateHeader()
    {
        var header = (IndexPageHeader*)_pageBuffer;
        header->LastUpdateTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    /// <summary>
    /// 获取所有有效条目
    /// </summary>
    public IEnumerable<KeyValuePair<string, IndexEntry>> GetAllEntries()
    {
        lock (_lock)
        {
            var entries = new List<KeyValuePair<string, IndexEntry>>();
            var header = (IndexPageHeader*)_pageBuffer;
            var currentOffset = IndexPageHeader.SIZE;

            for (int i = 0; i < header->EntryCount; i++)
            {
                try
                {
                    // 读取IndexEntry
                    var entry = *(IndexEntry*)(_pageBuffer + currentOffset);
                    currentOffset += IndexEntry.SIZE;

                    // 读取ProcessedKey数据长度
                    var keyDataLength = entry.KeyLength;

                    // 读取ProcessedKey数据
                    var processedKeyData = new byte[keyDataLength];
                    fixed (byte* keyPtr = processedKeyData)
                    {
                        Buffer.MemoryCopy(_pageBuffer + currentOffset, keyPtr, keyDataLength, keyDataLength);
                    }
                    currentOffset += keyDataLength;

                    // 只返回有效条目
                    if (entry.IsValidEntry)
                    {
                        var originalKey = UnprocessKey(processedKeyData);
                        entries.Add(new KeyValuePair<string, IndexEntry>(originalKey, entry));
                    }
                }
                catch (Exception ex)
                {
                    // 处理异常，可能是数据损坏或格式错误
                    Console.WriteLine($"Error reading entry at offset {currentOffset}: {ex.Message}");
                }
            }

            return entries;
        }
    }

    /// <summary>
    /// 获取统计信息
    /// </summary>
    public (int TotalEntries, int ActiveEntries, int DeletedEntries, int UsedSpace, int FreeSpace) GetStats()
    {
        lock (_lock)
        {
            var header = (IndexPageHeader*)_pageBuffer;
            var activeEntries = 0;
            var deletedEntries = 0;
            var currentOffset = IndexPageHeader.SIZE;

            for (int i = 0; i < header->EntryCount; i++)
            {
                // 读取IndexEntry
                var entry = *(IndexEntry*)(_pageBuffer + currentOffset);
                currentOffset += IndexEntry.SIZE;

                // 读取ProcessedKey数据长度并跳过
                var keyDataLength = entry.KeyLength;
                currentOffset += keyDataLength;

                if (entry.IsValidEntry)
                    activeEntries++;
                else
                    deletedEntries++;
            }

            return (header->EntryCount, activeEntries, deletedEntries, header->UsedSpace, header->FreeSpace);
        }
    }

    /// <summary>
    /// 获取原始数据
    /// </summary>
    public byte[] GetRawData()
    {
        lock (_lock)
        {
            var data = new byte[_pageSize];
            fixed (byte* dataPtr = data)
            {
                Buffer.MemoryCopy(_pageBuffer, dataPtr, _pageSize, _pageSize);
            }
            return data;
        }
    }

    /// <summary>
    /// 检查页面是否需要压缩（删除已标记删除的条目）
    /// </summary>
    public bool NeedsCompaction()
    {
        var (total, active, deleted, _, _) = GetStats();
        return deleted > 0 && deleted >= total * 0.3; // 超过30%的条目被删除
    }

    /// <summary>
    /// 压缩页面，移除已删除的条目
    /// </summary>
    public void CompactPage()
    {
        lock (_lock)
        {
            if (!NeedsCompaction())
                return;

            // 创建临时缓冲区
            var tempBuffer = (byte*)Marshal.AllocHGlobal(_pageSize);
            try
            {
                // 初始化临时页面
                new Span<byte>(tempBuffer, _pageSize).Clear();
                var tempHeader = (IndexPageHeader*)tempBuffer;
                tempHeader->Magic = IndexPageHeader.MAGIC_NUMBER;
                tempHeader->EntryCount = 0;
                tempHeader->UsedSpace = IndexPageHeader.SIZE;
                tempHeader->FreeSpace = _pageSize - IndexPageHeader.SIZE;

                var writeOffset = IndexPageHeader.SIZE;
                var currentOffset = IndexPageHeader.SIZE;
                var header = (IndexPageHeader*)_pageBuffer;

                // 复制有效条目
                for (int i = 0; i < header->EntryCount; i++)
                {
                    // 读取IndexEntry
                    var entry = *(IndexEntry*)(_pageBuffer + currentOffset);
                    var entryStartOffset = currentOffset;
                    currentOffset += IndexEntry.SIZE;

                    // 读取ProcessedKey数据长度
                    var keyDataLength = entry.KeyLength;

                    if (entry.IsValidEntry)
                    {
                        // 复制IndexEntry + KeyDataLength + ProcessedKeyData到临时缓冲区
                        var entrySize = IndexEntry.SIZE + keyDataLength;
                        Buffer.MemoryCopy(_pageBuffer + entryStartOffset, tempBuffer + writeOffset, entrySize, entrySize);

                        writeOffset += entrySize;
                        tempHeader->EntryCount++;
                    }

                    currentOffset += keyDataLength;
                }

                // 更新临时头部
                tempHeader->UsedSpace = writeOffset;
                tempHeader->FreeSpace = _pageSize - writeOffset;
                tempHeader->MaxEntries = CalculateMaxEntries();
                tempHeader->LastUpdateTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                // 复制回原缓冲区
                Buffer.MemoryCopy(tempBuffer, _pageBuffer, _pageSize, _pageSize);
                _isDirty = true;
            }
            finally
            {
                Marshal.FreeHGlobal((IntPtr)tempBuffer);
            }
        }
    }

    /// <summary>
    /// 检查页面是否包含指定的key
    /// </summary>
    public bool ContainsKey(string key)
    {
        if (string.IsNullOrEmpty(key))
            return false;

        lock (_lock)
        {
            return FindEntryOffset(key) >= 0;
        }
    }

    /// <summary>
    /// 确认条目已成功写入数据，更新实际位置
    /// </summary>
    public void UpdateEntryConfirmed(string key, IndexEntry confirmedEntry)
    {
        lock (_lock)
        {
            var offset = FindEntryOffset(key);
            if (offset >= 0)
            {
                var entryPtr = (IndexEntry*)(_pageBuffer + offset);
                *entryPtr = confirmedEntry; // 更新为包含实际数据位置的条目

                _isDirty = true;
                UpdateHeader();
            }
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            if (_pageBuffer != null)
            {
                Marshal.FreeHGlobal((IntPtr)_pageBuffer);
            }
            _disposed = true;
        }
    }
}