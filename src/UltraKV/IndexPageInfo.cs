using System.Runtime.InteropServices;
using System.Text;

namespace UltraKV;

/// <summary>
/// 索引页信息 - 管理单个索引页内的多个索引条目
/// </summary>
public unsafe class IndexPageInfo : IDisposable
{
    private readonly byte* _pageBuffer;
    private readonly int _pageSize;
    private readonly object _lock = new object();
    private bool _disposed;
    private bool _isDirty;

    // 索引页头部结构
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    private struct IndexPageHeader
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
    }

    public IndexPageInfo(int pageSize)
    {
        _pageSize = pageSize;
        _pageBuffer = (byte*)Marshal.AllocHGlobal(pageSize);

        // 初始化页面头部
        InitializePage();
    }

    public IndexPageInfo(byte[] data)
    {
        _pageSize = data.Length;
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

    private int CalculateMaxEntries()
    {
        // 计算页面能容纳的最大条目数
        // 每个条目包括：IndexEntry(16字节) + Key数据
        // 这里预估平均Key长度为32字节
        var avgEntrySize = IndexEntry.SIZE + 32; // IndexEntry + 平均Key长度
        var availableSpace = _pageSize - IndexPageHeader.SIZE;
        return availableSpace / avgEntrySize;
    }

    /// <summary>
    /// 添加或更新索引条目
    /// </summary>
    public bool AddOrUpdateEntry(string key, IndexEntry entry)
    {
        if (string.IsNullOrEmpty(key))
            return false;

        lock (_lock)
        {
            var keyBytes = Encoding.UTF8.GetBytes(key);

            // 更新IndexEntry中的KeyLength字段
            entry.KeyLength = keyBytes.Length;

            var requiredSpace = IndexEntry.SIZE + keyBytes.Length; // 只需要IndexEntry + Key数据

            var header = (IndexPageHeader*)_pageBuffer;

            // 检查是否已存在该Key
            var existingOffset = FindEntryOffset(key);
            if (existingOffset >= 0)
            {
                // 更新现有条目
                UpdateExistingEntry(existingOffset, entry);
                return true;
            }

            // 检查空间是否足够
            if (header->FreeSpace < requiredSpace)
            {
                return false; // 空间不足
            }

            // 添加新条目
            AddNewEntry(keyBytes, entry);
            return true;
        }
    }

    /// <summary>
    /// 添加新条目 - 只写入IndexEntry和Key数据
    /// </summary>
    private void AddNewEntry(byte[] keyBytes, IndexEntry entry)
    {
        var header = (IndexPageHeader*)_pageBuffer;
        var writeOffset = header->UsedSpace;

        // 写入IndexEntry（包含KeyLength信息）
        var entryPtr = (IndexEntry*)(_pageBuffer + writeOffset);
        *entryPtr = entry;
        writeOffset += IndexEntry.SIZE;

        // 写入Key数据
        fixed (byte* keyPtr = keyBytes)
        {
            Buffer.MemoryCopy(keyPtr, _pageBuffer + writeOffset, keyBytes.Length, keyBytes.Length);
        }
        writeOffset += keyBytes.Length;

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
                return entryPtr->IsValidEntry ? entryPtr->Position : -1;
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
    /// 查找条目在页面中的偏移位置 - 使用IndexEntry中的KeyLength
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

            // 读取Key数据
            var keyData = new string((sbyte*)(_pageBuffer + currentOffset), 0, keyLength, Encoding.UTF8);
            currentOffset += keyLength;

            // 比较Key
            if (keyData == key)
            {
                return entryOffset; // 返回IndexEntry的偏移位置
            }
        }

        return -1;
    }

    /// <summary>
    /// 更新现有条目
    /// </summary>
    private void UpdateExistingEntry(int entryOffset, IndexEntry newEntry)
    {
        var entryPtr = (IndexEntry*)(_pageBuffer + entryOffset);
        *entryPtr = newEntry;

        _isDirty = true;
        UpdateHeader();
    }

    /// <summary>
    /// 更新页面头部
    /// </summary>
    private void UpdateHeader()
    {
        var header = (IndexPageHeader*)_pageBuffer;
        header->LastUpdateTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    /// <summary>
    /// 获取所有有效条目 - 使用IndexEntry中的KeyLength
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
                // 读取IndexEntry
                var entry = *(IndexEntry*)(_pageBuffer + currentOffset);
                currentOffset += IndexEntry.SIZE;

                // 从IndexEntry中获取Key长度
                var keyLength = entry.KeyLength;

                // 读取Key数据
                var keyData = new string((sbyte*)(_pageBuffer + currentOffset), 0, keyLength, Encoding.UTF8);
                currentOffset += keyLength;

                // 只返回有效条目
                if (entry.IsValidEntry)
                {
                    entries.Add(new KeyValuePair<string, IndexEntry>(keyData, entry));
                }
            }

            return entries;
        }
    }

    /// <summary>
    /// 获取统计信息 - 使用IndexEntry中的KeyLength
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

                // 从IndexEntry中获取Key长度
                var keyLength = entry.KeyLength;
                currentOffset += keyLength;

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
    /// 压缩页面，移除已删除的条目 - 使用IndexEntry中的KeyLength
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

                    // 从IndexEntry中获取Key长度
                    var keyLength = entry.KeyLength;

                    if (entry.IsValidEntry)
                    {
                        // 复制IndexEntry + Key数据到临时缓冲区
                        var entrySize = IndexEntry.SIZE + keyLength;
                        Buffer.MemoryCopy(_pageBuffer + entryStartOffset, tempBuffer + writeOffset, entrySize, entrySize);

                        writeOffset += entrySize;
                        tempHeader->EntryCount++;
                    }

                    currentOffset += keyLength;
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
    /// 检查页面是否包含指定的key - 使用IndexEntry中的KeyLength
    /// </summary>
    public bool ContainsKey(string key)
    {
        if (string.IsNullOrEmpty(key))
            return false;

        lock (_lock)
        {
            var header = (IndexPageHeader*)_pageBuffer;
            var currentOffset = IndexPageHeader.SIZE;

            for (int i = 0; i < header->EntryCount; i++)
            {
                // 读取IndexEntry
                var entry = *(IndexEntry*)(_pageBuffer + currentOffset);
                currentOffset += IndexEntry.SIZE;

                // 从IndexEntry中获取Key长度
                var keyLength = entry.KeyLength;

                // 读取Key数据并比较
                var keyData = new string((sbyte*)(_pageBuffer + currentOffset), 0, keyLength, Encoding.UTF8);
                currentOffset += keyLength;

                if (keyData == key)
                {
                    return true;
                }
            }

            return false;
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