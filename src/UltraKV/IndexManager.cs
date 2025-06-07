using System.Collections.Concurrent;
using System.Text;

namespace UltraKV;

/// <summary>
/// 索引管理器 - 管理最多32个索引页
/// </summary>
public unsafe class IndexManager : IDisposable
{
    private readonly FileStream _file;
    private readonly DatabaseHeader _databaseHeader;
    private readonly DataProcessor _dataProcessor;
    private readonly object _lock = new object();

    private IndexHeader _indexHeader;
    private readonly IndexBlock[] _indexBlocks;
    private readonly ConcurrentDictionary<uint, IndexPageInfo> _indexPages;
    private bool _disposed;
    private bool _isDirty;

    /// <summary>
    /// 索引头信息位置
    /// </summary>
    public const int INDEX_HEADER_OFFSET = DatabaseHeader.SIZE + FreeSpaceHeader.SIZE;

    /// <summary>
    /// 索引块信息起始位置
    /// </summary>
    public const long INDEX_BLOCKS_OFFSET = INDEX_HEADER_OFFSET + IndexHeader.SIZE;

    /// <summary>
    /// 第一个索引数据区域起始位置
    /// </summary>
    public readonly long _firstIndexDataStartPosition;

    public IndexManager(FileStream file, DatabaseHeader databaseHeader, DataProcessor dataProcessor, long firstIndexDataStartPosition)
    {
        _firstIndexDataStartPosition = firstIndexDataStartPosition;
        _file = file;
        _databaseHeader = databaseHeader;
        _dataProcessor = dataProcessor;
        _indexBlocks = new IndexBlock[IndexHeader.MAX_INDEX_PAGES];
        _indexPages = new ConcurrentDictionary<uint, IndexPageInfo>();

        LoadIndexHeader();
        LoadIndexBlocks();
        LoadIndexPages();
    }

    /// <summary>
    /// 加载索引头信息
    /// </summary>
    private void LoadIndexHeader()
    {
        if (_file.Length < INDEX_HEADER_OFFSET + IndexHeader.SIZE)
        {
            // 创建新的索引头
            _indexHeader = IndexHeader.Create();
            SaveIndexHeader();
            return;
        }

        _file.Seek(INDEX_HEADER_OFFSET, SeekOrigin.Begin);
        var buffer = new byte[IndexHeader.SIZE];
        if (_file.Read(buffer, 0, IndexHeader.SIZE) == IndexHeader.SIZE)
        {
            fixed (byte* ptr = buffer)
            {
                _indexHeader = *(IndexHeader*)ptr;
            }

            if (!_indexHeader.IsValid)
            {
                // 头信息无效，重新创建
                _indexHeader = IndexHeader.Create();
                SaveIndexHeader();
            }
        }
    }

    /// <summary>
    /// 保存索引头信息
    /// </summary>
    private void SaveIndexHeader()
    {
        lock (_lock)
        {
            EnsureFileSize(INDEX_HEADER_OFFSET + IndexHeader.SIZE);

            _file.Seek(INDEX_HEADER_OFFSET, SeekOrigin.Begin);
            var buffer = new byte[IndexHeader.SIZE];
            fixed (byte* ptr = buffer)
            {
                *(IndexHeader*)ptr = _indexHeader;
            }
            _file.Write(buffer, 0, IndexHeader.SIZE);
            _file.Flush();
        }
    }

    /// <summary>
    /// 加载索引块信息
    /// </summary>
    private void LoadIndexBlocks()
    {
        if (_file.Length < INDEX_BLOCKS_OFFSET + (IndexHeader.MAX_INDEX_PAGES * IndexBlock.SIZE))
            return;

        _file.Seek(INDEX_BLOCKS_OFFSET, SeekOrigin.Begin);
        var buffer = new byte[IndexHeader.MAX_INDEX_PAGES * IndexBlock.SIZE];
        if (_file.Read(buffer, 0, buffer.Length) == buffer.Length)
        {
            fixed (byte* ptr = buffer)
            {
                var blockPtr = (IndexBlock*)ptr;
                for (int i = 0; i < IndexHeader.MAX_INDEX_PAGES; i++)
                {
                    _indexBlocks[i] = blockPtr[i];
                }
            }
        }
    }

    /// <summary>
    /// 保存索引块信息
    /// </summary>
    private void SaveIndexBlocks()
    {
        lock (_lock)
        {
            EnsureFileSize(INDEX_BLOCKS_OFFSET + (IndexHeader.MAX_INDEX_PAGES * IndexBlock.SIZE));

            _file.Seek(INDEX_BLOCKS_OFFSET, SeekOrigin.Begin);
            var buffer = new byte[IndexHeader.MAX_INDEX_PAGES * IndexBlock.SIZE];
            fixed (byte* ptr = buffer)
            {
                var blockPtr = (IndexBlock*)ptr;
                for (int i = 0; i < IndexHeader.MAX_INDEX_PAGES; i++)
                {
                    blockPtr[i] = _indexBlocks[i];
                }
            }
            _file.Write(buffer, 0, buffer.Length);
            _file.Flush();
        }
    }

    /// <summary>
    /// 加载索引页
    /// </summary>
    private void LoadIndexPages()
    {
        for (int i = 0; i < _indexHeader.IndexPageCount; i++)
        {
            var block = _indexBlocks[i];
            if (block.IsValid)
            {
                try
                {
                    var pageInfo = LoadIndexPage(block);
                    _indexPages.TryAdd((uint)i, pageInfo);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Failed to load index page {i}: {ex.Message}");
                }
            }
        }
    }

    /// <summary>
    /// 从文件加载索引页
    /// </summary>
    private IndexPageInfo LoadIndexPage(IndexBlock block)
    {
        _file.Seek(block.Position, SeekOrigin.Begin);
        var buffer = new byte[block.Size];
        _file.Read(buffer, 0, (int)block.Size);

        // 如果启用了压缩/加密，需要解压缩/解密
        if (_databaseHeader.CompressionType != CompressionType.None ||
            _databaseHeader.EncryptionType != EncryptionType.None)
        {
            buffer = _dataProcessor.UnprocessData(buffer);
        }

        return new IndexPageInfo(buffer);
    }

    /// <summary>
    /// 添加或更新索引条目 - 查找有空闲位置的索引页
    /// </summary>
    /// <param name="key">键</param>
    /// <returns>返回预留的索引条目信息</returns>
    public IndexReservation? AddOrUpdateIndex(string key)
    {
        if (string.IsNullOrEmpty(key))
            return null;

        var keyBytes = Encoding.UTF8.GetBytes(key);

        lock (_lock)
        {
            try
            {
                // 预分配数据位置
                // 不确定当前 key 应该存储到哪个位置
                // -1 表示未分配
                var valuePosition = -1;

                // 2. 创建索引条目，只记录位置和长度
                var entry = new IndexEntry(valuePosition, keyBytes.Length);

                // 3. 查找有空闲空间的索引页
                var pageIndex = FindAvailableIndexPage(key, entry);
                if (pageIndex == null)
                {
                    // 没有可用页面，创建新的索引页
                    pageIndex = CreateNewIndexPage();
                    if (pageIndex == null)
                    {
                        return null; // 创建失败
                    }
                }

                // 4. 在找到的索引页中写入索引信息和key值
                if (_indexPages.TryGetValue(pageIndex.Value, out var pageInfo))
                {
                    if (pageInfo.AddOrUpdateEntry(key, entry))
                    {
                        _isDirty = true;
                        UpdateIndexStats();

                        // 返回预留信息
                        return new IndexReservation
                        {
                            Key = key,
                            DataPosition = valuePosition,
                            IndexEntry = entry,
                            PageIndex = pageIndex.Value,
                            ReservationTime = DateTime.UtcNow
                        };
                    }
                }

                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error adding index for key '{key}': {ex.Message}");
                return null;
            }
        }
    }

    /// <summary>
    /// 查找有足够空闲空间的索引页
    /// </summary>
    /// <param name="key">键</param>
    /// <param name="entry">索引条目</param>
    /// <returns>可用的页面索引，如果没有则返回null</returns>
    private uint? FindAvailableIndexPage(string key, IndexEntry entry)
    {
        var keyBytes = Encoding.UTF8.GetBytes(key);
        var requiredSpace = IndexEntry.SIZE + keyBytes.Length; // Entry + Key + KeyLength

        // 1. 首先检查是否已存在该key，如果存在则直接更新
        for (uint i = 0; i < _indexHeader.IndexPageCount; i++)
        {
            if (_indexPages.TryGetValue(i, out var pageInfo))
            {
                if (pageInfo.ContainsKey(key))
                {
                    return i; // 找到已存在的key，可以直接更新
                }
            }
        }

        // 2. 查找有足够空闲空间的页面
        for (uint i = 0; i < _indexHeader.IndexPageCount; i++)
        {
            if (_indexPages.TryGetValue(i, out var pageInfo))
            {
                var (_, _, _, _, freeSpace) = pageInfo.GetStats();
                if (freeSpace >= requiredSpace)
                {
                    return i; // 找到有足够空间的页面
                }
            }
        }

        // 3. 尝试压缩现有页面释放空间
        for (uint i = 0; i < _indexHeader.IndexPageCount; i++)
        {
            if (_indexPages.TryGetValue(i, out var pageInfo))
            {
                if (pageInfo.NeedsCompaction())
                {
                    pageInfo.CompactPage();

                    var (_, _, _, _, freeSpace) = pageInfo.GetStats();
                    if (freeSpace >= requiredSpace)
                    {
                        return i; // 压缩后有足够空间
                    }
                }
            }
        }

        return null; // 没有找到可用页面
    }

    /// <summary>
    /// 创建新的索引页
    /// </summary>
    /// <returns>新创建的页面索引，如果创建失败返回null</returns>
    private uint? CreateNewIndexPage()
    {
        // 检查是否已达到最大页面数
        if (_indexHeader.IndexPageCount >= IndexHeader.MAX_INDEX_PAGES)
        {
            throw new InvalidOperationException($"Cannot create new index page: maximum index pages {IndexHeader.MAX_INDEX_PAGES}.");
        }

        // 计算新索引页大小
        var newPageSize = _indexHeader.IndexPageCount > 0
            ? Math.Min(_indexHeader.TotalIndexSize * 2, int.MaxValue) // 当前总大小的2倍
            : _databaseHeader.DefaultIndexPageSizeKB * 1024L; // 默认大小

        // 确保页面大小合理
        newPageSize = Math.Max(newPageSize, 1024);           // 最小1KB
        newPageSize = Math.Min(newPageSize, int.MaxValue); // 最大16MB

        try
        {
            // 在文件末尾分配空间，如果是第一个索引页，则从指定位置开始
            if (_indexHeader.IndexPageCount == 0)
            {
                _file.Seek(_firstIndexDataStartPosition, SeekOrigin.Begin);
            }
            else
            {
                _file.Seek(0, SeekOrigin.End);
            }
            var position = _file.Position;
            EnsureFileSize(position + newPageSize);

            // 创建索引块信息
            var block = new IndexBlock(position, newPageSize);

            // 创建索引页信息
            var pageInfo = new IndexPageInfo((int)newPageSize);

            // 将空页面写入文件
            SaveIndexPageToFile(pageInfo, block);

            // 更新管理信息
            var newPageIndex = (uint)_indexHeader.IndexPageCount;
            _indexBlocks[_indexHeader.IndexPageCount] = block;
            _indexPages.TryAdd(newPageIndex, pageInfo);

            // 更新头信息
            _indexHeader.IndexPageCount++;
            _indexHeader.TotalIndexSize += newPageSize;
            if (_indexHeader.IndexRegionStartPos == 0)
            {
                _indexHeader.IndexRegionStartPos = position;
            }

            _isDirty = true;

            Console.WriteLine($"Created new index page {newPageIndex}: Size={newPageSize / 1024}KB at position {position}");

            return newPageIndex;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to create new index page: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// 查找索引条目
    /// </summary>
    /// <param name="key">键</param>
    /// <returns>数据位置，如果未找到返回-1</returns>
    public long FindIndex(string key)
    {
        if (string.IsNullOrEmpty(key))
            return -1;

        lock (_lock)
        {
            // 遍历所有索引页查找key
            for (uint i = 0; i < _indexHeader.IndexPageCount; i++)
            {
                if (_indexPages.TryGetValue(i, out var pageInfo))
                {
                    var position = pageInfo.FindEntry(key);
                    if (position >= 0)
                    {
                        return position;
                    }
                }
            }

            return -1; // 未找到
        }
    }

    /// <summary>
    /// 删除索引条目
    /// </summary>
    /// <param name="key">键</param>
    /// <returns>是否成功删除</returns>
    public bool RemoveIndex(string key)
    {
        if (string.IsNullOrEmpty(key))
            return false;

        lock (_lock)
        {
            // 遍历所有索引页查找并删除key
            for (uint i = 0; i < _indexHeader.IndexPageCount; i++)
            {
                if (_indexPages.TryGetValue(i, out var pageInfo))
                {
                    if (pageInfo.RemoveEntry(key))
                    {
                        _isDirty = true;
                        UpdateIndexStats();
                        return true;
                    }
                }
            }

            return false; // 未找到要删除的key
        }
    }

    /// <summary>
    /// 更新索引统计信息
    /// </summary>
    private void UpdateIndexStats()
    {
        uint totalEntries = 0;
        uint activeEntries = 0;
        uint deletedEntries = 0;

        foreach (var pageInfo in _indexPages.Values)
        {
            var (total, active, deleted, _, _) = pageInfo.GetStats();
            totalEntries += (uint)total;
            activeEntries += (uint)active;
            deletedEntries += (uint)deleted;
        }

        _indexHeader.UpdateStats(totalEntries, activeEntries, deletedEntries);
    }

    /// <summary>
    /// 合并索引页 - 将所有索引合并到第一个页面
    /// </summary>
    public void ConsolidateIndexPages()
    {
        lock (_lock)
        {
            if (_indexHeader.IndexPageCount <= 1)
                return;

            Console.WriteLine("Starting index page consolidation...");

            // 收集所有索引条目
            var allEntries = new Dictionary<string, IndexEntry>();

            foreach (var pageInfo in _indexPages.Values)
            {
                foreach (var kvp in pageInfo.GetAllEntries())
                {
                    allEntries[kvp.Key] = kvp.Value;
                }
            }

            // 清空现有索引页
            _indexPages.Clear();
            Array.Clear(_indexBlocks, 0, _indexBlocks.Length);

            // 创建新的第一个索引页，大小为默认大小
            var newPageSize = _databaseHeader.DefaultIndexPageSizeKB * 1024L;
            var position = _firstIndexDataStartPosition;

            // 确保有足够空间
            EnsureFileSize(position + newPageSize);

            var newPageInfo = new IndexPageInfo((int)newPageSize);

            // 添加所有条目到新页面
            foreach (var kvp in allEntries)
            {
                newPageInfo.AddOrUpdateEntry(kvp.Key, kvp.Value);
            }

            // 保存新页面
            var block = new IndexBlock(position, newPageSize);
            SaveIndexPageToFile(newPageInfo, block);

            // 更新管理信息
            _indexBlocks[0] = block;
            _indexPages.TryAdd(0, newPageInfo);

            // 更新头信息
            _indexHeader.IndexPageCount = 1;
            _indexHeader.TotalIndexSize = newPageSize;
            _indexHeader.IndexRegionStartPos = position;
            _indexHeader.UpdateRebuildTime();

            // 截断文件，移除多余的索引页
            _file.SetLength(position + newPageSize);

            _isDirty = true;

            Console.WriteLine($"Index consolidation completed: {allEntries.Count} entries in 1 page");
        }
    }

    /// <summary>
    /// 保存索引页到文件
    /// </summary>
    private void SaveIndexPageToFile(IndexPageInfo pageInfo, IndexBlock block)
    {
        var data = pageInfo.GetRawData();

        // 如果启用了压缩/加密，需要压缩/加密数据
        if (_databaseHeader.CompressionType != CompressionType.None ||
            _databaseHeader.EncryptionType != EncryptionType.None)
        {
            data = _dataProcessor.ProcessData(data);
        }

        _file.Seek(block.Position, SeekOrigin.Begin);
        _file.Write(data);
    }

    /// <summary>
    /// 保存所有更改
    /// </summary>
    public void SaveChanges()
    {
        if (!_isDirty)
            return;

        lock (_lock)
        {
            // 保存所有索引页
            for (int i = 0; i < _indexHeader.IndexPageCount; i++)
            {
                if (_indexPages.TryGetValue((uint)i, out var pageInfo) && _indexBlocks[i].IsValid)
                {
                    SaveIndexPageToFile(pageInfo, _indexBlocks[i]);
                }
            }

            // 保存索引块信息
            SaveIndexBlocks();

            // 保存索引头信息
            SaveIndexHeader();

            _isDirty = false;
        }
    }

    /// <summary>
    /// 计算Key的哈希值
    /// </summary>
    private uint CalculateKeyHash(string key)
    {
        var bytes = Encoding.UTF8.GetBytes(key);
        uint hash = 2166136261u; // FNV-1a初始值

        foreach (byte b in bytes)
        {
            hash ^= b;
            hash *= 16777619u; // FNV-1a素数
        }

        return hash;
    }

    /// <summary>
    /// 根据哈希值获取页面索引
    /// </summary>
    private uint GetPageIndexForHash(uint hash)
    {
        return _indexHeader.IndexPageCount > 0 ? hash % (uint)_indexHeader.IndexPageCount : 0;
    }

    /// <summary>
    /// 确保文件有足够大小
    /// </summary>
    private void EnsureFileSize(long requiredSize)
    {
        if (_file.Length < requiredSize)
        {
            _file.SetLength(requiredSize);
        }
    }

    /// <summary>
    /// 获取索引统计信息
    /// </summary>
    public IndexStats GetStats()
    {
        lock (_lock)
        {
            uint totalEntries = 0;
            uint activeEntries = 0;
            uint deletedEntries = 0;

            foreach (var pageInfo in _indexPages.Values)
            {
                var stats = pageInfo.GetStats();
                totalEntries += (uint)stats.TotalEntries;
                activeEntries += (uint)stats.ActiveEntries;
                deletedEntries += (uint)stats.DeletedEntries;
            }

            return new IndexStats
            {
                IndexPageCount = _indexHeader.IndexPageCount,
                MaxIndexPages = _indexHeader.MaxIndexPages,
                TotalIndexSize = _indexHeader.TotalIndexSize,
                TotalEntries = totalEntries,
                ActiveEntries = activeEntries,
                DeletedEntries = deletedEntries,
                IndexUtilization = _indexHeader.IndexUtilization,
                AveragePageSize = _indexHeader.IndexPageCount > 0 ? _indexHeader.TotalIndexSize / _indexHeader.IndexPageCount : 0
            };
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            SaveChanges();

            foreach (var pageInfo in _indexPages.Values)
            {
                pageInfo.Dispose();
            }

            _indexPages.Clear();
            _disposed = true;
        }
    }
}

/// <summary>
/// 索引统计信息
/// </summary>
public struct IndexStats
{
    public byte IndexPageCount;
    public byte MaxIndexPages;
    public long TotalIndexSize;
    public uint TotalEntries;
    public uint ActiveEntries;
    public uint DeletedEntries;
    public double IndexUtilization;
    public long AveragePageSize;

    public readonly double PageUtilization => IndexPageCount > 0 ? (double)IndexPageCount / MaxIndexPages : 0.0;
    public readonly bool ShouldConsolidate => IndexPageCount > 1 && IndexUtilization < 0.7;

    public override readonly string ToString()
    {
        return $"Index: {IndexPageCount}/{MaxIndexPages} pages, " +
               $"{ActiveEntries}/{TotalEntries} entries " +
               $"(Util: {IndexUtilization:P1}), " +
               $"Size: {TotalIndexSize / 1024.0:F1}KB";
    }
}