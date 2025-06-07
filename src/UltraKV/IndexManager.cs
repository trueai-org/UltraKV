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

    /// <summary>
    /// 记录 key 和 keyEntry 的映射关系
    /// </summary>
    private readonly ConcurrentDictionary<string, IndexEntry> _indexEntrys;

    public IndexManager(FileStream file, DatabaseHeader databaseHeader, DataProcessor dataProcessor, long firstIndexDataStartPosition)
    {
        _firstIndexDataStartPosition = firstIndexDataStartPosition;
        _file = file;
        _databaseHeader = databaseHeader;
        _dataProcessor = dataProcessor;
        _indexBlocks = new IndexBlock[IndexHeader.MAX_INDEX_PAGES];
        _indexPages = new ConcurrentDictionary<uint, IndexPageInfo>();
        _indexEntrys = new ConcurrentDictionary<string, IndexEntry>();

        LoadIndexHeader();
        LoadIndexBlocks();
        LoadIndexPages();
    }

    /// <summary>
    /// 所有索引信息
    /// </summary>
    public ConcurrentDictionary<string, IndexEntry> IndexEntrys => _indexEntrys;

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
        if (_file.Length < INDEX_BLOCKS_OFFSET)
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
                var pageInfo = LoadIndexPage(block);
                _indexPages.TryAdd((uint)i, pageInfo);

                var kvs = pageInfo.GetAllEntries();
                foreach (var item in kvs)
                {
                    // 将key和条目信息添加到映射中
                    _indexEntrys.TryAdd(item.Key, item.Value);
                }
            }
        }
    }

    /// <summary>
    /// 从文件加载索引页
    /// </summary>
    private IndexPageInfo LoadIndexPage(IndexBlock block)
    {
        if (_file.Length < block.Position + block.Size)
        {
            throw new InvalidOperationException($"Index page at position {block.Position} with size {block.Size} is invalid.");
        }

        _file.Seek(block.Position, SeekOrigin.Begin);
        var buffer = new byte[block.Size];
        if (_file.Read(buffer, 0, buffer.Length) != buffer.Length)
        {
            throw new InvalidOperationException($"Failed to read index page at position {block.Position}.");
        }

        //// 从文件加载索引页头信息
        //fixed (byte* ptr = buffer)
        //{
        //    var header = *(IndexPageHeader*)ptr;
        //    if (!header.IsValid)
        //    {
        //        throw new InvalidOperationException($"Invalid index page header at position {block.Position}.");
        //    }
        //}

        return new IndexPageInfo(buffer, _dataProcessor);
    }

    /// <summary>
    /// 查找已存在的索引条目（基于原始key比较）
    /// </summary>
    /// <param name="key">要查找的key</param>
    /// <returns>如果找到返回条目信息，否则返回null</returns>
    private (IndexEntry Entry, uint PageIndex)? FindExistingIndexEntry(string key)
    {
        // 遍历所有索引页查找key
        for (uint i = 0; i < _indexHeader.IndexPageCount; i++)
        {
            if (_indexPages.TryGetValue(i, out var pageInfo))
            {
                var entry = pageInfo.FindEntryByKey(key);
                if (entry.HasValue)
                {
                    return (entry.Value, i);
                }
            }
        }

        return null;
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

        lock (_lock)
        {
            if (_indexEntrys.TryGetValue(key, out var v) && v.IsValidEntry)
            {
                // Key已存在，直接返回现有的索引信息
                return new IndexReservation(key, v.ValuePosition, v, v.PageIndex);
            }

            //// 1. 首先检查key是否已经存在（基于原始key，而非加密内容）
            //var existingEntry = FindExistingIndexEntry(key);
            //if (existingEntry.HasValue)
            //{
            //    // Key已存在，只需要返回现有的索引信息，不需要更新加密内容
            //    return new IndexReservation
            //    {
            //        Key = key,
            //        DataPosition = existingEntry.Value.Entry.Position, // 使用现有位置
            //        IndexEntry = existingEntry.Value.Entry,
            //        PageIndex = existingEntry.Value.PageIndex,
            //        ReservationTime = DateTime.UtcNow,
            //    };
            //}

            // 处理key（可能需要加密/压缩）
            var processedKeyData = ProcessKey(key);

            // 创建索引条目，值位置暂时设为-1（未分配）
            var entry = new IndexEntry(processedKeyData.Length);

            // 查找有空闲空间的索引页
            var pageIndex = FindAvailableIndexPage(key, entry, processedKeyData);
            if (pageIndex == null)
            {
                // 没有可用页面，创建新的索引页
                pageIndex = CreateNewIndexPage();
                if (pageIndex == null)
                {
                    return null; // 创建失败
                }
            }

            // 在找到的索引页中写入索引信息和key值
            if (_indexPages.TryGetValue(pageIndex.Value, out var pageInfo))
            {
                entry.PageIndex = pageIndex.Value;
                if (pageInfo.AddOrUpdateEntry(key, entry, processedKeyData))
                {
                    _isDirty = true;
                    UpdateIndexStats();

                    _indexEntrys.AddOrUpdate(key, entry, (k, v) => entry);

                    // 返回预留信息
                    return new IndexReservation(key, -1, entry, pageIndex.Value);
                }
            }

            return null;
        }
    }

    /// <summary>
    /// 确认索引条目 - 数据写入成功后更新实际的数据位置
    /// </summary>
    /// <param name="updatedEntry">索引信息</param>
    public void ConfirmIndex(string key, IndexEntry updatedEntry)
    {
        lock (_lock)
        {
            // 更新索引条目的实际数据位置
            if (_indexPages.TryGetValue(updatedEntry.PageIndex, out var pageInfo))
            {
                // 更新索引页中的条目
                updatedEntry.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                pageInfo.UpdateEntryConfirmed(key, updatedEntry);

                _isDirty = true;

                UpdateIndexStats();
            }
        }
    }

    /// <summary>
    /// 回滚索引条目 - 数据写入失败时调用
    /// </summary>
    /// <param name="reservation">预留信息</param>
    public void RollbackIndex(IndexReservation reservation)
    {
        lock (_lock)
        {
            // 移除预留的索引条目
            if (_indexPages.TryGetValue(reservation.PageIndex, out var pageInfo))
            {
                pageInfo.RemoveEntry(reservation.Key);
                _isDirty = true;
                UpdateIndexStats();
            }
        }
    }

    /// <summary>
    /// 处理Key数据 - 应用压缩和加密
    /// </summary>
    /// <param name="key">原始key</param>
    /// <returns>处理后的key数据</returns>
    private byte[] ProcessKey(string key)
    {
        var keyBytes = Encoding.UTF8.GetBytes(key);

        // 但如果配置了压缩，我们仍然应用压缩逻辑
        if (_databaseHeader.CompressionType != CompressionType.None ||
            _databaseHeader.EncryptionType != EncryptionType.None)
        {
            return _dataProcessor.ProcessData(keyBytes);
        }

        return keyBytes;
    }

    /// <summary>
    /// 反处理Key数据 - 解密和解压缩
    /// </summary>
    /// <param name="processedKeyData">处理后的key数据</param>
    /// <returns>原始key字符串</returns>
    private string UnprocessKey(byte[] processedKeyData)
    {
        byte[] keyBytes;

        if (_databaseHeader.CompressionType != CompressionType.None ||
            _databaseHeader.EncryptionType != EncryptionType.None)
        {
            keyBytes = _dataProcessor.UnprocessData(processedKeyData);
        }
        else
        {
            keyBytes = processedKeyData;
        }

        return Encoding.UTF8.GetString(keyBytes);
    }

    /// <summary>
    /// 查找有足够空闲空间的索引页
    /// </summary>
    /// <param name="key">键</param>
    /// <param name="entry">索引条目</param>
    /// <param name="processedKeyData">处理后的key数据</param>
    /// <returns>可用的页面索引，如果没有则返回null</returns>
    private byte? FindAvailableIndexPage(string key, IndexEntry entry, byte[] processedKeyData)
    {
        var requiredSpace = IndexEntry.SIZE + processedKeyData.Length; // Entry + ProcessedKey

        // 1. 首先检查是否已存在该key，如果存在则直接更新
        for (byte i = 0; i < _indexHeader.IndexPageCount; i++)
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
        for (byte i = 0; i < _indexHeader.IndexPageCount; i++)
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
        for (byte i = 0; i < _indexHeader.IndexPageCount; i++)
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
    private byte? CreateNewIndexPage()
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
        newPageSize = Math.Min(newPageSize, int.MaxValue); // 最大约2GB

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
            var pageInfo = new IndexPageInfo((int)newPageSize, _dataProcessor);

            // 将空页面写入文件
            SaveIndexPageToFile(pageInfo, block);

            // 更新管理信息
            var newPageIndex = _indexHeader.IndexPageCount;
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

        // 如果key已存在，直接返回其位置
        if (_indexEntrys.TryGetValue(key, out var v))
        {
            return v.IsValidEntryValue ? v.ValuePosition : -1;
        }

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
    /// 尝试获取索引条目位置
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public bool TryGetValue(string key, out long position)
    {
        position = FindIndex(key);

        return position >= 0;
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

            var newPageInfo = new IndexPageInfo((int)newPageSize, _dataProcessor);

            // 添加所有条目到新页面
            foreach (var kvp in allEntries)
            {
                var processedKeyData = ProcessKey(kvp.Key);
                newPageInfo.AddOrUpdateEntry(kvp.Key, kvp.Value, processedKeyData);
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
            int totalEntries = 0;
            int activeEntries = 0;
            int deletedEntries = 0;

            foreach (var pageInfo in _indexPages.Values)
            {
                var stats = pageInfo.GetStats();
                totalEntries += stats.TotalEntries;
                activeEntries += stats.ActiveEntries;
                deletedEntries += stats.DeletedEntries;
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

    /// <summary>
    /// 尝试从索引中移除条目
    /// </summary>
    /// <param name="key"></param>
    internal void TryRemove(string key, out long position)
    {
        position = -1;
        if (string.IsNullOrEmpty(key))
            return;

        lock (_lock)
        {
            // 尝试从映射中移除
            if (_indexEntrys.TryRemove(key, out var entry))
            {
                position = entry.ValuePosition;

                RemoveIndex(key);
            }
        }
    }

    /// <summary>
    /// 清除所有索引数据
    /// </summary>
    /// <exception cref="NotImplementedException"></exception>
    internal void Clear()
    {
        lock (_lock)
        {
            // 清空索引页和索引块
            _indexPages.Clear();

            Array.Clear(_indexBlocks, 0, _indexBlocks.Length);

            _indexHeader.IndexPageCount = 0;
            _indexHeader.TotalIndexSize = 0;
            _indexHeader.IndexRegionStartPos = 0;

            // 清空映射
            _indexEntrys.Clear();

            // 标记为脏数据
            _isDirty = true;

            // 保存更改
            SaveChanges();
        }
    }

    // ---------------



    /// <summary>
    /// 将当前 IndexManager 的所有索引合并到目标文件的第一个索引分区
    /// </summary>
    /// <param name="targetFile">目标文件流</param>
    /// <param name="targetFirstIndexDataPosition">目标文件第一个索引数据区域起始位置</param>
    /// <returns>合并操作是否成功</returns>
    public bool MergeIndexesToTargetFile(FileStream targetFile, long targetFirstIndexDataPosition)
    {
        if (targetFile == null)
            throw new ArgumentNullException(nameof(targetFile));

        lock (_lock)
        {
            try
            {
                Console.WriteLine("Starting simplified index merge operation...");

                // 1. 收集所有索引条目数据
                var allIndexData = new List<byte[]>();
                var allEntries = new List<KeyValuePair<string, IndexEntry>>();
                long totalDataSize = IndexPageHeader.SIZE;


                //// 1. 收集当前所有索引条目
                //var allIndexEntries = new Dictionary<string, IndexEntry>();

                //// 优先从内存映射获取
                //foreach (var kvp in _indexEntrys)
                //{
                //    if (kvp.Value.IsValidEntryValue)
                //    {
                //        allIndexEntries[kvp.Key] = kvp.Value;
                //    }
                //}

                //// 如果内存映射为空，从索引页中获取
                //if (allIndexEntries.Count == 0)
                //{
                //    foreach (var pageInfo in _indexPages.Values)
                //    {
                //        foreach (var kvp in pageInfo.GetAllEntries())
                //        {
                //            if (kvp.Value.IsValidEntryValue)
                //            {
                //                allIndexEntries[kvp.Key] = kvp.Value;
                //            }
                //        }
                //    }
                //}

                // 遍历所有索引页收集数据
                for (uint i = 0; i < _indexHeader.IndexPageCount; i++)
                {
                    if (_indexPages.TryGetValue(i, out var pageInfo))
                    {
                        foreach (var kvp in pageInfo.GetAllEntries())
                        {
                            if (kvp.Value.IsValidEntryValue)
                            {
                                // 获取处理后的key数据
                                var processedKeyData = ProcessKey(kvp.Key);
                                var entryDataSize = IndexEntry.SIZE + processedKeyData.Length;

                                totalDataSize += entryDataSize;

                                // 检查是否超过int.MaxValue
                                if (totalDataSize > int.MaxValue)
                                {
                                    throw new InvalidOperationException($"Total index size ({totalDataSize}) exceeds maximum allowed size ({int.MaxValue})");
                                }

                                allIndexData.Add(processedKeyData);
                                allEntries.Add(kvp);
                            }
                        }
                    }
                }

                if (allEntries.Count == 0)
                {
                    Console.WriteLine("No index entries to merge.");
                    return true;
                }

                Console.WriteLine($"Collected {allEntries.Count} entries, total size: {totalDataSize} bytes");

                // 2. 在目标文件创建合并后的索引页
                var mergedPageSize = (int)totalDataSize;
                var mergedPageData = CreateMergedIndexPageData(allEntries, allIndexData, mergedPageSize);

                // 3. 写入到目标文件
                targetFile.Seek(targetFirstIndexDataPosition, SeekOrigin.Begin);
                targetFile.Write(mergedPageData);
                targetFile.Flush();

                // 4. 更新目标文件的索引头信息
                UpdateTargetIndexHeader(targetFile, allEntries.Count, mergedPageSize, targetFirstIndexDataPosition);

                // 5. 更新目标文件的索引块信息
                UpdateTargetIndexBlocks(targetFile, targetFirstIndexDataPosition, mergedPageSize);

                Console.WriteLine($"Index merge completed: {allEntries.Count} entries merged into {mergedPageSize} bytes at position {targetFirstIndexDataPosition}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during index merge: {ex.Message}");
                return false;
            }
        }
    }

    /// <summary>
    /// 创建合并后的索引页数据
    /// </summary>
    /// <param name="allEntries">所有索引条目</param>
    /// <param name="allIndexData">所有处理后的key数据</param>
    /// <param name="pageSize">页面大小</param>
    /// <returns>合并后的页面数据</returns>
    private byte[] CreateMergedIndexPageData(List<KeyValuePair<string, IndexEntry>> allEntries,
                                           List<byte[]> allIndexData,
                                           int pageSize)
    {
        var pageData = new byte[pageSize];
        var currentOffset = 0;

        // 1. 写入IndexPageHeader
        fixed (byte* pagePtr = pageData)
        {
            var header = (IndexPageHeader*)pagePtr;
            header->Magic = 0x49445850; // "IDXP"
            header->EntryCount = allEntries.Count;
            header->MaxEntries = allEntries.Count;
            header->UsedSpace = pageSize;
            header->FreeSpace = 0;
            header->LastUpdateTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        }
        currentOffset = 32; // IndexPageHeader.SIZE

        // 2. 写入所有索引条目
        for (int i = 0; i < allEntries.Count; i++)
        {
            var entry = allEntries[i];
            var processedKeyData = allIndexData[i];

            // 更新条目信息
            var updatedEntry = entry.Value;
            updatedEntry.PageIndex = 0; // 合并到第一个页面
            updatedEntry.KeyLength = processedKeyData.Length;
            updatedEntry.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // 写入IndexEntry
            fixed (byte* pagePtr = pageData)
            {
                var entryPtr = (IndexEntry*)(pagePtr + currentOffset);
                *entryPtr = updatedEntry;
            }
            currentOffset += IndexEntry.SIZE;

            // 写入处理后的key数据
            Array.Copy(processedKeyData, 0, pageData, currentOffset, processedKeyData.Length);
            currentOffset += processedKeyData.Length;
        }

        return pageData;
    }

    /// <summary>
    /// 更新目标文件的索引头信息
    /// </summary>
    /// <param name="targetFile">目标文件</param>
    /// <param name="entryCount">条目数量</param>
    /// <param name="pageSize">页面大小</param>
    /// <param name="regionStartPos">索引区域起始位置</param>
    private void UpdateTargetIndexHeader(FileStream targetFile, int entryCount, int pageSize, long regionStartPos)
    {
        var headerOffset = DatabaseHeader.SIZE + FreeSpaceHeader.SIZE;

        // 创建新的索引头
        var indexHeader = IndexHeader.Create();
        indexHeader.IndexPageCount = 1;
        indexHeader.TotalIndexSize = pageSize;
        indexHeader.IndexRegionStartPos = regionStartPos;
        indexHeader.UpdateStats((uint)entryCount, (uint)entryCount, 0);
        indexHeader.UpdateRebuildTime();

        // 写入索引头
        var headerBuffer = new byte[IndexHeader.SIZE];
        fixed (byte* ptr = headerBuffer)
        {
            *(IndexHeader*)ptr = indexHeader;
        }

        targetFile.Seek(headerOffset, SeekOrigin.Begin);
        targetFile.Write(headerBuffer, 0, IndexHeader.SIZE);
        targetFile.Flush();

        Console.WriteLine($"Updated target index header: {entryCount} entries, size: {pageSize} bytes");
    }

    /// <summary>
    /// 更新目标文件的索引块信息
    /// </summary>
    /// <param name="targetFile">目标文件</param>
    /// <param name="blockPosition">块位置</param>
    /// <param name="blockSize">块大小</param>
    private void UpdateTargetIndexBlocks(FileStream targetFile, long blockPosition, int blockSize)
    {
        var blocksOffset = DatabaseHeader.SIZE + FreeSpaceHeader.SIZE + IndexHeader.SIZE;

        // 创建索引块数组
        var blocksBuffer = new byte[IndexHeader.MAX_INDEX_PAGES * IndexBlock.SIZE];

        fixed (byte* ptr = blocksBuffer)
        {
            var blockPtr = (IndexBlock*)ptr;

            // 第一个块设置为合并后的索引块
            blockPtr[0] = new IndexBlock(blockPosition, blockSize);

            // 其余块保持默认（无效）
            for (int i = 1; i < IndexHeader.MAX_INDEX_PAGES; i++)
            {
                blockPtr[i] = new IndexBlock(); // 默认无效块
            }
        }

        targetFile.Seek(blocksOffset, SeekOrigin.Begin);
        targetFile.Write(blocksBuffer, 0, blocksBuffer.Length);
        targetFile.Flush();

        Console.WriteLine($"Updated target index blocks: first block at position {blockPosition}, size: {blockSize} bytes");
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
    public int TotalEntries;
    public int ActiveEntries;
    public int DeletedEntries;
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