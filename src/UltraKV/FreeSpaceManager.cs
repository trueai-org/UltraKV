using System.Runtime.InteropServices;

namespace UltraKV;

/// <summary>
/// 空闲块结构
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct FreeBlock
{
    public long Position;           // 8 bytes - 文件位置
    public long Size;               // 8 bytes - 块大小
                                    // Total: 16 bytes

    public const int SIZE = 16;

    public FreeBlock(long position, long size)
    {
        Position = position;
        Size = size;
    }

    public bool IsAdjacentTo(FreeBlock other)
    {
        return Position + Size == other.Position || other.Position + other.Size == Position;
    }

    public FreeBlock MergeWith(FreeBlock other)
    {
        var startPos = Math.Min(Position, other.Position);
        var endPos = Math.Max(Position + Size, other.Position + other.Size);
        return new FreeBlock(startPos, endPos - startPos);
    }

    public override string ToString()
    {
        return $"Pos: {Position}, Size: {Size / 1024.0:F1}KB";
    }
}

/// <summary>
/// 空闲空间区域头部结构（固定64字节），存储到数据库头的后面
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct FreeSpaceHeader
{
    public uint Magic;                  // 4 bytes - 魔法数字
    public int BlockCount;              // 4 bytes - 空闲块数量
    public long RegionSize;             // 8 bytes - 区域大小
    public byte IsEnabled;              // 1 byte - 是否启用空闲空间重用
    public byte Version;                // 1 byte - 版本号
                                        // total = 18 bytes

    public long TotalAllocationCount;   // 4 bytes - 累计分配次数
    public long TotalRecycleCount;      // 4 bytes - 累计回收次数
    public long LastUsedTimestamp;      // 8 bytes - 最后使用空闲空间的时间戳
    public long TotalSpaceRecycled;     // 8 bytes - 累计回收的空间大小
    public long LargestBlockSize;       // 8 bytes - 历史最大块大小
                                        // 统计信息
                                        // total = 50

    public ushort Reserved3;            // 2 bytes - 保留字段
    public uint Reserved2;              // 4 bytes - 保留字段
    public long Reserved1;              // 8 bytes - 保留字段
                                        // 64 - 50 = 14 bytes
                                        // Total: 64 bytes

    public const uint MAGIC_NUMBER = 0x46535053; // "FSPS" - Free Space
    public const byte CURRENT_VERSION = 1;
    public const int SIZE = 64; // 固定64字节

    public static FreeSpaceHeader Create(long regionSize, bool isEnabled)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return new FreeSpaceHeader
        {
            Magic = MAGIC_NUMBER,
            BlockCount = 0,
            RegionSize = regionSize,
            IsEnabled = isEnabled ? (byte)1 : (byte)0,
            Version = CURRENT_VERSION,
            TotalAllocationCount = 0,
            TotalRecycleCount = 0,
            LastUsedTimestamp = now,
            TotalSpaceRecycled = 0,
            LargestBlockSize = 0,
            Reserved1 = 0,
            Reserved2 = 0,
            Reserved3 = 0
        };
    }

    public readonly bool IsValid => Magic == MAGIC_NUMBER &&
                                   Version <= CURRENT_VERSION &&
                                   BlockCount >= 0 &&
                                   RegionSize >= 0;

    public readonly bool IsFreeSpaceEnabled => IsEnabled != 0;

    public void UpdateAllocationStats(long allocatedSize)
    {
        TotalAllocationCount++;
        LastUsedTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    public void UpdateRecycleStats(long recycledSize)
    {
        TotalRecycleCount++;
        TotalSpaceRecycled += recycledSize;
        LargestBlockSize = Math.Max(LargestBlockSize, recycledSize);
        LastUsedTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    public override string ToString()
    {
        return $"FreeSpace Header v{Version} - Enabled: {IsFreeSpaceEnabled}, " +
               $"Blocks: {BlockCount}, Region: {RegionSize / 1024}KB, " +
               $"Allocations: {TotalAllocationCount}, Recycles: {TotalRecycleCount}, " +
               $"Total Recycled: {TotalSpaceRecycled / 1024.0:F1}KB";
    }
}

/// <summary>
/// 优化的空闲空间管理器
/// </summary>
public unsafe class FreeSpaceManager : IDisposable
{
    /// <summary>
    /// 空闲空间区域的起始位置（固定在1024字节开始）
    /// </summary>
    public const long FreeSpaceStartPosition = 1024;

    private readonly FileStream _file;
    private readonly int _regionSize;
    private readonly byte* _regionBuffer;
    private readonly List<FreeBlock> _freeBlocks;
    private readonly object _lock = new object();
    private readonly bool _enableFreeSpaceReuse;
    private FreeSpaceHeader _header;
    private bool _disposed;

    // **脏标记（Dirty Flag）**的作用，这是一个常见的性能优化模式。
    // 避免不必要的磁盘写入

    private bool _isDirty;

    public FreeSpaceManager(FileStream file, int regionSize, bool enableFreeSpaceReuse)
    {
        _enableFreeSpaceReuse = enableFreeSpaceReuse;
        _file = file;
        _regionSize = enableFreeSpaceReuse ? regionSize : 0; // 禁用时不创建空闲空间

        _regionBuffer = (byte*)Marshal.AllocHGlobal(_regionSize);
        _freeBlocks = new List<FreeBlock>();

        // 加载空闲空间头信息
        LoadFreeSpaceHeader();
    }

    /// <summary>
    /// 加载空闲空间头部信息（如果有头部信息）
    /// </summary>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public void LoadFreeSpaceHeader()
    {
        if (_file.Length < DatabaseHeader.SIZE + FreeSpaceHeader.SIZE)
            return;

        _file.Seek(DatabaseHeader.SIZE, SeekOrigin.Begin);
        var headerBuffer = new byte[FreeSpaceHeader.SIZE];
        if (_file.Read(headerBuffer, 0, FreeSpaceHeader.SIZE) != FreeSpaceHeader.SIZE)
            throw new InvalidOperationException("Failed to read free space header.");

        fixed (byte* headerPtr = headerBuffer)
        {
            _header = *(FreeSpaceHeader*)headerPtr;
        }

        if (!_header.IsValid)
            throw new InvalidOperationException("Invalid free space header.");
    }

    /// <summary>
    /// 创建新的空闲空间头部
    /// </summary>
    public void CreateNewFreeSpaceHeader()
    {
        _header = FreeSpaceHeader.Create(_regionSize, _enableFreeSpaceReuse);

        // 立即保存头部信息
        SaveFreeSpaceHeader();

        Console.WriteLine($"Created new free space header: Enabled={_header.IsEnabled}, RegionSize={_header.IsFreeSpaceEnabled}");
    }

    /// <summary>
    /// 保存空闲空间头部信息
    /// </summary>
    public void SaveFreeSpaceHeader()
    {
        lock (_lock)
        {
            // 确保文件有足够的空间
            var requiredLength = DatabaseHeader.SIZE + FreeSpaceHeader.SIZE;
            if (_file.Length < requiredLength)
            {
                _file.SetLength(requiredLength);
            }

            // 写入头部到文件
            _file.Seek(DatabaseHeader.SIZE, SeekOrigin.Begin);
            var headerBytes = new byte[FreeSpaceHeader.SIZE];
            fixed (byte* ptr = headerBytes)
            {
                *(FreeSpaceHeader*)ptr = _header;
            }
            _file.Write(headerBytes, 0, FreeSpaceHeader.SIZE);
            _file.Flush();
        }
    }

    /// <summary>
    /// 判断是否需要重建空闲空间区域
    /// 如果开启或关闭空闲空间重用，或者区域大小发生变化，则需要重建
    /// </summary>
    /// <param name="databaseHeader"></param>
    /// <returns></returns>
    public bool NeedsRebuild(int regionSize, bool enableFreeSpaceReuse = true)
    {
        if (_header.IsFreeSpaceEnabled != enableFreeSpaceReuse)
        {
            Console.WriteLine("Free space reuse configuration changed, needs rebuild.");
            return true;
        }
        else if (enableFreeSpaceReuse && _header.RegionSize != regionSize)
        {
            Console.WriteLine("Free space region size changed, needs rebuild.");
            return true;
        }
        return false;
    }

    /// <summary>
    /// 加载空闲空间区域
    /// </summary>
    public void LoadFreeSpaceRegion()
    {
        // 检查文件是否包含空闲空间区域
        if (_file.Length >= FreeSpaceStartPosition)
        {
            // 加载现有区域
            LoadExistingFreeSpaceRegion();
        }
    }

    /// <summary>
    /// 创建新的空闲空间区域
    /// </summary>
    public void CreateNewFreeSpaceRegion()
    {
        _freeBlocks.Clear();

        // 确保文件有足够空间
        var requiredSize = FreeSpaceStartPosition + _regionSize;
        if (_file.Length < requiredSize)
        {
            _file.SetLength(requiredSize);
        }

        _isDirty = true;

        SaveFreeSpaceRegion();

        Console.WriteLine($"Created new free space region: Size={_regionSize}, Enabled={_header.IsEnabled}");
    }

    /// <summary>
    /// 加载现有的空闲空间区域
    /// </summary>
    private void LoadExistingFreeSpaceRegion()
    {
        _freeBlocks.Clear();

        if (_header.IsFreeSpaceEnabled && _header.BlockCount > 0)
        {
            LoadFreeBlocks(_header);
        }

        Console.WriteLine($"Loaded existing free space region: {_header}");
    }

    /// <summary>
    /// 加载空闲块数据
    /// </summary>
    private void LoadFreeBlocks(FreeSpaceHeader header)
    {
        try
        {
            _file.Seek(FreeSpaceStartPosition, SeekOrigin.Begin);
            var maxBlocks = _regionSize / FreeBlock.SIZE;
            var blocksToRead = Math.Min(header.BlockCount, maxBlocks);

            for (int i = 0; i < blocksToRead; i++)
            {
                var blockBuffer = new byte[FreeBlock.SIZE];
                if (_file.Read(blockBuffer, 0, FreeBlock.SIZE) == FreeBlock.SIZE)
                {
                    fixed (byte* blockPtr = blockBuffer)
                    {
                        var block = *(FreeBlock*)blockPtr;
                        if (block.Position >= GetDataStartPosition() && block.Size > 0)
                        {
                            _freeBlocks.Add(block);
                        }
                    }
                }
            }

            _freeBlocks.Sort((a, b) => a.Size.CompareTo(b.Size));
        }
        catch (Exception ex)
        {
            _freeBlocks.Clear();

            Console.WriteLine($"Error loading free blocks: {ex.Message}");
        }
    }

    /// <summary>
    /// 添加空闲空间
    /// </summary>
    public void AddFreeSpace(long position, long size)
    {
        // 如果禁用空闲空间重用，直接返回
        if (!_header.IsFreeSpaceEnabled)
            return;

        if (position < GetDataStartPosition() || size <= 0)
            return;

        lock (_lock)
        {
            var newBlock = new FreeBlock(position, size);

            // 查找可以合并的相邻块
            var toMerge = new List<int>();
            for (int i = 0; i < _freeBlocks.Count; i++)
            {
                if (_freeBlocks[i].IsAdjacentTo(newBlock))
                {
                    toMerge.Add(i);
                }
            }

            // 合并所有相邻块
            var mergedBlock = newBlock;
            for (int i = toMerge.Count - 1; i >= 0; i--)
            {
                var index = toMerge[i];
                mergedBlock = mergedBlock.MergeWith(_freeBlocks[index]);
                _freeBlocks.RemoveAt(index);
            }

            _freeBlocks.Add(mergedBlock);
            _freeBlocks.Sort((a, b) => a.Size.CompareTo(b.Size));

            // 更新统计信息
            _header.UpdateRecycleStats(size);
            _header.BlockCount = _freeBlocks.Count;

            EnsureSpaceCapacity();
            _isDirty = true;
        }
    }

    /// <summary>
    /// 尝试获取空闲空间
    /// </summary>
    public bool TryGetFreeSpace(int requiredSize, out FreeBlock block)
    {
        block = default;

        // 如果禁用空闲空间重用，总是返回 false
        if (!_header.IsFreeSpaceEnabled)
            return false;

        lock (_lock)
        {
            for (int i = 0; i < _freeBlocks.Count; i++)
            {
                if (_freeBlocks[i].Size >= requiredSize)
                {
                    block = _freeBlocks[i];
                    _freeBlocks.RemoveAt(i);

                    var wasteThreshold = Math.Max(64, requiredSize / 4);
                    if (block.Size > requiredSize + wasteThreshold)
                    {
                        var remainingBlock = new FreeBlock(
                            block.Position + requiredSize,
                            block.Size - requiredSize);

                        _freeBlocks.Add(remainingBlock);
                        _freeBlocks.Sort((a, b) => a.Size.CompareTo(b.Size));

                        block = new FreeBlock(block.Position, requiredSize);
                    }

                    // 更新统计信息
                    _header.UpdateAllocationStats(requiredSize);
                    _header.BlockCount = _freeBlocks.Count;

                    _isDirty = true;
                    return true;
                }
            }

            return false;
        }
    }

    private void EnsureSpaceCapacity()
    {
        if (!_header.IsFreeSpaceEnabled)
            return;

        var maxBlocks = (_regionSize - FreeSpaceHeader.SIZE) / FreeBlock.SIZE;

        while (_freeBlocks.Count > maxBlocks)
        {
            _freeBlocks.RemoveAt(0); // 移除最小的块
        }
    }

    /// <summary>
    /// 保存空闲空间区域
    /// </summary>
    public void SaveFreeSpaceRegion()
    {
        if (!_isDirty) return;

        lock (_lock)
        {
            try
            {
                // 清空缓冲区
                new Span<byte>(_regionBuffer, _regionSize).Clear();

                // 只有启用时才写入空闲块数据
                if (_header.IsFreeSpaceEnabled)
                {
                    var blockPtr = _regionBuffer + FreeSpaceHeader.SIZE;
                    for (int i = 0; i < _freeBlocks.Count; i++)
                    {
                        *(FreeBlock*)blockPtr = _freeBlocks[i];
                        blockPtr += FreeBlock.SIZE;
                    }
                }

                // 写入文件
                _file.Seek(FreeSpaceStartPosition, SeekOrigin.Begin);
                var span = new ReadOnlySpan<byte>(_regionBuffer, _regionSize);
                _file.Write(span);
                _file.Flush();

                _isDirty = false;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Failed to save free space region: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// 获取数据开始位置
    /// </summary>
    public long GetDataStartPosition() => FreeSpaceStartPosition + _regionSize;

    /// <summary>
    /// 获取第一个索引数据开始位置
    /// </summary>
    public long GetFirstIndexDataStartPosition() => FreeSpaceStartPosition + _regionSize;

    /// <summary>
    /// 获取统计信息
    /// </summary>
    public FreeSpaceStats GetStats()
    {
        lock (_lock)
        {
            var totalFreeSpace = _header.IsFreeSpaceEnabled ? _freeBlocks.Sum(b => b.Size) : 0;
            var largestBlock = _header.IsFreeSpaceEnabled && _freeBlocks.Count > 0 ? _freeBlocks.Max(b => b.Size) : 0;
            var maxBlocks = _header.IsFreeSpaceEnabled ? (_regionSize - FreeSpaceHeader.SIZE) / FreeBlock.SIZE : 0;

            return new FreeSpaceStats
            {
                BlockCount = _header.IsFreeSpaceEnabled ? _freeBlocks.Count : 0,
                MaxBlocks = maxBlocks,
                TotalFreeSpace = totalFreeSpace,
                LargestBlock = largestBlock,
                RegionSize = _regionSize,
                RegionUtilization = (double)FreeSpaceHeader.SIZE / _regionSize,
                IsEnabled = _header.IsFreeSpaceEnabled,
                TotalAllocationCount = _header.TotalAllocationCount,
                TotalRecycleCount = _header.TotalRecycleCount,
                TotalSpaceRecycled = _header.TotalSpaceRecycled,
                LargestBlockSize = _header.LargestBlockSize,
                LastUsedTime = DateTimeOffset.FromUnixTimeMilliseconds(_header.LastUsedTimestamp)
            };
        }
    }

    /// <summary>
    /// 清空所有空闲空间记录
    /// </summary>
    public void Clear()
    {
        lock (_lock)
        {
            _freeBlocks.Clear();
            _header.BlockCount = 0;
            _isDirty = true;
            SaveFreeSpaceRegion();
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            SaveFreeSpaceRegion();
            Marshal.FreeHGlobal((IntPtr)_regionBuffer);
            _disposed = true;
        }
    }
}

/// <summary>
/// 增强的空闲空间统计信息
/// </summary>
public struct FreeSpaceStats
{
    public int BlockCount;
    public int MaxBlocks;
    public long TotalFreeSpace;
    public long LargestBlock;
    public int RegionSize;
    public double RegionUtilization;
    public bool IsEnabled;

    // 新增统计字段
    public long TotalAllocationCount;

    public long TotalRecycleCount;
    public long TotalSpaceRecycled;
    public long LargestBlockSize;
    public DateTimeOffset LastUsedTime;

    public readonly double FragmentationRatio => BlockCount > 0 ? 1.0 - (double)LargestBlock / (TotalFreeSpace / BlockCount) : 0.0;
    public readonly bool IsFragmented => FragmentationRatio > 0.3;
    public readonly double RecycleEfficiency => TotalRecycleCount > 0 ? (double)TotalAllocationCount / TotalRecycleCount : 0.0;

    public override readonly string ToString()
    {
        if (!IsEnabled)
            return "Free Space Reuse: Disabled";

        return $"Blocks: {BlockCount}/{MaxBlocks}, Free: {TotalFreeSpace / 1024.0:F1}KB, " +
               $"Largest: {LargestBlock / 1024.0:F1}KB, Fragmentation: {FragmentationRatio:P1}, " +
               $"Allocations: {TotalAllocationCount}, Recycles: {TotalRecycleCount}, " +
               $"Efficiency: {RecycleEfficiency:F2}, Last Used: {LastUsedTime:HH:mm:ss}";
    }
}