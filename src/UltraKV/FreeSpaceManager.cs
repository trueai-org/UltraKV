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
        return new FreeBlock(startPos, (endPos - startPos));
    }

    public override string ToString()
    {
        return $"Pos: {Position}, Size: {Size / 1024.0:F1}KB";
    }
}

/// <summary>
/// 空闲空间区域头部结构
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal struct FreeSpaceHeader
{
    public uint Magic;              // 4 bytes - 魔法数字
    public int BlockCount;          // 4 bytes - 空闲块数量
    public long RegionSize;          // 8 bytes - 区域大小
                                     // Total: 16 bytes

    public const uint MAGIC_NUMBER = 0x46535053; // "FSPS" - Free Space
    public const int SIZE = 16;

    public bool IsValid => Magic == MAGIC_NUMBER && BlockCount >= 0 && RegionSize > 0;
}

/// <summary>
/// 空闲空间管理器
/// </summary>
public unsafe class FreeSpaceManager : IDisposable
{
    private readonly FileStream _file;
    private readonly int _regionSize;
    private readonly byte* _regionBuffer;
    private readonly List<FreeBlock> _freeBlocks;
    private readonly object _lock = new object();
    private readonly bool _enableFreeSpaceReuse; // 新增：是否启用空闲空间重用
    private bool _disposed;
    private bool _isDirty;

    public FreeSpaceManager(FileStream file, int regionSize, bool enableFreeSpaceReuse = true)
    {
        _file = file;
        _regionSize = regionSize;
        _enableFreeSpaceReuse = enableFreeSpaceReuse;
        _regionBuffer = (byte*)Marshal.AllocHGlobal(regionSize);
        _freeBlocks = new List<FreeBlock>();

        // 只有启用空闲空间重用时才加载
        if (_enableFreeSpaceReuse)
        {
            LoadFreeSpaceRegion();
        }
        else
        {
            // 禁用时，初始化空的区域
            InitializeFreeSpaceRegion();
        }
    }

    /// <summary>
    /// 添加空闲空间
    /// </summary>
    public void AddFreeSpace(long position, long size)
    {
        // 如果禁用空闲空间重用，直接返回，不记录空闲空间
        if (!_enableFreeSpaceReuse)
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

        // 如果禁用空闲空间重用，总是返回 false，强制追加到文件末尾
        if (!_enableFreeSpaceReuse)
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

                    _isDirty = true;
                    return true;
                }
            }

            return false;
        }
    }

    private void EnsureSpaceCapacity()
    {
        var maxBlocks = (_regionSize - FreeSpaceHeader.SIZE) / FreeBlock.SIZE;

        while (_freeBlocks.Count > maxBlocks)
        {
            _freeBlocks.RemoveAt(0);
        }
    }

    private void LoadFreeSpaceRegion()
    {
        try
        {
            if (_file.Length < _regionSize)
            {
                InitializeFreeSpaceRegion();
                return;
            }

            _file.Seek(DatabaseHeader.SIZE, SeekOrigin.Begin); // 跳过数据库头
            var buffer = new byte[_regionSize];

            if (_file.Read(buffer, 0, _regionSize) != _regionSize)
            {
                InitializeFreeSpaceRegion();
                return;
            }

            fixed (byte* bufferPtr = buffer)
            {
                var header = *(FreeSpaceHeader*)bufferPtr;

                if (!header.IsValid || header.RegionSize != _regionSize)
                {
                    InitializeFreeSpaceRegion();
                    return;
                }

                _freeBlocks.Clear();
                var blockPtr = bufferPtr + FreeSpaceHeader.SIZE;

                for (int i = 0; i < header.BlockCount; i++)
                {
                    var block = *(FreeBlock*)blockPtr;
                    if (block.Position >= GetDataStartPosition() && block.Size > 0)
                    {
                        _freeBlocks.Add(block);
                    }
                    blockPtr += FreeBlock.SIZE;
                }

                _freeBlocks.Sort((a, b) => a.Size.CompareTo(b.Size));
            }
        }
        catch
        {
            InitializeFreeSpaceRegion();
        }
    }

    private void InitializeFreeSpaceRegion()
    {
        _freeBlocks.Clear();

        var totalHeaderSize = DatabaseHeader.SIZE + _regionSize;
        if (_file.Length < totalHeaderSize)
        {
            _file.SetLength(totalHeaderSize);
        }

        _isDirty = true;
        SaveFreeSpaceRegion();
    }

    public void SaveFreeSpaceRegion()
    {
        if (!_isDirty) return;

        lock (_lock)
        {
            try
            {
                new Span<byte>(_regionBuffer, _regionSize).Clear();

                var header = new FreeSpaceHeader
                {
                    Magic = FreeSpaceHeader.MAGIC_NUMBER,
                    BlockCount = _enableFreeSpaceReuse ? _freeBlocks.Count : 0, // 禁用时记录为0
                    RegionSize = _regionSize
                };

                *(FreeSpaceHeader*)_regionBuffer = header;

                // 只有启用时才写入空闲块数据
                if (_enableFreeSpaceReuse)
                {
                    var blockPtr = _regionBuffer + FreeSpaceHeader.SIZE;
                    for (int i = 0; i < _freeBlocks.Count; i++)
                    {
                        *(FreeBlock*)blockPtr = _freeBlocks[i];
                        blockPtr += FreeBlock.SIZE;
                    }
                }

                _file.Seek(DatabaseHeader.SIZE, SeekOrigin.Begin);
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

    public long GetDataStartPosition() => DatabaseHeader.SIZE + _regionSize;

    public FreeSpaceStats GetStats()
    {
        lock (_lock)
        {
            var totalFreeSpace = _enableFreeSpaceReuse ? _freeBlocks.Sum(b => (long)b.Size) : 0;
            var largestBlock = _enableFreeSpaceReuse && _freeBlocks.Count > 0 ? _freeBlocks.Max(b => b.Size) : 0;
            var maxBlocks = (_regionSize - FreeSpaceHeader.SIZE) / FreeBlock.SIZE;

            return new FreeSpaceStats
            {
                BlockCount = _enableFreeSpaceReuse ? _freeBlocks.Count : 0,
                MaxBlocks = maxBlocks,
                TotalFreeSpace = totalFreeSpace,
                LargestBlock = largestBlock,
                RegionSize = _regionSize,
                RegionUtilization = _enableFreeSpaceReuse
                    ? (double)(_freeBlocks.Count * FreeBlock.SIZE + FreeSpaceHeader.SIZE) / _regionSize
                    : (double)FreeSpaceHeader.SIZE / _regionSize,
                IsEnabled = _enableFreeSpaceReuse // 新增字段
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

// 更新 FreeSpaceStats 结构
public struct FreeSpaceStats
{
    public int BlockCount;
    public int MaxBlocks;
    public long TotalFreeSpace;
    public long LargestBlock;
    public int RegionSize;
    public double RegionUtilization;
    public bool IsEnabled; // 新增：是否启用空闲空间重用

    public readonly double FragmentationRatio => BlockCount > 0 ? 1.0 - (double)LargestBlock / (TotalFreeSpace / BlockCount) : 0.0;
    public readonly bool IsFragmented => FragmentationRatio > 0.3;

    public override readonly string ToString()
    {
        if (!IsEnabled)
            return "Free Space Reuse: Disabled";

        return $"Blocks: {BlockCount}/{MaxBlocks}, Free: {TotalFreeSpace / 1024.0:F1}KB, " +
               $"Largest: {LargestBlock / 1024.0:F1}KB, Fragmentation: {FragmentationRatio:P1}";
    }
}