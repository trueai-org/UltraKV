using System.Runtime.InteropServices;

namespace UltraKV;

/// <summary>
/// 数据库文件头结构（存储数据库级别的配置信息）- 固定 128 字节
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct DatabaseHeader
{
    public uint Magic;                      // 4 bytes - 魔法数字
    public ushort Version;                  // 2 bytes - 版本号
    public CompressionType CompressionType; // 1 byte - 压缩类型
    public EncryptionType EncryptionType;   // 1 byte - 加密类型
    public byte EnableFreeSpaceReuse;       // 1 byte - 是否启用空间重复利用
    public byte EnableMemoryMode;           // 1 byte - 是否开启内存模式
    public int FreeSpaceRegionSizeKB;       // 4 bytes - 空闲空间区域大小
    public byte AllocationMultiplier;       // 1 byte - 预分配空间倍数百分比，实际倍数算法：1 + n/100.0
    public uint WriteBufferSizeKB;          // 4 bytes - 写缓冲区大小（KB）
    public uint ReadBufferSizeKB;           // 4 bytes - 读缓冲区大小（KB）
    public long CreatedTime;                // 8 bytes - 创建时间
    public long LastAccessTime;             // 8 bytes - 最后访问时间
                                            // total 39 byte

    public byte GcAutoRecycleEnabled;       // 1 byte - 是否开启自动GC
    public byte GcFreeSpaceThreshold;       // 1 byte - GC空闲空间阈值百分比，实际阈值：n/100.0
    public ushort GcMinRecordCount;         // 2 bytes - GC最小记录数要求
    public uint GcMinFileSizeKB;            // 4 bytes - GC最小文件大小要求（KB）
    public ushort GcFlushInterval;          // 2 bytes - GC刷盘间隔（秒）
    public long GcLastTime;                 // 8 bytes - 最后GC时间
    public uint GcTotalCount;               // 4 bytes - 总GC次数
                                            // total 61 byte

    public byte EnableUpdateValidation;     // 1 byte - 是否开启更新验证
    public int MaxKeyLength;                // 4 bytes - 限制 Key 的最大长度
    public int DefaultIndexPageSizeKB;      // 4 bytes - 默认索引页大小（KB）
                                            // total 70 byte

    public ushort Reserved8;                // 2 bytes
    public int Reserved7;                   // 4 bytes
    public long Reserved6;                  // 8 bytes
    public long Reserved5;                  // 8 bytes
    public long Reserved4;                  // 8 bytes
    public long Reserved3;                  // 8 bytes
    public long Reserved2;                  // 8 bytes
    public long Reserved1;                  // 8 bytes
                                            // 保留字节 - 使用多个字段组合 128 - 70 - 4 = 54 字节
                                            // total 124 byte

    // 校验和字段
    public uint Checksum;                   // 4 bytes - 校验和

    public const uint MAGIC_NUMBER = 0x554B5644; // "UKVD" - UltraKV Database
    public const ushort CURRENT_VERSION = 1;
    public const int SIZE = 128;

    public static DatabaseHeader Create(UltraKVConfig config)
    {
        var header = new DatabaseHeader
        {
            Magic = MAGIC_NUMBER,
            Version = CURRENT_VERSION,
            CompressionType = config.CompressionType,
            EncryptionType = config.EncryptionType,
            FreeSpaceRegionSizeKB = config.FreeSpaceRegionSizeKB,
            AllocationMultiplier = config.AllocationMultiplier,
            GcFreeSpaceThreshold = config.GcFreeSpaceThreshold,
            GcMinRecordCount = Math.Min(config.GcMinRecordCount, ushort.MaxValue),
            WriteBufferSizeKB = config.WriteBufferSizeKB,
            ReadBufferSizeKB = config.ReadBufferSizeKB,
            GcMinFileSizeKB = config.GcMinFileSizeKB, // 存储为KB
            GcFlushInterval = Math.Min(config.GcFlushInterval, ushort.MaxValue),
            GcAutoRecycleEnabled = config.GcAutoRecycleEnabled ? (byte)1 : (byte)0,
            EnableFreeSpaceReuse = config.EnableFreeSpaceReuse ? (byte)1 : (byte)0,
            EnableMemoryMode = config.EnableMemoryMode ? (byte)1 : (byte)0,
            CreatedTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            LastAccessTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            GcLastTime = 0,
            GcTotalCount = 0,
            Reserved1 = 0,
            Reserved2 = 0,
            Reserved3 = 0,
            Reserved4 = 0,
            Reserved5 = 0,
            Reserved6 = 0
        };

        header.Checksum = CalculateChecksum(header);
        return header;
    }

    public bool IsValid()
    {
        return Magic == MAGIC_NUMBER &&
               Version <= CURRENT_VERSION &&
               Checksum == CalculateChecksum(this);
    }

    public void UpdateAccessTime()
    {
        LastAccessTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        Checksum = CalculateChecksum(this);
    }

    public void UpdateGcTime()
    {
        GcLastTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        GcTotalCount++;
        Checksum = CalculateChecksum(this);
    }

    /// <summary>
    /// 获取实际的分配倍数
    /// </summary>
    public readonly double GetActualAllocationMultiplier()
    {
        return 1.0 + AllocationMultiplier / 100.0;
    }

    /// <summary>
    /// 获取实际的GC空闲空间阈值
    /// </summary>
    public readonly double GetActualGcFreeSpaceThreshold()
    {
        return GcFreeSpaceThreshold / 100.0;
    }

    /// <summary>
    /// 获取实际的GC最小文件大小（字节）
    /// </summary>
    public readonly long GetActualGcMinFileSize()
    {
        return (long)GcMinFileSizeKB * 1024;
    }

    /// <summary>
    /// 获取实际的GC刷盘间隔（毫秒）
    /// </summary>
    public readonly int GetActualGcFlushInterval()
    {
        return GcFlushInterval * 1000;
    }

    /// <summary>
    /// 是否启用自动GC
    /// </summary>
    public readonly bool IsGcAutoRecycleEnabled => GcAutoRecycleEnabled != 0;

    /// <summary>
    /// 是否启用空间重复利用
    /// </summary>
    public readonly bool IsFreeSpaceReuseEnabled => EnableFreeSpaceReuse != 0;

    /// <summary>
    /// 检查是否需要GC
    /// </summary>
    public readonly bool ShouldGC(long fileSize, long freeSpaceSize, int recordCount, bool forceGc = false)
    {
        if (forceGc)
            return true;

        // 检查最小文件大小
        if (fileSize < GetActualGcMinFileSize())
            return false;

        // 检查最小记录数
        if (recordCount < GcMinRecordCount)
            return false;

        // 检查空闲空间比例
        var freeSpaceRatio = fileSize > 0 ? (double)freeSpaceSize / fileSize : 0.0;
        return freeSpaceRatio >= GetActualGcFreeSpaceThreshold();
    }

    private static unsafe uint CalculateChecksum(DatabaseHeader header)
    {
        var tempHeader = header;
        tempHeader.Checksum = 0; // 清零校验和字段

        var bytes = new ReadOnlySpan<byte>(&tempHeader, SIZE - 4); // 排除校验和字段和保留字段
        uint hash = 2166136261u; // FNV-1a初始值

        foreach (byte b in bytes)
        {
            hash ^= b;
            hash *= 16777619u; // FNV-1a素数
        }

        return hash;
    }

    public override readonly string ToString()
    {
        return $"Magic: 0x{Magic:X8}, Version: {Version}, " +
               $"Compression: {CompressionType}, Encryption: {EncryptionType}, " +
               $"EnableMemoryMode: {EnableMemoryMode}, " +
               $"EnableFreeSpaceReuse: {EnableFreeSpaceReuse}, " +
               $"FreeSpaceRegion: {FreeSpaceRegionSizeKB}KB, " +
               $"AllocationMultiplier: {GetActualAllocationMultiplier():F1}x, " +
               $"GcThreshold: {GetActualGcFreeSpaceThreshold():P1}, " +
               $"GcMinRecords: {GcMinRecordCount}, " +
               $"GcMinFileSize: {GcMinFileSizeKB}KB, " +
               $"GcFlushInterval: {GcFlushInterval}s, " +
               $"AutoGC: {IsGcAutoRecycleEnabled}, " +
               $"FreeSpaceReuse: {IsFreeSpaceReuseEnabled}, " +
               $"GcCount: {GcTotalCount}, " +
               $"Created: {DateTimeOffset.FromUnixTimeMilliseconds(CreatedTime):yyyy-MM-dd HH:mm:ss}";
    }
}