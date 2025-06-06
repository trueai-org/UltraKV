namespace UltraKV;

/// <summary>
/// UltraKV 数据库配置
/// </summary>
public class UltraKVConfig
{
    /// <summary>
    /// 启用空间重复利用功能
    /// </summary>
    public bool EnableFreeSpaceReuse { get; set; } = true;

    /// <summary>
    /// 是否开启内存模式，开启内存模式时，读/写将会直接从内存中获取数据，而不是从磁盘读取，数据将全部加载到内存中
    /// </summary>
    public bool EnableMemoryMode { get; set; } = false;

    /// <summary>
    /// 空间重复利用空闲空间存储区域大小（单位：KB），最小为 1K
    /// 默认保留 4K = 256 个空闲区域
    /// 0~2147483648（2GB）
    /// 每个空闲空间区域占用 16 字节
    /// 推荐配置规则：未来数据总量以及更新/删除频率预估，建议最高不超过 10 万个区域，对于高频读写/删除的场景，建议使用偏大的空闲区域数。
    /// 参考示例：
    /// 0 ~ 100 条，1K = 64 个区域
    /// 100 ~ 1000 条，4K = 256 个区域
    /// 1000 ~ 10000 条，16K = 1024 个区域
    /// 10000 ~ 100000 条，64K = 4096 个区域
    /// 100000 ~ 1000000 条，256K = 16384 个区域
    /// 1000000 ~ 10000000 条，1024K = 65536 个区域
    /// </summary>
    public int FreeSpaceRegionSizeKB { get; set; } = 4;

    /// <summary>
    /// 原地更新时的空间分配倍数（百分比，0~255），默认 20，即 1.20 倍，配置规则为：1+n/100
    /// 例如：
    /// 最小值为 0
    /// 配置值为 20，则分配倍数为 1 + 20/100 = 1.20
    /// 最大值为 255，则分配倍数为 1 + 255/100 = 3.55
    /// </summary>
    public byte AllocationMultiplier { get; set; } = 2;

    /// <summary>
    /// 压缩算法类型（数据库级别，创建后不可变更）
    /// </summary>
    public CompressionType CompressionType { get; set; } = CompressionType.None;

    /// <summary>
    /// 加密算法类型（数据库级别，创建后不可变更）
    /// </summary>
    public EncryptionType EncryptionType { get; set; } = EncryptionType.None;

    /// <summary>
    /// 加密密钥（仅在创建数据库时使用）
    /// </summary>
    public string? EncryptionKey { get; set; }

    /// <summary>
    /// 写缓冲区大小，单位：KB，默认 64KB
    /// </summary>
    public uint WriteBufferSizeKB { get; set; } = 64;

    /// <summary>
    /// 读缓冲区大小，单位：KB，默认 64KB
    /// </summary>
    public uint ReadBufferSizeKB { get; set; } = 64;

    // ==================== GC 配置项 ====================

    /// <summary>
    /// GC 最小文件大小要求（单位 KB），默认 1024KB = 1MB
    /// 只有当文件大小超过此值时才会触发GC
    /// </summary>
    public uint GcMinFileSizeKB { get; set; } = 1024; // 1024KB = 1MB

    /// <summary>
    /// GC 累计空闲空间百分比阈值（百分比，0~255），默认 20，即：20%
    /// 当累计空闲空间占总文件大小的百分比超过此值时触发GC
    /// </summary>
    public byte GcFreeSpaceThreshold { get; set; } = 20; // 20%

    /// <summary>
    /// GC 最小数据条数要求，默认 100 条，取值：0~65535
    /// 只有当数据条数超过此值时才会触发GC
    /// </summary>
    public ushort GcMinRecordCount { get; set; } = 100;

    /// <summary>
    /// 是否开启空闲时自动回收，默认 false
    /// 当设置为 true 时，系统会在空闲时自动触发GC
    /// </summary>
    public bool GcAutoRecycleEnabled { get; set; } = false;

    /// <summary>
    /// 定期刷磁盘时间间隔（秒）（0~65535），默认 5 秒，为 0 时表示不刷盘
    /// 控制数据刷新到磁盘的频率
    /// </summary>
    public ushort GcFlushInterval { get; set; } = 5; // 5 seconds

    /// <summary>
    /// 验证配置有效性
    /// </summary>
    public void Validate()
    {
        if (FreeSpaceRegionSizeKB < 0)
        {
            FreeSpaceRegionSizeKB = 0;
            EnableFreeSpaceReuse = false;
        }

        if (WriteBufferSizeKB < 4)
            throw new ArgumentException("WriteBufferSize must be at least 4KB");

        if (ReadBufferSizeKB < 4)
            throw new ArgumentException("ReadBufferSize must be at least 4KB");

        // 验证加密配置
        if (EncryptionType != EncryptionType.None)
        {
            if (string.IsNullOrWhiteSpace(EncryptionKey))
                throw new ArgumentException("EncryptionKey is required when encryption is enabled");

            if (EncryptionKey.Length < 16)
                throw new ArgumentException("EncryptionKey must be at least 16 characters");
        }

        // 验证GC配置
        if (GcMinFileSizeKB < 0)
            GcMinFileSizeKB = 0;

        if (GcMinRecordCount < 0)
            GcMinRecordCount = 0;

        if (GcFlushInterval < 0)
            GcFlushInterval = 0;

        if (GcFlushInterval < 0)
            GcFlushInterval = 0;

        if (GcFlushInterval > 65535)
            GcFlushInterval = 65535; // 最大值为 65535 秒
    }

    /// <summary>
    /// 检查是否应该触发GC
    /// </summary>
    /// <param name="stats">数据库统计信息</param>
    /// <param name="forceGc">是否强制GC（忽略所有条件检查）</param>
    /// <returns>是否应该触发GC</returns>
    public bool ShouldTriggerGC(DatabaseStats stats, bool forceGc = false)
    {
        // 强制GC模式
        if (forceGc)
            return true;

        // 检查最小文件大小要求
        if (stats.TotalFileSize < GcMinFileSizeKB * 1024)
            return false;

        // 检查最小记录数要求
        if (stats.ValidRecordCount < GcMinRecordCount)
            return false;

        // 检查空闲空间百分比
        var freeSpaceRatio = stats.TotalFileSize > 0
            ? (double)stats.WastedSpace / stats.TotalFileSize
            : 0.0;

        return freeSpaceRatio >= (GcFreeSpaceThreshold / 100.0);
    }

    /// <summary>
    /// 获取实际的分配倍数
    /// </summary>
    public double GetActualAllocationMultiplier()
    {
        return 1.0 + AllocationMultiplier / 100.0;
    }

    /// <summary>
    /// 获取实际的GC空闲空间阈值百分比
    /// </summary>
    public double GetActualGcFreeSpaceThreshold()
    {
        return GcFreeSpaceThreshold / 100.0;
    }

    /// <summary>
    /// 创建默认配置
    /// </summary>
    public static UltraKVConfig Default => new();

    /// <summary>
    /// 创建最小配置
    /// </summary>
    public static UltraKVConfig Minimal => new()
    {
        FreeSpaceRegionSizeKB = 1, // 1KB
        WriteBufferSizeKB = 16, // 16KB
        ReadBufferSizeKB = 16, // 16KB
    };

    /// <summary>
    /// 创建高性能配置
    /// </summary>
    public static UltraKVConfig HighPerformance => new()
    {
        AllocationMultiplier = 50,
        FreeSpaceRegionSizeKB = 64, // 64K
        WriteBufferSizeKB = 1024, // 1MB
        ReadBufferSizeKB = 1024, // 1MB
    };

    /// <summary>
    /// SSD 优化配置
    /// </summary>
    public static UltraKVConfig SSDOptimized => new()
    {
        AllocationMultiplier = 50,
        FreeSpaceRegionSizeKB = 64, // 64K
        WriteBufferSizeKB = 256, // 256KB
        ReadBufferSizeKB = 256, // 256KB
    };

    /// <summary>
    /// HDD 优化配置
    /// </summary>
    public static UltraKVConfig HDDOptimized => new()
    {
        FreeSpaceRegionSizeKB = 16, // 16K
    };

    /// <summary>
    /// 创建空间优化配置
    /// </summary>
    public static UltraKVConfig SpaceOptimized => new()
    {
        FreeSpaceRegionSizeKB = 16,  // 16K
        AllocationMultiplier = 10,
        CompressionType = CompressionType.Gzip,
    };

    /// <summary>
    /// 创建安全配置
    /// </summary>
    public static UltraKVConfig Secure(string encryptionKey) => new()
    {
        FreeSpaceRegionSizeKB = 16, // 16K
        CompressionType = CompressionType.Gzip,
        EncryptionType = EncryptionType.AES256GCM,
        EncryptionKey = encryptionKey,
    };

    public override string ToString()
    {
        return
            $"EnableMemoryMode: {EnableMemoryMode}, " +
            $"EnableFreeSpaceReuse: {EnableFreeSpaceReuse}, " +
            $"FreeSpace: {FreeSpaceRegionSizeKB}KB, " +
            $"FreeSpaceRegions: {FreeSpaceRegionSizeKB / FreeBlock.SIZE}, " +
            $"Multiplier: {GetActualAllocationMultiplier():F1}x, " +
            $"Compression: {CompressionType}, " +
            $"Encryption: {EncryptionType}, " +
            $"WriteBuffer: {WriteBufferSizeKB}KB, " +
            $"ReadBuffer: {ReadBufferSizeKB}KB, " +
            $"GC: MinFile={GcMinFileSizeKB}KB, " +
            $"FreeThreshold={GetActualGcFreeSpaceThreshold():P1}, " +
            $"MinRecords={GcMinRecordCount}, " +
            $"AutoRecycle={GcAutoRecycleEnabled}, " +
            $"FlushInterval={GcFlushInterval}s";
    }
}