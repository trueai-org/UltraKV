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
    /// 空闲空间存储区域大小（字节），默认 n * 12 = 3840 字节
    /// 每个空闲空间区域占用 12 字节，默认 320 个区域，总计 3840 字节
    /// </summary>
    public int FreeSpaceRegionSize { get; set; } = GetFreeSpaceRegionSize(320); // 320 regions * 12 bytes each = 3840 bytes

    /// <summary>
    /// 原地更新时的空间分配倍数，默认 1.2 倍
    /// </summary>
    public double AllocationMultiplier { get; set; } = 1.2;

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
    /// 写缓冲区大小，默认 64KB
    /// </summary>
    public int WriteBufferSize { get; set; } = 64 * 1024;

    /// <summary>
    /// 读缓冲区大小，默认 64KB
    /// </summary>
    public int ReadBufferSize { get; set; } = 64 * 1024;

    // ==================== GC 配置项 ====================

    /// <summary>
    /// GC 最小文件大小要求（字节），默认 1MB
    /// 只有当文件大小超过此值时才会触发GC
    /// </summary>
    public long GcMinFileSize { get; set; } = 1024 * 1024; // 1MB

    /// <summary>
    /// GC 累计空闲空间百分比阈值，默认 20%
    /// 当累计空闲空间占总文件大小的百分比超过此值时触发GC
    /// </summary>
    public double GcFreeSpaceThreshold { get; set; } = 0.20; // 20%

    /// <summary>
    /// GC 最小数据条数要求，默认 100 条
    /// 只有当数据条数超过此值时才会触发GC
    /// </summary>
    public int GcMinRecordCount { get; set; } = 100;

    /// <summary>
    /// 是否开启空闲时自动回收，默认 false
    /// 当设置为 true 时，系统会在空闲时自动触发GC
    /// </summary>
    public bool GcAutoRecycleEnabled { get; set; } = false;

    /// <summary>
    /// 定期刷磁盘时间间隔（毫秒），默认 5 秒，最少 100ms，为 0 时表示不刷盘
    /// 控制数据刷新到磁盘的频率
    /// </summary>
    public int GcFlushIntervalMs { get; set; } = 5000; // 5 seconds

    /// <summary>
    /// 验证配置有效性
    /// </summary>
    public void Validate()
    {
        if (FreeSpaceRegionSize < 1024)
            throw new ArgumentException("FreeSpaceRegionSize must be at least 1KB");

        if (AllocationMultiplier < 1.0 || AllocationMultiplier > 100.0)
            throw new ArgumentException("AllocationMultiplier must be between 1.0 and 100.0");

        if (WriteBufferSize < 4096)
            throw new ArgumentException("WriteBufferSize must be at least 4KB");

        if (ReadBufferSize < 4096)
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
        if (GcMinFileSize < 0)
            GcMinFileSize = 0;

        if (GcFreeSpaceThreshold < 0.0)
            GcFreeSpaceThreshold = 0;

        if (GcMinRecordCount < 0)
            GcMinRecordCount = 0;

        if (GcFlushIntervalMs < 0)
            GcFlushIntervalMs = 0;

        if (GcFlushIntervalMs > 0 && GcFlushIntervalMs < 100)
            GcFlushIntervalMs = 100;
    }

    /// <summary>
    /// 传入一个区域数量，返回字节大小
    /// </summary>
    /// <param name="regionCount"></param>
    /// <returns></returns>
    public static int GetFreeSpaceRegionSize(int regionCount)
    {
        return regionCount * FreeBlock.SIZE; // 每个区域占用 12 字节
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
        FreeSpaceRegionSize = GetFreeSpaceRegionSize(80), // 960 bytes (80 regions)
        WriteBufferSize = 16 * 1024, // 16KB
        ReadBufferSize = 16 * 1024, // 16KB
    };

    /// <summary>
    /// 创建高性能配置
    /// </summary>
    public static UltraKVConfig HighPerformance => new()
    {
        FreeSpaceRegionSize = GetFreeSpaceRegionSize(5120), // 61440 bytes (5120 regions)
        AllocationMultiplier = 1.5,
        WriteBufferSize = 1024 * 1024,
        ReadBufferSize = 1024 * 1024,
    };

    /// <summary>
    /// SSD 优化配置
    /// </summary>
    public static UltraKVConfig SSDOptimized => new()
    {
        FreeSpaceRegionSize = GetFreeSpaceRegionSize(5120), // 61440 bytes (5120 regions)
        AllocationMultiplier = 1.5,
        WriteBufferSize = 256 * 1024, // 256KB
        ReadBufferSize = 256 * 1024, // 256KB
    };

    /// <summary>
    /// HDD 优化配置
    /// </summary>
    public static UltraKVConfig HDDOptimized => new()
    {
        FreeSpaceRegionSize = GetFreeSpaceRegionSize(1280), // 15360 bytes (1280 regions)
    };

    /// <summary>
    /// 创建空间优化配置
    /// </summary>
    public static UltraKVConfig SpaceOptimized => new()
    {
        FreeSpaceRegionSize = GetFreeSpaceRegionSize(640), // 7680 bytes (640 regions)
        AllocationMultiplier = 1.1,
        CompressionType = CompressionType.Gzip,
    };

    /// <summary>
    /// 创建安全配置
    /// </summary>
    public static UltraKVConfig Secure(string encryptionKey) => new()
    {
        FreeSpaceRegionSize = GetFreeSpaceRegionSize(640), // 7680 bytes (640 regions)
        CompressionType = CompressionType.Gzip,
        EncryptionType = EncryptionType.AES256GCM,
        EncryptionKey = encryptionKey,
    };

    public override string ToString()
    {
        return $"FreeSpace: {FreeSpaceRegionSize / 1024}KB, " +
            $"FreeSpaceRegions: {FreeSpaceRegionSize / FreeBlock.SIZE}, " +
            $"Multiplier: {AllocationMultiplier:F1}x, " +
            $"Compression: {CompressionType}, " +
            $"Encryption: {EncryptionType}, " +
            $"WriteBuffer: {WriteBufferSize / 1024}KB, " +
            $"ReadBuffer: {ReadBufferSize / 1024}KB, " +
            $"GC: MinFile={GcMinFileSize / 1024}KB, " +
            $"FreeThreshold={GcFreeSpaceThreshold:P1}, " +
            $"MinRecords={GcMinRecordCount}, " +
            $"AutoRecycle={GcAutoRecycleEnabled}, " +
            $"FlushInterval={GcFlushIntervalMs}ms";
    }
}