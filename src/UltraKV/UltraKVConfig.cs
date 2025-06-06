namespace UltraKV;

/// <summary>
/// UltraKV 数据库配置
/// </summary>
public class UltraKVConfig
{
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
        ReadBufferSize = 16 * 1024 // 16KB
    };

    /// <summary>
    /// 创建高性能配置
    /// </summary>
    public static UltraKVConfig HighPerformance => new()
    {
        FreeSpaceRegionSize = GetFreeSpaceRegionSize(5120), // 61440 bytes (5120 regions)
        AllocationMultiplier = 1.5,
        WriteBufferSize = 1024 * 1024,
        ReadBufferSize = 1024 * 1024
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

    // HDD 优化配置
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
        EncryptionKey = encryptionKey
    };

    public override string ToString()
    {
        return $"FreeSpace: {FreeSpaceRegionSize / 1024}KB, " +
            $"FreeSpaceRegions: {FreeSpaceRegionSize / FreeBlock.SIZE}, " +
            $"Multiplier: {AllocationMultiplier:F1}x, " +
            $"Compression: {CompressionType}, " +
            $"Encryption: {EncryptionType}, " +
            $"WriteBuffer: {WriteBufferSize / 1024}KB, " +
            $"ReadBuffer: {ReadBufferSize / 1024}KB";
    }
}