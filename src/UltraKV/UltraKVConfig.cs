namespace UltraKV;

/// <summary>
/// UltraKV 数据库配置
/// </summary>
public class UltraKVConfig
{
    /// <summary>
    /// 空闲空间存储区域大小（字节），默认 4KB
    /// </summary>
    public int FreeSpaceRegionSize { get; set; } = 4 * 1024;

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
    /// 创建默认配置
    /// </summary>
    public static UltraKVConfig Default => new();

    /// <summary>
    /// 创建最小配置
    /// </summary>
    public static UltraKVConfig Minimal => new()
    {
        FreeSpaceRegionSize = 1 * 1024, // 1KB
        AllocationMultiplier = 1.0,
        CompressionType = CompressionType.None,
        EncryptionType = EncryptionType.None,
        WriteBufferSize = 4 * 1024, // 4KB
        ReadBufferSize = 4 * 1024 // 4KB
    };

    /// <summary>
    /// 创建高性能配置
    /// </summary>
    public static UltraKVConfig HighPerformance => new()
    {
        FreeSpaceRegionSize = 16 * 1024,
        AllocationMultiplier = 1.5,
        CompressionType = CompressionType.None,
        EncryptionType = EncryptionType.None,
        WriteBufferSize = 256 * 1024,
        ReadBufferSize = 256 * 1024
    };

    /// <summary>
    /// 创建空间优化配置
    /// </summary>
    public static UltraKVConfig SpaceOptimized => new()
    {
        FreeSpaceRegionSize = 8 * 1024,
        AllocationMultiplier = 1.1,
        CompressionType = CompressionType.Gzip,
        EncryptionType = EncryptionType.None,
        WriteBufferSize = 32 * 1024,
        ReadBufferSize = 32 * 1024
    };

    /// <summary>
    /// 创建安全配置
    /// </summary>
    public static UltraKVConfig Secure(string encryptionKey) => new()
    {
        FreeSpaceRegionSize = 8 * 1024,
        AllocationMultiplier = 1.2,
        CompressionType = CompressionType.Gzip,
        EncryptionType = EncryptionType.AES256GCM,
        EncryptionKey = encryptionKey,
        WriteBufferSize = 64 * 1024,
        ReadBufferSize = 64 * 1024
    };

    public override string ToString()
    {
        return $"FreeSpace: {FreeSpaceRegionSize / 1024}KB, " +
               $"Multiplier: {AllocationMultiplier:F1}x, " +
               $"Compression: {CompressionType}, " +
               $"Encryption: {EncryptionType}, " +
               $"WriteBuffer: {WriteBufferSize / 1024}KB, " +
               $"ReadBuffer: {ReadBufferSize / 1024}KB";
    }
}