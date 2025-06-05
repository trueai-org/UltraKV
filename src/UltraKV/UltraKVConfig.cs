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
    /// 压缩算法类型
    /// </summary>
    public CompressionType CompressionType { get; set; } = CompressionType.None;

    /// <summary>
    /// 加密算法类型
    /// </summary>
    public EncryptionType EncryptionType { get; set; } = EncryptionType.None;

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

        if (AllocationMultiplier < 1.0 || AllocationMultiplier > 10.0)
            throw new ArgumentException("AllocationMultiplier must be between 1.0 and 10.0");

        if (WriteBufferSize < 4096)
            throw new ArgumentException("WriteBufferSize must be at least 4KB");

        if (ReadBufferSize < 4096)
            throw new ArgumentException("ReadBufferSize must be at least 4KB");
    }
}