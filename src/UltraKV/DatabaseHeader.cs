using System.Runtime.InteropServices;

namespace UltraKV;

/// <summary>
/// 数据库文件头结构（存储数据库级别的配置信息）
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct DatabaseHeader
{
    public uint Magic;                      // 4 bytes - 魔法数字
    public ushort Version;                  // 2 bytes - 版本号
    public CompressionType CompressionType; // 1 byte - 压缩类型
    public EncryptionType EncryptionType;   // 1 byte - 加密类型
    public uint FreeSpaceRegionSize;        // 4 bytes - 空闲空间区域大小
    public double AllocationMultiplier;     // 8 bytes - 分配倍数
    public uint WriteBufferSize;            // 4 bytes - 写缓冲区大小
    public uint ReadBufferSize;             // 4 bytes - 读缓冲区大小
    public long CreatedTime;                // 8 bytes - 创建时间
    public long LastAccessTime;             // 8 bytes - 最后访问时间
    public uint Checksum;                   // 4 bytes - 校验和
                                            // Total: 52 bytes

    public const uint MAGIC_NUMBER = 0x554B5644; // "UKVD" - UltraKV Database
    public const ushort CURRENT_VERSION = 1;
    public const int SIZE = 52;

    public static DatabaseHeader Create(UltraKVConfig config)
    {
        var header = new DatabaseHeader
        {
            Magic = MAGIC_NUMBER,
            Version = CURRENT_VERSION,
            CompressionType = config.CompressionType,
            EncryptionType = config.EncryptionType,
            FreeSpaceRegionSize = (uint)config.FreeSpaceRegionSize,
            AllocationMultiplier = config.AllocationMultiplier,
            WriteBufferSize = (uint)config.WriteBufferSize,
            ReadBufferSize = (uint)config.ReadBufferSize,
            CreatedTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            LastAccessTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
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

    private static unsafe uint CalculateChecksum(DatabaseHeader header)
    {
        var tempHeader = header;
        tempHeader.Checksum = 0; // 清零校验和字段

        var bytes = new ReadOnlySpan<byte>(&tempHeader, SIZE - 4); // 排除校验和字段
        uint hash = 2166136261u; // FNV-1a初始值

        foreach (byte b in bytes)
        {
            hash ^= b;
            hash *= 16777619u; // FNV-1a素数
        }

        return hash;
    }
}