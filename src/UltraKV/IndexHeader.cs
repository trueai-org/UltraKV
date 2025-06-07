using System.Runtime.InteropServices;

namespace UltraKV;

/// <summary>
/// 索引块结构 - 固定16字节
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct IndexBlock
{
    public long Position;           // 8 bytes - 索引页文件位置
    public long Size;               // 8 bytes - 索引页大小
                                    // Total: 16 bytes

    public const int SIZE = 16;

    public IndexBlock(long position, long size)
    {
        Position = position;
        Size = size;
    }

    public bool IsValid => Position > 0 && Size > 0;

    public override string ToString()
    {
        return $"IndexBlock: Pos={Position}, Size={Size / 1024.0:F1}KB";
    }
}

/// <summary>
/// 索引页内的单个索引条目 - 固定16字节
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct IndexEntry
{
    public long Position;           // 8 bytes - 值数据在文件中的位置
    public int KeyLength;           // 4 bytes - Key长度
    public byte IsDeleted;          // 1 byte - 删除标记
    public byte PageIndex;          // 1 byte - 所在索引页的索引
                                    // Total: 14 bytes


    public const int SIZE = 16;

    public IndexEntry(long position, int keyLength, bool isDeleted = false)
    {
        Position = position;
        KeyLength = keyLength;
        IsDeleted = isDeleted ? (byte)1 : (byte)0;
        PageIndex = 0;
    }

    /// <summary>
    /// 不验证 position
    /// </summary>
    public bool IsValidEntry => KeyLength > 0 && IsDeleted == 0;

    /// <summary>
    /// 
    /// </summary>
    public bool IsValidEntryValue => IsValidEntry && Position >= 0;

    public override string ToString()
    {
        return $"IndexEntry: Pos={Position}, KeyLen={KeyLength}, Deleted={IsDeleted}";
    }
}

/// <summary>
/// 索引空间头信息结构 - 固定64字节
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct IndexHeader
{
    public uint Magic;                      // 4 bytes - 魔法数字
    public byte Version;                    // 1 byte - 版本号
    public byte IndexPageCount;             // 1 byte - 当前索引页数量
    public byte MaxIndexPages;              // 1 byte - 最大索引页数量 (32)
    public long TotalIndexSize;             // 8 bytes - 总索引大小（字节）
    public long IndexRegionStartPos;        // 8 bytes - 索引区域起始位置
                                            // total = 23 bytes

    public long CreatedTime;                // 8 bytes - 创建时间
    public long LastRebuildTime;            // 8 bytes - 最后重建时间  
    public uint TotalIndexEntries;          // 4 bytes - 总索引条目数
    public uint ActiveIndexEntries;         // 4 bytes - 活跃索引条目数
    public uint DeletedIndexEntries;        // 4 bytes - 已删除索引条目数
                                            // total = 51 bytes

    public int Reserved3;                   // 4
    public byte Reserved2;                  // 1
    public long Reserved1;                  // 8 bytes - 保留字段
                                            // 64 - 51 = 13 bytes
                                            // Total: 64 bytes

    public const uint MAGIC_NUMBER = 0x49445848; // "IDXH" - Index Header
    public const byte CURRENT_VERSION = 1;
    public const byte MAX_INDEX_PAGES = 32;
    public const int SIZE = 64;

    public static IndexHeader Create()
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return new IndexHeader
        {
            Magic = MAGIC_NUMBER,
            Version = CURRENT_VERSION,
            IndexPageCount = 0,
            MaxIndexPages = MAX_INDEX_PAGES,
            TotalIndexSize = 0,
            IndexRegionStartPos = 0,
            CreatedTime = now,
            LastRebuildTime = now,
            TotalIndexEntries = 0,
            ActiveIndexEntries = 0,
            DeletedIndexEntries = 0,
            Reserved1 = 0,
            Reserved2 = 0
        };
    }

    public readonly bool IsValid => Magic == MAGIC_NUMBER &&
                                   Version <= CURRENT_VERSION &&
                                   IndexPageCount <= MaxIndexPages;


    public void UpdateStats(uint totalEntries, uint activeEntries, uint deletedEntries)
    {
        TotalIndexEntries = totalEntries;
        ActiveIndexEntries = activeEntries;
        DeletedIndexEntries = deletedEntries;
    }

    public void UpdateRebuildTime()
    {
        LastRebuildTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }

    public readonly double IndexUtilization => TotalIndexEntries > 0 ?
        (double)ActiveIndexEntries / TotalIndexEntries : 0.0;

    public override readonly string ToString()
    {
        return $"IndexHeader v{Version}, " +
               $"Pages: {IndexPageCount}/{MaxIndexPages}, " +
               $"Entries: {ActiveIndexEntries}/{TotalIndexEntries} " +
               $"(Util: {IndexUtilization:P1})";
    }
}