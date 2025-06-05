using System.Runtime.InteropServices;
using System.Text;

namespace UltraKV;

// 内存对齐的记录头
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public unsafe struct RecordHeader
{
    public uint KeyLen;      // 4 bytes
    public uint ValueLen;    // 4 bytes
    public long Timestamp;   // 8 bytes
    public byte IsDeleted;   // 1 byte
                             // Total: 17 bytes

    public const int SIZE = 17;
}

// 零拷贝记录结构
public readonly unsafe struct Record
{
    private readonly byte* _data;
    private readonly int _totalSize;

    public Record(string key, string value, bool isDeleted = false)
    {
        var keyBytes = Encoding.UTF8.GetBytes(key);
        var valueBytes = Encoding.UTF8.GetBytes(value);

        _totalSize = RecordHeader.SIZE + keyBytes.Length + valueBytes.Length;
        _data = (byte*)Marshal.AllocHGlobal(_totalSize);

        var header = new RecordHeader
        {
            KeyLen = (uint)keyBytes.Length,
            ValueLen = (uint)valueBytes.Length,
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            IsDeleted = isDeleted ? (byte)1 : (byte)0
        };

        // 写入头部
        *(RecordHeader*)_data = header;

        // 写入键值
        fixed (byte* keyPtr = keyBytes)
        fixed (byte* valuePtr = valueBytes)
        {
            Buffer.MemoryCopy(keyPtr, _data + RecordHeader.SIZE, keyBytes.Length, keyBytes.Length);
            Buffer.MemoryCopy(valuePtr, _data + RecordHeader.SIZE + keyBytes.Length, valueBytes.Length, valueBytes.Length);
        }
    }

    public RecordHeader Header => *(RecordHeader*)_data;
    public int TotalSize => _totalSize;

    public string GetKey()
    {
        var header = Header;
        return Encoding.UTF8.GetString(_data + RecordHeader.SIZE, (int)header.KeyLen);
    }

    public string GetValue()
    {
        var header = Header;
        return Encoding.UTF8.GetString(_data + RecordHeader.SIZE + header.KeyLen, (int)header.ValueLen);
    }

    public ReadOnlySpan<byte> GetRawData()
    {
        return new ReadOnlySpan<byte>(_data, _totalSize);
    }

    public void Dispose()
    {
        if (_data != null)
            Marshal.FreeHGlobal((IntPtr)_data);
    }
}