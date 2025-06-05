using System.Runtime.InteropServices;
using System.Text;

namespace UltraKV;

// 记录头
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public unsafe struct RecordHeader
{
    public uint KeyLen;         // 4 bytes - 键长度
    public uint ValueLen;       // 4 bytes - 值长度
    public long Timestamp;      // 8 bytes - 时间戳
    public byte IsDeleted;      // 1 byte - 删除标记

    public const int SIZE = 17;

    public RecordHeader(uint keyLen, uint valueLen, bool isDeleted = false)
    {
        KeyLen = keyLen;
        ValueLen = valueLen;
        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        IsDeleted = isDeleted ? (byte)1 : (byte)0;
    }
}

// 零拷贝记录结构
public readonly unsafe struct Record : IDisposable
{
    private readonly byte* _data;
    private readonly int _totalSize;

    public Record(string key, string value, bool isDeleted = false)
    {
        var keyBytes = Encoding.UTF8.GetBytes(key);
        var valueBytes = Encoding.UTF8.GetBytes(value);

        _totalSize = RecordHeader.SIZE + keyBytes.Length + valueBytes.Length;
        _data = (byte*)Marshal.AllocHGlobal(_totalSize);

        var header = new RecordHeader((uint)keyBytes.Length, (uint)valueBytes.Length, isDeleted);

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

    public Record(byte* data, int totalSize)
    {
        _data = data;
        _totalSize = totalSize;
    }

    public RecordHeader Header => *(RecordHeader*)_data;
    public int TotalSize => _totalSize;
    public bool IsValid => _data != null && _totalSize >= RecordHeader.SIZE;

    public string GetKey()
    {
        if (!IsValid) return string.Empty;
        var header = Header;
        return Encoding.UTF8.GetString(_data + RecordHeader.SIZE, (int)header.KeyLen);
    }

    public string GetValue()
    {
        if (!IsValid) return string.Empty;
        var header = Header;
        return Encoding.UTF8.GetString(_data + RecordHeader.SIZE + header.KeyLen, (int)header.ValueLen);
    }

    public ReadOnlySpan<byte> GetKeyBytes()
    {
        if (!IsValid) return ReadOnlySpan<byte>.Empty;
        var header = Header;
        return new ReadOnlySpan<byte>(_data + RecordHeader.SIZE, (int)header.KeyLen);
    }

    public ReadOnlySpan<byte> GetValueBytes()
    {
        if (!IsValid) return ReadOnlySpan<byte>.Empty;
        var header = Header;
        return new ReadOnlySpan<byte>(_data + RecordHeader.SIZE + header.KeyLen, (int)header.ValueLen);
    }

    public ReadOnlySpan<byte> GetRawData()
    {
        if (!IsValid) return ReadOnlySpan<byte>.Empty;
        return new ReadOnlySpan<byte>(_data, _totalSize);
    }

    public void Dispose()
    {
        if (_data != null)
            Marshal.FreeHGlobal((IntPtr)_data);
    }
}