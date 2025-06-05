using System.Buffers;

namespace UltraKV;

public unsafe class FastFileReader : IDisposable
{
    private readonly FileStream _file;
    private readonly ArrayPool<byte> _pool;
    private byte[]? _rentedBuffer;
    private readonly int _bufferSize;

    public FastFileReader(string path, int bufferSize = 64 * 1024)
    {
        _file = new FileStream(path, FileMode.Open, FileAccess.Read,
            FileShare.ReadWrite, 0, FileOptions.SequentialScan);
        _pool = ArrayPool<byte>.Shared;
        _bufferSize = bufferSize;
    }

    public bool TryReadAt(long position, int length, out string key, out string value, out bool isDeleted)
    {
        key = "";
        value = "";
        isDeleted = false;

        if (_rentedBuffer == null || _rentedBuffer.Length < length)
        {
            if (_rentedBuffer != null)
                _pool.Return(_rentedBuffer);
            _rentedBuffer = _pool.Rent(Math.Max(length, _bufferSize));
        }

        _file.Seek(position, SeekOrigin.Begin);
        var bytesRead = _file.Read(_rentedBuffer, 0, length);

        if (bytesRead != length)
            return false;

        fixed (byte* ptr = _rentedBuffer)
        {
            var header = *(RecordHeader*)ptr;
            isDeleted = header.IsDeleted != 0;

            key = System.Text.Encoding.UTF8.GetString(ptr + RecordHeader.SIZE, (int)header.KeyLen);
            value = System.Text.Encoding.UTF8.GetString(ptr + RecordHeader.SIZE + header.KeyLen, (int)header.ValueLen);
        }

        return true;
    }

    public void Dispose()
    {
        if (_rentedBuffer != null)
        {
            _pool.Return(_rentedBuffer);
            _rentedBuffer = null;
        }
        _file?.Dispose();
    }
}