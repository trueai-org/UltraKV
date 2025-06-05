using System.Runtime.InteropServices;

namespace UltraKV;

public unsafe class FastFileWriter : IDisposable
{
    private readonly FileStream _file;
    private readonly byte* _buffer;
    private readonly int _bufferSize;
    private int _bufferPos;
    private long _filePos;
    private bool _disposed;

    public FastFileWriter(string path, int bufferSize = 64 * 1024)
    {
        _bufferSize = bufferSize;
        _buffer = (byte*)Marshal.AllocHGlobal(bufferSize);
        _bufferPos = 0;

        // 确保目录存在
        Directory.CreateDirectory(Path.GetDirectoryName(path) ?? ".");

        // 使用正确的文件共享模式
        _file = new FileStream(path, FileMode.Append, FileAccess.Write,
            FileShare.ReadWrite, 0, FileOptions.SequentialScan);

        _filePos = _file.Length;
    }

    public long Write(ReadOnlySpan<byte> data)
    {
        var position = _filePos + _bufferPos;

        if (_bufferPos + data.Length > _bufferSize)
        {
            FlushBuffer();
        }

        if (data.Length > _bufferSize)
        {
            // 大数据直接写入
            _file.Write(data);
            _filePos = _file.Position;
        }
        else
        {
            // 写入缓冲区
            fixed (byte* dataPtr = data)
            {
                Buffer.MemoryCopy(dataPtr, _buffer + _bufferPos, data.Length, data.Length);
            }
            _bufferPos += data.Length;
        }

        return position;
    }

    public void FlushBuffer()
    {
        if (_bufferPos > 0)
        {
            var span = new ReadOnlySpan<byte>(_buffer, _bufferPos);
            _file.Write(span);
            _filePos = _file.Position;
            _bufferPos = 0;
        }
    }

    public void Flush()
    {
        FlushBuffer();
        _file.Flush(true);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                FlushBuffer();
                _file?.Dispose();
            }
            catch { }

            if (_buffer != null)
                Marshal.FreeHGlobal((IntPtr)_buffer);
            _disposed = true;
        }
    }
}