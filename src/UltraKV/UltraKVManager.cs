using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace UltraKV;

public class UltraKVManager : IDisposable
{
    private readonly string _basePath;
    private readonly ConcurrentDictionary<string, UltraKVEngine> _engines;
    private readonly ReaderWriterLockSlim _createLock;
    private volatile bool _disposed;

    public UltraKVManager(string basePath)
    {
        _basePath = basePath;
        _engines = new ConcurrentDictionary<string, UltraKVEngine>();
        _createLock = new ReaderWriterLockSlim();
        Directory.CreateDirectory(basePath);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public UltraKVEngine GetEngine(string name)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(UltraKVManager));

        return _engines.GetOrAdd(name, CreateEngine);
    }

    private UltraKVEngine CreateEngine(string name)
    {
        _createLock.EnterWriteLock();
        try
        {
            // 双重检查锁定
            if (_engines.TryGetValue(name, out var existing))
                return existing;

            var path = Path.Combine(_basePath, $"{name}.ultradb");
            return new UltraKVEngine(path);
        }
        finally
        {
            _createLock.ExitWriteLock();
        }
    }

    public void FlushAll()
    {
        if (_disposed) return;

        Parallel.ForEach(_engines.Values, engine =>
        {
            try
            {
                engine.Flush();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error flushing engine: {ex.Message}");
            }
        });
    }

    public void CloseEngine(string name)
    {
        if (_engines.TryRemove(name, out var engine))
        {
            engine.Dispose();
        }
    }

    public IEnumerable<string> GetEngineNames()
    {
        return _engines.Keys.ToList();
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;

            // 并行释放所有引擎
            Parallel.ForEach(_engines.Values, engine =>
            {
                try
                {
                    engine.Dispose();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error disposing engine: {ex.Message}");
                }
            });

            _engines.Clear();
            _createLock?.Dispose();
        }
    }
}