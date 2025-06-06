using System.Diagnostics;

namespace UltraKV;

/// <summary>
/// 垃圾回收管理器
/// </summary>
public class GcManager : IDisposable
{
    private readonly UltraKVEngine _engine;
    private readonly DatabaseHeader _header;
    private readonly Timer? _flushTimer;
    private readonly object _gcLock = new object();
    private bool _disposed;
    private DateTime _lastGcTime = DateTime.MinValue;

    public GcManager(UltraKVEngine engine, DatabaseHeader header)
    {
        _engine = engine;
        _header = header;

        // 如果配置了刷盘间隔，启动定时器
        var flushInterval = _header.GetActualGcFlushInterval();
        if (flushInterval > 0)
        {
            _flushTimer = new Timer(OnFlushTimer, null, flushInterval, flushInterval);
        }
    }

    /// <summary>
    /// 检查是否应该触发GC
    /// </summary>
    public bool ShouldTriggerGC(bool forceGc = false)
    {
        var stats = _engine.GetStats();
        return _header.ShouldGC(stats.TotalFileSize, stats.WastedSpace, stats.ValidRecordCount, forceGc);
    }

    /// <summary>
    /// 执行垃圾回收
    /// </summary>
    public GcResult PerformGC(bool forceGc = false)
    {
        lock (_gcLock)
        {
            if (!ShouldTriggerGC(forceGc) && !forceGc)
            {
                return new GcResult
                {
                    Success = false,
                    Reason = "GC conditions not met",
                    ElapsedMilliseconds = 0
                };
            }

            var stopwatch = Stopwatch.StartNew();
            var statsBefore = _engine.GetStats();

            try
            {
                // 执行数据库收缩操作
                _engine.Shrink();

                var statsAfter = _engine.GetStats();
                stopwatch.Stop();

                _lastGcTime = DateTime.UtcNow;

                return new GcResult
                {
                    Success = true,
                    Reason = forceGc ? "Force GC" : "Auto GC",
                    OriginalFileSize = statsBefore.TotalFileSize,
                    NewFileSize = statsAfter.TotalFileSize,
                    SpaceSaved = statsBefore.TotalFileSize - statsAfter.TotalFileSize,
                    SpaceSavedPercentage = statsBefore.TotalFileSize > 0
                        ? (double)(statsBefore.TotalFileSize - statsAfter.TotalFileSize) / statsBefore.TotalFileSize * 100.0
                        : 0.0,
                    ValidRecords = statsAfter.ValidRecordCount,
                    TotalRecordsProcessed = statsBefore.ValidRecordCount,
                    FreeSpaceBefore = statsBefore.WastedSpace,
                    FreeSpaceAfter = statsAfter.WastedSpace,
                    ElapsedMilliseconds = stopwatch.ElapsedMilliseconds
                };
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                return new GcResult
                {
                    Success = false,
                    Reason = $"GC failed: {ex.Message}",
                    ElapsedMilliseconds = stopwatch.ElapsedMilliseconds
                };
            }
        }
    }

    /// <summary>
    /// 执行强制GC
    /// </summary>
    public GcResult ForceGC()
    {
        return PerformGC(forceGc: true);
    }

    /// <summary>
    /// 检查是否可以进行自动GC
    /// </summary>
    public bool CanAutoGC()
    {
        if (!_header.IsGcAutoRecycleEnabled)
            return false;

        // 避免频繁GC，至少间隔1分钟
        var timeSinceLastGc = DateTime.UtcNow - _lastGcTime;
        return timeSinceLastGc.TotalMinutes >= 1.0;
    }

    /// <summary>
    /// 尝试自动GC
    /// </summary>
    public GcResult? TryAutoGC()
    {
        if (!CanAutoGC())
            return null;

        if (!ShouldTriggerGC())
            return null;

        return PerformGC();
    }

    private void OnFlushTimer(object? state)
    {
        try
        {
            // 执行定期刷盘
            _engine.Flush();

            // 检查是否需要自动GC
            if (_header.IsGcAutoRecycleEnabled)
            {
                TryAutoGC();
            }
        }
        catch (Exception ex)
        {
            // 记录日志但不抛出异常
            Console.WriteLine($"Flush timer error: {ex.Message}");
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _flushTimer?.Dispose();
            _disposed = true;
        }
    }
}

/// <summary>
/// GC结果
/// </summary>
public struct GcResult
{
    public bool Success;
    public string Reason;
    public long OriginalFileSize;
    public long NewFileSize;
    public long SpaceSaved;
    public double SpaceSavedPercentage;
    public int ValidRecords;
    public int TotalRecordsProcessed;
    public long FreeSpaceBefore;
    public long FreeSpaceAfter;
    public long ElapsedMilliseconds;

    public override readonly string ToString()
    {
        if (!Success)
        {
            return $"GC Failed: {Reason} (took {ElapsedMilliseconds}ms)";
        }

        return $"GC Completed: {SpaceSaved / 1024.0:F1}KB saved ({SpaceSavedPercentage:F1}%), " +
               $"{ValidRecords}/{TotalRecordsProcessed} records kept, " +
               $"File: {OriginalFileSize / 1024.0:F1}KB -> {NewFileSize / 1024.0:F1}KB, " +
               $"Free: {FreeSpaceBefore / 1024.0:F1}KB -> {FreeSpaceAfter / 1024.0:F1}KB, " +
               $"took {ElapsedMilliseconds}ms ({Reason})";
    }
}