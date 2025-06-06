using System.Diagnostics;

namespace UltraKV
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine("UltraKV Performance Benchmark - With Delete & Shrink Tests");
            Console.WriteLine($"Started at: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");

            // 清理可能存在的文件
            var dataDir = "./ultra_data";
            if (Directory.Exists(dataDir))
            {
                try
                {
                    Directory.Delete(dataDir, true);
                    Thread.Sleep(100); // 等待文件系统释放句柄
                }
                catch { }
            }

            using var manager = new UltraKVManager(dataDir);

            var bigValue = new string('x', 1024 * 1); // ""; // new string('x', 1024 * 4); // 4K 大小的值

            try
            {
                const int WARM_UP = 1000;
                const int ITERATIONS = 50_000; // 减少测试量避免文件锁定
                var sw = Stopwatch.StartNew();

                //========数据库压缩与不压缩测试============
                var engine1 = manager.GetEngine("benchmark_compressed", new UltraKVConfig
                {
                    CompressionType = CompressionType.Gzip,
                    EncryptionType = EncryptionType.AES256GCM,
                    EncryptionKey = "MyFixedTestKey12345678901234567890"
                });
                var engine2 = manager.GetEngine("benchmark_uncompressed", new UltraKVConfig
                {
                    CompressionType = CompressionType.Brotli
                });

                // 显示初始统计信息
                Console.WriteLine("=== Initial Stats ===");
                Console.WriteLine($"Compressed Engine Stats: {engine1.GetStats()}");
                Console.WriteLine($"Uncompressed Engine Stats: {engine2.GetStats()}");

                // 预热
                Console.WriteLine("Warming up compressed engine...");
                for (int i = 0; i < 1000; i++)
                {
                    engine1.Put($"warmup_{i}", $"value_{bigValue}{i}");
                    var x1 = engine1.Get($"warmup_{i}"); // 预热读取
                }
                engine1.Flush();
                Console.WriteLine("Warming up uncompressed engine...");

                for (int i = 0; i < 1000; i++)
                {
                    engine2.Put($"warmup_{i}", $"value_{bigValue}{i}");
                    var x1 = engine2.Get($"warmup_{i}"); // 预热读取
                }
                engine2.Flush();

                Console.WriteLine($"Compressed Engine Stats after warmup: {engine1.GetStats()}");
                Console.WriteLine($"Uncompressed Engine Stats after warmup: {engine2.GetStats()}");

                // 压缩测试
                Console.WriteLine("\n=== Write Performance (Compressed) ===");
                sw.Start();
                for (int i = 0; i < ITERATIONS; i++)
                {
                    engine1.Put($"key_{i}", $"{bigValue}test_value_number_{i}_with_some_additional_data");
                    // 每1000次操作刷写一次
                    if (i % 1000 == 0)
                    {
                        engine1.Flush();
                    }
                }
                engine1.Flush(); // 最终刷写
                sw.Stop();

                var writeOpsPerSecCompressed = ITERATIONS * 1000.0 / sw.ElapsedMilliseconds;
                Console.WriteLine($"Compressed Write {ITERATIONS:N0} records: {sw.ElapsedMilliseconds}ms");
                Console.WriteLine($"Compressed Write Performance: {writeOpsPerSecCompressed:N0} ops/sec");
                Console.WriteLine($"After Compressed Write - {engine1.GetStats()}");

                // 不压缩测试
                Console.WriteLine("\n=== Write Performance (Uncompressed) ===");
                sw.Restart();
                for (int i = 0; i < ITERATIONS; i++)
                {
                    engine2.Put($"key_{i}", $"{bigValue}test_value_number_{i}_with_some_additional_data");
                    // 每1000次操作刷写一次
                    if (i % 1000 == 0)
                    {
                        engine2.Flush();
                    }
                }
                engine2.Flush(); // 最终刷写
                sw.Stop();
                var writeOpsPerSecUncompressed = ITERATIONS * 1000.0 / sw.ElapsedMilliseconds;
                Console.WriteLine($"Uncompressed Write {ITERATIONS:N0} records: {sw.ElapsedMilliseconds}ms");
                Console.WriteLine($"Uncompressed Write Performance: {writeOpsPerSecUncompressed:N0} ops/sec");
                Console.WriteLine($"After Uncompressed Write - {engine2.GetStats()}");

                //=================================

                var engine = manager.GetEngine("benchmark");

                // 预热
                Console.WriteLine("Warming up...");
                for (int i = 0; i < WARM_UP; i++)
                {
                    engine.Put($"warmup_{i}", $"value_{bigValue}{i}");
                }

                Console.WriteLine("\n=== Write Performance ===");

                for (int i = 0; i < ITERATIONS; i++)
                {
                    engine.Put($"key_{i}", $"{bigValue}test_value_number_{i}_with_some_additional_data");

                    // 每1000次操作刷写一次
                    if (i % 1000 == 0)
                    {
                        engine.Flush();
                    }
                }

                engine.Flush(); // 最终刷写
                sw.Stop();

                var writeOpsPerSec = ITERATIONS * 1000.0 / sw.ElapsedMilliseconds;
                Console.WriteLine($"Write {ITERATIONS:N0} records: {sw.ElapsedMilliseconds}ms");
                Console.WriteLine($"Write Performance: {writeOpsPerSec:N0} ops/sec");

                // 显示写入后的统计信息
                var statsAfterWrite = engine.GetStats();
                Console.WriteLine($"After Write - {statsAfterWrite}");

                // 对同一个key进行多次写入测试
                Console.WriteLine("\n=== Update Existing Keys Performance ===");
                sw.Restart();
                for (int i = 0; i < WARM_UP; i++)
                {
                    // 随机1k - 4k的字符串作为值
                    var randomValueString = new string('y', new Random().Next(1024, 4096));
                    engine.Put($"key_0", $"{randomValueString}updated_value_for_key_{i}");
                    engine.Flush();
                }

                var key0value = engine.Get("key_0");

                sw.Stop();
                var updateOpsPerSec1 = WARM_UP * 1000.0 / sw.ElapsedMilliseconds;
                Console.WriteLine($"Update {WARM_UP:N0} existing keys: {sw.ElapsedMilliseconds}ms");
                Console.WriteLine($"Update Performance: {updateOpsPerSec1:N0} ops/sec");

                Console.WriteLine("\n=== Read Performance ===");
                sw.Restart();

                for (int i = 0; i < ITERATIONS; i++)
                {
                    var value = engine.Get($"key_{i}");
                    if (value == null && i < ITERATIONS) // 简单验证
                    {
                        Console.WriteLine($"Warning: Missing value for key_{i}");
                    }
                }

                engine.Flush();
                sw.Stop();
                var readOpsPerSec = ITERATIONS * 1000.0 / sw.ElapsedMilliseconds;
                Console.WriteLine($"Read {ITERATIONS:N0} records: {sw.ElapsedMilliseconds}ms");
                Console.WriteLine($"Read Performance: {readOpsPerSec:N0} ops/sec");

                // =================== 新增：删除操作测试 ===================
                Console.WriteLine("\n=== Single Delete Performance ===");
                var deleteCount = ITERATIONS / 2; // 删除50%的记录
                sw.Restart();

                var deletedKeys = new List<string>();
                for (int i = 0; i < deleteCount; i += 2) // 删除偶数索引的键
                {
                    var key = $"key_{i}";
                    var deleted = engine.Delete(key);
                    if (deleted)
                    {
                        deletedKeys.Add(key);
                    }
                    else
                    {
                        Console.WriteLine($"Warning: Failed to delete {key}");
                    }
                }

                engine.Flush(); // 最终刷写
                sw.Stop();
                var deleteOpsPerSec = deletedKeys.Count * 1000.0 / sw.ElapsedMilliseconds;
                Console.WriteLine($"Single Delete {deletedKeys.Count:N0} records: {sw.ElapsedMilliseconds}ms");
                Console.WriteLine($"Delete Performance: {deleteOpsPerSec:N0} ops/sec");

                // 验证删除结果
                Console.WriteLine("\n=== Verifying Single Deletes ===");
                var verifyCount = 0;
                var actuallyDeleted = 0;

                for (int i = 0; i < Math.Min(1000, deleteCount); i += 2)
                {
                    var value = engine.Get($"key_{i}");
                    if (value == null)
                        actuallyDeleted++;
                    verifyCount++;
                }

                Console.WriteLine($"Verified {verifyCount} deleted keys, {actuallyDeleted} actually deleted");

                var statsAfterSingleDelete = engine.GetStats();
                Console.WriteLine($"After Single Delete - {statsAfterSingleDelete}");

                // =================== 新增：批量删除测试 ===================
                Console.WriteLine("\n=== Batch Delete Performance ===");
                var batchKeys = new List<string>();
                for (int i = 1; i < deleteCount && i < ITERATIONS; i += 2) // 删除奇数索引的键
                {
                    batchKeys.Add($"key_{i}");
                }

                sw.Restart();
                var batchDeleted = engine.DeleteBatch(batchKeys);
                engine.Flush();
                sw.Stop();

                var batchDeleteOpsPerSec = batchDeleted * 1000.0 / sw.ElapsedMilliseconds;
                Console.WriteLine($"Batch delete {batchDeleted:N0} records: {sw.ElapsedMilliseconds}ms");
                Console.WriteLine($"Batch Delete Performance: {batchDeleteOpsPerSec:N0} ops/sec");

                var statsAfterBatchDelete = engine.GetStats();
                Console.WriteLine($"After Batch Delete - {statsAfterBatchDelete}");

                // =================== 新增：ContainsKey 测试 ===================
                Console.WriteLine("\n=== ContainsKey Performance ===");
                sw.Restart();

                var existCount = 0;
                for (int i = 0; i < ITERATIONS; i++)
                {
                    if (engine.ContainsKey($"key_{i}"))
                        existCount++;
                }

                engine.Flush();
                sw.Stop();
                var containsOpsPerSec = ITERATIONS * 1000.0 / sw.ElapsedMilliseconds;
                Console.WriteLine($"ContainsKey {ITERATIONS:N0} checks: {sw.ElapsedMilliseconds}ms");
                Console.WriteLine($"ContainsKey Performance: {containsOpsPerSec:N0} ops/sec");
                Console.WriteLine($"Found {existCount:N0} existing keys (expected: ~{ITERATIONS - deletedKeys.Count - batchDeleted:N0})");

                // =================== 新增：GetAllKeys 测试 ===================
                Console.WriteLine("\n=== GetAllKeys Performance ===");
                sw.Restart();
                var allKeys = engine.GetAllKeys().ToList();
                engine.Flush();
                sw.Stop();

                Console.WriteLine($"GetAllKeys returned {allKeys.Count:N0} keys in {sw.ElapsedMilliseconds}ms");

                // =================== 新增：收缩测试 ===================
                if (engine.ShouldShrink())
                {
                    Console.WriteLine("\n=== Database Shrink Test ===");
                    var statsBeforeShrink = engine.GetStats();
                    Console.WriteLine($"Before Shrink - {statsBeforeShrink}");

                    sw.Restart();
                    engine.Shrink(); //  var shrinkResult =
                    sw.Stop();

                    Console.WriteLine($"Shrink Result: {engine.GetStats()}");

                    var statsAfterShrink = engine.GetStats();
                    Console.WriteLine($"After Shrink - {statsAfterShrink}");

                    // 验证收缩后数据完整性
                    Console.WriteLine("\n=== Verifying Data After Shrink ===");
                    var verifyErrors = 0;
                    var sampleSize = Math.Min(1000, allKeys.Count);

                    for (int i = 0; i < sampleSize; i++)
                    {
                        var key = allKeys[i];
                        var value = engine.Get(key);
                        if (value == null)
                        {
                            verifyErrors++;
                            if (verifyErrors <= 5) // 只显示前5个错误
                            {
                                Console.WriteLine($"Error: Missing value for key {key} after shrink");
                            }
                        }
                    }

                    Console.WriteLine($"Verified {sampleSize} keys after shrink, {verifyErrors} errors found");
                }
                else
                {
                    Console.WriteLine("\n=== Shrink Not Needed ===");
                    Console.WriteLine($"Deletion ratio too low for shrinking (current: {engine.GetStats()})");
                }

                // =================== 新增：更新已删除键的测试 ===================
                Console.WriteLine("\n=== Update Deleted Keys Performance ===");
                var updateKeys = deletedKeys.Take(10000).ToList();
                sw.Restart();

                foreach (var key in updateKeys)
                {
                    engine.Put(key, $"{bigValue}updated_value_for_{key}_after_deletion");
                }
                engine.Flush();
                sw.Stop();
                var updateOpsPerSec = updateKeys.Count * 1000.0 / sw.ElapsedMilliseconds;
                Console.WriteLine($"Update {updateKeys.Count:N0} previously deleted keys: {sw.ElapsedMilliseconds}ms");
                Console.WriteLine($"Update Performance: {updateOpsPerSec:N0} ops/sec");

                // 验证更新结果
                var updateVerifyCount = 0;
                for (int i = 0; i < Math.Min(1000, updateKeys.Count); i++)
                {
                    var value = engine.Get(updateKeys[i]);
                    if (value != null && value.Contains("updated_value"))
                        updateVerifyCount++;
                }
                Console.WriteLine($"Verified {updateVerifyCount}/1000 updated keys");

                Console.WriteLine("\n=== Random Access Test ===");
                var random = new Random(42);
                var randomTests = ITERATIONS / 10;
                sw.Restart();

                for (int i = 0; i < randomTests; i++)
                {
                    var key = $"key_{random.Next(ITERATIONS)}";
                    var value = engine.Get(key);
                }

                sw.Stop();
                var randomReadOpsPerSec = randomTests * 1000.0 / sw.ElapsedMilliseconds;
                Console.WriteLine($"Random read {randomTests:N0} records: {sw.ElapsedMilliseconds}ms");
                Console.WriteLine($"Random Read Performance: {randomReadOpsPerSec:N0} ops/sec");

                Console.WriteLine("\n=== Multi-Engine Test ===");
                sw.Restart();

                // 顺序创建引擎以避免文件冲突
                for (int i = 0; i < 50; i++)
                {
                    var db = manager.GetEngine($"db_{i}");
                    for (int j = 0; j < 100; j++)
                    {
                        db.Put($"key_{j}", $"{bigValue}value_{i}_{j}");
                    }
                    db.Flush();
                }

                sw.Stop();
                Console.WriteLine($"Created 50 engines with 100 ops each: {sw.ElapsedMilliseconds}ms");
                Console.WriteLine($"Multi-Engine Performance: {5000 * 1000.0 / sw.ElapsedMilliseconds:N0} ops/sec");

                // =================== 新增：Clear 测试 ===================
                Console.WriteLine("\n=== Clear Performance Test ===");
                var clearEngine = manager.GetEngine("clear_test");

                // 先写入一些数据
                for (int i = 0; i < 5000; i++)
                {
                    clearEngine.Put($"clear_key_{i}", $"{bigValue}clear_value_{i}");
                }

                var statsBeforeClear = clearEngine.GetStats();
                Console.WriteLine($"Before Clear - Records: {statsBeforeClear}");

                sw.Restart();
                clearEngine.Clear();
                sw.Stop();

                var statsAfterClear = clearEngine.GetStats();
                Console.WriteLine($"Clear 5,000 records: {sw.ElapsedMilliseconds}ms");
                Console.WriteLine($"After Clear - {statsAfterClear}");

                //// =================== 新增：异步收缩测试 ===================
                //if (clearEngine.ShouldShrink())
                //{
                //    Console.WriteLine("\n=== Async Shrink Test ===");
                //    sw.Restart();
                //    var asyncShrinkResult = await clearEngine.ShrinkAsync();
                //    sw.Stop();

                //    Console.WriteLine($"Async Shrink Result: {asyncShrinkResult}");
                //}

                //// 最终统计信息
                //var finalStats = engine.GetStats();
                //Console.WriteLine("\n=== Final Stats ===");
                //Console.WriteLine($"Final Engine Stats: {finalStats}");
                //Console.WriteLine($"Records: {finalStats.RecordCount:N0}");
                //Console.WriteLine($"Deleted: {finalStats.DeletedCount:N0}");
                //Console.WriteLine($"Index Size: {finalStats.IndexSize:N0}");
                //Console.WriteLine($"File Size: {finalStats.FileSize / 1024.0 / 1024.0:F2} MB");
                //Console.WriteLine($"Deletion Ratio: {finalStats.DeletionRatio:P1}");
                //Console.WriteLine($"Shrink Recommended: {finalStats.ShrinkRecommended}");

                // 内存使用
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                var memUsage = GC.GetTotalMemory(false);
                Console.WriteLine($"Memory Usage: {memUsage / 1024.0 / 1024.0:F2} MB");

                Console.WriteLine($"\nCompleted at: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");

                // =================== 新增：性能总结 ===================
                Console.WriteLine("\n=== Performance Summary ===");
                Console.WriteLine($"Write Performance: {writeOpsPerSec:N0} ops/sec");
                Console.WriteLine($"Read Performance: {readOpsPerSec:N0} ops/sec");
                Console.WriteLine($"Delete Performance: {deleteOpsPerSec:N0} ops/sec");
                Console.WriteLine($"Batch Delete Performance: {batchDeleteOpsPerSec:N0} ops/sec");
                Console.WriteLine($"ContainsKey Performance: {containsOpsPerSec:N0} ops/sec");
                Console.WriteLine($"Update Performance: {updateOpsPerSec:N0} ops/sec");
                Console.WriteLine($"Random Read Performance: {randomReadOpsPerSec:N0} ops/sec");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }
    }
}