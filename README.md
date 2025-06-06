# UltraKV

UltraKV is a high-performance key-value store designed for fast data retrieval and storage. It supports various data types and provides efficient indexing mechanisms to ensure quick access to data.

## Features

- High-performance key-value storage
- max value length = int.MaxValue = ~2.1 GB

## Testing

```txt
UltraKV Performance Benchmark - With Delete & Shrink Tests
Started at: 2025-06-05 07:32:56 UTC
Warming up...

=== Write Performance ===
Write 50,000 records: 64ms
Write Performance: 781,250 ops/sec
After Write - Records: 51000, Deleted: 0, File: 3675.4 KB, Deletion ratio: 0.0%, Shrink recommended: False

=== Update Existing Keys Performance ===
Update 1,000 existing keys: 440ms
Update Performance: 2,273 ops/sec

=== Read Performance ===
Read 50,000 records: 1286ms
Read Performance: 38,880 ops/sec

=== Single Delete Performance ===
Single Delete 12,500 records: 4ms
Delete Performance: 3,125,000 ops/sec

=== Verifying Single Deletes ===
Verified 500 deleted keys, 500 actually deleted
After Single Delete - Records: 52000, Deleted: 12500, File: 6623.2 KB, Deletion ratio: 24.0%, Shrink recommended: True

=== Batch Delete Performance ===
Batch delete 12,500 records: 5ms
Batch Delete Performance: 2,500,000 ops/sec
After Batch Delete - Records: 52000, Deleted: 25000, File: 6935.1 KB, Deletion ratio: 48.1%, Shrink recommended: True

=== ContainsKey Performance ===
ContainsKey 50,000 checks: 2ms
ContainsKey Performance: 25,000,000 ops/sec
Found 25,000 existing keys (expected: ~25,000)

=== GetAllKeys Performance ===
GetAllKeys returned 26,000 keys in 1ms

=== Database Shrink Test ===
Before Shrink - Records: 52000, Deleted: 25000, File: 6935.1 KB, Deletion ratio: 48.1%, Shrink recommended: True
[07:32:58] Starting database shrink...
[07:32:58] Processed 77000 records, kept 26000 valid records
[07:32:58] Shrink completed: 5069.1 KB saved (73.1%) in 102ms
Shrink Result: Shrink completed: 5069.1 KB saved (73.1%), 26000/77000 records kept, took 102ms
After Shrink - Records: 26000, Deleted: 0, File: 1866.0 KB, Deletion ratio: 0.0%, Shrink recommended: False

=== Verifying Data After Shrink ===
Verified 1000 keys after shrink, 0 errors found

=== Update Deleted Keys Performance ===
Update 10,000 previously deleted keys: 3ms
Update Performance: 3,333,333 ops/sec
Verified 1000/1000 updated keys

=== Random Access Test ===
Random read 5,000 records: 96ms
Random Read Performance: 52,083 ops/sec

=== Multi-Engine Test ===
Created 50 engines with 100 ops each: 49ms
Multi-Engine Performance: 102,041 ops/sec

=== Clear Performance Test ===
Before Clear - Records: 5000, File: 191.9 KB
Clear 5,000 records: 1ms
After Clear - Records: 5000, Deleted: 5000, File: 319.9 KB, Deletion ratio: 100.0%, Shrink recommended: True

=== Async Shrink Test ===
[07:32:59] Starting database shrink...
[07:32:59] Processed 8080 records, kept 0 valid records
[07:32:59] Shrink completed: 319.9 KB saved (100.0%) in 26ms
Async Shrink Result: Shrink completed: 319.9 KB saved (100.0%), 0/8080 records kept, took 26ms

=== Final Stats ===
Final Engine Stats: Records: 36000, Deleted: 0, File: 2519.2 KB, Deletion ratio: 0.0%, Shrink recommended: False
Records: 36,000
Deleted: 0
Index Size: 36,000
File Size: 2.46 MB
Deletion Ratio: 0.0%
Shrink Recommended: False
Memory Usage: 7.07 MB

Completed at: 2025-06-05 07:32:59 UTC

=== Performance Summary ===
Write Performance: 781,250 ops/sec
Read Performance: 38,880 ops/sec
Delete Performance: 3,125,000 ops/sec
Batch Delete Performance: 2,500,000 ops/sec
ContainsKey Performance: 25,000,000 ops/sec
Update Performance: 3,333,333 ops/sec
Random Read Performance: 52,083 ops/sec
```