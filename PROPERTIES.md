# YCSB-cpp Properties

## Core Workload Properties

These properties control the behavior of the YCSB workload itself.

### Data Model Properties

| Property | Default | Description |
|----------|---------|-------------|
| `table` | `usertable` | The name of the database table to run queries against |
| `fieldcount` | `10` | The number of fields in a record |
| `fieldlength` | `100` | The length of each field in bytes |
| `field_len_dist` | `constant` | Field length distribution: `uniform`, `zipfian`, `constant` |
| `fieldnameprefix` | `field` | Prefix for field names (e.g., field0, field1, ...) |
| `recordcount` | - | Total number of records to load (required for load phase) |
| `operationcount` | - | Total number of operations to execute (required for run phase) |

### Operation Mix Properties

| Property | Default | Description |
|----------|---------|-------------|
| `readproportion` | `0.95` | Proportion of read operations (0.0 to 1.0) |
| `updateproportion` | `0.05` | Proportion of update operations (0.0 to 1.0) |
| `insertproportion` | `0.0` | Proportion of insert operations (0.0 to 1.0) |
| `scanproportion` | `0.0` | Proportion of scan operations (0.0 to 1.0) |
| `readmodifywriteproportion` | `0.0` | Proportion of read-modify-write operations (0.0 to 1.0) |

### Access Pattern Properties

| Property | Default | Description |
|----------|---------|-------------|
| `requestdistribution` | `uniform` | Distribution of request keys: `uniform`, `zipfian`, `latest` |
| `zipfian_const` | - | Zipfian constant for skewed access (optional) |
| `readallfields` | `true` | Whether to read all fields (`true`) or one field (`false`) |
| `writeallfields` | `false` | Whether to write all fields (`true`) or one field (`false`) |

### Scan Properties

| Property | Default | Description |
|----------|---------|-------------|
| `minscanlength` | `1` | Minimum number of records to scan |
| `maxscanlength` | `1000` | Maximum number of records to scan |
| `scanlengthdistribution` | `uniform` | Distribution of scan lengths: `uniform`, `zipfian` |

### Insert Properties

| Property | Default | Description |
|----------|---------|-------------|
| `insertorder` | `hashed` | Order to insert records: `ordered`, `hashed` |
| `insertstart` | `0` | Starting key for inserts |
| `zeropadding` | `1` | Minimum number of digits for zero-padding keys (e.g., 1 = no padding, 2 = 01, 4 = 0001) |

## Runtime Properties

These properties control the runtime behavior of YCSB-cpp.

| Property | Default | Description |
|----------|---------|-------------|
| `threadcount` | `1` | Number of client threads |
| `dbname` | `basic` | Database binding to use. (`-db` flag) |
| `status` | `false` | Whether to print status every 10 seconds. (`-s` flag) |
| `status.interval` | `10` | Status reporting interval in seconds |
| `sleepafterload` | `0` | Sleep time in seconds after load phase |
| `doload` | `false` | Whether to run the load phase. (`-load` flag) |
| `dotransaction` | `false` | Whether to run the transaction phase. (`-run` flag) |

### Rate Limiting Properties

| Property | Default | Description |
|----------|---------|-------------|
| `limit.ops` | `0` | Initial operations per second limit (0 = unlimited) |
| `limit.file` | - | Path to rate file for dynamic rate limiting |

Rate File Format: Each line contains `timestamp_seconds new_ops_per_second`

## Database-Specific Properties

### LevelDB Properties

LevelDB properties are prefixed with `leveldb.`. Example configuration file: `leveldb/leveldb.properties`.

| Property | Default | Description |
|----------|---------|-------------|
| `leveldb.dbname` | - | Database path |
| `leveldb.destroy` | `false` | Destroy database on startup |
| `leveldb.write_buffer_size` | `0` | Write buffer size, set to 0 for leveldb default |
| `leveldb.max_file_size` | `0` | Maximum file size, set to 0 for leveldb default |
| `leveldb.max_open_files` | `0` | Maximum open files, set to 0 for leveldb default |
| `leveldb.compression` | `no` | Compression type: `snappy`, `no` |
| `leveldb.cache_size` | `0` | Block cache size, set to 0 for leveldb default |
| `leveldb.filter_bits` | `0` | Bloom filter bits per key, 0 = disabled |
| `leveldb.block_size` | `0` | Block size, set to 0 for leveldb default |
| `leveldb.block_restart_interval` | `0` | Block restart interval, set to 0 for leveldb default |

### RocksDB Properties

RocksDB properties are prefixed with `rocksdb.`. Example configuration file: `rocksdb/rocksdb.properties`.

#### Basic Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `rocksdb.dbname` | - | Database path |
| `rocksdb.destroy` | `false` | Destroy database on startup |
| `rocksdb.mergeupdate` | `false` | Use merge operator for updates, ycsb should be compiled with `-DUSE_MERGEUPDATE` |
| `rocksdb.optionsfile` | - | Path to RocksDB options file, if specified, properties below will be ignored |

#### Performance Tuning

| Property | Default | Description |
|----------|---------|-------------|
| `rocksdb.compression` | `no` | Compression type: `none`, `snappy`, `lz4`, `zlib`, `zstd` |
| `rocksdb.max_background_jobs` | `0` | Maximum background jobs, 0 = RocksDB default |
| `rocksdb.target_file_size_base` | `0` | Base target file size 0 = RocksDB default |
| `rocksdb.target_file_size_multiplier` | `0` | Target file size multiplier, 0 = RocksDB default |
| `rocksdb.max_bytes_for_level_base` | `0` | Max bytes for level base, 0 = RocksDB default |
| `rocksdb.write_buffer_size` | `0` | Write buffer size, 0 = RocksDB default |
| `rocksdb.max_write_buffer_number` | `0` | Maximum write buffer number, 0 = RocksDB default |
| `rocksdb.max_open_files` | `-1` | Maximum open files (-1 = unlimited) |

#### Compaction Settings

| Property | Default | Description |
|----------|---------|-------------|
| `rocksdb.compaction_pri` | `-1` | Compaction priority, set to -1 for RocksDB default |
| `rocksdb.level0_file_num_compaction_trigger` | `0` | L0 compaction trigger, 0 = RocksDB default |
| `rocksdb.level0_slowdown_writes_trigger` | `0` | L0 slowdown trigger, 0 = RocksDB default |
| `rocksdb.level0_stop_writes_trigger` | `0` | L0 stop trigger, 0 = RocksDB default |

#### I/O Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `rocksdb.use_direct_io_for_flush_compaction` | `false` | Use direct I/O for flush/compaction |
| `rocksdb.use_direct_reads` | `false` | Use direct I/O for reads |
| `rocksdb.allow_mmap_writes` | `false` | Allow memory-mapped writes |
| `rocksdb.allow_mmap_reads` | `false` | Allow memory-mapped reads |

#### Cache Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `rocksdb.cache_size` | `0` | Block cache size, set to 0 for RocksDB default |
| `rocksdb.compressed_cache_size` | `0` | Compressed cache size (deprecated in RocksDB 8.0+) |
| `rocksdb.bloom_bits` | `0` | Bloom filter bits per key (0 = disabled) |

#### Optimization Flags

| Property | Default | Description |
|----------|---------|-------------|
| `rocksdb.increase_parallelism` | `false` | Increase parallelism |
| `rocksdb.optimize_level_style_compaction` | `false` | Optimize for level-style compaction |

### LMDB Properties

LMDB properties are prefixed with `lmdb.`. Example configuration file: `lmdb/lmdb.properties`.

| Property | Default | Description |
|----------|---------|-------------|
| `lmdb.dbpath` | - | Database path |
| `lmdb.mapsize` | `-1` | Map size, -1 = use default |
| `lmdb.nosync` | `true` | Skip fsync for performance |
| `lmdb.nometasync` | `false` | Skip metadata sync |
| `lmdb.noreadahead` | `false` | Disable read-ahead |
| `lmdb.writemap` | `false` | Use writable memory map |
| `lmdb.mapasync` | `false` | Use asynchronous msync |

### WiredTiger Properties

WiredTiger properties are prefixed with `wiredtiger.`. Example configuration file: `wiredtiger/wiredtiger.properties`.

#### Basic Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `wiredtiger.home` | - | Database home directory |
| `wiredtiger.cache_size` | `100MB` | Cache size |
| `wiredtiger.direct_io` | `[]` | Direct I/O configuration |
| `wiredtiger.in_memory` | `false` | In-memory mode |

#### LSM Manager

| Property | Default | Description |
|----------|---------|-------------|
| `wiredtiger.lsm_mgr.merge` | `true` | Enable LSM merge |
| `wiredtiger.lsm_mgr.max_workers` | `4` | Maximum LSM workers |

#### Block Manager

| Property | Default | Description |
|----------|---------|-------------|
| `wiredtiger.blk_mgr.allocation_size` | `4KB` | File allocation size |
| `wiredtiger.blk_mgr.bloom_bit_count` | `16` | Bloom filter bits |
| `wiredtiger.blk_mgr.bloom_hash_count` | `8` | Bloom filter hash count |
| `wiredtiger.blk_mgr.chunk_max` | `5GB` | Maximum chunk size |
| `wiredtiger.blk_mgr.chunk_size` | `10MB` | Chunk size |
| `wiredtiger.blk_mgr.compressor` | `snappy` | Compression: `snappy`, `lz4`, `zlib`, `zstd` |

#### B-Tree Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `wiredtiger.blk_mgr.btree.internal_page_max` | `4KB` | Maximum internal page size |
| `wiredtiger.blk_mgr.btree.leaf_key_max` | `0` | Maximum leaf key size (0 = auto) |
| `wiredtiger.blk_mgr.btree.leaf_value_max` | `0` | Maximum leaf value size (0 = auto) |
| `wiredtiger.blk_mgr.btree.leaf_page_max` | `32KB` | Maximum leaf page size |

### SQLite Properties

SQLite properties are prefixed with `sqlite.`. Example configuration file: `sqlite/sqlite.properties`.

| Property | Default | Description |
|----------|---------|-------------|
| `sqlite.dbpath` | - | Database file path |
| `sqlite.cache_size` | `-2000` | Cache size (negative = KB, positive = pages) |
| `sqlite.page_size` | `4096` | Page size in bytes |
| `sqlite.journal_mode` | `WAL` | Journal mode: `DELETE`, `TRUNCATE`, `PERSIST`, `MEMORY`, `WAL`, `OFF` |
| `sqlite.synchronous` | `NORMAL` | Synchronous mode: `OFF`, `NORMAL`, `FULL`, `EXTRA` |
| `sqlite.create_table` | `true` | Create table if not exists |

## Usage Examples

### Basic Usage

```bash
# Load data
./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties

# Run workload
./ycsb -run -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties
```

### Custom Properties

```bash
# Override default properties
./ycsb -load -db rocksdb -P workloads/workloada \
  -p rocksdb.dbname=/tmp/mydb \
  -p threadcount=4 \
  -p recordcount=1000000
```

### Rate Limiting

```bash
# Limit to 1000 ops/sec
./ycsb -run -db rocksdb -P workloads/workloada \
  -p limit.ops=1000

# Dynamic rate limiting from file
./ycsb -run -db rocksdb -P workloads/workloada \
  -p limit.file=rate_schedule.txt
```

### Status Monitoring

```bash
# Enable status reporting every 5 seconds
./ycsb -run -db rocksdb -P workloads/workloada \
  -s -p status.interval=5
```

## Property File Format

Properties can be specified in files using the format:

```
# Comments start with #
property.name=value
another.property=another_value
```

Load multiple property files:

```bash
./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -P custom.properties
```
