////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "rocksdb_option_feature.h"

#include <rocksdb/advanced_options.h>
#include <rocksdb/cache.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/memory_allocator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/sst_partitioner.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/transaction_db.h>

#include <algorithm>
#include <cstddef>
#include <ios>
#include <limits>
#include <memory>

#include "app/app_server.h"
#include "app/options/option.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "basics/application-exit.h"
#include "basics/logger/logger.h"
#include "basics/number_of_cores.h"
#include "basics/physical_memory.h"
#include "basics/process-utils.h"
#include "basics/system-functions.h"
#include "catalog/table_options.h"
#include "rocksdb_engine_catalog/options.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_prefix_extractor.h"

// It's not atomic because it shouldn't change after initilization.
// And initialization should happen before rocksdb initialization.
static bool gIoUringEnabled = true;

// weak symbol from rocksdb
extern "C" bool RocksDbIOUringEnable() { return gIoUringEnabled; }

using namespace sdb;
using namespace sdb::app;
using namespace sdb::options;

namespace {

const std::string kCompressionTypeLZ4 = "lz4";
const std::string kCompressionTypeLZ4HC = "lz4hc";
const std::string kCompressionTypeNone = "none";

const containers::FlatHashSet<std::string> kCompressionTypes = {
  kCompressionTypeLZ4,
  kCompressionTypeLZ4HC,
  kCompressionTypeNone,
};

rocksdb::CompressionType CompressionTypeFromString(std::string_view type) {
  if (type == kCompressionTypeNone) {
    return rocksdb::kNoCompression;
  }
  if (type == kCompressionTypeLZ4) {
    return rocksdb::kLZ4Compression;
  }
  if (type == kCompressionTypeLZ4HC) {
    return rocksdb::kLZ4HCCompression;
  }
  SDB_ASSERT(false);
  SDB_FATAL("xxxxx", sdb::Logger::STARTUP, "unexpected compression type '",
            type, "'");
}

// types of block cache
const std::string kBlockCacheTypeLRU = "lru";
const std::string kBlockCacheTypeHyperClock = "hyper-clock";

const containers::FlatHashSet<std::string> kBlockCacheTypes = {
  kBlockCacheTypeLRU,
  kBlockCacheTypeHyperClock,
};

// checksum types
const std::string kChecksumTypeCRC32C = "crc32c";
const std::string kChecksumTypeXXHash = "xxHash";
const std::string kChecksumTypeXXHash64 = "xxHash64";
const std::string kChecksumTypeXXH3 = "XXH3";

const containers::FlatHashSet<std::string> kChecksumTypes = {
  kChecksumTypeCRC32C,
  kChecksumTypeXXHash,
  kChecksumTypeXXHash64,
  kChecksumTypeXXH3,
};

// compaction styles
const std::string kCompactionStyleLevel = "level";
const std::string kCompactionStyleUniversal = "universal";
const std::string kCompactionStyleFifo = "fifo";
const std::string kCompactionStyleNone = "none";

const containers::FlatHashSet<std::string> kCompactionStyles = {
  kCompactionStyleLevel,
  kCompactionStyleUniversal,
  kCompactionStyleFifo,
  kCompactionStyleNone,
};

rocksdb::CompactionStyle CompactionStyleFromString(std::string_view type) {
  if (type == kCompactionStyleLevel) {
    return rocksdb::kCompactionStyleLevel;
  }
  if (type == kCompactionStyleUniversal) {
    return rocksdb::kCompactionStyleUniversal;
  }
  if (type == kCompactionStyleFifo) {
    return rocksdb::kCompactionStyleFIFO;
  }
  if (type == kCompactionStyleNone) {
    return rocksdb::kCompactionStyleNone;
  }

  SDB_ASSERT(false);
  SDB_FATAL("xxxxx", sdb::Logger::STARTUP, "unexpected compaction style '",
            type, "'");
}

// defaults
rocksdb::TransactionDBOptions gRocksDbTrxDefaults;
rocksdb::Options gRocksDbDefaults;
rocksdb::BlockBasedTableOptions gRocksDbTableOptionsDefaults;

// minimum size of a block cache shard. we want to at least store
// that much data in each shard (rationale: a data block read from
// disk must fit into the block cache if the block cache's strict
// capacity limit is set. otherwise the block cache will fail reads
// with Status::Incomplete() or Status::MemoryLimit()).
constexpr uint64_t kMinShardSize = 128 * 1024 * 1024;

uint64_t DefaultBlockCacheSize() {
  if (physical_memory::GetValue() >= (static_cast<uint64_t>(4) << 30)) {
    // if we have at least 4GB of RAM, the default size is (RAM - 2GB) * 0.3
    return static_cast<uint64_t>(
      (physical_memory::GetValue() - (static_cast<uint64_t>(2) << 30)) * 0.3);
  }
  if (physical_memory::GetValue() >= (static_cast<uint64_t>(2) << 30)) {
    // if we have at least 2GB of RAM, the default size is 512MB
    return (static_cast<uint64_t>(512) << 20);
  }
  if (physical_memory::GetValue() >= (static_cast<uint64_t>(1) << 30)) {
    // if we have at least 1GB of RAM, the default size is 256MB
    return (static_cast<uint64_t>(256) << 20);
  }
  // for everything else the default size is 128MB
  return (static_cast<uint64_t>(128) << 20);
}

uint64_t DefaultTotalWriteBufferSize() {
  if (physical_memory::GetValue() >= (static_cast<uint64_t>(4) << 30)) {
    // if we have at least 4GB of RAM, the default size is (RAM - 2GB) * 0.4
    return static_cast<uint64_t>(
      (physical_memory::GetValue() - (static_cast<uint64_t>(2) << 30)) * 0.4);
  }
  if (physical_memory::GetValue() >= (static_cast<uint64_t>(1) << 30)) {
    // if we have at least 1GB of RAM, the default size is 512MB
    return (static_cast<uint64_t>(512) << 20);
  }
  // for everything else the default size is 256MB
  return (static_cast<uint64_t>(256) << 20);
}

uint64_t DefaultMinWriteBufferNumberToMerge(uint64_t total_size,
                                            uint64_t size_per_buffer,
                                            uint64_t max_buffers) {
  uint64_t safe = gRocksDbDefaults.min_write_buffer_number_to_merge;
  uint64_t test = safe + 1;

  // increase it to as much as 4 if it makes sense
  for (; test <= 4; ++test) {
    // next make sure we have enough buffers for it to matter
    uint64_t min_buffers = 1 + (2 * test);
    if (max_buffers < min_buffers) {
      break;
    }

    // next make sure we have enough space for all the buffers
    if (min_buffers * size_per_buffer *
          RocksDBColumnFamilyManager::kNumberOfColumnFamilies >
        total_size) {
      break;
    }

    safe = test;
  }

  return safe;
}

// minimum value for --rocksdb.sync-interval (in ms)
// a value of 0 however means turning off the syncing altogether!
static constexpr uint64_t kMinSyncInterval = 5;

}  // namespace

RocksDBOptionFeature::RocksDBOptionFeature(Server& server)
  : SerenedFeature{server, name()},
    // number of lock stripes for the transaction lock manager. we bump this
    // to at least 16 to reduce contention for small scale systems.
    _transaction_lock_stripes(
      std::max(number_of_cores::GetValue(), size_t(16))),
    _transaction_lock_timeout(gRocksDbTrxDefaults.transaction_lock_timeout),
    _total_write_buffer_size(gRocksDbDefaults.db_write_buffer_size),
    _write_buffer_size(gRocksDbDefaults.write_buffer_size),
    _max_write_buffer_number(
      RocksDBColumnFamilyManager::kNumberOfColumnFamilies +
      2),  // number of column families plus 2
    _max_write_buffer_size_to_maintain(0),
    _max_total_wal_size(256 << 20),
    _delayed_write_rate(gRocksDbDefaults.delayed_write_rate),
    _min_write_buffer_number_to_merge(DefaultMinWriteBufferNumberToMerge(
      _total_write_buffer_size, _write_buffer_size, _max_write_buffer_number)),
    _num_levels(gRocksDbDefaults.num_levels),
    _num_uncompressed_levels(2),
    _max_bytes_for_level_base(gRocksDbDefaults.max_bytes_for_level_base),
    _max_bytes_for_level_multiplier(
      gRocksDbDefaults.max_bytes_for_level_multiplier),
    // set max number of background jobs to the number of available cores
    _max_background_jobs(static_cast<int32_t>(
      std::max(static_cast<size_t>(2), number_of_cores::GetValue()))),
    _max_subcompactions(4),
    _num_threads_high(0),
    _num_threads_low(0),
    _target_file_size_base(gRocksDbDefaults.target_file_size_base),
    _target_file_size_multiplier(gRocksDbDefaults.target_file_size_multiplier),
    _block_cache_size(::DefaultBlockCacheSize()),
    _block_cache_shard_bits(-1),
    _block_cache_estimated_entry_charge(0),
    _min_blob_size(256),
    _blob_file_size(1ULL << 30),
    _blob_file_starting_level(0),
    _enable_blob_files(false),
    _enable_blob_cache(false),
    _blob_garbage_collection_age_cutoff(0.25),
    _blob_garbage_collection_force_threshold(1.0),
    _bloom_bits_per_key(10.0),
    _table_block_size(std::max(
      gRocksDbTableOptionsDefaults.block_size,
      static_cast<decltype(gRocksDbTableOptionsDefaults.block_size)>(16 *
                                                                     1024))),
    _compaction_readahead_size(8 * 1024 * 1024),
    _level0_compaction_trigger(2),
    _level0_slowdown_trigger(16),
    _level0_stop_trigger(256),
    // pending compactions slowdown trigger is set to 1GB
    _pending_compaction_bytes_slowdown_trigger(1024ULL * 1024ULL * 1024ULL),
    // pending compactions stop trigger is set to 32GB
    _pending_compaction_bytes_stop_trigger(32ULL * 1024ULL * 1024ULL * 1024ULL),
    // note: this is a default value from RocksDB (db/column_family.cc,
    // kAdjustedTtl):
    _periodic_compaction_ttl(30 * 24 * 60 * 60),
    _recycle_log_file_num(0),
    _compression_type(::kCompressionTypeLZ4),
    _blob_compression_type(::kCompressionTypeLZ4),
    _block_cache_type(::kBlockCacheTypeHyperClock),
    _checksum_type(::kChecksumTypeXXH3),
    _compaction_style(::kCompactionStyleLevel),
    _format_version(6),
    _optimize_filters_for_memory(true),
    _enable_index_compression(
      gRocksDbTableOptionsDefaults.enable_index_compression),
    _use_jemalloc_allocator(false),
    _prepopulate_block_cache(false),
    _prepopulate_blob_cache(false),
    _reserve_block_cache_memory{true},
    _enforce_block_cache_size_limit(false),
    _cache_index_and_filter_blocks{true},
    _cache_index_and_filter_blocks_with_high_priority{true},
    _block_align_data_blocks(gRocksDbTableOptionsDefaults.block_align),
    _enable_pipelined_write(true),
    _optimize_filters_for_hits(gRocksDbDefaults.optimize_filters_for_hits),
    // TODO(mbkkt) try direct io
    _use_direct_reads{false},
    _use_direct_io_for_flush_and_compaction{false},
    _use_fsync(gRocksDbDefaults.use_fsync),
    _skip_corrupted(false),
    _dynamic_level_bytes(true),
    _enable_statistics(false),
    _limit_open_files_at_startup(false),
    _allow_fallocate(true),
    _enable_blob_garbage_collection(true),
    _min_write_buffer_number_to_merge_touched(false),
    _partition_files_for_documents_cf(true),
    _partition_files_for_primary_index_cf(false),
    _partition_files_for_edge_index_cf(false),
    _partition_files_for_vpack_index_cf(false),
    _max_transaction_size(transaction::Options::gDefaultMaxTransactionSize),
    _intermediate_commit_size(
      transaction::Options::gDefaultIntermediateCommitSize),
    _intermediate_commit_count(
      transaction::Options::gDefaultIntermediateCommitCount),
    _vpack_cmp(std::make_unique<RocksDBVPackComparator>()),
    _max_write_buffer_number_cf{0, 0, 0, 0, 0} {
  if (_total_write_buffer_size == 0) {
    // unlimited write buffer size... now set to some fraction of physical RAM
    _total_write_buffer_size = ::DefaultTotalWriteBufferSize();
  }

  setOptional(true);
}

const rocksdb::Options& RocksDBOptionFeature::getOptions() const {
  if (!_options) {
    _options = doGetOptions();
  }
  return *_options;
}

const rocksdb::BlockBasedTableOptions& RocksDBOptionFeature::getTableOptions()
  const {
  if (!_table_options) {
    _table_options = doGetTableOptions();
  }
  return *_table_options;
}

void RocksDBOptionFeature::collectOptions(
  std::shared_ptr<ProgramOptions> options) {
  options->addSection("rocksdb", "RocksDB engine");

  options->addOption(
    "--rocksdb.wal-directory",
    "Absolute path for RocksDB WAL files. If not set, a "
    "subdirectory `journals` inside the database directory "
    "is used.",
    new StringParameter(&_wal_directory),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.target-file-size-base",
    "Per-file target file size for compaction (in bytes). The "
    "actual target file size for each level is "
    "`--rocksdb.target-file-size-base` multiplied by "
    "`--rocksdb.target-file-size-multiplier` ^ (level - 1)",
    new UInt64Parameter(&_target_file_size_base),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.target-file-size-multiplier",
    "The multiplier for `--rocksdb.target-file-size`. A value of 1 means "
    "that files in different levels will have the same size.",
    new UInt64Parameter(&_target_file_size_multiplier, /*base*/ 1,
                        /*minValue*/ 1),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  SDB_ASSERT(::kCompressionTypes.contains(_compression_type));
  options->addOption("--rocksdb.compression-type",
                     "The compression algorithm to use within RocksDB.",
                     new DiscreteValuesParameter<StringParameter>(
                       &_compression_type, ::kCompressionTypes));

  options
    ->addOption("--rocksdb.transaction-lock-stripes",
                "The number of lock stripes to use for transaction locks.",
                new UInt64Parameter(&_transaction_lock_stripes),
                options::MakeFlags(
                  options::Flags::Dynamic, options::Flags::DefaultNoComponents,
                  options::Flags::OnAgent, options::Flags::OnDBServer,
                  options::Flags::OnSingle))

    .setLongDescription(R"(You can control the number of lock stripes to use
for RocksDB's transaction lock manager with this option. You can use higher
values to reduce a potential contention in the lock manager.

The option defaults to the number of available cores, but is increased to a
value of `16` if the number of cores is lower.)");

  options->addOption(
    "--rocksdb.transaction-lock-timeout",
    "If positive, specifies the wait timeout in milliseconds when "
    " a transaction attempts to lock a document. A negative value "
    "is not recommended as it can lead to deadlocks (0 = no waiting, < 0 no "
    "timeout)",
    new Int64Parameter(&_transaction_lock_timeout),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options
    ->addOption(
      "--rocksdb.total-write-buffer-size",
      "The maximum total size of in-memory write buffers (0 = unbounded).",
      new UInt64Parameter(&_total_write_buffer_size),
      options::MakeFlags(options::Flags::Dynamic,
                         options::Flags::DefaultNoComponents,
                         options::Flags::OnAgent, options::Flags::OnDBServer,
                         options::Flags::OnSingle))
    .setLongDescription(R"(The total amount of data to build up in all
in-memory buffers (backed by log files). You can use this option together with
the block cache size configuration option to limit memory usage.

If set to `0`, the memory usage is not limited.

If set to a value larger than `0`, this caps memory usage for write buffers but
may have an effect on performance. If there is more than 4 GiB of RAM in the
system, the default value is `(system RAM size - 2 GiB) * 0.5`.

For systems with less RAM, the default values are:

- 512 MiB for systems with between 1 and 4 GiB of RAM.
- 256 MiB for systems with less than 1 GiB of RAM.)");

  options
    ->addOption("--rocksdb.write-buffer-size",
                "The amount of data to build up in memory before "
                "converting to a sorted on-disk file (0 = disabled).",
                new UInt64Parameter(&_write_buffer_size),
                options::MakeFlags(
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))
    .setLongDescription(R"(The amount of data to build up in each in-memory
buffer (backed by a log file) before closing the buffer and queuing it to be
flushed to standard storage. Larger values than the default may improve
performance, especially for bulk loads.)");

  options
    ->addOption("--rocksdb.max-write-buffer-number",
                "The maximum number of write buffers that build up in memory "
                "(default: number of column families + 2 = 12 write buffers). "
                "You can only increase the number.",
                new UInt64Parameter(&_max_write_buffer_number),
                options::MakeFlags(
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))
    .setLongDescription(R"(If this number is reached before the buffers can
be flushed, writes are slowed or stalled.)");

  options
    ->addOption(
      "--rocksdb.max-write-buffer-size-to-maintain",
      "The maximum size of immutable write buffers that build up in memory "
      "per column family. Larger values mean that more in-memory data "
      "can be used for transaction conflict checking (-1 = use automatic "
      "default value, 0 = do not keep immutable flushed write buffers, "
      "which is the default and usually correct).",
      new Int64Parameter(&_max_write_buffer_size_to_maintain),
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnAgent, options::Flags::OnDBServer,
                         options::Flags::OnSingle))
    .setLongDescription(R"(The default value `0` restores the memory usage
pattern of version 3.6. This makes RocksDB not keep any flushed immutable
write-buffers in memory.)");

  options
    ->addOption("--rocksdb.max-total-wal-size",
                "The maximum total size of WAL files that force a flush "
                "of stale column families.",
                new UInt64Parameter(&_max_total_wal_size),
                options::MakeFlags(
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))
    .setLongDescription(R"(When reached, force a flush of all column families
whose data is backed by the oldest WAL files. If you set this option to a low
value, regular flushing of column family data from memtables is triggered, so
that WAL files can be moved to the archive.

If you set this option to a high value, regular flushing is avoided but may
prevent WAL files from being moved to the archive and being removed.)");

  options->addOption(
    "--rocksdb.delayed-write-rate",
    "Limit the write rate to the database (in bytes per second) when writing "
    "to the last mem-table allowed and if more than 3 mem-tables are "
    "allowed, or if a certain number of level-0 files are surpassed and "
    "writes need to be slowed down.",
    new UInt64Parameter(&_delayed_write_rate),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.min-write-buffer-number-to-merge",
    "The minimum number of write buffers that are merged "
    "together before writing to storage.",
    new UInt64Parameter(&_min_write_buffer_number_to_merge),
    options::MakeFlags(options::Flags::Dynamic,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.num-levels",
    "The number of levels for the database in the LSM tree.",
    new UInt64Parameter(&_num_levels, /*base*/ 1,
                        /*minValue*/ 1, /*maxValue*/ 20),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options
    ->addOption("--rocksdb.num-uncompressed-levels",
                "The number of levels that do not use compression in the "
                "LSM tree.",
                new UInt64Parameter(&_num_uncompressed_levels),
                options::MakeFlags(
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))
    .setLongDescription(R"(Levels above the default of `2` use
compression to reduce the disk space requirements for storing data in these
levels.)");

  options
    ->addOption("--rocksdb.dynamic-level-bytes",
                "Whether to determine the number of bytes for each level "
                "dynamically to minimize space amplification.",
                new BooleanParameter(&_dynamic_level_bytes),
                options::MakeFlags(
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))
    .setLongDescription(R"(If set to `true`, the amount of data in each level
of the LSM tree is determined dynamically to minimize the space amplification.
Otherwise, the level sizes are fixed. The dynamic sizing allows RocksDB to
maintain a well-structured LSM tree regardless of total data size.)");

  options->addOption(
    "--rocksdb.max-bytes-for-level-base",
    "If not using dynamic level sizes, this controls the "
    "maximum total data size for level-1 of the LSM tree.",
    new UInt64Parameter(&_max_bytes_for_level_base),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.max-bytes-for-level-multiplier",
    "If not using dynamic level sizes, the maximum number of "
    "bytes for level L of the LSM tree can be calculated as "
    " max-bytes-for-level-base * "
    "(max-bytes-for-level-multiplier ^ (L-1))",
    new DoubleParameter(&_max_bytes_for_level_multiplier, /*base*/ 1.0,
                        /*minValue*/ 0.0,
                        /*maxValue*/ std::numeric_limits<double>::max(),
                        /*minInclusive*/ false),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options
    ->addOption(
      "--rocksdb.block-align-data-blocks",
      "If enabled, data blocks are aligned on the lesser of page size and "
      "block size.",
      new BooleanParameter(&_block_align_data_blocks),
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnAgent, options::Flags::OnDBServer,
                         options::Flags::OnSingle))
    .setLongDescription(R"(This may waste some memory but may reduce the
number of cross-page I/O operations.)");

  options->addOption(
    "--rocksdb.enable-pipelined-write",
    "If enabled, use a two stage write queue for WAL writes "
    "and memtable writes.",
    new BooleanParameter(&_enable_pipelined_write),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.enable-statistics",
    "Whether RocksDB statistics should be enabled.",
    new BooleanParameter(&_enable_statistics),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.optimize-filters-for-hits",
    "Whether the implementation should optimize the filters mainly for cases "
    "where keys are found rather than also optimize for keys missed. You can "
    "enable the option if you know that there are very few misses or the "
    "performance in the case of misses is not important for your "
    "application.",
    new BooleanParameter(&_optimize_filters_for_hits),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

#ifdef __linux__
  options->addOption(
    "--rocksdb.use-direct-reads", "Use O_DIRECT for reading files.",
    new BooleanParameter(&_use_direct_reads),
    options::MakeFlags(options::Flags::DefaultNoOs, options::Flags::OsLinux,
                       options::Flags::Uncommon));

  options->addOption(
    "--rocksdb.use-direct-io-for-flush-and-compaction",
    "Use O_DIRECT for writing files for flush and compaction.",
    new BooleanParameter(&_use_direct_io_for_flush_and_compaction),
    options::MakeFlags(options::Flags::DefaultNoOs, options::Flags::OsLinux,
                       options::Flags::Uncommon));
#endif

  options->addOption(
    "--rocksdb.use-fsync",
    "Whether to use fsync calls when writing to disk (set to false "
    "for issuing fdatasync calls only).",
    new BooleanParameter(&_use_fsync),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options
    ->addOption("--rocksdb.max-background-jobs",
                "The maximum number of concurrent background jobs "
                "(compactions and flushes).",
                new Int32Parameter(&_max_background_jobs),
                options::MakeFlags(
                  options::Flags::Uncommon, options::Flags::Dynamic,
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))
    .setLongDescription(R"(The jobs are submitted to the low priority thread
pool. The default value is the number of processors in the system.)");

  options->addOption(
    "--rocksdb.max-subcompactions",
    "The maximum number of concurrent sub-jobs for a "
    "background compaction.",
    new UInt32Parameter(&_max_subcompactions),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options
    ->addOption("--rocksdb.level0-compaction-trigger",
                "The number of level-0 files that triggers a compaction.",
                new Int64Parameter(&_level0_compaction_trigger),
                options::MakeFlags(
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))
    .setLongDescription(R"(Compaction of level-0 to level-1 is triggered when
this many files exist in level-0. If you set this option to a higher number, it
may help bulk writes at the expense of slowing down reads.)");

  options
    ->addOption("--rocksdb.level0-slowdown-trigger",
                "The number of level-0 files that triggers a write slowdown",
                new Int64Parameter(&_level0_slowdown_trigger),
                options::MakeFlags(
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))
    .setLongDescription(R"(When this many files accumulate in level-0, writes
are slowed down to `--rocksdb.delayed-write-rate` to allow compaction to
catch up.)");

  options
    ->addOption("--rocksdb.level0-stop-trigger",
                "The number of level-0 files that triggers a full write stop",
                new Int64Parameter(&_level0_stop_trigger),
                options::MakeFlags(
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))
    .setLongDescription(R"(When this many files accumulate in level-0, writes
are stopped to allow compaction to catch up.)");

  options->addOption(
    "--rocksdb.pending-compactions-slowdown-trigger",
    "The number of pending compaction bytes that triggers a "
    "write slowdown.",
    new UInt64Parameter(&_pending_compaction_bytes_slowdown_trigger),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.pending-compactions-stop-trigger",
    "The number of pending compaction bytes that triggers a full "
    "write stop.",
    new UInt64Parameter(&_pending_compaction_bytes_stop_trigger),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options
    ->addOption(
      "--rocksdb.num-threads-priority-high",
      "The number of threads for high priority operations (e.g. flush).",
      new UInt32Parameter(&_num_threads_high, /*base*/ 1, /*minValue*/ 0,
                          /*maxValue*/ 64),
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnAgent, options::Flags::OnDBServer,
                         options::Flags::OnSingle))

    .setLongDescription(R"(The recommended value is to set this equal to
`max-background-flushes`. The default value is `number of processors / 2`.)");

  options->addOption(
    "--rocksdb.block-cache-estimated-entry-charge",
    "The estimated charge of cache entries (in bytes) for the "
    "hyper-clock cache.",
    new UInt64Parameter(&_block_cache_estimated_entry_charge),
    options::MakeFlags(options::Flags::Experimental,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  SDB_ASSERT(::kBlockCacheTypes.contains(_block_cache_type));
  options->addOption("--rocksdb.block-cache-type",
                     "The block cache type to use.",
                     new DiscreteValuesParameter<StringParameter>(
                       &_block_cache_type, ::kBlockCacheTypes));

  options
    ->addOption(
      "--rocksdb.num-threads-priority-low",
      "The number of threads for low priority operations (e.g. "
      "compaction).",
      new UInt32Parameter(&_num_threads_low, /*base*/ 1, /*minValue*/ 0,
                          /*maxValue*/ 256),
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnAgent, options::Flags::OnDBServer,
                         options::Flags::OnSingle))
    .setLongDescription(R"(The default value is
`number of processors / 2`.)");

  options
    ->addOption("--rocksdb.block-cache-size",
                "The size of block cache (in bytes).",
                new UInt64Parameter(&_block_cache_size),
                options::MakeFlags(
                  options::Flags::Dynamic, options::Flags::DefaultNoComponents,
                  options::Flags::OnAgent, options::Flags::OnDBServer,
                  options::Flags::OnSingle))
    .setLongDescription(R"(This is the maximum size of the block cache in
bytes. Increasing this value may improve performance. If there is more than
4 GiB of RAM in the system, the default value is
`(system RAM size - 2GiB) * 0.3`.

For systems with less RAM, the default values are:

- 512 MiB for systems with between 2 and 4 GiB of RAM.
- 256 MiB for systems with between 1 and 2 GiB of RAM.
- 128 MiB for systems with less than 1 GiB of RAM.)");

  options
    ->addOption(
      "--rocksdb.block-cache-shard-bits",
      "The number of shard bits to use for the block cache "
      "(-1 = default value).",
      new Int32Parameter(&_block_cache_shard_bits, /*base*/ 1, /*minValue*/ -1,
                         /*maxValue*/ 20, /*minInclusive*/ true,
                         /*maxInclusive*/ false),
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnAgent, options::Flags::OnDBServer,
                         options::Flags::OnSingle))
    .setLongDescription(R"(The number of bits used to shard the block cache
to allow concurrent operations. To keep individual shards at a reasonable size
(i.e. at least 512 KiB), keep this value to at most
`block-cache-shard-bits / 512 KiB`. Default: `block-cache-size / 2^19`.)");

  options
    ->addOption("--rocksdb.enforce-block-cache-size-limit",
                "If enabled, strictly enforces the block cache size limit.",
                new BooleanParameter(&_enforce_block_cache_size_limit),
                options::MakeFlags(
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))
    .setLongDescription(R"(Whether the maximum size of the RocksDB block
cache is strictly enforced. You can set this option to limit the memory usage of
the block cache to at most the specified size. If inserting a data block into
the cache would exceed the cache's capacity, the data block is not inserted.
If disabled, a data block may still get inserted into the cache. It is evicted
later, but the cache may temporarily grow beyond its capacity limit.

To improve stability of memory usage and prevent exceeding the block cache
capacity limit (as configurable via `--rocksdb.block-cache-size`), it is
recommended to set this option to `true`.)");

  options
    ->addOption("--rocksdb.cache-index-and-filter-blocks",
                "If enabled, the RocksDB block cache quota also includes "
                "RocksDB memtable sizes.",
                new BooleanParameter(&_cache_index_and_filter_blocks),
                options::MakeFlags(
                  options::Flags::Uncommon, options::Flags::DefaultNoComponents,
                  options::Flags::OnAgent, options::Flags::OnDBServer,
                  options::Flags::OnSingle))
    .setLongDescription(R"(If you set this option to `true`, RocksDB tracks
all loaded index and filter blocks in the block cache, so that they count
towards RocksDB's block cache memory limit.

If you set this option to `false`, the memory usage for index and filter blocks
is not accounted for.

To improve stability of memory usage and avoid untracked memory allocations by
RocksDB, it is recommended to set this option to `true`. Note that tracking
index and filter blocks leaves less room for other data in the block cache, so
in case servers have unused RAM capacity available, it may be useful to increase
the overall size of the block cache.)");

  options->addOption(
    "--rocksdb.cache-index-and-filter-blocks-with-high-priority",
    "If enabled and `--rocksdb.cache-index-and-filter-blocks` is also "
    "enabled, cache index and filter blocks with high priority, "
    "making index and filter blocks be less likely to be evicted than "
    "data blocks.",
    new BooleanParameter(&_cache_index_and_filter_blocks_with_high_priority),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.table-block-size",
    "The approximate size (in bytes) of the user data packed "
    "per block for uncompressed data.",
    new UInt64Parameter(&_table_block_size),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.recycle-log-file-num",
    "If enabled, keep a pool of log files around for recycling.",
    new SizeTParameter(&_recycle_log_file_num),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.bloom-filter-bits-per-key",
    "The average number of bits to use per key in a Bloom filter.",
    new DoubleParameter(&_bloom_bits_per_key),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.compaction-read-ahead-size",
    "If non-zero, bigger reads are performed when doing compaction. If you "
    "run RocksDB on spinning disks, you should set this to at least 2 MB. "
    "That way, RocksDB's compaction does sequential instead of random reads.",
    new UInt64Parameter(&_compaction_readahead_size),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.wal-recovery-skip-corrupted",
    "Skip corrupted records in WAL recovery.",
    new BooleanParameter(&_skip_corrupted),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.limit-open-files-at-startup",
    "Limit the amount of .sst files RocksDB inspects at "
    "startup, in order to reduce the startup I/O operations.",
    new BooleanParameter(&_limit_open_files_at_startup),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options
    ->addOption("--rocksdb.allow-fallocate",
                "Whether to allow RocksDB to use fallocate calls. "
                "If disabled, fallocate calls are bypassed and no "
                "pre-allocation is done.",
                new BooleanParameter(&_allow_fallocate),
                options::MakeFlags(
                  options::Flags::Uncommon, options::Flags::DefaultNoComponents,
                  options::Flags::OnAgent, options::Flags::OnDBServer,
                  options::Flags::OnSingle))
    .setLongDescription(R"(Preallocation is turned on by default, but you can
turn it off for operating system versions that are known to have issues with it.
This option only has an effect on operating systems that support
`fallocate`.)");

  SDB_ASSERT(::kChecksumTypes.contains(_checksum_type));
  options->addOption(
    "--rocksdb.checksum-type", "The checksum type to use for table files.",
    new DiscreteValuesParameter<StringParameter>(&_checksum_type,
                                                 ::kChecksumTypes),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  SDB_ASSERT(::kCompactionStyles.contains(_compaction_style));
  options->addOption(
    "--rocksdb.compaction-style",
    "The compaction style which is used to pick the next file(s) to "
    "be compacted (note: all styles except 'level' are experimental).",
    new DiscreteValuesParameter<StringParameter>(&_compaction_style,
                                                 ::kCompactionStyles),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.format-version",
    "The table format version to use inside RocksDB.",
    new DiscreteValuesParameter<UInt32Parameter>(&_format_version,
                                                 {
                                                   6,
                                                 }),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.optimize-filters-for-memory",
    "Optimize RocksDB bloom filters to reduce internal memory "
    "fragmentation.",
    new BooleanParameter(&_optimize_filters_for_memory),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.enable-index-compression", "Enable index compression.",
    new BooleanParameter(&_enable_index_compression),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.enable-blob-files",
    "Enable blob files for the documents column family.",
    new BooleanParameter(&_enable_blob_files),
    options::MakeFlags(options::Flags::Experimental,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.enable-blob-cache",
    "Enable caching of blobs in the block cache for the documents "
    "column family.",
    new BooleanParameter(&_enable_blob_cache),
    options::MakeFlags(options::Flags::Experimental,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.min-blob-size",
    "The size threshold for storing documents in blob files (in "
    "bytes, 0 = store all documents in blob files). "
    "Requires `--rocks.enable-blob-files`.",
    new UInt64Parameter(&_min_blob_size),
    options::MakeFlags(options::Flags::Experimental,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.blob-file-size",
    "The size limit for blob files in the documents column "
    "family (in bytes). Requires `--rocksdb.enable-blob-files`.",
    new UInt64Parameter(&_blob_file_size),
    options::MakeFlags(options::Flags::Experimental,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.blob-file-starting-level",
    "The level from which on to use blob files in the documents "
    "column family. Requires `--rocksdb.enable-blob-files`.",
    new UInt32Parameter(&_blob_file_starting_level),
    options::MakeFlags(options::Flags::Experimental,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.blob-garbage-collection-age-cutoff",
    "The age cutoff for garbage collecting blob files in the documents "
    "column family (percentage value from 0 to 1 determines how many "
    "blob files are garbage collected during compaction). Requires "
    "`--rocksdb.enable-blob-files` and "
    "`--rocksdb.enable-blob-garbage-collection`.",
    new DoubleParameter(&_blob_garbage_collection_age_cutoff, 1.0, 0.0, 1.0),
    options::MakeFlags(options::Flags::Experimental,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.blob-garbage-collection-force-threshold",
    "The garbage ratio threshold for scheduling targeted compactions "
    "for the oldest blob files in the documents column family "
    "(percentage value between 0 and 1). "
    "Requires `--rocksdb.enable-blob-files` and "
    "`--rocksdb.enable-blob-garbage-collection`.",
    new DoubleParameter(&_blob_garbage_collection_force_threshold, 1.0, 0.0,
                        1.0),
    options::MakeFlags(options::Flags::Experimental,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  SDB_ASSERT(::kCompressionTypes.contains(_blob_compression_type));
  options->addOption(
    "--rocksdb.blob-compression-type",
    "The compression algorithm to use for blob data in the "
    "documents column family. "
    "Requires `--rocksdb.enable-blob-files`.",
    new DiscreteValuesParameter<StringParameter>(&_blob_compression_type,
                                                 ::kCompressionTypes),
    options::MakeFlags(options::Flags::Experimental,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.enable-blob-garbage-collection",
    "Enable blob garbage collection during compaction in the "
    "documents column family. Requires `--rocksdb.enable-blob-files`.",
    new BooleanParameter(&_enable_blob_garbage_collection),
    options::MakeFlags(options::Flags::Experimental,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.prepopulate-blob-cache",
    "Pre-populate the blob cache on flushes.",
    new BooleanParameter(&_prepopulate_blob_cache),
    options::MakeFlags(options::Flags::Experimental, options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options
    ->addOption(
      "--rocksdb.block-cache-jemalloc-allocator",
      "Use jemalloc-based memory allocator for RocksDB block cache.",
      new BooleanParameter(&_use_jemalloc_allocator),
      options::MakeFlags(options::Flags::Experimental, options::Flags::Uncommon,
                         options::Flags::OsLinux, options::Flags::OnAgent,
                         options::Flags::OnDBServer, options::Flags::OnSingle))

    .setLongDescription(
      R"(The jemalloc-based memory allocator for the RocksDB block cache
will also exclude the block cache contents from coredumps, potentially making generated
coredumps a lot smaller.
In order to use this option, the executable needs to be compiled with jemalloc
support (which is the default on Linux).)");

  options->addOption(
    "--rocksdb.prepopulate-block-cache", "Pre-populate block cache on flushes.",
    new BooleanParameter(&_prepopulate_block_cache),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.reserve-block-cache-memory",
    "account for some rocksdb memory in block cache",
    new BooleanParameter(&_reserve_block_cache_memory),
    options::MakeFlags(options::Flags::Uncommon,
                       options::Flags::DefaultNoComponents,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  options
    ->addOption("--rocksdb.periodic-compaction-ttl",
                "Time-to-live (in seconds) for periodic compaction of .sst "
                "files, based on the file age (0 = no periodic compaction).",
                new UInt64Parameter(&_periodic_compaction_ttl),
                options::MakeFlags(
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))

    .setLongDescription(R"(The default value from RocksDB is ~30 days. To
avoid periodic auto-compaction and the I/O caused by it, you can set this
option to `0`.)");

  options
    ->addOption("--rocksdb.partition-files-for-documents",
                "If enabled, the document data for different "
                "collections/shards will end up in "
                "different .sst files.",
                new BooleanParameter(&_partition_files_for_documents_cf),
                options::MakeFlags(
                  options::Flags::Uncommon, options::Flags::Experimental,
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))

    .setLongDescription(R"(Enabling this option will make RocksDB's
compaction write the document data for different collections/shards
into different .sst files. Otherwise the document data from different
collections/shards can be mixed and written into the same .sst files.

Enabling this option usually has the benefit of making the RocksDB
compaction more efficient when a lot of different collections/shards
are written to in parallel.
The disavantage of enabling this option is that there can be more .sst
files than when the option is turned off, and the disk space used by
these .sst files can be higher than if there are fewer .sst files (this
is because there is some per-.sst file overhead).
In particular on deployments with many collections/shards
this can lead to a very high number of .sst files, with the potential
of outgrowing the maximum number of file descriptors the SereneDB process
can open. Thus the option should only be enabled on deployments with a
limited number of collections/shards.)");

  options
    ->addOption("--rocksdb.partition-files-for-primary-index",
                "If enabled, the primary index data for different "
                "collections/shards will end up in "
                "different .sst files.",
                new BooleanParameter(&_partition_files_for_primary_index_cf),
                options::MakeFlags(
                  options::Flags::Uncommon, options::Flags::Experimental,
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))

    .setLongDescription(R"(Enabling this option will make RocksDB's
compaction write the primary index data for different collections/shards
into different .sst files. Otherwise the primary index data from different
collections/shards can be mixed and written into the same .sst files.

Enabling this option usually has the benefit of making the RocksDB
compaction more efficient when a lot of different collections/shards
are written to in parallel.
The disavantage of enabling this option is that there can be more .sst
files than when the option is turned off, and the disk space used by
these .sst files can be higher than if there are fewer .sst files (this
is because there is some per-.sst file overhead).
In particular on deployments with many collections/shards
this can lead to a very high number of .sst files, with the potential
of outgrowing the maximum number of file descriptors the SereneDB process
can open. Thus the option should only be enabled on deployments with a
limited number of collections/shards.)");

  options
    ->addOption("--rocksdb.partition-files-for-edge-index",
                "If enabled, the index data for different edge "
                "indexes will end up in different .sst files.",
                new BooleanParameter(&_partition_files_for_edge_index_cf),
                options::MakeFlags(
                  options::Flags::Uncommon, options::Flags::Experimental,
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))

    .setLongDescription(R"(Enabling this option will make RocksDB's
compaction write the edge index data for different edge collections/shards
into different .sst files. Otherwise the edge index data from different
edge collections/shards can be mixed and written into the same .sst files.

Enabling this option usually has the benefit of making the RocksDB
compaction more efficient when a lot of different edge collections/shards
are written to in parallel.
The disavantage of enabling this option is that there can be more .sst
files than when the option is turned off, and the disk space used by
these .sst files can be higher than if there are fewer .sst files (this
is because there is some per-.sst file overhead).
In particular on deployments with many edge collections/shards
this can lead to a very high number of .sst files, with the potential
of outgrowing the maximum number of file descriptors the SereneDB process
can open. Thus the option should only be enabled on deployments with a
limited number of edge collections/shards.)");

  options
    ->addOption("--rocksdb.partition-files-for-secondary-index",
                "If enabled, the index data for different secondary "
                "indexes will end up in different .sst files.",
                new BooleanParameter(&_partition_files_for_vpack_index_cf),
                options::MakeFlags(
                  options::Flags::Uncommon, options::Flags::Experimental,
                  options::Flags::DefaultNoComponents, options::Flags::OnAgent,
                  options::Flags::OnDBServer, options::Flags::OnSingle))

    .setLongDescription(R"(Enabling this option will make RocksDB's
compaction write the secondary index data for different secondary
indexes (also indexes from different collections/shards) into different
.sst files. Otherwise the secondary index data from different
collections/shards/indexes can be mixed and written into the same .sst files.

Enabling this option usually has the benefit of making the RocksDB
compaction more efficient when a lot of different collections/shards/indexes
are written to in parallel.
The disavantage of enabling this option is that there can be more .sst
files than when the option is turned off, and the disk space used by
these .sst files can be higher than if there are fewer .sst files (this
is because there is some per-.sst file overhead).
In particular on deployments with many collections/shards/indexes
this can lead to a very high number of .sst files, with the potential
of outgrowing the maximum number of file descriptors the SereneDB process
can open. Thus the option should only be enabled on deployments with a
limited number of edge collections/shards/indexes.)");

  options->addOption(
    "--rocksdb.use-io_uring",
    "Check for existence of io_uring at startup and use it if available. "
    "Should be set to false only to opt out of using io_uring.",
    new BooleanParameter(&gIoUringEnabled),
    options::MakeFlags(options::Flags::Uncommon, options::Flags::OsLinux,
                       options::Flags::OnAgent, options::Flags::OnDBServer,
                       options::Flags::OnSingle));

  /// minimum required percentage of free disk space for considering
  /// the server "healthy". this is expressed as a floating point value
  /// between 0 and 1! if set to 0.0, the % amount of free disk is ignored in
  /// checks.
  options->addOption(
    "--rocksdb.minimum-disk-free-percent",
    "The minimum percentage of free disk space for considering the "
    "server healthy in health checks (0 = disable the check).",
    new DoubleParameter(&_required_disk_free_percentage, /*base*/ 1.0,
                        /*minValue*/ 0.0, /*maxValue*/ 1.0),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnDBServer, options::Flags::OnSingle));

  /// minimum number of free bytes on disk for considering the server
  /// healthy. if set to 0, the number of free bytes on disk is ignored in
  /// checks.
  options->addOption(
    "--rocksdb.minimum-disk-free-bytes",
    "The minimum number of free disk bytes for considering the "
    "server healthy in health checks (0 = disable the check).",
    new UInt64Parameter(&_required_disk_free_bytes),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnDBServer, options::Flags::OnSingle));

  // control transaction size for RocksDB engine
  options
    ->addOption("--rocksdb.max-transaction-size",
                "The transaction size limit (in bytes).",
                new UInt64Parameter(&_max_transaction_size))
    .setLongDescription(R"(Transactions store all keys and values in RAM, so
large transactions run the risk of causing out-of-memory situations. This
setting allows you to ensure that it does not happen by limiting the size of
any individual transaction. Transactions whose operations would consume more
RAM than this threshold value are aborted automatically with error 32
("resource limit exceeded").)");

  options->addOption("--rocksdb.intermediate-commit-size",
                     "An intermediate commit is performed automatically "
                     "when a transaction has accumulated operations of this "
                     "size (in bytes), and a new transaction is started.",
                     new UInt64Parameter(&_intermediate_commit_size));

  options->addOption("--rocksdb.intermediate-commit-count",
                     "An intermediate commit is performed automatically "
                     "when this number of operations is reached in a "
                     "transaction, and a new transaction is started.",
                     new UInt64Parameter(&_intermediate_commit_count));

  options->addOption("--rocksdb.max-parallel-compactions",
                     "The maximum number of parallel compactions jobs.",
                     new UInt64Parameter(&_max_parallel_compactions));

  options
    ->addOption(
      "--rocksdb.sync-interval",
      "The interval for automatic, non-requested disk syncs (in "
      "milliseconds, 0 = turn automatic syncing off)",
      new UInt64Parameter(&_sync_interval),
      options::MakeFlags(options::Flags::OsLinux, options::Flags::OsMac,
                         options::Flags::OnDBServer, options::Flags::OnSingle))
    .setLongDescription(R"(Automatic synchronization of data from RocksDB's
write-ahead logs to disk is only performed for not-yet synchronized data, and
only for operations that have been executed without the `waitForSync`
attribute.)");

  options->addOption(
    "--rocksdb.sync-delay-threshold",
    "The threshold for self-observation of WAL disk syncs "
    "(in milliseconds, 0 = no warnings). Any WAL disk sync longer ago "
    "than this threshold triggers a warning ",
    new UInt64Parameter(&_sync_delay_threshold),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnDBServer, options::Flags::OnSingle,
                       options::Flags::Uncommon));

  options
    ->addOption(
      "--rocksdb.wal-file-timeout",
      "The timeout after which unused WAL files are deleted "
      "(in seconds).",
      new DoubleParameter(&_prune_wait_time),
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnDBServer, options::Flags::OnSingle))
    .setLongDescription(R"(Data of ongoing transactions is stored in RAM.
Transactions that get too big (in terms of number of operations involved or the
total size of data created or modified by the transaction) are committed
automatically. Effectively, this means that big user transactions are split into
multiple smaller RocksDB transactions that are committed individually.
The entire user transaction does not necessarily have ACID properties in this
case.)");

  options
    ->addOption(
      "--rocksdb.wal-file-timeout-initial",
      "The initial timeout (in seconds) after which unused WAL "
      "files deletion kicks in after server start.",
      new DoubleParameter(&_prune_wait_time_initial),
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnDBServer, options::Flags::OnSingle,
                         options::Flags::Uncommon))
    .setLongDescription(R"(If you decrease the value, the server starts the
removal of obsolete WAL files earlier after server start. This is useful in
testing environments that are space-restricted and do not require keeping much
WAL file data at all.)");

  options
    ->addOption(
      "--rocksdb.debug-logging", "Whether to enable RocksDB debug logging.",
      new BooleanParameter(&_debug_logging),
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnDBServer, options::Flags::OnSingle,
                         options::Flags::Uncommon))
    .setLongDescription(R"(If set to `true`, enables verbose logging of
RocksDB's actions into the logfile written by SereneDB (if the
`--rocksdb.use-file-logging` option is off), or RocksDB's own log (if the
`--rocksdb.use-file-logging` option is on).

This option is turned off by default, but you can enable it for debugging
RocksDB internals and performance.)");

  options
    ->addOption("--rocksdb.verify-sst",
                "Verify the validity of .sst files present in the "
                "`engine_rocksdb` directory on startup.",
                new BooleanParameter(&_verify_sst),
                options::MakeFlags(
                  options::Flags::Command, options::Flags::DefaultNoComponents,
                  options::Flags::OnAgent, options::Flags::OnDBServer,
                  options::Flags::OnSingle, options::Flags::Uncommon))

    .setLongDescription(R"(If set to `true`, during startup, all .sst files
in the `engine_rocksdb` folder in the database directory are checked for
potential corruption and errors. The server process stops after the check and
returns an exit code of `0` if the validation was successful, or a non-zero
exit code if there is an error in any of the .sst files.)");

  options
    ->addOption(
      "--rocksdb.wal-archive-size-limit",
      "The maximum total size (in bytes) of archived WAL files to "
      "keep on the leader (0 = unlimited).",
      new options::UInt64Parameter(&_max_wal_archive_size_limit),
      options::MakeFlags(options::Flags::DefaultNoComponents,
                         options::Flags::OnDBServer, options::Flags::OnSingle,
                         options::Flags::Uncommon))
    .setLongDescription(R"(A value of `0` does not restrict the size of the
archive, so the leader removes archived WAL files when there are no replication
clients needing them. Any non-zero value restricts the size of the WAL files
archive to about the specified value and trigger WAL archive file deletion once
the threshold is reached. You can use this to get rid of archived WAL files in
a disk size-constrained environment.

**Note**: The value is only a threshold, so the archive may get bigger than
the configured value until the background thread actually deletes files from
the archive. Also note that deletion from the archive only kicks in after
`--rocksdb.wal-file-timeout-initial` seconds have elapsed after server start.

Archived WAL files are normally deleted automatically after a short while when
there is no follower attached that may read from the archive. However, in case
when there are followers attached that may read from the archive, WAL files
normally remain in the archive until their contents have been streamed to the
followers. In case there are slow followers that cannot catch up, this causes a
growth of the WAL files archive over time.

You can use the option to force a deletion of WAL files from the archive even if
there are followers attached that may want to read the archive. In case the
option is set and a leader deletes files from the archive that followers want to
read, this aborts the replication on the followers. Followers can restart the
replication doing a resync, though, but they may not be able to catch up if WAL
file deletion happens too early.

Thus it is best to leave this option at its default value of `0` except in cases
when disk size is very constrained and no replication is used.)");

  options->addOption(
    "--rocksdb.auto-flush-min-live-wal-files",
    "The minimum number of live WAL files that triggers an "
    "auto-flush of WAL "
    "and column family data.",
    new UInt64Parameter(&_auto_flush_min_wal_files),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnDBServer, options::Flags::OnSingle));

  options->addOption(
    "--rocksdb.auto-flush-check-interval",
    "The interval (in seconds) in which auto-flushes of WAL and column "
    "family data is executed.",
    new DoubleParameter(&_auto_flush_check_interval),
    options::MakeFlags(options::Flags::DefaultNoComponents,
                       options::Flags::OnDBServer, options::Flags::OnSingle));

  //////////////////////////////////////////////////////////////////////////////
  /// add column family-specific options now
  //////////////////////////////////////////////////////////////////////////////
  auto add_max_write_buffer_number_cf =
    [this, &options](RocksDBColumnFamilyManager::Family family) {
      std::string name = RocksDBColumnFamilyManager::name(
        family, RocksDBColumnFamilyManager::NameMode::External);
      size_t index = std::to_underlying(family);
      options->addOption(
        "--rocksdb.max-write-buffer-number-" + name,
        "If non-zero, overrides the value of "
        "`--rocksdb.max-write-buffer-number` for the " +
          name + " column family",
        new UInt64Parameter(&_max_write_buffer_number_cf[index]),
        options::MakeDefaultFlags(options::Flags::Uncommon));
    };
  for (auto family : {
         RocksDBColumnFamilyManager::Family::Definitions,
         RocksDBColumnFamilyManager::Family::Documents,
         RocksDBColumnFamilyManager::Family::PrimaryIndex,
         RocksDBColumnFamilyManager::Family::EdgeIndex,
         RocksDBColumnFamilyManager::Family::VPackIndex,
       }) {
    add_max_write_buffer_number_cf(family);
  }
}

void RocksDBOptionFeature::validateOptions(
  std::shared_ptr<ProgramOptions> options) {
  transaction::Options::setLimits(_max_transaction_size,
                                  _intermediate_commit_size,
                                  _intermediate_commit_count);
  if (_sync_interval > 0) {
    if (_sync_interval < kMinSyncInterval) {
      // _sync_interval = 0 means turned off!
      SDB_FATAL(
        "xxxxx", Logger::CONFIG,
        "invalid value for --rocksdb.sync-interval. Please use a value ",
        "of at least ", kMinSyncInterval);
    }

    if (_sync_delay_threshold > 0 && _sync_delay_threshold <= _sync_interval) {
      if (!options->processingResult().touched("rocksdb.sync-interval") &&
          options->processingResult().touched("rocksdb.sync-delay-threshold")) {
        // user has not set --rocksdb.sync-interval, but set
        // --rocksdb.sync-delay-threshold
        SDB_WARN("xxxxx", Logger::CONFIG,
                 "invalid value for --rocksdb.sync-delay-threshold. should be "
                 "higher ",
                 "than the value of --rocksdb.sync-interval (", _sync_interval,
                 ")");
      }

      _sync_delay_threshold = 10 * _sync_interval;
      SDB_WARN("xxxxx", Logger::CONFIG,
               "auto-adjusting value of --rocksdb.sync-delay-threshold to ",
               _sync_delay_threshold, " ms");
    }
  }

  if (_prune_wait_time_initial < 10) {
    SDB_WARN("xxxxx", Logger::ENGINES,
             "consider increasing the value for "
             "--rocksdb.wal-file-timeout-initial. ",
             "Replication clients might have trouble to get in sync");
  }

  if (_write_buffer_size > 0 && _write_buffer_size < 1024 * 1024) {
    SDB_FATAL("xxxxx", sdb::Logger::STARTUP,
              "invalid value for '--rocksdb.write-buffer-size'");
  }
  if (_total_write_buffer_size > 0 &&
      _total_write_buffer_size < 64 * 1024 * 1024) {
    SDB_FATAL("xxxxx", sdb::Logger::STARTUP,
              "invalid value for '--rocksdb.total-write-buffer-size'");
  }
  if (_max_background_jobs != -1 && _max_background_jobs < 1) {
    SDB_FATAL("xxxxx", sdb::Logger::STARTUP,
              "invalid value for '--rocksdb.max-background-jobs'");
  }

  _min_write_buffer_number_to_merge_touched =
    options->processingResult().touched(
      "--rocksdb.min-write-buffer-number-to-merge");

#ifdef SDB_CLUSTER
  // limit memory usage of agent instances, if not otherwise configured
  if (server().hasFeature<AgencyFeature>()) {
    AgencyFeature& feature = server().getFeature<AgencyFeature>();
    if (feature.activated()) {
      // if we are an agency instance...
      if (!options->processingResult().touched("--rocksdb.block-cache-size")) {
        // restrict block cache size to 1 GB if not set explicitly
        _block_cache_size =
          std::min<uint64_t>(_block_cache_size, uint64_t(1) << 30);
      }
      if (!options->processingResult().touched(
            "--rocksdb.total-write-buffer-size")) {
        // restrict total write buffer size to 512 MB if not set explicitly
        _total_write_buffer_size =
          std::min<uint64_t>(_total_write_buffer_size, uint64_t(512) << 20);
      }
    }
  }
#endif

  if (_block_cache_type == ::kBlockCacheTypeLRU &&
      options->processingResult().touched(
        "--rocksdb.block-cache-estimated-entry-charge")) {
    SDB_WARN("xxxxx", Logger::ENGINES,
             "Setting value of '--rocksdb.block-cache-estimated-entry-charge' "
             "has no effect when using LRU block cache");
  }

  if (_enforce_block_cache_size_limit &&
      !options->processingResult().touched(
        "--rocksdb.block-cache-shard-bits")) {
    // if block cache size limit is enforced, and the number of shard bits for
    // the block cache hasn't been set, we set it dynamically:
    // we would like that each block cache shard can hold data blocks of
    // at least a common size. Rationale: data blocks can be quite large. if
    // they don't fit into the block cache upon reading, the block cache will
    // return Status::Incomplete() or Status::MemoryLimit() when the block
    // cache's strict capacity limit is set. then we cannot read data anymore.
    // we are limiting the maximum number of shard bits to 10 here, which is
    // 1024 shards. that should be enough shards even for very big caches.
    // note that RocksDB also has an internal upper bound for the number of
    // shards bits, which is 20.
    _block_cache_shard_bits = std::clamp<int>(
      std::floor(
        std::log2(static_cast<double>(_block_cache_size) / ::kMinShardSize)),
      1, 10);

    // TODO: hyper clock cache probably doesn't need as many shards. check this.
  }

#ifndef SERENEDB_HAVE_JEMALLOC
  // on some platforms, jemalloc is not available, because it is not compiled
  // in by default. to make the startup of the server not fail in such
  // environment, turn off the option automatically
  if (_use_jemalloc_allocator) {
    _use_jemalloc_allocator = false;
    SDB_INFO(
      "xxxxx", Logger::STARTUP,
      "disabling jemalloc allocator for RocksDB - jemalloc not compiled");
  }
#endif

  if (!_enable_blob_files) {
    // turn off blob garbage collection to avoid potential side effects
    // for performance
    _enable_blob_garbage_collection = false;
  }

  if (_enforce_block_cache_size_limit && _block_cache_size > 0) {
    uint64_t shard_size =
      _block_cache_size / (uint64_t(1) << _block_cache_shard_bits);
    // if we can't store a data block of the mininmum size in the block cache,
    // we may run into problems when trying to put a large data block into the
    // cache. in this case the block cache may return a Status::Incomplete()
    // or Status::MemoryLimit() error and fail the entire read.
    // warn the user about it!
    if (shard_size < ::kMinShardSize) {
      SDB_WARN("xxxxx", Logger::ENGINES,
               "size of RocksDB block cache shards seems to be too low. ",
               "block cache size: ", _block_cache_size,
               ", shard bits: ", _block_cache_shard_bits,
               ", shard size: ", shard_size,
               ". it is probably useful to set "
               "`--rocksdb.enforce-block-cache-size-limit` to false ",
               "to avoid incomplete cache reads.");
    }
  }

  uint32_t max = _max_background_jobs / 2;
  uint32_t clamped = std::max(
    std::min(static_cast<uint32_t>(number_of_cores::GetValue()), max), 1U);
  // lets test this out
  if (_num_threads_high == 0) {
    _num_threads_high = clamped;
  }
  if (_num_threads_low == 0) {
    _num_threads_low = clamped;
  }

  if (_max_subcompactions > _num_threads_low) {
    if (server().options()->processingResult().touched(
          "--rocksdb.max-subcompactions")) {
      SDB_WARN("xxxxx", Logger::ENGINES,
               "overriding value for option `--rocksdb.max-subcompactions` to ",
               _num_threads_low,
               " because the specified value is greater than the number of "
               "threads for low priority operations");
    }
    _max_subcompactions = _num_threads_low;
  }

  SDB_TRACE(
    "xxxxx", Logger::ENGINES, "using RocksDB options: wal_dir: '",
    _wal_directory, "'", ", compression type: ", _compression_type,
    ", write_buffer_size: ", _write_buffer_size,
    ", total_write_buffer_size: ", _total_write_buffer_size,
    ", max_write_buffer_number: ", _max_write_buffer_number,
    ", max_write_buffer_size_to_maintain: ", _max_write_buffer_size_to_maintain,
    ", max_total_wal_size: ", _max_total_wal_size,
    ", delayed_write_rate: ", _delayed_write_rate,
    ", min_write_buffer_number_to_merge: ", _min_write_buffer_number_to_merge,
    ", num_levels: ", _num_levels,
    ", num_uncompressed_levels: ", _num_uncompressed_levels,
    ", max_bytes_for_level_base: ", _max_bytes_for_level_base,
    ", max_bytes_for_level_multiplier: ", _max_bytes_for_level_multiplier,
    ", max_background_jobs: ", _max_background_jobs,
    ", max_sub_compactions: ", _max_subcompactions,
    ", target_file_size_base: ", _target_file_size_base,
    ", target_file_size_multiplier: ", _target_file_size_multiplier,
    ", num_threads_high: ", _num_threads_high,
    ", num_threads_low: ", _num_threads_low,
    ", block_cache_type: ", _block_cache_type,
    ", use_jemalloc_allocator: ", _use_jemalloc_allocator,
    ", block_cache_size: ", _block_cache_size,
    ", block_cache_shard_bits: ", _block_cache_shard_bits,
    ", block_cache_estimated_entry_charge: ",
    _block_cache_estimated_entry_charge,
    ", block_cache_strict_capacity_limit: ", _enforce_block_cache_size_limit,
    ", cache_index_and_filter_blocks: ", _cache_index_and_filter_blocks,
    ", cache_index_and_filter_blocks_with_high_priority: ",
    _cache_index_and_filter_blocks_with_high_priority,
    ", table_block_size: ", _table_block_size,
    ", recycle_log_file_num: ", _recycle_log_file_num,
    ", compaction_read_ahead_size: ", _compaction_readahead_size,
    ", level0_compaction_trigger: ", _level0_compaction_trigger,
    ", level0_slowdown_trigger: ", _level0_slowdown_trigger,
    ", periodic_compaction_ttl: ", _periodic_compaction_ttl,
    ", checksum: ", _checksum_type, ", format_version: ", _format_version,
    ", bloom_bits_per_key: ", _bloom_bits_per_key,
    ", enable_blob_files: ", _enable_blob_files,
    ", enable_blob_cache: ", _enable_blob_cache,
    ", min_blob_size: ", _min_blob_size, ", blob_file_size: ", _blob_file_size,
    ", blob_file_starting_level: ", _blob_file_starting_level,
    ", blob_compression type: ", _blob_compression_type,
    ", enable_blob_garbage_collection: ", _enable_blob_garbage_collection,
    ", blob_garbage_collection_age_cutoff: ",
    _blob_garbage_collection_age_cutoff,
    ", blob_garbage_collection_force_threshold: ",
    _blob_garbage_collection_force_threshold,
    ", prepopulate_blob_cache: ", _prepopulate_blob_cache,
    ", enable_index_compression: ", _enable_index_compression,
    ", prepopulate_block_cache: ", _prepopulate_block_cache,
    ", enable_pipelined_write: ", _enable_pipelined_write,
    ", optimize_filters_for_hits: ", _optimize_filters_for_hits,
    ", use_direct_reads: ", _use_direct_reads,
    ", use_direct_io_for_flush_and_compaction: ",
    _use_direct_io_for_flush_and_compaction, ", use_fsync: ", _use_fsync,
    ", allow_fallocate: ", _allow_fallocate,
    ", max_open_files limit: ", _limit_open_files_at_startup,
    ", dynamic_level_bytes: ", _dynamic_level_bytes);
}

rocksdb::TransactionDBOptions RocksDBOptionFeature::getTransactionDBOptions()
  const {
  rocksdb::TransactionDBOptions result;
  // number of locks per column_family
  result.num_stripes =
    std::max(size_t(1), static_cast<size_t>(_transaction_lock_stripes));
  result.transaction_lock_timeout = _transaction_lock_timeout;
  return result;
}

rocksdb::Options RocksDBOptionFeature::doGetOptions() const {
  rocksdb::Options result;
  // TODO disable by default result.paranoid_checks
  // TODO disable by default result.flush_verify_memtable_count
  // TODO disable by default result.compaction_verify_record_count
  // TODO disable by default result.verify_sst_unique_id_in_manifest
  // TODO disable by default result.force_consistency_checks
  // TODO ability to enable result.track_and_verify_wals_in_manifest
  // TODO ability to enable result.memtable_whole_key_filtering
  // TODO ability to enable result.paranoid_file_checks
  // TODO ability to enable result.memtable_protection_bytes_per_key
  // TODO ability to enable result.block_protection_bytes_per_key
  // TODO ability to enable result.paranoid_memory_checks
  // TODO result.max_sequential_skip_in_iterations
  result.allow_fallocate = _allow_fallocate;
  result.enable_pipelined_write = _enable_pipelined_write;
  result.write_buffer_size = static_cast<size_t>(_write_buffer_size);
  result.max_write_buffer_number = static_cast<int>(_max_write_buffer_number);
  result.max_write_buffer_size_to_maintain = _max_write_buffer_size_to_maintain;
  result.delayed_write_rate = _delayed_write_rate;
  result.min_write_buffer_number_to_merge =
    static_cast<int>(_min_write_buffer_number_to_merge);
  result.num_levels = static_cast<int>(_num_levels);
  result.level_compaction_dynamic_level_bytes = _dynamic_level_bytes;
  result.max_bytes_for_level_base = _max_bytes_for_level_base;
  result.max_bytes_for_level_multiplier =
    static_cast<int>(_max_bytes_for_level_multiplier);
  result.optimize_filters_for_hits = _optimize_filters_for_hits;
  result.use_direct_reads = _use_direct_reads;
  result.use_direct_io_for_flush_and_compaction =
    _use_direct_io_for_flush_and_compaction;

  result.target_file_size_base = _target_file_size_base;
  result.target_file_size_multiplier =
    static_cast<int>(_target_file_size_multiplier);
  // during startup, limit the total WAL size to a small value so we do not see
  // large WAL files created at startup.
  // Instead, we will start with a small value here and up it later in the
  // startup process
  result.max_total_wal_size = 4 * 1024 * 1024;

  result.wal_dir = _wal_directory;

  if (_skip_corrupted) {
    result.wal_recovery_mode =
      rocksdb::WALRecoveryMode::kSkipAnyCorruptedRecords;
  } else {
    result.wal_recovery_mode = rocksdb::WALRecoveryMode::kPointInTimeRecovery;
  }

  result.max_background_jobs = static_cast<int>(_max_background_jobs);
  result.max_subcompactions = _max_subcompactions;
  result.use_fsync = _use_fsync;

  rocksdb::CompressionType compression_type =
    ::CompressionTypeFromString(_compression_type);

  // only compress levels >= 2
  result.compression_per_level.resize(result.num_levels);
  for (int level = 0; level < result.num_levels; ++level) {
    result.compression_per_level[level] =
      ((static_cast<uint64_t>(level) >= _num_uncompressed_levels)
         ? compression_type
         : rocksdb::kNoCompression);
  }

  result.compaction_style = ::CompactionStyleFromString(_compaction_style);
  result.compaction_pri = rocksdb::kMinOverlappingRatio;

  // Number of files to trigger level-0 compaction. A value <0 means that
  // level-0 compaction will not be triggered by number of files at all.
  // Default: 4
  result.level0_file_num_compaction_trigger =
    static_cast<int>(_level0_compaction_trigger);

  // Soft limit on number of level-0 files. We start slowing down writes at this
  // point. A value <0 means that no writing slow down will be triggered by
  // number of files in level-0.
  result.level0_slowdown_writes_trigger =
    static_cast<int>(_level0_slowdown_trigger);

  // Maximum number of level-0 files.  We stop writes at this point.
  result.level0_stop_writes_trigger = static_cast<int>(_level0_stop_trigger);

  // Soft limit on pending compaction bytes. We start slowing down writes
  // at this point.
  result.soft_pending_compaction_bytes_limit =
    _pending_compaction_bytes_slowdown_trigger;

  // Maximum number of pending compaction bytes. We stop writes at this point.
  result.hard_pending_compaction_bytes_limit =
    _pending_compaction_bytes_stop_trigger;

  // table cache is only used when max_open_files != -1
  result.table_cache_numshardbits = 8;

  result.recycle_log_file_num = _recycle_log_file_num;
  result.compaction_readahead_size =
    static_cast<size_t>(_compaction_readahead_size);

  if (_enable_statistics) {
    result.statistics = rocksdb::CreateDBStatistics();
    // result.stats_dump_period_sec = 1;
  }

  result.table_factory.reset(
    rocksdb::NewBlockBasedTableFactory(getTableOptions()));

  result.create_if_missing = true;
  result.create_missing_column_families = true;

  if (_limit_open_files_at_startup) {
    result.max_open_files = 16;
    result.skip_stats_update_on_db_open = true;
    result.avoid_flush_during_recovery = true;
  } else {
    result.max_open_files = -1;
  }

  if (_total_write_buffer_size > 0) {
    result.db_write_buffer_size = _total_write_buffer_size;
  }

  // we manage WAL file deletion ourselves, don't let RocksDB garbage-collect
  // obsolete files.
  result.WAL_ttl_seconds =
    933120000;  // ~30 years (60 * 60 * 24 * 30 * 12 * 30)
  result.WAL_size_limit_MB = 0;
  result.memtable_prefix_bloom_size_ratio = 0.2;  // TODO: pick better value?
  // TODO: enable memtable_insert_with_hint_prefix_extractor?
  result.bloom_locality = 1;

  if (!server().options()->processingResult().touched(
        "rocksdb.max-write-buffer-number")) {
    // TODO It is unclear if this value makes sense as a default, but we aren't
    // changing it yet, in order to maintain backwards compatibility.

    // user hasn't explicitly set the number of write buffers, so we use a
    // default value based on the number of column families this is
    // cfFamilies.size() + 2 ... but _option needs to be set before
    //  building cfFamilies
    // Update max_write_buffer_number above if you change number of families
    // used
    result.max_write_buffer_number = 8 + 2;
  } else if (result.max_write_buffer_number < 4) {
    // user set the value explicitly, and it is lower than recommended
    result.max_write_buffer_number = 4;
    SDB_WARN("xxxxx", Logger::ENGINES,
             "overriding value for option `--rocksdb.max-write-buffer-number` "
             "to 4 because it is lower than recommended");
  }

  return result;
}

rocksdb::BlockBasedTableOptions RocksDBOptionFeature::doGetTableOptions()
  const {
  rocksdb::BlockBasedTableOptions result;

  result.cache_index_and_filter_blocks = _cache_index_and_filter_blocks;
  result.cache_index_and_filter_blocks_with_high_priority =
    _cache_index_and_filter_blocks_with_high_priority;

  // TODO make it parameters
  result.metadata_cache_options.top_level_index_pinning =
    rocksdb::PinningTier::kFlushedAndSimilar;
  result.metadata_cache_options.partition_pinning =
    rocksdb::PinningTier::kFlushedAndSimilar;
  result.metadata_cache_options.unpartitioned_pinning =
    rocksdb::PinningTier::kFlushedAndSimilar;

  if (_block_cache_size > 0) {
    std::shared_ptr<rocksdb::MemoryAllocator> allocator;

#ifdef SERENEDB_HAVE_JEMALLOC
    if (_use_jemalloc_allocator) {
      rocksdb::JemallocAllocatorOptions jopts;
      rocksdb::Status s =
        rocksdb::NewJemallocNodumpAllocator(jopts, &allocator);
      if (!s.ok()) {
        SDB_FATAL(
          "xxxxx", Logger::STARTUP,
          "unable to use jemalloc allocator for RocksDB: ", s.ToString());
      }
    }
#endif

    if (_block_cache_type == ::kBlockCacheTypeLRU) {
      rocksdb::LRUCacheOptions opts;
      opts.capacity = _block_cache_size;
      opts.num_shard_bits = _block_cache_shard_bits;
      opts.strict_capacity_limit = _enforce_block_cache_size_limit;
      opts.memory_allocator = std::move(allocator);

      result.block_cache = opts.MakeSharedCache();
    } else if (_block_cache_type == ::kBlockCacheTypeHyperClock) {
      rocksdb::HyperClockCacheOptions opts{
        _block_cache_size, _block_cache_estimated_entry_charge,
        _block_cache_shard_bits, _enforce_block_cache_size_limit,
        std::move(allocator)};

      result.block_cache = opts.MakeSharedCache();
    }
  } else {
    result.no_block_cache = true;
  }

  result.block_size = _table_block_size;
  result.filter_policy.reset(
    rocksdb::NewRibbonFilterPolicy(_bloom_bits_per_key));
  result.enable_index_compression = _enable_index_compression;
  result.format_version = _format_version;
  result.optimize_filters_for_memory = _optimize_filters_for_memory;
  result.prepopulate_block_cache =
    _prepopulate_block_cache
      ? rocksdb::BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly
      : rocksdb::BlockBasedTableOptions::PrepopulateBlockCache::kDisable;

  if (_enforce_block_cache_size_limit) {
    auto decision = [&] {
      return _reserve_block_cache_memory
               ? rocksdb::CacheEntryRoleOptions::Decision::kEnabled
               : rocksdb::CacheEntryRoleOptions::Decision::kDisabled;
    };
    result.cache_usage_options.options_overrides.emplace(
      rocksdb::CacheEntryRole::kCompressionDictionaryBuildingBuffer,
      decision());
    result.cache_usage_options.options_overrides.emplace(
      rocksdb::CacheEntryRole::kFilterConstruction, decision());
    result.cache_usage_options.options_overrides.emplace(
      rocksdb::CacheEntryRole::kBlockBasedTableReader, decision());
    result.cache_usage_options.options_overrides.emplace(
      rocksdb::CacheEntryRole::kFileMetadata, decision());
  }

  result.block_align = _block_align_data_blocks;

  if (_checksum_type == ::kChecksumTypeCRC32C) {
    result.checksum = rocksdb::ChecksumType::kCRC32c;
  } else if (_checksum_type == ::kChecksumTypeXXHash) {
    result.checksum = rocksdb::ChecksumType::kxxHash;
  } else if (_checksum_type == ::kChecksumTypeXXHash64) {
    result.checksum = rocksdb::ChecksumType::kxxHash64;
  } else if (_checksum_type == ::kChecksumTypeXXH3) {
    result.checksum = rocksdb::ChecksumType::kXXH3;
  } else {
    SDB_ASSERT(false);
    SDB_WARN("xxxxx", sdb::Logger::STARTUP,
             "unexpected value for '--rocksdb.checksum-type'");
  }

  // TODO
  // result.detect_filter_construct_corruption
  // result.verify_compression

  return result;
}

rocksdb::ColumnFamilyOptions RocksDBOptionFeature::getColumnFamilyOptions(
  RocksDBColumnFamilyManager::Family family) const {
  auto result = getColumnFamilyOptionsDefault(family);

  if (family == RocksDBColumnFamilyManager::Family::Documents ||
      family == RocksDBColumnFamilyManager::Family::Data) {
    result.enable_blob_files = _enable_blob_files;
    result.min_blob_size = _min_blob_size;
    result.blob_file_size = _blob_file_size;
    result.blob_compression_type =
      ::CompressionTypeFromString(_blob_compression_type);
    result.enable_blob_garbage_collection = _enable_blob_garbage_collection;
    result.blob_garbage_collection_age_cutoff =
      _blob_garbage_collection_age_cutoff;
    result.blob_garbage_collection_force_threshold =
      _blob_garbage_collection_force_threshold;
    result.blob_file_starting_level = _blob_file_starting_level;
    result.prepopulate_blob_cache =
      _prepopulate_blob_cache ? rocksdb::PrepopulateBlobCache::kFlushOnly
                              : rocksdb::PrepopulateBlobCache::kDisable;
    if (_enable_blob_cache) {
      // use whatever block cache we use for blobs as well
      result.blob_cache = getTableOptions().block_cache;
    }
    if (_partition_files_for_documents_cf) {
      // partition .sst files by object id prefix
      if (family == RocksDBColumnFamilyManager::Family::Data) {
        result.sst_partitioner_factory =
          rocksdb::NewSstPartitionerFixedPrefixFactory(
            RocksDBKey::objectIdSize() + sizeof(catalog::Column::Id));
      } else {
        result.sst_partitioner_factory =
          rocksdb::NewSstPartitionerFixedPrefixFactory(sizeof(uint64_t));
      }
    }
  } else if (family == RocksDBColumnFamilyManager::Family::PrimaryIndex) {
    // partition .sst files by object id prefix
    if (_partition_files_for_primary_index_cf) {
      result.sst_partitioner_factory =
        rocksdb::NewSstPartitionerFixedPrefixFactory(sizeof(uint64_t));
    }
    // keep immutable mem tables around in memory for conflict checking
    result.max_write_buffer_size_to_maintain = 64 << 20;
  } else if (family == RocksDBColumnFamilyManager::Family::EdgeIndex) {
    // partition .sst files by object id prefix
    if (_partition_files_for_edge_index_cf) {
      result.sst_partitioner_factory =
        rocksdb::NewSstPartitionerFixedPrefixFactory(sizeof(uint64_t));
    }
  } else if (family == RocksDBColumnFamilyManager::Family::VPackIndex) {
    // partition .sst files by object id prefix
    if (_partition_files_for_vpack_index_cf) {
      result.sst_partitioner_factory =
        rocksdb::NewSstPartitionerFixedPrefixFactory(sizeof(uint64_t));
    }
  }

  // override
  const auto index = std::to_underlying(family);
  SDB_ASSERT(index < _max_write_buffer_number_cf.size());
  if (_max_write_buffer_number_cf[index] > 0) {
    result.max_write_buffer_number =
      static_cast<int>(_max_write_buffer_number_cf[index]);
  }
  if (!_min_write_buffer_number_to_merge_touched) {
    result.min_write_buffer_number_to_merge =
      static_cast<int>(DefaultMinWriteBufferNumberToMerge(
        _total_write_buffer_size, _write_buffer_size,
        result.max_write_buffer_number));
  }

  return result;
}

rocksdb::ColumnFamilyOptions
RocksDBOptionFeature::getColumnFamilyOptionsDefault(
  RocksDBColumnFamilyManager::Family family) const {
  rocksdb::ColumnFamilyOptions result(getOptions());

  auto make_column_optimized_for_get = [&] {
    result.prefix_extractor.reset(
      rocksdb::NewFixedPrefixTransform(RocksDBKey::objectIdSize()));

    rocksdb::BlockBasedTableOptions table_options(getTableOptions());
    table_options.data_block_index_type =
      rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
    result.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(table_options));
  };

  // TODO(mbkkt) current column families are wrong
  // Instead we want to have such column families:
  // clang-format off
  // | name      | key             | value      | prefix        | filter        | index    | data blocks |
  // | Documents | objID, pk       | rev, data  | objID         | hits + key    | binary*  | hash        |
  // | Unique    | objID, data     | pk         | objID         | key           | binary*  | hash        |
  // | Sorted    | objID, data, pk |            | objID         | hits + prefix | firstKey | binary      |
  // | Lookup    | objID, data, pk |            | objID, data   | prefix        | firstKey | binary      |
  // clang-format on
  // * -- maybe hash if shouldn't be sorted
  // Documents and PrimaryIndex  => Documents
  // PrimaryIndex and VPackIndex => Unique
  // VPackIndex                  => Sorted
  // EdgeIndex                   => Lookup
  switch (family) {
    case sdb::RocksDBColumnFamilyManager::Family::Data: {
      result.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(
        RocksDBKey::objectIdSize() + sizeof(catalog::Column::Id)));

      auto table_options = getTableOptions();
      result.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));
    } break;
    case RocksDBColumnFamilyManager::Family::Definitions:
    case RocksDBColumnFamilyManager::Family::Invalid:
      break;
    case RocksDBColumnFamilyManager::Family::Documents: {
      result.optimize_filters_for_hits = true;
      make_column_optimized_for_get();
    } break;
    case RocksDBColumnFamilyManager::Family::PrimaryIndex: {
      make_column_optimized_for_get();
    } break;
    case RocksDBColumnFamilyManager::Family::EdgeIndex: {
      // we don't expect a lot of source vertexes with 0 outbound edges
      result.optimize_filters_for_hits = true;
      result.prefix_extractor = std::make_shared<RocksDBPrefixExtractor>();

      rocksdb::BlockBasedTableOptions table_options(getTableOptions());
      // there is not a lot of sense in bloom filter for each edge
      // because it can be used only to remove single edge
      table_options.whole_key_filtering = false;
      // kBinarySearchWithFirstKey + kNoShortening best for short scans
      table_options.index_type =
        rocksdb::BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
      table_options.index_shortening =
        rocksdb::BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
      result.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));
    } break;
    case RocksDBColumnFamilyManager::Family::VPackIndex: {
      // we expect that index exist
      result.optimize_filters_for_hits = true;
      result.prefix_extractor.reset(
        rocksdb::NewFixedPrefixTransform(RocksDBKey::objectIdSize()));
      // vpack based index variants with custom comparator
      // TODO(mbkkt) in general it's unnecessary, we should write vpack value in
      // another format, like icu::Collator::getSortKey
      result.comparator = _vpack_cmp.get();
      rocksdb::BlockBasedTableOptions table_options(getTableOptions());
      // there is not any sense in bloom filter for each value
      // because we never makes Get or full key Seek
      table_options.whole_key_filtering = false;
      result.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));
    } break;
  }

  // set TTL for .sst file compaction
  result.ttl = periodicCompactionTtl();

  return result;
}
