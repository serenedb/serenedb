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
#include <utilities/merge_operators/uint64add.h>

#include <algorithm>
#include <cstddef>
#include <ios>
#include <limits>
#include <memory>

#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>
#endif

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

namespace {

bool IsIoUringSupported() {
#if defined(__linux__) && defined(__NR_io_uring_setup)
  // Try to call io_uring_setup with invalid parameters
  // If it returns ENOSYS, io_uring is not supported
  long ret = syscall(__NR_io_uring_setup, 0, nullptr);
  return ret != -1 || errno != ENOSYS;
#else
  return false;
#endif
}

}  // namespace

using namespace sdb;
using namespace sdb::app;
using namespace sdb::options;

namespace sdb {

bool IsIOUringEnabled() {
#ifdef SDB_GTEST
  static const bool kSupported = IsIoUringSupported();
  return kSupported;
#else
  return SerenedServer::Instance()
    .getFeature<RocksDBOptionFeature>()
    .ioUringEnabled();
#endif
}

}  // namespace sdb
namespace {

const std::string kIoUringEnabled = "enabled";
const std::string kIoUringDisabled = "disabled";
const std::string kIoUringNotSupported = "not-supported";

const containers::FlatHashSet<std::string> kIoUringValues = {
  kIoUringEnabled,
  kIoUringDisabled,
  kIoUringNotSupported,
};

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
  SDB_FATAL(STARTUP, "unexpected compression type '",
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
  SDB_FATAL(STARTUP, "unexpected compaction style '",
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
    _partition_files_for_default_cf(true),
    _io_uring(IsIoUringSupported() ? kIoUringEnabled : kIoUringNotSupported),
    _max_transaction_size(transaction::Options::gDefaultMaxTransactionSize),
    _intermediate_commit_size(
      transaction::Options::gDefaultIntermediateCommitSize),
    _intermediate_commit_count(
      transaction::Options::gDefaultIntermediateCommitCount),
    _vpack_cmp(std::make_unique<RocksDBVPackComparator>()),
    _max_write_buffer_number_cf{0, 0} {
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

bool RocksDBOptionFeature::ioUringEnabled() const noexcept {
  return _io_uring == kIoUringEnabled;
}

void RocksDBOptionFeature::validateOptions(
  std::shared_ptr<ProgramOptions> options) {
  transaction::Options::setLimits(_max_transaction_size,
                                  _intermediate_commit_size,
                                  _intermediate_commit_count);

  if (_io_uring == kIoUringNotSupported &&
      options->processingResult().touched("--rocksdb.io-uring")) {
    SDB_WARN(STARTUP,
             "io_uring is not supported on this system, ignoring "
             "--rocksdb.io-uring configuration");
  }

  if (_sync_interval > 0) {
    if (_sync_interval < kMinSyncInterval) {
      // _sync_interval = 0 means turned off!
      SDB_FATAL(STARTUP,
        "invalid value for --rocksdb.sync-interval. Please use a value ",
        "of at least ", kMinSyncInterval);
    }

    if (_sync_delay_threshold > 0 && _sync_delay_threshold <= _sync_interval) {
      if (!options->processingResult().touched("rocksdb.sync-interval") &&
          options->processingResult().touched("rocksdb.sync-delay-threshold")) {
        // user has not set --rocksdb.sync-interval, but set
        // --rocksdb.sync-delay-threshold
        SDB_WARN(STARTUP,
                 "invalid value for --rocksdb.sync-delay-threshold. should be "
                 "higher ",
                 "than the value of --rocksdb.sync-interval (", _sync_interval,
                 ")");
      }

      _sync_delay_threshold = 10 * _sync_interval;
      SDB_WARN(STARTUP,
               "auto-adjusting value of --rocksdb.sync-delay-threshold to ",
               _sync_delay_threshold, " ms");
    }
  }

  if (_prune_wait_time_initial < 10) {
    SDB_WARN(STORAGE,
             "consider increasing the value for "
             "--rocksdb.wal-file-timeout-initial. ",
             "Replication clients might have trouble to get in sync");
  }

  if (_write_buffer_size > 0 && _write_buffer_size < 1024 * 1024) {
    SDB_FATAL(STARTUP,
              "invalid value for '--rocksdb.write-buffer-size'");
  }
  if (_total_write_buffer_size > 0 &&
      _total_write_buffer_size < 64 * 1024 * 1024) {
    SDB_FATAL(STARTUP,
              "invalid value for '--rocksdb.total-write-buffer-size'");
  }
  if (_max_background_jobs != -1 && _max_background_jobs < 1) {
    SDB_FATAL(STARTUP,
              "invalid value for '--rocksdb.max-background-jobs'");
  }

  _min_write_buffer_number_to_merge_touched =
    options->processingResult().touched(
      "--rocksdb.min-write-buffer-number-to-merge");

  if (_block_cache_type == ::kBlockCacheTypeLRU &&
      options->processingResult().touched(
        "--rocksdb.block-cache-estimated-entry-charge")) {
    SDB_WARN(STORAGE,
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
    SDB_INFO(STARTUP,
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
      SDB_WARN(STORAGE,
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
      SDB_WARN(STORAGE,
               "overriding value for option `--rocksdb.max-subcompactions` to ",
               _num_threads_low,
               " because the specified value is greater than the number of "
               "threads for low priority operations");
    }
    _max_subcompactions = _num_threads_low;
  }

  SDB_TRACE(STORAGE, "using RocksDB options: wal_dir: '",
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
    SDB_WARN(STORAGE,
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
        SDB_FATAL(STARTUP,
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
    SDB_WARN(STARTUP,
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

  if (family == RocksDBColumnFamilyManager::Family::Default) {
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
    if (_partition_files_for_default_cf) {
      // partition .sst files by object id prefix
      if (family == RocksDBColumnFamilyManager::Family::Default) {
        result.sst_partitioner_factory =
          rocksdb::NewSstPartitionerFixedPrefixFactory(
            sizeof(ObjectId) + sizeof(catalog::Column::Id));
      } else {
        result.sst_partitioner_factory =
          rocksdb::NewSstPartitionerFixedPrefixFactory(sizeof(ObjectId));
      }
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

  // TODO(mbkkt) current column families are wrong
  switch (family) {
    case RocksDBColumnFamilyManager::Family::Default: {
      // TODO(mbkkt) make it fixed?
      result.prefix_extractor.reset(rocksdb::NewCappedPrefixTransform(
        sizeof(ObjectId) + sizeof(catalog::Column::Id)));

      auto table_options = getTableOptions();
      result.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));
    } break;
    case RocksDBColumnFamilyManager::Family::Definitions:
      break;
    case RocksDBColumnFamilyManager::Family::Sequences:
      result.merge_operator = std::make_shared<rocksdb::UInt64AddOperator>();
      break;
    default:
      SDB_UNREACHABLE();
      break;
  }

  // set TTL for .sst file compaction
  result.ttl = periodicCompactionTtl();

  return result;
}
