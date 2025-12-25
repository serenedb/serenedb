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

#pragma once

#include <rocksdb/options.h>
#include <rocksdb/table.h>

#include <cstdint>
#include <memory>
#include <string>

#include "basics/common.h"
#include "rest_server/serened.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_comparator.h"

namespace sdb {
namespace options {
class ProgramOptions;
}

class RocksDBOptionFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "RocksDBOption"; }

  explicit RocksDBOptionFeature(Server& server);

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void validateOptions(std::shared_ptr<options::ProgramOptions>) final;

  const rocksdb::Options& getOptions() const;
  const rocksdb::BlockBasedTableOptions& getTableOptions() const;

  rocksdb::TransactionDBOptions getTransactionDBOptions() const;
  rocksdb::ColumnFamilyOptions getColumnFamilyOptions(
    RocksDBColumnFamilyManager::Family family) const;

  bool limitOpenFilesAtStartup() const noexcept {
    return _limit_open_files_at_startup;
  }
  uint64_t maxTotalWalSize() const noexcept { return _max_total_wal_size; }
  uint32_t numThreadsHigh() const noexcept { return _num_threads_high; }
  uint32_t numThreadsLow() const noexcept { return _num_threads_low; }
  uint64_t periodicCompactionTtl() const noexcept {
    return _periodic_compaction_ttl;
  }
  auto pruneWaitTimeInitial() const noexcept {
    return _prune_wait_time_initial;
  }

 private:
  uint64_t _transaction_lock_stripes;
  int64_t _transaction_lock_timeout;
  std::string _wal_directory;
  uint64_t _total_write_buffer_size;
  uint64_t _write_buffer_size;
  // Update max_write_buffer_number above if you change number of families used
  uint64_t _max_write_buffer_number;
  int64_t _max_write_buffer_size_to_maintain;
  uint64_t _max_total_wal_size;
  uint64_t _delayed_write_rate;
  uint64_t _min_write_buffer_number_to_merge;
  uint64_t _num_levels;
  uint64_t _num_uncompressed_levels;
  uint64_t _max_bytes_for_level_base;
  double _max_bytes_for_level_multiplier;
  int32_t _max_background_jobs;
  uint32_t _max_subcompactions;
  uint32_t _num_threads_high;
  uint32_t _num_threads_low;
  uint64_t _target_file_size_base;
  uint64_t _target_file_size_multiplier;
  uint64_t _block_cache_size;
  int _block_cache_shard_bits;
  // only used for HyperClockCache
  uint64_t _block_cache_estimated_entry_charge;
  uint64_t _min_blob_size;
  uint64_t _blob_file_size;
  uint32_t _blob_file_starting_level;
  bool _enable_blob_files;
  bool _enable_blob_cache;
  double _blob_garbage_collection_age_cutoff;
  double _blob_garbage_collection_force_threshold;
  double _bloom_bits_per_key;
  uint64_t _table_block_size;
  uint64_t _compaction_readahead_size;
  int64_t _level0_compaction_trigger;
  int64_t _level0_slowdown_trigger;
  int64_t _level0_stop_trigger;
  uint64_t _pending_compaction_bytes_slowdown_trigger;
  uint64_t _pending_compaction_bytes_stop_trigger;
  uint64_t _periodic_compaction_ttl;
  size_t _recycle_log_file_num;
  std::string _compression_type;
  std::string _blob_compression_type;
  std::string _block_cache_type;
  std::string _checksum_type;
  std::string _compaction_style;
  uint32_t _format_version;
  bool _optimize_filters_for_memory;
  bool _enable_index_compression;
  bool _use_jemalloc_allocator;
  bool _prepopulate_block_cache;
  bool _prepopulate_blob_cache;
  bool _reserve_block_cache_memory;
  bool _enforce_block_cache_size_limit;
  bool _cache_index_and_filter_blocks;
  bool _cache_index_and_filter_blocks_with_high_priority;
  bool _block_align_data_blocks;
  bool _enable_pipelined_write;
  bool _optimize_filters_for_hits;
  bool _use_direct_reads;
  bool _use_direct_io_for_flush_and_compaction;
  bool _use_fsync;
  bool _skip_corrupted;
  bool _dynamic_level_bytes;
  bool _enable_statistics;
  bool _limit_open_files_at_startup;
  bool _allow_fallocate;
  bool _enable_blob_garbage_collection;
  bool _min_write_buffer_number_to_merge_touched;
  bool _partition_files_for_documents_cf;
  bool _partition_files_for_primary_index_cf;
  bool _partition_files_for_edge_index_cf;
  bool _partition_files_for_vpack_index_cf;

 public:
  /// minimum required percentage of free disk space for considering the
  /// server "healthy". this is expressed as a floating point value between 0
  /// and 1! if set to 0.0, the % amount of free disk is ignored in checks.
  double _required_disk_free_percentage = 0.01;
  /// minimum number of free bytes on disk for considering the server
  /// healthy. if set to 0, the number of free bytes on disk is ignored in
  /// checks.
  uint64_t _required_disk_free_bytes = 16 * 1024 * 1024;
  uint64_t _max_transaction_size;      // maximum allowed size for a transaction
  uint64_t _intermediate_commit_size;  // maximum size for a
                                       // transaction before an
                                       // intermediate commit is performed
  uint64_t _intermediate_commit_count;  // limit of transaction count
                                        // for intermediate commit
  uint64_t _max_parallel_compactions = 2;
  // WAL sync interval, specified in milliseconds by end user, but uses
  // microseconds internally
  uint64_t _sync_interval = 100;
  // WAL sync delay threshold. Any WAL disk sync longer ago than this value
  // will trigger a warning (in milliseconds)
  uint64_t _sync_delay_threshold = 5000;
  // number of seconds to wait before an obsolete WAL file is actually pruned
  double _prune_wait_time = 10.0;
  // number of seconds to wait initially after server start before WAL file
  // deletion kicks in
  double _prune_wait_time_initial = 60.0;
  /// activate rocksdb's debug logging
  bool _debug_logging = false;
  // interval (in s) in which auto-flushing is tried
  double _auto_flush_check_interval = 60.0 * 30.0;
  // minimum number of live WAL files that need to be present to trigger
  // an auto-flush
  uint64_t _auto_flush_min_wal_files = 20;
  /// maximum total size (in bytes) of archived WAL files
  uint64_t _max_wal_archive_size_limit = 0;
  /// whether or not to verify the sst files present in the db path
  bool _verify_sst = false;

 private:
  rocksdb::Options doGetOptions() const;
  rocksdb::BlockBasedTableOptions doGetTableOptions() const;
  rocksdb::ColumnFamilyOptions getColumnFamilyOptionsDefault(
    RocksDBColumnFamilyManager::Family family) const;

  std::unique_ptr<RocksDBVPackComparator> _vpack_cmp;
  mutable std::optional<rocksdb::Options> _options;
  mutable std::optional<rocksdb::BlockBasedTableOptions> _table_options;

  /// per column family write buffer limits
  std::array<uint64_t, RocksDBColumnFamilyManager::kNumberOfColumnFamilies>
    _max_write_buffer_number_cf;
};

}  // namespace sdb
