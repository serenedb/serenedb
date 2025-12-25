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

#include "rocksdb_engine_catalog.h"

#include <absl/strings/str_cat.h>
#include <rocksdb/advanced_cache.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/transaction_log.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/write_batch.h>
#include <vpack/builder.h>
#include <vpack/collection.h>
#include <vpack/iterator.h>

#include <iomanip>
#include <limits>
#include <memory>
#include <utility>

#include "app/app_server.h"
#include "app/language.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "basics/application-exit.h"
#include "basics/assert.h"
#include "basics/build.h"
#include "basics/down_cast.h"
#include "basics/error_code.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/exitcodes.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/result.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "basics/system-functions.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/types.h"
#include "connector/key_utils.hpp"
#include "database/ticks.h"
#include "general_server/rest_handler_factory.h"
#include "general_server/scheduler_feature.h"
#include "general_server/server_options_feature.h"
#include "general_server/state.h"
#include "metrics/counter_builder.h"
#include "metrics/gauge_builder.h"
#include "metrics/histogram_builder.h"
#include "metrics/metric.h"
#include "metrics/metrics_feature.h"
#include "rest/version.h"
#include "rest_server/database_path_feature.h"
#include "rest_server/flush_feature.h"
#include "rest_server/serened_single.h"
#include "rest_server/server_id_feature.h"
#include "rocksdb_engine_catalog/listeners/rocksdb_background_error_listener.h"
#include "rocksdb_engine_catalog/listeners/rocksdb_metrics_listener.h"
#include "rocksdb_engine_catalog/options.h"
#include "rocksdb_engine_catalog/rocksdb_background_thread.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_comparator.h"
#include "rocksdb_engine_catalog/rocksdb_format.h"
#include "rocksdb_engine_catalog/rocksdb_key.h"
#include "rocksdb_engine_catalog/rocksdb_key_bounds.h"
#include "rocksdb_engine_catalog/rocksdb_log_value.h"
#include "rocksdb_engine_catalog/rocksdb_option_feature.h"
#include "rocksdb_engine_catalog/rocksdb_recovery_manager.h"
#include "rocksdb_engine_catalog/rocksdb_settings_manager.h"
#include "rocksdb_engine_catalog/rocksdb_sync_thread.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"
#include "rocksdb_engine_catalog/rocksdb_value.h"
#include "rocksdb_engine_catalog/rocksdb_wal_access.h"
#include "storage_engine/table_shard.h"
#include "vpack/serializer.h"
#include "vpack/slice.h"
#include "vpack/vpack_helper.h"

#ifdef SDB_CLUSTER
#include "catalog/search_common.h"
#include "rocksdb_engine/rocksdb_collection.h"
#include "rocksdb_engine/rocksdb_dump_manager.h"
#include "rocksdb_engine/rocksdb_replication_manager.h"
#endif

// we will not use the multithreaded index creation that uses rocksdb's sst
// file ingestion until rocksdb external file ingestion is fixed to have
// correct sequence numbers for the files without gaps
// #define USE_SST_INGESTION

namespace sdb {
namespace {
void StartupVersionCheck(SerenedServer& server, rocksdb::TransactionDB* db,
                         bool db_existed) {
  // try to find version, using the version key
  RocksDBKeyWithBuffer version_key;
  version_key.constructSettingsValue(RocksDBSettingsType::Version);

  if (db_existed) {
    rocksdb::PinnableSlice old_version;
    rocksdb::Status s =
      db->Get({},
              RocksDBColumnFamilyManager::get(
                RocksDBColumnFamilyManager::Family::Definitions),
              version_key.string(), &old_version);

    if (s.IsNotFound() || old_version.size() != 1) {
      SDB_FATAL("xxxxx", Logger::ENGINES,
                "Error reading stored version from database: ",
                rocksutils::ConvertStatus(s).errorMessage());
    } else if (old_version.data()[0] < kRocksDBFormatVersion) {
      // Performing 'upgrade' routine
      if (old_version.data()[0] != '0' || kRocksDBFormatVersion != '1') {
        SDB_FATAL("xxxxx", Logger::ENGINES, "Your database is in an old ",
                  "format. Please downgrade the server, ",
                  "dump & restore the data");
      }

    } else if (old_version.data()[0] > kRocksDBFormatVersion) {
      SDB_FATAL("xxxxx", Logger::ENGINES,
                "You are using an old version of SereneDB, please update ",
                "before opening this database");
    } else {
      SDB_ASSERT(old_version.data()[0] == kRocksDBFormatVersion);
    }
  }

  if (!db_existed) {
    // store current version
    auto s = db->Put(
      rocksdb::WriteOptions(),
      RocksDBColumnFamilyManager::get(
        RocksDBColumnFamilyManager::Family::Definitions),
      version_key.string(),
      rocksdb::Slice{&kRocksDBFormatVersion, sizeof(kRocksDBFormatVersion)});

    if (!s.ok()) {
      SDB_FATAL("xxxxx", Logger::ENGINES, "Error storing endianess/version: ",
                rocksutils::ConvertStatus(s).errorMessage());
    }
  }
}
}  // namespace

DECLARE_GAUGE(rocksdb_wal_released_tick_flush, uint64_t,
              "Released tick for RocksDB WAL deletion (flush-induced)");
DECLARE_GAUGE(rocksdb_wal_sequence, uint64_t, "Current RocksDB WAL sequence");
DECLARE_GAUGE(
  rocksdb_wal_sequence_lower_bound, uint64_t,
  "RocksDB WAL sequence number until which background thread has caught up");
DECLARE_GAUGE(rocksdb_live_wal_files, uint64_t,
              "Number of live RocksDB WAL files");
DECLARE_GAUGE(rocksdb_live_wal_files_size, uint64_t,
              "Cumulated size of live RocksDB WAL files");
DECLARE_GAUGE(rocksdb_archived_wal_files, uint64_t,
              "Number of archived RocksDB WAL files");
DECLARE_GAUGE(rocksdb_archived_wal_files_size, uint64_t,
              "Cumulated size of archived RocksDB WAL files");
DECLARE_GAUGE(rocksdb_prunable_wal_files, uint64_t,
              "Number of prunable RocksDB WAL files");
DECLARE_GAUGE(rocksdb_wal_pruning_active, uint64_t,
              "Whether or not RocksDB WAL file pruning is active");
DECLARE_GAUGE(serenedb_revision_tree_memory_usage, uint64_t,
              "Total memory consumed by all revision trees");
DECLARE_GAUGE(
  serenedb_revision_tree_buffered_memory_usage, uint64_t,
  "Total memory consumed by buffered updates for all revision trees");
DECLARE_GAUGE(serenedb_index_estimates_memory_usage, uint64_t,
              "Total memory consumed by all index selectivity estimates");
DECLARE_COUNTER(serenedb_revision_tree_rebuilds_success_total,
                "Number of successful revision tree rebuilds");
DECLARE_COUNTER(serenedb_revision_tree_rebuilds_failure_total,
                "Number of failed revision tree rebuilds");
DECLARE_COUNTER(serenedb_revision_tree_hibernations_total,
                "Number of revision tree hibernations");
DECLARE_COUNTER(serenedb_revision_tree_resurrections_total,
                "Number of revision tree resurrections");
DECLARE_COUNTER(rocksdb_cache_edge_inserts_uncompressed_entries_size_total,
                "Total gross memory size of all edge cache entries ever stored "
                "in memory");
DECLARE_COUNTER(rocksdb_cache_edge_inserts_effective_entries_size_total,
                "Total effective memory size of all edge cache entries ever "
                "stored in memory (after compression)");
DECLARE_GAUGE(rocksdb_cache_edge_compression_ratio, double,
              "Overall compression ratio for all edge cache entries ever "
              "stored in memory");
DECLARE_COUNTER(rocksdb_cache_edge_inserts_total,
                "Number of inserts into the edge cache");
DECLARE_COUNTER(rocksdb_cache_edge_compressed_inserts_total,
                "Number of compressed inserts into the edge cache");
DECLARE_COUNTER(
  rocksdb_cache_edge_empty_inserts_total,
  "Number of inserts into the edge cache that were an empty array");

// global flag to cancel all compactions. will be flipped to true on shutdown
static std::atomic_bool gCancelCompactions = false;

static constexpr ObjectId kDatabaseIdForGlobalApplier;

RocksDBFilePurgePreventer::RocksDBFilePurgePreventer(
  RocksDBEngineCatalog* engine)
  : _engine(engine) {
  SDB_ASSERT(_engine != nullptr);
  _engine->_purge_lock.lockRead();
}

RocksDBFilePurgePreventer::~RocksDBFilePurgePreventer() {
  if (_engine != nullptr) {
    _engine->_purge_lock.unlockRead();
  }
}

RocksDBFilePurgePreventer::RocksDBFilePurgePreventer(
  RocksDBFilePurgePreventer&& other)
  : _engine(other._engine) {
  // steal engine from other
  other._engine = nullptr;
}

RocksDBFilePurgeEnabler::RocksDBFilePurgeEnabler(RocksDBEngineCatalog* engine)
  : _engine(nullptr) {
  SDB_ASSERT(engine != nullptr);

  if (engine->_purge_lock.tryLockWrite()) {
    // we got the lock
    _engine = engine;
  }
}

RocksDBFilePurgeEnabler::~RocksDBFilePurgeEnabler() {
  if (_engine != nullptr) {
    _engine->_purge_lock.unlockWrite();
  }
}

RocksDBFilePurgeEnabler::RocksDBFilePurgeEnabler(
  RocksDBFilePurgeEnabler&& other)
  : _engine(other._engine) {
  // steal engine from other
  other._engine = nullptr;
}

Result DeleteDefinition(rocksdb::DB* db, auto&& make_key,
                        auto&& make_log_value) {
  rocksdb::WriteBatch batch;
  if (ServerState::instance()->IsSingle()) {
    // No need to write DDL events in cluster mode,
    // as they are not replicated.
    auto log_value = make_log_value();

    if (!log_value.empty()) [[likely]] {
      batch.PutLogData({log_value.data(), log_value.size()});
    }
  }

  auto key = make_key();
  batch.Delete(RocksDBColumnFamilyManager::get(
                 RocksDBColumnFamilyManager::Family::Definitions),
               key.string());

  rocksdb::WriteOptions wo;
  return rocksutils::ConvertStatus(db->Write(wo, &batch));
}

Result WriteDefinition(rocksdb::DB* db, auto&& make_key, auto&& make_value,
                       auto&& make_log_value) {
  rocksdb::WriteBatch batch;

  if (ServerState::instance()->IsSingle()) {
    // No need to write DDL events in cluster mode,
    // as they are not replicated.
    auto log_value = make_log_value();

    if (!log_value.empty()) [[likely]] {
      batch.PutLogData({log_value.data(), log_value.size()});
    }
  }

  auto key = make_key();
  auto value = make_value();
  batch.Put(RocksDBColumnFamilyManager::get(
              RocksDBColumnFamilyManager::Family::Definitions),
            key.string(), value.string());

  rocksdb::WriteOptions wo;
  return rocksutils::ConvertStatus(db->Write(wo, &batch));
}

Result WriteDefinition(rocksdb::DB* db, auto&& make_old_key,
                       auto&& make_new_key, auto&& make_value,
                       auto&& make_log_value) {
  rocksdb::WriteBatch batch;

  if (ServerState::instance()->IsSingle()) {
    // No need to write DDL events in cluster mode,
    // as they are not replicated.
    auto log_value = make_log_value();

    if (!log_value.empty()) [[likely]] {
      batch.PutLogData({log_value.data(), log_value.size()});
    }
  }

  auto* column = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Definitions);

  auto old_key = make_old_key();
  auto new_key = make_new_key();
  auto value = make_value();

  batch.Delete(column, old_key.string());
  batch.Put(column, new_key.string(), value.string());

  rocksdb::WriteOptions wo;
  return rocksutils::ConvertStatus(db->Write(wo, &batch));
}

vpack::Slice GetTableProperties(vpack::Builder& builder,
                                const catalog::Table& collection,
                                const TableShard& physical, bool internal) {
  builder.clear();
  builder.openObject();

  if (internal) {
    collection.WriteInternal(builder);
  } else {
    collection.WriteProperties(builder);
  }

  builder.add(StaticStrings::kIndexes);
  physical.getAllIndexesInternal(builder);

  builder.close();
  return builder.slice();
}

template<typename T>
vpack::Slice GetObjectProperties(vpack::Builder& builder, const T& object,
                                 bool internal) {
  builder.clear();
  builder.openObject();

  if (internal) {
    object.WriteInternal(builder);
  } else {
    object.WriteProperties(builder);
  }

  builder.close();
  return builder.slice();
}

RocksDBEngineCatalog::RocksDBEngineCatalog(SerenedServer& server)
  : RocksDBEngineCatalog(server.getFeature<RocksDBOptionFeature>(),
                         server.getFeature<metrics::MetricsFeature>()) {}

RocksDBEngineCatalog::RocksDBEngineCatalog(
  const RocksDBOptionFeature& options_provider,
  metrics::MetricsFeature& metrics)
  : _options_provider(options_provider),
    _metrics(metrics),
    _wal_access(std::make_unique<RocksDBWalAccess>(*this)),
    _metrics_index_estimator_memory_usage(
      metrics.add(serenedb_index_estimates_memory_usage{})),
    _metrics_wal_released_tick_flush(
      metrics.add(rocksdb_wal_released_tick_flush{})),
    _metrics_wal_sequence_lower_bound(
      metrics.add(rocksdb_wal_sequence_lower_bound{})),
    _metrics_live_wal_files(metrics.add(rocksdb_live_wal_files{})),
    _metrics_archived_wal_files(metrics.add(rocksdb_archived_wal_files{})),
    _metrics_live_wal_files_size(metrics.add(rocksdb_live_wal_files_size{})),
    _metrics_archived_wal_files_size(
      metrics.add(rocksdb_archived_wal_files_size{})),
    _metrics_prunable_wal_files(metrics.add(rocksdb_prunable_wal_files{})),
    _metrics_wal_pruning_active(metrics.add(rocksdb_wal_pruning_active{})),
    _metrics_tree_memory_usage(
      metrics.add(serenedb_revision_tree_memory_usage{})),
    _metrics_tree_buffered_memory_usage(
      metrics.add(serenedb_revision_tree_buffered_memory_usage{})),
    _metrics_tree_rebuilds_success(
      metrics.add(serenedb_revision_tree_rebuilds_success_total{})),
    _metrics_tree_rebuilds_failure(
      metrics.add(serenedb_revision_tree_rebuilds_failure_total{})),
    _metrics_tree_hibernations(
      metrics.add(serenedb_revision_tree_hibernations_total{})),
    _metrics_tree_resurrections(
      metrics.add(serenedb_revision_tree_resurrections_total{})),
    _metrics_edge_cache_entries_size_initial(metrics.add(
      rocksdb_cache_edge_inserts_uncompressed_entries_size_total{})),
    _metrics_edge_cache_entries_size_effective(
      metrics.add(rocksdb_cache_edge_inserts_effective_entries_size_total{})),
    _metrics_edge_cache_inserts(
      metrics.add(rocksdb_cache_edge_inserts_total{})),
    _metrics_edge_cache_compressed_inserts(
      metrics.add(rocksdb_cache_edge_compressed_inserts_total{})),
    _metrics_edge_cache_empty_inserts(
      metrics.add(rocksdb_cache_edge_empty_inserts_total{})) {
  // inherits order from StorageEngine but requires "RocksDBOption" that is
  // used to configure this engine
}

RocksDBEngineCatalog::~RocksDBEngineCatalog() {
  gRecoveryHelpers.clear();
  shutdownRocksDBInstance();
}

/// shuts down the RocksDB instance. this is called from unprepare
/// and the dtor
void RocksDBEngineCatalog::shutdownRocksDBInstance() noexcept {
  if (_db == nullptr) {
    return;
  }

  for (rocksdb::ColumnFamilyHandle* h :
       RocksDBColumnFamilyManager::allHandles()) {
    _db->DestroyColumnFamilyHandle(h);
  }

  // now prune all obsolete WAL files
  try {
    determinePrunableWalFiles(0);
    pruneWalFiles();
  } catch (...) {
    // this is allowed to go wrong on shutdown
    // we must not throw an exception from here
  }

  try {
    // do a final WAL sync here before shutting down
    Result res = RocksDBSyncThread::sync(_db->GetBaseDB());
    if (res.fail()) {
      SDB_WARN("xxxxx", Logger::ENGINES,
               "could not sync RocksDB WAL: ", res.errorMessage());
    }

    rocksdb::Status status = _db->Close();

    if (!status.ok()) {
      Result res = rocksutils::ConvertStatus(status);
      SDB_ERROR("xxxxx", Logger::ENGINES,
                "could not shutdown RocksDB: ", res.errorMessage());
    }
  } catch (...) {
    // this is allowed to go wrong on shutdown
    // we must not throw an exception from here
  }

  delete _db;
  _db = nullptr;
}

void RocksDBEngineCatalog::flushOpenFilesIfRequired() {
  if (_metrics_live_wal_files.load() <
      _options_provider._auto_flush_min_wal_files) {
    return;
  }

  auto now = std::chrono::steady_clock::now();
  if (_auto_flush_last_executed.time_since_epoch().count() == 0 ||
      (now - _auto_flush_last_executed) >=
        std::chrono::duration<double>(
          _options_provider._auto_flush_check_interval)) {
    SDB_INFO("xxxxx", Logger::ENGINES,
             "auto flushing RocksDB wal and column families because number of "
             "live WAL files is ",
             _metrics_live_wal_files.load());
    Result res = flushWal(/*waitForSync*/ true, /*flushColumnFamilies*/ true);
    if (res.fail()) {
      SDB_WARN("xxxxx", Logger::ENGINES,
               "unable to flush RocksDB wal: ", res.errorMessage());
    }
    // set _auto_flush_last_executed regardless of whether flushing has worked
    // or not. we don't want to put too much stress onto the db
    _auto_flush_last_executed = now;
  }
}

// preparation phase for storage engine. can be used for internal setup.
// the storage engine must not start any threads here or write any files
void RocksDBEngineCatalog::prepare() {
  _base_path =
    SerenedServer::Instance().getFeature<DatabasePathFeature>().directory();
  SDB_ASSERT(!_base_path.empty());
}

void RocksDBEngineCatalog::verifySstFiles() const {
  SDB_ASSERT(!_path.empty());

  SDB_INFO("xxxxx", Logger::STARTUP, "verifying RocksDB .sst files in path '",
           _path, "'");

  rocksdb::Options options;
  rocksdb::SstFileReader sst_reader(options);
  for (const auto& file_name : SdbFullTreeDirectory(_path.c_str())) {
    if (!file_name.ends_with(".sst")) {
      continue;
    }
    std::string filename = basics::file_utils::BuildFilename(_path, file_name);
    rocksdb::Status res = sst_reader.Open(filename);
    if (res.ok()) {
      res = sst_reader.VerifyChecksum();
    }
    if (!res.ok()) {
      auto result = rocksutils::ConvertStatus(res);
      SDB_FATAL_EXIT_CODE("xxxxx", Logger::STARTUP, EXIT_SST_FILE_CHECK,
                          "error when verifying .sst file '", filename,
                          "': ", result.errorMessage());
    }
  }

  SDB_INFO("xxxxx", Logger::STARTUP,
           "verification of RocksDB .sst files in path '", _path,
           "' completed successfully");
  log::Flush();
  // exit with status code = 0, without leaking
  int exit_code = static_cast<int>(ERROR_OK);
  gExitFunction(exit_code, nullptr);
  exit(exit_code);
}

rocksdb::Options RocksDBEngineCatalog::makeOptions(bool is_new_dir) {
  auto options = _options_provider.getOptions();
  if (options.wal_dir.empty()) {
    options.wal_dir = basics::file_utils::BuildFilename(_path, "journals");
  }
  options.env = rocksdb::Env::Default();
  return options;
}

void RocksDBEngineCatalog::start() {
  SDB_TRACE("xxxxx", Logger::ENGINES, "rocksdb version ",
            rest::Version::getRocksDBVersion(),
            ", supported compression types: ", getCompressionSupport());

  _path = SerenedServer::Instance()
            .getFeature<DatabasePathFeature>()
            .subdirectoryName(StaticStrings::kRocksDbEngineRoot);

  [[maybe_unused]] bool created_engine_dir = false;
  if (!basics::file_utils::IsDirectory(_path)) {
    std::string system_error_str;
    long error_no;

    auto res = SdbCreateRecursiveDirectory(_path, error_no, system_error_str);

    if (res == ERROR_OK) {
      SDB_TRACE("xxxxx", Logger::ENGINES, "created RocksDB data directory '",
                _path, "'");
      created_engine_dir = true;
    } else {
      SDB_FATAL("xxxxx", Logger::ENGINES,
                "unable to create RocksDB data directory '", _path,
                "': ", system_error_str);
    }
  }

#ifdef USE_SST_INGESTION
  _idx_path = basics::file_utils::BuildFilename(_path, "tmp-idx-creation");
  if (basics::file_utils::isDirectory(_idx_path)) {
    for (const auto& fileName : SdbFullTreeDirectory(_idx_path.c_str())) {
      SdbUnlinkFile(basics::file_utils::BuildFilename(path, fileName).data());
    }
  } else {
    auto errorMsg = ERROR_OK;
    if (!basics::file_utils::createDirectory(_idx_path, &errorMsg)) {
      SDB_FATAL("xxxxx", Logger::ENGINES,
                "Cannot create tmp-idx-creation directory: ", LastError());
    }
  }
#endif

  uint64_t total_space;
  uint64_t free_space;
  if (SdbGetDiskSpaceInfo(_path.c_str(), total_space, free_space).ok() &&
      total_space != 0) {
    SDB_DEBUG("xxxxx", Logger::ENGINES,
              "total disk space for database directory mount: ",
              basics::string_utils::FormatSize(total_space),
              ", free disk space for database directory mount: ",
              basics::string_utils::FormatSize(free_space), " (",
              (100.0 * double(free_space) / double(total_space)), "% free)");
  }

  auto transaction_options = _options_provider.getTransactionDBOptions();

  _db_options = makeOptions(created_engine_dir);

  SDB_TRACE("xxxxx", Logger::ENGINES, "initializing RocksDB, path: '", _path,
            "', WAL directory '", _db_options.wal_dir, "'");

  if (_options_provider._verify_sst) {
    verifySstFiles();
    SDB_ASSERT(false);
  }

  _db_options.env->SetBackgroundThreads(
    static_cast<int>(_options_provider.numThreadsHigh()),
    rocksdb::Env::Priority::HIGH);
  _db_options.env->SetBackgroundThreads(
    static_cast<int>(_options_provider.numThreadsLow()),
    rocksdb::Env::Priority::LOW);

  if (_options_provider._debug_logging) {
    _db_options.info_log_level = rocksdb::InfoLogLevel::DEBUG_LEVEL;
  }

  _error_listener = std::make_shared<RocksDBBackgroundErrorListener>();
  _db_options.listeners.push_back(_error_listener);
  _db_options.listeners.push_back(
    std::make_shared<RocksDBMetricsListener>(SerenedServer::Instance()));

  // create column families
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_families;
  auto add_family = [this,
                     &cf_families](RocksDBColumnFamilyManager::Family family) {
    rocksdb::ColumnFamilyOptions specialized =
      _options_provider.getColumnFamilyOptions(family);
    std::string name = RocksDBColumnFamilyManager::name(family);
    cf_families.emplace_back(name, specialized);
  };
  // no prefix families for default column family (Has to be there)
  add_family(RocksDBColumnFamilyManager::Family::Definitions);
  add_family(RocksDBColumnFamilyManager::Family::Documents);
  add_family(RocksDBColumnFamilyManager::Family::PrimaryIndex);
  add_family(RocksDBColumnFamilyManager::Family::EdgeIndex);
  add_family(RocksDBColumnFamilyManager::Family::VPackIndex);
  add_family(RocksDBColumnFamilyManager::Family::Data);

  bool db_existed = checkExistingDB(cf_families);

  SDB_DEBUG("xxxxx", Logger::STARTUP, "opening RocksDB instance in '", _path,
            "'");

  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;

  rocksdb::Status status = rocksdb::TransactionDB::Open(
    _db_options, transaction_options, _path, cf_families, &cf_handles, &_db);

  if (!status.ok()) {
    std::string error;
    if (status.IsIOError()) {
      error =
        "; Maybe your filesystem doesn't provide required features? (Cifs? "
        "NFS?)";
    }

    SDB_FATAL("xxxxx", Logger::STARTUP,
              "unable to initialize RocksDB engine: ", status.ToString(),
              error);
  }
  if (cf_families.size() != cf_handles.size()) {
    SDB_FATAL("xxxxx", Logger::STARTUP,
              "unable to initialize RocksDB column families");
  }
  if (cf_handles.size() <
      RocksDBColumnFamilyManager::kMinNumberOfColumnFamilies) {
    SDB_FATAL("xxxxx", Logger::STARTUP,
              "unexpected number of column families found in database. got ",
              cf_handles.size(), ", expecting at least ",
              RocksDBColumnFamilyManager::kMinNumberOfColumnFamilies);
  }

  SDB_ASSERT(_db != nullptr);

  // set our column families
  RocksDBColumnFamilyManager::set(RocksDBColumnFamilyManager::Family::Invalid,
                                  _db->DefaultColumnFamily());
  RocksDBColumnFamilyManager::set(
    RocksDBColumnFamilyManager::Family::Definitions, cf_handles[0]);
  RocksDBColumnFamilyManager::set(RocksDBColumnFamilyManager::Family::Documents,
                                  cf_handles[1]);
  RocksDBColumnFamilyManager::set(
    RocksDBColumnFamilyManager::Family::PrimaryIndex, cf_handles[2]);
  RocksDBColumnFamilyManager::set(RocksDBColumnFamilyManager::Family::EdgeIndex,
                                  cf_handles[3]);
  RocksDBColumnFamilyManager::set(
    RocksDBColumnFamilyManager::Family::VPackIndex, cf_handles[4]);

  RocksDBColumnFamilyManager::set(RocksDBColumnFamilyManager::Family::Data,
                                  cf_handles[5]);

  SDB_ASSERT(RocksDBColumnFamilyManager::get(
               RocksDBColumnFamilyManager::Family::Definitions)
               ->GetID() == 0);

  // will crash the process if version does not match
  StartupVersionCheck(SerenedServer::Instance(), _db, db_existed);

  _db_existed = db_existed;

  if (_options_provider.limitOpenFilesAtStartup()) {
    _db->SetDBOptions({{"max_open_files", "-1"}});
  }

  // limit the total size of WAL files. This forces the flush of memtables of
  // column families still backed by WAL files. If we would not do this, WAL
  // files may linger around forever and will not get removed
  _db->SetDBOptions({{"max_total_wal_size",
                      std::to_string(_options_provider.maxTotalWalSize())}});

  {
    auto& feature = SerenedServer::Instance().getFeature<FlushFeature>();
    _use_released_tick = feature.isEnabled();
  }

  // useReleasedTick should be true on DB servers and single servers
  SDB_ASSERT((ServerState::instance()->IsCoordinator() ||
              ServerState::instance()->IsAgent()) ||
             _use_released_tick);

  if (_options_provider._sync_interval > 0) {
    _sync_thread = std::make_unique<RocksDBSyncThread>(
      *this, std::chrono::milliseconds(_options_provider._sync_interval),
      std::chrono::milliseconds(_options_provider._sync_delay_threshold));
    if (!_sync_thread->start()) {
      SDB_FATAL("xxxxx", Logger::ENGINES,
                "could not start rocksdb sync thread");
    }
  }

  SDB_ASSERT(_db != nullptr);
  _settings_manager = std::make_unique<RocksDBSettingsManager>(*this);
#ifdef SDB_CLUSTER
  _replication_manager = std::make_shared<RocksDBReplicationManager>();
  _dump_manager = std::make_shared<RocksDBDumpManager>(
    *_db, _metrics, GetServerOptions().dump_limits);
#endif

  _settings_manager->retrieveInitialValues();

  const double counter_sync_seconds = 2.5;
  _background_thread =
    std::make_unique<RocksDBBackgroundThread>(*this, counter_sync_seconds);
  if (!_background_thread->start()) {
    SDB_FATAL("xxxxx", Logger::ENGINES,
              "could not start rocksdb counter manager thread");
  }

  EnsureSystemDatabase();

  // to populate initial health check data
  if (auto hd = healthCheck(); hd.res.fail()) {
    SDB_ERROR("xxxxx", Logger::ENGINES, hd.res.errorMessage());
  }

  // make an initial inventory of WAL files, so that all WAL files
  // metrics are correctly populated once the HTTP interface comes
  // up
  determineWalFilesInitial();
}

void RocksDBEngineCatalog::beginShutdown() {
#ifdef SDB_CLUSTER
  // block the creation of new replication contexts
  if (_replication_manager != nullptr) {
    _replication_manager->beginShutdown();
  }

  if (_dump_manager != nullptr) {
    _dump_manager->garbageCollect(/*force*/ true);
  }
#endif

  // from now on, all started compactions can be canceled.
  // note that this is only a best-effort hint to RocksDB and
  // may not be followed immediately.
  gCancelCompactions.store(true, std::memory_order_release);
}

void RocksDBEngineCatalog::stop() {
#ifdef SDB_CLUSTER
  // in case we missed the beginShutdown somehow, call it again
  replicationManager()->beginShutdown();
  replicationManager()->dropAll();

  if (_dump_manager != nullptr) {
    _dump_manager->garbageCollect(/*force*/ true);
  }
#endif

  if (_background_thread) {
    // stop the press
    _background_thread->beginShutdown();

    if (_settings_manager) {
      auto sync_res = _settings_manager->sync(/*force*/ true);
      if (!sync_res) {
        SDB_WARN("xxxxx", Logger::ENGINES,
                 "caught exception while shutting down RocksDB engine: ",
                 sync_res.error().errorMessage());
      }
    }

    // wait until background thread stops
    while (_background_thread->isRunning()) {
      std::this_thread::yield();
    }
    _background_thread.reset();
  }

  if (_sync_thread) {
    // _sync_thread may be a nullptr, in case automatic syncing is turned off
    _sync_thread->beginShutdown();

    // wait until sync thread stops
    while (_sync_thread->isRunning()) {
      std::this_thread::yield();
    }
    _sync_thread.reset();
  }

  waitForCompactionJobsToFinish();
}

void RocksDBEngineCatalog::unprepare() {
  waitForCompactionJobsToFinish();
  shutdownRocksDBInstance();
}

void RocksDBEngineCatalog::trackRevisionTreeHibernation() noexcept {
  ++_metrics_tree_hibernations;
}

void RocksDBEngineCatalog::trackRevisionTreeResurrection() noexcept {
  ++_metrics_tree_resurrections;
}

void RocksDBEngineCatalog::trackRevisionTreeMemoryIncrease(
  uint64_t value) noexcept {
  _metrics_tree_memory_usage.fetch_add(value);
}

void RocksDBEngineCatalog::trackRevisionTreeMemoryDecrease(
  uint64_t value) noexcept {
  [[maybe_unused]] auto old = _metrics_tree_memory_usage.fetch_sub(value);
  SDB_ASSERT(old >= value);
}

void RocksDBEngineCatalog::trackRevisionTreeBufferedMemoryIncrease(
  uint64_t value) noexcept {
  _metrics_tree_buffered_memory_usage.fetch_add(value);
}

void RocksDBEngineCatalog::trackRevisionTreeBufferedMemoryDecrease(
  uint64_t value) noexcept {
  [[maybe_unused]] auto old =
    _metrics_tree_buffered_memory_usage.fetch_sub(value);
  SDB_ASSERT(old >= value);
}

bool RocksDBEngineCatalog::hasBackgroundError() const {
  return _error_listener != nullptr && _error_listener->called();
}

Result RocksDBEngineCatalog::VisitDatabases(
  absl::FunctionRef<Result(vpack::Slice database)> visitor) {
  rocksdb::ReadOptions read_options;
  read_options.async_io = true;
  // TODO: more options?
  std::unique_ptr<rocksdb::Iterator> iter(_db->NewIterator(
    read_options, RocksDBColumnFamilyManager::get(
                    RocksDBColumnFamilyManager::Family::Definitions)));
  auto r_slice = RocksDbSlice(RocksDBEntryType::Database);
  for (iter->Seek(r_slice); iter->Valid() && iter->key().starts_with(r_slice);
       iter->Next()) {
    auto slice =
      vpack::Slice(reinterpret_cast<const uint8_t*>(iter->value().data()));
    auto r = visitor(slice);
    if (!r.ok()) {
      return r;
    }
  }
  return {};
}

// TODO: Rewrite this to use single scan.
Result RocksDBEngineCatalog::VisitObjectsImpl(
  const RocksDBKeyBounds& bounds,
  absl::FunctionRef<Result(rocksdb::Slice, vpack::Slice)> visitor) {
  const auto upper = bounds.end();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Definitions);
  rocksdb::ReadOptions ro;
  ro.iterate_upper_bound = &upper;
  // TODO: more options?
  ro.async_io = true;
  std::unique_ptr<rocksdb::Iterator> iter{_db->NewIterator(ro, cf)};
  for (iter->Seek(bounds.start()); iter->Valid(); iter->Next()) {
    SDB_ASSERT(iter->key().compare(bounds.end()) < 0);
    vpack::Slice slice{reinterpret_cast<const uint8_t*>(iter->value().data())};
    if (auto r = visitor(iter->key(), slice); !r.ok()) {
      return r;
    }
  }

  return rocksutils::ConvertStatus(iter->status());
}

Result RocksDBEngineCatalog::VisitObjects(
  ObjectId database_id, RocksDBEntryType entry,
  absl::FunctionRef<Result(rocksdb::Slice key, vpack::Slice)> visitor) {
  return VisitObjectsImpl(RocksDBKeyBounds::DatabaseObjects(entry, database_id),
                          visitor);
}

Result RocksDBEngineCatalog::VisitSchemaObjects(
  ObjectId database_id, ObjectId schema_id, RocksDBEntryType entry,
  absl::FunctionRef<Result(rocksdb::Slice key, vpack::Slice)> visitor) {
  return VisitObjectsImpl(
    RocksDBKeyBounds::SchemaObjects(entry, database_id, schema_id), visitor);
}

Result RocksDBEngineCatalog::DeleteSchemaObject(
  ObjectId db_id, ObjectId schema_id, ObjectId object_id,
  std::string_view object_name, RocksDBEntryType entry, RocksDBLogType log) {
  return DeleteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchemaObject(entry, db_id, schema_id, object_id);
      return key;
    },
    [&] {
      const wal::SchemaObjectDrop entry{
        .database_id = db_id,
        .schema_id = schema_id,
        .object_id = object_id,
        .uuid = object_name,
      };
      return wal::Write(log, entry);
    });
}

Result RocksDBEngineCatalog::PutSchemaObject(ObjectId db, ObjectId schema_id,
                                             ObjectId id,
                                             WriteProperties properties,
                                             RocksDBEntryType entry,
                                             RocksDBLogType log) {
  return WriteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchemaObject(entry, db, schema_id, id);
      return key;
    },
    [&] { return RocksDBValue::Object(entry, properties(true)); },
    [&] {
      const wal::SchemaObjectPut entry{
        .database_id = db,
        .schema_id = schema_id,
        .object_id = id,
        .data = properties(false),
      };
      return wal::Write(log, entry);
    });
}

Result RocksDBEngineCatalog::PutObject(ObjectId database_id, ObjectId object_id,
                                       WriteProperties properties,
                                       RocksDBEntryType entry,
                                       RocksDBLogType log) {
  return WriteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructObject(entry, database_id, object_id);
      return key;
    },
    [&] { return RocksDBValue::Object(entry, properties(true)); },
    [&] {
      const wal::ObjectPut entry{
        .database_id = database_id,
        .object_id = object_id,
        .data = properties(false),
      };
      return wal::Write(log, entry);
    });
}

Result RocksDBEngineCatalog::DeleteObject(ObjectId db_id, ObjectId object_id,
                                          std::string_view object_name,
                                          RocksDBEntryType entry,
                                          RocksDBLogType log) {
  return DeleteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructObject(entry, db_id, object_id);
      return key;
    },
    [&] {
      const wal::ObjectDrop entry{
        .database_id = db_id,
        .object_id = object_id,
        .uuid = object_name,
      };
      return wal::Write(log, entry);
    });
}

std::string RocksDBEngineCatalog::versionFilename(ObjectId id) const {
  return absl::StrCat(_base_path, SERENEDB_DIR_SEPARATOR_STR, "VERSION-", id);
}

void RocksDBEngineCatalog::cleanupReplicationContexts() {
#ifdef SDB_CLUSTER
  if (_replication_manager) {
    _replication_manager->dropAll();
  }
#endif
}

ErrorCode RocksDBEngineCatalog::getReplicationApplierConfiguration(
  ObjectId database, vpack::Builder& builder) {
  RocksDBKeyWithBuffer key;
  key.constructReplicationApplierConfig(database);
  return getReplicationApplierConfiguration(key, builder);
}

ErrorCode RocksDBEngineCatalog::getReplicationApplierConfiguration(
  vpack::Builder& builder) {
  RocksDBKeyWithBuffer key;
  key.constructReplicationApplierConfig(kDatabaseIdForGlobalApplier);
  return getReplicationApplierConfiguration(key, builder);
}

ErrorCode RocksDBEngineCatalog::getReplicationApplierConfiguration(
  const RocksDBKey& key, vpack::Builder& builder) {
  rocksdb::PinnableSlice value;

  auto s = _db->Get({},
                    RocksDBColumnFamilyManager::get(
                      RocksDBColumnFamilyManager::Family::Definitions),
                    key.string(), &value);
  if (!s.ok()) {
    return ERROR_FILE_NOT_FOUND;
  }

  builder.add(RocksDBValue::data(value));
  return ERROR_OK;
}

ErrorCode RocksDBEngineCatalog::removeReplicationApplierConfiguration(
  ObjectId database) {
  RocksDBKeyWithBuffer key;

  key.constructReplicationApplierConfig(database);

  return removeReplicationApplierConfiguration(key);
}

ErrorCode RocksDBEngineCatalog::removeReplicationApplierConfiguration() {
  RocksDBKeyWithBuffer key;
  key.constructReplicationApplierConfig(kDatabaseIdForGlobalApplier);
  return removeReplicationApplierConfiguration(key);
}

ErrorCode RocksDBEngineCatalog::removeReplicationApplierConfiguration(
  const RocksDBKey& key) {
  auto status = rocksutils::ConvertStatus(
    _db->Delete(rocksdb::WriteOptions(),
                RocksDBColumnFamilyManager::get(
                  RocksDBColumnFamilyManager::Family::Definitions),
                key.string()));
  if (!status.ok()) {
    return status.errorNumber();
  }

  return ERROR_OK;
}

ErrorCode RocksDBEngineCatalog::saveReplicationApplierConfiguration(
  ObjectId database, vpack::Slice slice, bool do_sync) {
  RocksDBKeyWithBuffer key;

  key.constructReplicationApplierConfig(database);

  return saveReplicationApplierConfiguration(key, slice, do_sync);
}

ErrorCode RocksDBEngineCatalog::saveReplicationApplierConfiguration(
  vpack::Slice slice, bool do_sync) {
  RocksDBKeyWithBuffer key;
  key.constructReplicationApplierConfig(kDatabaseIdForGlobalApplier);
  return saveReplicationApplierConfiguration(key, slice, do_sync);
}

ErrorCode RocksDBEngineCatalog::saveReplicationApplierConfiguration(
  const RocksDBKey& key, vpack::Slice slice, bool do_sync) {
  auto value = RocksDBValue::ReplicationApplierConfig(slice);

  auto status = rocksutils::ConvertStatus(
    _db->Put(rocksdb::WriteOptions(),
             RocksDBColumnFamilyManager::get(
               RocksDBColumnFamilyManager::Family::Definitions),
             key.string(), value.string()));
  if (!status.ok()) {
    return status.errorNumber();
  }

  return ERROR_OK;
}

Result RocksDBEngineCatalog::writeCreateTableMarker(
  ObjectId database_id, ObjectId schema_id, ObjectId cid, vpack::Slice slice,
  std::string_view log_value) {
  return WriteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchemaObject(RocksDBEntryType::Collection, database_id,
                                schema_id, cid);
      return key;
    },
    [&] {
      return RocksDBValue::Object(RocksDBEntryType::Collection, slice);
    },  //
    [&] { return log_value; });
}

Result RocksDBEngineCatalog::createDatabase(ObjectId id, vpack::Slice slice) {
  return WriteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructDatabase(id);
      return key;
    },
    [&] { return RocksDBValue::Database(slice); },
    [&] {
      const wal::Database entry{
        .database_id = id,
        .data = slice,
      };
      return wal::Write(RocksDBLogType::DatabaseCreate, entry);
    });
}

Result RocksDBEngineCatalog::MarkDeleted(const catalog::Database& database) {
  vpack::Builder b;
  database.WriteInternal(b);

  return WriteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructDatabase(database.GetId());
      return key;
    },
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchemaObject(RocksDBEntryType::ScopeTombstone,
                                id::kTombstoneDatabase, database.GetId(),
                                ObjectId{});
      return key;
    },
    [&] {
      // TODO(gnusi): write empty value
      return RocksDBValue::Object(RocksDBEntryType::ScopeTombstone,
                                  vpack::Slice::emptyArraySlice());
    },
    [&] {
      const wal::Database entry{
        .database_id = database.GetId(),
        .data = b.slice(),
      };
      return wal::Write(RocksDBLogType::DatabaseDrop, entry);
    });
}

Result RocksDBEngineCatalog::MarkDeleted(const catalog::Schema& schema) {
  vpack::Builder b;
  schema.WriteInternal(b);

  return WriteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchema(schema.GetDatabaseId(), schema.GetId());
      return key;
    },
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchemaObject(RocksDBEntryType::ScopeTombstone,
                                id::kTombstoneDatabase, schema.GetDatabaseId(),
                                schema.GetId());
      return key;
    },
    [&] {
      // TODO(gnusi): write empty value
      return RocksDBValue::Object(RocksDBEntryType::ScopeTombstone,
                                  vpack::Slice::emptyArraySlice());
    },
    [&] {
      const wal::Database entry{
        .database_id = schema.GetId(),
        .data = b.slice(),
      };
      return wal::Write(RocksDBLogType::SchemaDrop, entry);
    });
}

RecoveryState RocksDBEngineCatalog::recoveryState() noexcept {
  return SerenedServer::Instance()
    .getFeature<RocksDBRecoveryManager>()
    .recoveryState();
}

Tick RocksDBEngineCatalog::recoveryTick() noexcept {
  return SerenedServer::Instance()
    .getFeature<RocksDBRecoveryManager>()
    .recoverySequenceNumber();
}

void RocksDBEngineCatalog::scheduleTreeRebuild(ObjectId database,
                                               ObjectId collection) {
  std::lock_guard locker{_rebuild_collections_lock};
  _rebuild_collections.emplace(std::pair{database, collection},
                               /*started*/ false);
}

void RocksDBEngineCatalog::processTreeRebuilds() {
  Scheduler* scheduler = SchedulerFeature::gScheduler;
  if (scheduler == nullptr) {
    return;
  }

  auto& server = SerenedServer::Instance();

  uint64_t max_parallel_rebuilds = 2;
  uint64_t iterations = 0;
  while (++iterations <= max_parallel_rebuilds) {
    if (server.isStopping()) {
      // don't fire off more tree rebuilds while we are shutting down
      return;
    }

    std::pair<ObjectId, ObjectId> candidate{};

    {
      std::lock_guard locker{_rebuild_collections_lock};
      if (_rebuild_collections.empty() ||
          _running_rebuilds >= max_parallel_rebuilds) {
        // nothing to do, or too much to do
        return;
      }

      for (auto& it : _rebuild_collections) {
        if (!it.second) {
          // set to started
          it.second = true;
          candidate = it.first;
          ++_running_rebuilds;
          break;
        }
      }
    }

    if (!candidate.first.isSet() || !candidate.second.isSet()) {
      return;
    }

    if (SerenedServer::Instance().isStopping()) {
      return;
    }

    // TODO(mbkkt) make it coroutine with return type yaclib::Task
    scheduler->queue(RequestLane::ClientSlow, [this, candidate]() {
      if (!SerenedServer::Instance().isStopping()) {
        try {
          auto collection = catalog::GetTableShard(candidate.second);

          if (collection != nullptr && !collection->deleted()) {
            SDB_INFO("xxxxx", Logger::ENGINES,
                     "starting background rebuild of revision tree for "
                     "collection ",
                     candidate.first, "/", collection->GetMeta().name);

            auto res = collection->rebuildRevisionTree().Get().Ok();
            if (res.ok()) {
              ++_metrics_tree_rebuilds_success;
              SDB_INFO("xxxxx", Logger::ENGINES,
                       "successfully rebuilt revision tree for collection ",
                       candidate.first, "/", collection->GetMeta().name);
            } else {
              ++_metrics_tree_rebuilds_failure;
              if (res.is(ERROR_LOCK_TIMEOUT)) {
                SDB_WARN("xxxxx", Logger::ENGINES,
                         "failure during revision tree rebuilding for "
                         "collection ",
                         candidate.first, "/", collection->GetMeta().name, ": ",
                         res.errorMessage());
              } else {
                SDB_ERROR("xxxxx", Logger::ENGINES,
                          "failure during revision tree rebuilding for "
                          "collection ",
                          candidate.first, "/", collection->GetMeta().name,
                          ": ", res.errorMessage());
              }
              {
                // mark as to-be-done again
                std::lock_guard locker{_rebuild_collections_lock};
                auto it = _rebuild_collections.find(candidate);
                if (it != _rebuild_collections.end()) {
                  (*it).second = false;
                }
              }
              // rethrow exception
              SDB_THROW(std::move(res));
            }
          }

          // tree rebuilding finished successfully. now remove from the list
          // to-be-rebuilt candidates
          std::lock_guard locker{_rebuild_collections_lock};
          _rebuild_collections.erase(candidate);

        } catch (const std::exception& ex) {
          SDB_WARN("xxxxx", Logger::ENGINES,
                   "caught exception during tree rebuilding: ", ex.what());
        } catch (...) {
          SDB_WARN("xxxxx", Logger::ENGINES,
                   "caught unknown exception during tree rebuilding");
        }
      }

      // always count down _running_rebuilds!
      std::lock_guard locker{_rebuild_collections_lock};
      SDB_ASSERT(_running_rebuilds > 0);
      --_running_rebuilds;
    });
  }
}

void RocksDBEngineCatalog::compactRange(RocksDBKeyBounds bounds) {
  {
    absl::WriterMutexLock locker{&_pending_compactions_lock};
    _pending_compactions.push_back(std::move(bounds));
  }

  // directly kick off compactions if there is enough processing
  // capacity
  processCompactions();
}

void RocksDBEngineCatalog::processCompactions() {
  Scheduler* scheduler = SchedulerFeature::gScheduler;
  if (scheduler == nullptr) {
    return;
  }

  auto& server = SerenedServer::Instance();
  uint64_t max_iterations = _options_provider._max_parallel_compactions;
  uint64_t iterations = 0;
  while (++iterations <= max_iterations) {
    if (server.isStopping()) {
      // don't fire off more compactions while we are shutting down
      return;
    }

    RocksDBKeyBounds bounds = RocksDBKeyBounds::Empty();
    {
      absl::WriterMutexLock locker{&_pending_compactions_lock};
      if (_pending_compactions.empty() ||
          _running_compactions >= _options_provider._max_parallel_compactions) {
        // nothing to do, or too much to do
        SDB_TRACE(
          "xxxxx", Logger::ENGINES,
          "not scheduling compactions. pending: ", _pending_compactions.size(),
          ", running: ", _running_compactions);
        return;
      }
      rocksdb::ColumnFamilyHandle* cfh =
        _pending_compactions.front().columnFamily();

      if (!_running_compactions_column_families.emplace(cfh).second) {
        // a compaction is already running for the same column family.
        // we don't want to schedule parallel compactions for the same column
        // family because they can lead to shutdown issues (this is an issue of
        // RocksDB).
        SDB_TRACE(
          "xxxxx", Logger::ENGINES,
          "not scheduling compactions. already have a compaction running "
          "for column family '",
          cfh->GetName(), "', running: ", _running_compactions);
        return;
      }

      // found something to do, now steal the item from the queue
      bounds = std::move(_pending_compactions.front());
      _pending_compactions.pop_front();

      if (SerenedServer::Instance().isStopping()) {
        // if we are stopping, it is ok to not process but lose any pending
        // compactions
        return;
      }

      // set it to running already, so that concurrent callers of this method
      // will not kick off additional jobs
      ++_running_compactions;

      SDB_TRACE("xxxxx", Logger::ENGINES,
                "scheduling compaction in column family '", cfh->GetName(),
                "' for execution");
    }

    scheduler->queue(RequestLane::ClientSlow, [this, bounds]() {
      if (SerenedServer::Instance().isStopping()) {
        SDB_TRACE("xxxxx", Logger::ENGINES,
                  "aborting pending compaction due to server shutdown");
      } else {
        SDB_TRACE("xxxxx", Logger::ENGINES, "executing compaction for range ",
                  bounds);
        double start = utilities::GetMicrotime();
        try {
          rocksdb::CompactRangeOptions opts;
          opts.exclusive_manual_compaction = false;
          opts.allow_write_stall = true;
          opts.canceled = &gCancelCompactions;
          rocksdb::Slice b = bounds.start(), e = bounds.end();
          _db->CompactRange(opts, bounds.columnFamily(), &b, &e);
        } catch (const std::exception& ex) {
          SDB_WARN("xxxxx", Logger::ENGINES, "compaction for range ", bounds,
                   " failed with error: ", ex.what());
        } catch (...) {
          // whatever happens, we need to count down _running_compactions in
          // all cases
        }

        SDB_TRACE("xxxxx", Logger::ENGINES, "finished compaction for range ",
                  bounds, ", took: ",
                  absl::StrFormat("%.6f", utilities::GetMicrotime() - start));
      }
      // always count down _running_compactions!
      absl::WriterMutexLock locker{&_pending_compactions_lock};

      SDB_ASSERT(_running_compactions_column_families.size() ==
                 _running_compactions);
      SDB_ASSERT(_running_compactions > 0);
      --_running_compactions;

      [[maybe_unused]] const auto count =
        _running_compactions_column_families.erase(bounds.columnFamily());
      SDB_ASSERT(count);
    });
  }
}

Result RocksDBEngineCatalog::CreateIndex(const catalog::Index& index) {
  const auto db_id = index.GetDatabaseId();
  const auto schema_id = index.GetSchemaId();
  const auto index_id = index.GetId();
  SDB_ASSERT(index_id.isSet());

  vpack::Builder b;
  index.WriteInternal(b);

  return WriteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchemaObject(RocksDBEntryType::Index, db_id, schema_id,
                                index_id);
      return key;
    },
    [&] { return RocksDBValue::Object(RocksDBEntryType::Index, b.slice()); },
    [&] { return std::string_view{}; });
}

Result RocksDBEngineCatalog::MarkDeleted(const catalog::Index& index,
                                         const IndexTombstone& tombstone) {
  const auto db_id = index.GetDatabaseId();
  const auto schema_id = index.GetSchemaId();
  const auto relation_id = index.GetId();
  SDB_ASSERT(relation_id.isSet());

  vpack::Builder b;
  vpack::WriteTuple(b, tombstone);

  return WriteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchemaObject(RocksDBEntryType::Index, db_id, schema_id,
                                relation_id);
      return key;
    },
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructObject(RocksDBEntryType::IndexTombstone,
                          id::kTombstoneDatabase, tombstone.id);
      return key;
    },
    [&] {
      return RocksDBValue::Object(RocksDBEntryType::IndexTombstone, b.slice());
    },
    [&] { return std::string_view{}; });
}

Result RocksDBEngineCatalog::MarkDeleted(const catalog::Table& c,
                                         const TableShard& physical,
                                         const TableTombstone& tombstone) {
  const auto db_id = c.GetDatabaseId();
  const auto schema_id = c.GetSchemaId();
  const auto collection_id = c.GetId();
  SDB_ASSERT(collection_id.isSet());

  vpack::Builder b;

  return WriteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchemaObject(RocksDBEntryType::Collection, db_id, schema_id,
                                collection_id);
      return key;
    },
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructObject(RocksDBEntryType::TableTombstone,
                          id::kTombstoneDatabase, collection_id);
      return key;
    },
    [&] {
      b.clear();
      vpack::WriteTuple(b, tombstone);

      return RocksDBValue::Object(RocksDBEntryType::TableTombstone, b.slice());
    },
    [&] {
      const wal::SchemaObjectDrop entry{
        .database_id = db_id,
        .schema_id = schema_id,
        .object_id = c.GetId(),
        .uuid = c.GetName(),
      };
      return wal::Write(RocksDBLogType::TableDrop, entry);
    });
}

Result RocksDBEngineCatalog::createTableShard(
  const catalog::Table& collection, bool is_new,
  std::shared_ptr<TableShard>& physical) {
  physical = std::make_shared<TableShard>(MakeTableMeta(collection));
  return {};
}

void RocksDBEngineCatalog::createTable(const catalog::Table& c,
                                       TableShard& physical) {
  const auto db_id = c.GetDatabaseId();
  const auto schema_id = c.GetSchemaId();
  const auto collection_id = c.GetId();
  SDB_ASSERT(collection_id.isSet());

  vpack::Builder b;

  auto r = WriteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchemaObject(RocksDBEntryType::Collection, db_id, schema_id,
                                collection_id);
      return key;
    },
    [&] {
      return RocksDBValue::Object(RocksDBEntryType::Collection,
                                  GetTableProperties(b, c, physical, true));
    },
    [&] {
      const wal::SchemaObjectPut entry{
        .database_id = db_id,
        .schema_id = schema_id,
        .object_id = collection_id,
        .data = GetTableProperties(b, c, physical, false),
      };
      return wal::Write(RocksDBLogType::TableCreate, entry);
    });

  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

void RocksDBEngineCatalog::prepareDropTable(ObjectId collection) {
#ifdef SDB_CLUSTER
  replicationManager()->dropCollection(collection);
#endif
}

Result RocksDBEngineCatalog::DropIndex(IndexTombstone tombstone) {
  SDB_ASSERT(tombstone.type != IndexType::kTypeUnknown &&
             tombstone.type != IndexType::kTypeNoAccessIndex);

  rocksdb::DB* db = _db->GetRootDB();

  auto r = DeleteIndexEstimate(db, tombstone.id.id());

  if (!r.ok()) {
    SDB_WARN("xxxxx", Logger::ENGINES,
             "could not delete index estimate: ", r.errorMessage());
  }

  if (tombstone.type == IndexType::kTypeInvertedIndex) {
    // TODO(gnusi): handle it here?

    // rocksdb does not store inverted index data
    return r;
  }

  const bool prefix_same_as_start = tombstone.type != IndexType::kTypeEdgeIndex;
  const bool use_range_delete =
    UseRangeDelete(tombstone.id, tombstone.number_documents);
  const auto bounds =
    GetIndexBounds(tombstone.type, tombstone.id.id(), tombstone.unique);

  r = rocksutils::RemoveLargeRange(db, bounds.start(), bounds.end(),
                                   bounds.columnFamily(), prefix_same_as_start,
                                   use_range_delete);

  if (use_range_delete) {
    // TODO(gnusi): ignore errors?
    compactRange(bounds);
  }

#ifdef SDB_DEV
  // check if documents have been deleted
  if (size_t num_docs =
        rocksutils::CountKeyRange(db, bounds, nullptr, prefix_same_as_start);
      num_docs > 0) {
    SDB_THROW(
      ERROR_INTERNAL,
      "deletion check in index drop failed - not all documents in the index "
      "have been deleted. remaining: ",
      num_docs);
  }
#endif

  return r;
}

bool RocksDBEngineCatalog::UseRangeDelete(ObjectId id,
                                          uint64_t number_documents) {
  if (number_documents == kRead) {
    const auto cnt = LoadCollectionCount(_db, id.id());
    number_documents = cnt.added - cnt.removed;
  }
  return number_documents >= 32 * 1024;
}

Result RocksDBEngineCatalog::DropTable(const TableTombstone& tombstone) {
  const bool use_range_delete =
    UseRangeDelete(tombstone.table, tombstone.number_documents);
  rocksdb::DB* db = _db->GetRootDB();

  // Unregister collection metadata
  auto r = DeleteTableMeta(db, tombstone.table.id());
  if (!r.ok()) {
    SDB_ERROR("xxxxx", Logger::ENGINES, "error removing collection meta-data: ",
              r.errorMessage());  // continue regardless
  }

  // Delete documents
  auto bounds = RocksDBKeyBounds::CollectionDocuments(tombstone.table.id());
  r =
    rocksutils::RemoveLargeRange(db, bounds.start(), bounds.end(),
                                 bounds.columnFamily(), true, use_range_delete);
  if (!r.ok()) {
    // We try to remove all documents.
    // If it does not work they cannot be accessed any more and leaked.
    SDB_ERROR("xxxxx", Logger::ENGINES, "error removing collection data: ",
              r.errorMessage());  // continue regardless
  }

  // Delete columns
  auto [lower, upper] = connector::key_utils::CreateTableRange(tombstone.table);
  auto* table_cf =
    RocksDBColumnFamilyManager::get(RocksDBColumnFamilyManager::Family::Data);
  r = rocksutils::RemoveLargeRange(db, lower, upper, table_cf, true,
                                   true);  // TODO(gnusi): Check rows*columns
  if (!r.ok()) {
    // We try to remove all columns.
    // If it does not work they cannot be accessed any more and leaked.
    SDB_ERROR("xxxxx", Logger::ENGINES, "error removing table data: ",
              r.errorMessage());  // continue regardless
  }

  // Remove tombstone definition
  r = DeleteDefinition(
    db,
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructObject(RocksDBEntryType::TableTombstone,
                          id::kTombstoneDatabase, tombstone.table);
      return key;
    },
    [] { return std::string_view{}; });

  if (!r.ok()) {
    return r;
  }

  // run compaction for data only if collection contained a considerable
  // amount of documents. otherwise don't run compaction, because it will
  // slow things down a lot, especially during tests that create/drop LOTS
  // of collections
  if (use_range_delete) {
    compactRange(bounds);
  }

#ifdef SDB_DEV
  // check if documents have been deleted
  if (size_t num_docs = rocksutils::CountKeyRange(_db, bounds, nullptr, true);
      num_docs > 0) {
    SDB_THROW(ERROR_INTERNAL,
              "deletion check in collection drop failed - not all documents "
              "have been deleted. remaining: ",
              num_docs);
  }
  // check if columns have been deleted
  if (size_t num_values =
        rocksutils::CountKeyRange(_db, lower, upper, table_cf, nullptr, true);
      num_values > 0) {
    SDB_THROW(ERROR_INTERNAL,
              "deletion check in table drop failed - not all values have been "
              "deleted. remaining: ",
              num_values);
  }
#endif

  // if we get here all documents / indexes are gone.
  // We have no data garbage left.
  return {};
}

void RocksDBEngineCatalog::ChangeTable(const catalog::Table& c,
                                       const TableShard& physical) {
  const auto db_id = c.GetDatabaseId();
  const auto schema_id = c.GetSchemaId();

  vpack::Builder b;

  auto r = WriteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchemaObject(RocksDBEntryType::Collection, db_id, schema_id,
                                c.GetId());
      return key;
    },
    [&] {
      return RocksDBValue::Object(RocksDBEntryType::Collection,
                                  GetTableProperties(b, c, physical, true));
    },
    [&] {
      const wal::SchemaObjectPut entry{
        .database_id = db_id,
        .schema_id = schema_id,
        .object_id = c.GetId(),
        .data = GetTableProperties(b, c, physical, false),
      };
      return wal::Write(RocksDBLogType::TableChange, entry);
    });

  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

Result RocksDBEngineCatalog::RenameTable(const catalog::Table& c,
                                         const TableShard& physical,
                                         std::string_view old_name) {
  const auto db_id = c.GetDatabaseId();
  const auto cid = c.GetId();

  vpack::Builder b;

  return WriteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchemaObject(RocksDBEntryType::Collection, db_id,
                                c.GetSchemaId(), cid);
      return key;
    },
    [&] {
      return RocksDBValue::Object(RocksDBEntryType::Collection,
                                  GetTableProperties(b, c, physical, true));
    },
    [&] {
      b.clear();
      b.openObject(true);
      b.add("name", old_name);
      b.add("new", c.GetName());
      b.close();

      const wal::SchemaObjectPut entry{
        .database_id = db_id,
        .schema_id = c.GetSchemaId(),
        .object_id = cid,
        .data = b.slice(),
      };
      return wal::Write(RocksDBLogType::TableRename, entry);
    });
}

Result RocksDBEngineCatalog::CreateFunction(ObjectId db, ObjectId schema_id,
                                            ObjectId id,
                                            WriteProperties properties) {
  return PutSchemaObject(db, schema_id, id, properties,
                         RocksDBEntryType::Function,
                         RocksDBLogType::FunctionCreate);
}

Result RocksDBEngineCatalog::DropFunction(ObjectId db, ObjectId schema_id,
                                          ObjectId id, std::string_view name) {
  return DeleteSchemaObject(db, schema_id, id, name, RocksDBEntryType::Function,
                            RocksDBLogType::FunctionDrop);
}

Result RocksDBEngineCatalog::CreateView(ObjectId db, ObjectId schema_id,
                                        ObjectId id,
                                        WriteProperties properties) {
  return PutSchemaObject(db, schema_id, id, properties, RocksDBEntryType::View,
                         RocksDBLogType::ViewCreate);
}

Result RocksDBEngineCatalog::DropView(ObjectId db, ObjectId schema_id,
                                      ObjectId id, std::string_view name) {
  return DeleteSchemaObject(db, schema_id, id, name, RocksDBEntryType::View,
                            RocksDBLogType::ViewDrop);
}

Result RocksDBEngineCatalog::CreateSchema(ObjectId db, ObjectId id,
                                          WriteProperties properties) {
  return PutObject(db, id, properties, RocksDBEntryType::Schema,
                   RocksDBLogType::SchemaCreate);
}

Result RocksDBEngineCatalog::ChangeSchema(ObjectId db, ObjectId id,
                                          WriteProperties properties) {
  return PutObject(db, id, properties, RocksDBEntryType::Schema,
                   RocksDBLogType::SchemaChange);
}

Result RocksDBEngineCatalog::DropSchema(ObjectId db, ObjectId id) {
  // Remove tombstone definition
  return DeleteDefinition(
    _db->GetRootDB(),
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchemaObject(RocksDBEntryType::ScopeTombstone,
                                id::kTombstoneDatabase, db, id);
      return key;
    },
    [] { return std::string_view{}; });
}

Result RocksDBEngineCatalog::ChangeView(ObjectId db, ObjectId schema_id,
                                        ObjectId id,
                                        WriteProperties properties) {
  return PutSchemaObject(db, schema_id, id, properties, RocksDBEntryType::View,
                         RocksDBLogType::ViewChange);
}

Result RocksDBEngineCatalog::CreateRole(const catalog::Role& role) {
  vpack::Builder b;
  return PutObject(
    id::kSystemDB, role.GetId(),
    [&](bool internal) { return GetObjectProperties(b, role, internal); },
    RocksDBEntryType::Role, RocksDBLogType::RoleCreate);
}

Result RocksDBEngineCatalog::DropRole(const catalog::Role& role) {
  return DeleteObject(id::kSystemDB, role.GetId(), role.GetName(),
                      RocksDBEntryType::Role, RocksDBLogType::RoleDrop);
}

Result RocksDBEngineCatalog::ChangeRole(ObjectId id,
                                        WriteProperties properties) {
  return PutObject(id::kSystemDB, id, properties, RocksDBEntryType::Role,
                   RocksDBLogType::RoleChange);
}

yaclib::Future<Result> RocksDBEngineCatalog::compactAll(
  bool change_level, bool compact_bottom_most_level) {
  return yaclib::MakeFuture(
    rocksutils::CompactAll(_db->GetRootDB(), change_level,
                           compact_bottom_most_level, &gCancelCompactions));
}

void RocksDBEngineCatalog::addIndexMapping(uint64_t object_id, ObjectId did,
                                           ObjectId cid, IndexId iid) {
  if (object_id != 0) {
    absl::WriterMutexLock guard{&_map_lock};
#ifdef SDB_DEV
    auto it = _index_map.find(object_id);
    if (it != _index_map.end()) {
      SDB_ASSERT(std::get<0>(it->second) == did);
      SDB_ASSERT(std::get<1>(it->second) == cid);
      SDB_ASSERT(std::get<2>(it->second) == iid);
    }
#endif
    _index_map[object_id] = std::make_tuple(did, cid, iid);
  }
}

void RocksDBEngineCatalog::removeIndexMapping(uint64_t object_id) {
  if (object_id != 0) {
    absl::WriterMutexLock guard{&_map_lock};
    _index_map.erase(object_id);
  }
}

auto RocksDBEngineCatalog::mapObjectToIndex(uint64_t object_id) const
  -> IndexTriple {
  absl::ReaderMutexLock guard{&_map_lock};
  auto it = _index_map.find(object_id);
  if (it == _index_map.end()) {
    return IndexTriple(0, 0, 0);
  }
  return it->second;
}

/// flushes the RocksDB WAL.
/// the optional parameter "waitForSync" is currently only used when the
/// "flushColumnFamilies" parameter is also set to true. If
/// "flushColumnFamilies" is true, all the RocksDB column family memtables are
/// flushed, and, if "waitForSync" is set, additionally synced to disk. The
/// only call site that uses "flushColumnFamilies" currently is backup.
/// The function parameter name are a remainder from MMFiles times, when they
/// made more sense. This can be refactored at any point, so that flushing
/// column families becomes a separate API.
Result RocksDBEngineCatalog::flushWal(bool wait_for_sync,
                                      bool flush_column_families) {
  Result res;

  if (_sync_thread) {
    // _sync_thread may be a nullptr, in case automatic syncing is turned off
    res = _sync_thread->syncWal();
  } else {
    // no syncThread...
    res = RocksDBSyncThread::sync(_db->GetBaseDB());
  }

  if (res.ok() && flush_column_families) {
    rocksdb::FlushOptions flush_options;
    flush_options.wait = wait_for_sync;

    for (auto cf : RocksDBColumnFamilyManager::allHandles()) {
      rocksdb::Status status = _db->GetBaseDB()->Flush(flush_options, cf);
      if (!status.ok()) {
        res = rocksutils::ConvertStatus(status);
        break;
      }
    }
  }

  return res;
}

void RocksDBEngineCatalog::waitForEstimatorSync() {
  // release all unused ticks from flush feature
  SerenedServer::Instance().getFeature<FlushFeature>().releaseUnusedTicks();

  // force-flush
  std::ignore = _settings_manager->sync(/*force*/ true);
}

Result RocksDBEngineCatalog::RegisterRecoveryHelper(
  std::shared_ptr<RocksDBRecoveryHelper> helper) {
  try {
    gRecoveryHelpers.emplace_back(std::move(helper));
  } catch (const std::bad_alloc&) {
    return {ERROR_OUT_OF_MEMORY};
  }

  return {};
}

const std::vector<std::shared_ptr<RocksDBRecoveryHelper>>&
RocksDBEngineCatalog::recoveryHelpers() {
  return gRecoveryHelpers;
}

void RocksDBEngineCatalog::determineWalFilesInitial() {
  absl::WriterMutexLock lock{&_wal_file_lock};
  // Retrieve the sorted list of all wal files with earliest file first
  rocksdb::VectorLogPtr files;
  auto status = _db->GetSortedWalFiles(files);
  if (!status.ok()) {
    SDB_WARN("xxxxx", Logger::ENGINES,
             "could not get WAL files: ", status.ToString());
    return;
  }

  size_t live_files = 0;
  size_t archived_files = 0;
  uint64_t live_files_size = 0;
  uint64_t archived_files_size = 0;
  for (size_t current = 0; current < files.size(); current++) {
    const auto& f = files[current].get();

    if (f->Type() == rocksdb::WalFileType::kArchivedLogFile) {
      ++archived_files;
      archived_files_size += f->SizeFileBytes();
    } else if (f->Type() == rocksdb::WalFileType::kAliveLogFile) {
      ++live_files;
      live_files_size += f->SizeFileBytes();
    }
  }
  _metrics_wal_sequence_lower_bound.store(
    _settings_manager->earliestSeqNeeded(), std::memory_order_relaxed);
  _metrics_live_wal_files.store(live_files, std::memory_order_relaxed);
  _metrics_archived_wal_files.store(archived_files, std::memory_order_relaxed);
  _metrics_live_wal_files_size.store(live_files_size,
                                     std::memory_order_relaxed);
  _metrics_archived_wal_files_size.store(archived_files_size,
                                         std::memory_order_relaxed);
}

void RocksDBEngineCatalog::determinePrunableWalFiles(Tick min_tick_external) {
  absl::WriterMutexLock lock{&_wal_file_lock};
  Tick min_tick_to_keep = std::min(
    _use_released_tick ? _released_tick : std::numeric_limits<Tick>::max(),
    min_tick_external);

  uint64_t min_log_number_to_keep = 0;
  std::string v;
  if (_db->GetProperty(rocksdb::DB::Properties::kMinLogNumberToKeep, &v)) {
    min_log_number_to_keep =
      static_cast<uint64_t>(basics::string_utils::Int64(v));
  }

  SDB_DEBUG("xxxxx", Logger::ENGINES,
            "determining prunable WAL files, minTickToKeep: ", min_tick_to_keep,
            ", minTickExternal: ", min_tick_external,
            ", releasedTick: ", _released_tick,
            ", minLogNumberToKeep: ", min_log_number_to_keep);

  // Retrieve the sorted list of all wal files with earliest file first
  rocksdb::VectorLogPtr files;
  auto status = _db->GetSortedWalFiles(files);
  if (!status.ok()) {
    SDB_WARN("xxxxx", Logger::ENGINES,
             "could not get WAL files: ", status.ToString());
    return;
  }

  // number of live WAL files
  size_t live_files = 0;
  // number of archived WAL files
  size_t archived_files = 0;
  // cumulated size of live WAL files
  uint64_t live_files_size = 0;
  // cumulated size of archived WAL files
  uint64_t archived_files_size = 0;

  for (size_t current = 0; current < files.size(); current++) {
    const auto& f = files[current].get();

    if (f->Type() == rocksdb::WalFileType::kAliveLogFile) {
      ++live_files;
      live_files_size += f->SizeFileBytes();
      SDB_TRACE("xxxxx", Logger::ENGINES, "live WAL file #", current, "/",
                files.size(), ", filename: '", f->PathName(),
                "', start sequence: ", f->StartSequence());
      continue;
    }

    if (f->Type() != rocksdb::WalFileType::kArchivedLogFile) {
      // we are mostly interested in files of the archive
      continue;
    }

    ++archived_files;
    archived_files_size += f->SizeFileBytes();

    // check if there is another WAL file coming after the currently-looked-at
    // There should be at least one live WAL file after it, however, let's be
    // paranoid and do a proper check. If there is at least one WAL file
    // following, we need to take its start tick into account as well, because
    // the following file's start tick can be assumed to be the end tick of
    // the current file!
    bool eligible_step1 = false;
    bool eligible_step2 = false;
    if (f->StartSequence() < min_tick_to_keep && current < files.size() - 1) {
      eligible_step1 = true;
      const auto& n = files[current + 1].get();
      if (n->StartSequence() < min_tick_to_keep) {
        // this file will be removed because it does not contain any data we
        // still need
        eligible_step2 = true;

        double stamp =
          utilities::GetMicrotime() + _options_provider._prune_wait_time;
        const auto [it, emplaced] =
          _prunable_wal_files.try_emplace(f->PathName(), stamp);

        if (emplaced) {
          SDB_DEBUG("xxxxx", Logger::ENGINES, "RocksDB WAL file '",
                    f->PathName(), "' with start sequence ", f->StartSequence(),
                    ", expire stamp ", stamp,
                    " added to prunable list because it is not needed anymore");
          SDB_ASSERT(it != _prunable_wal_files.end());
        } else {
          SDB_TRACE("xxxxx", Logger::ENGINES, "unable to add WAL file #",
                    current, "/", files.size(), ", filename: '", f->PathName(),
                    "', start sequence: ", f->StartSequence(),
                    " to list of prunable WAL files. file already present in "
                    "list "
                    "with expire stamp ",
                    it->second);
        }
      }
    }

    SDB_TRACE("xxxxx", Logger::ENGINES, "inspected WAL file #", current, "/",
              files.size(), ", filename: '", f->PathName(),
              "', start sequence: ", f->StartSequence(),
              ", eligible step1: ", eligible_step1,
              ", step2: ", eligible_step2);
  }

  SDB_DEBUG("xxxxx", Logger::ENGINES, "found ", files.size(),
            " WAL file(s), with ", live_files, " live file(s) and ",
            archived_files, " file(s) in the archive, ",
            "number of prunable files: ", _prunable_wal_files.size(),
            ", live file size: ", live_files_size,
            ", archived file size: ", archived_files_size);

  if (_options_provider._max_wal_archive_size_limit > 0 &&
      archived_files_size > _options_provider._max_wal_archive_size_limit) {
    // size of the archive is restricted, and we overflowed the limit.

    // print current archive size
    SDB_TRACE(
      "xxxxx", Logger::ENGINES,
      "total size of the RocksDB WAL file archive: ", archived_files_size,
      ", limit: ", _options_provider._max_wal_archive_size_limit);

    // we got more archived files than configured. time for purging some
    // files!
    for (size_t current = 0; current < files.size(); current++) {
      const auto& f = files[current].get();

      if (f->Type() != rocksdb::WalFileType::kArchivedLogFile) {
        continue;
      }

      // force pruning
      bool do_print = false;
      auto [it, emplaced] =
        _prunable_wal_files.try_emplace(f->PathName(), -1.0);
      if (emplaced) {
        do_print = true;
      } else {
        // file already in list. now set its expiration time to the past
        // so we are sure it will get deleted

        // using an expiration time of -1.0 indicates the file is subject to
        // deletion because the archive outgrew the maximum allowed size
        if ((*it).second > 0.0) {
          do_print = true;
        }
        (*it).second = -1.0;
      }

      if (do_print) {
        SDB_ASSERT(archived_files_size >
                   _options_provider._max_wal_archive_size_limit);

        // never change this id without adjusting wal-archive-size-limit tests
        // in tests/js/client/server-parameters
        SDB_WARN("xxxxx", Logger::ENGINES,
                 "forcing removal of RocksDB WAL file '", f->PathName(),
                 "' with start sequence ", f->StartSequence(),
                 " because of overflowing archive. configured maximum archive "
                 "size is ",
                 _options_provider._max_wal_archive_size_limit,
                 ", actual archive size is: ", archived_files_size,
                 ". if these warnings persist, try to increase the value of ",
                 "the startup option `--rocksdb.wal-archive-size-limit`");
      }

      SDB_ASSERT(archived_files_size >= f->SizeFileBytes());
      archived_files_size -= f->SizeFileBytes();

      if (archived_files_size <=
          _options_provider._max_wal_archive_size_limit) {
        // got enough files to remove
        break;
      }
    }
  }

  _metrics_wal_sequence_lower_bound.store(
    _settings_manager->earliestSeqNeeded(), std::memory_order_relaxed);
  _metrics_live_wal_files.store(live_files, std::memory_order_relaxed);
  _metrics_archived_wal_files.store(archived_files, std::memory_order_relaxed);
  _metrics_live_wal_files_size.store(live_files_size,
                                     std::memory_order_relaxed);
  _metrics_archived_wal_files_size.store(archived_files_size,
                                         std::memory_order_relaxed);
  _metrics_prunable_wal_files.store(_prunable_wal_files.size(),
                                    std::memory_order_relaxed);
  _metrics_wal_pruning_active.store(1, std::memory_order_relaxed);
}

RocksDBFilePurgePreventer RocksDBEngineCatalog::disallowPurging() noexcept {
  return RocksDBFilePurgePreventer(this);
}

RocksDBFilePurgeEnabler RocksDBEngineCatalog::startPurging() noexcept {
  return RocksDBFilePurgeEnabler(this);
}

void RocksDBEngineCatalog::pruneWalFiles() {
  // this struct makes sure that no other threads enter WAL tailing while we
  // are in here. If there are already other threads in WAL tailing while we
  // get here, we go on and only remove the WAL files that are really safe
  // to remove
  RocksDBFilePurgeEnabler purge_enabler(startPurging());

  absl::WriterMutexLock lock{&_wal_file_lock};

  // used for logging later
  const size_t initial_size = _prunable_wal_files.size();

  // go through the map of WAL files that we have already and check if they
  // are "expired"
  for (auto it = _prunable_wal_files.begin(); it != _prunable_wal_files.end();
       /* no hoisting */) {
    // check if WAL file is expired
    auto delete_file = purge_enabler.canPurge();
    SDB_TRACE("xxxxx", Logger::ENGINES, "pruneWalFiles checking file '",
              (*it).first, "', canPurge: ", delete_file);

    if (delete_file) {
      SDB_DEBUG("xxxxx", Logger::ENGINES, "deleting RocksDB WAL file '",
                (*it).first, "'");
      rocksdb::Status s;
      if (basics::file_utils::Exists(basics::file_utils::BuildFilename(
            _db_options.wal_dir, (*it).first))) {
        // only attempt file deletion if the file actually exists.
        // otherwise RocksDB may complain about non-existing files and log a
        // big error message
        s = _db->DeleteFile((*it).first);
        SDB_DEBUG("xxxxx", Logger::ENGINES,
                  "calling RocksDB DeleteFile for WAL file '", (*it).first,
                  "'. status: ", rocksutils::ConvertStatus(s).errorMessage());
      } else {
        SDB_DEBUG("xxxxx", Logger::ENGINES, "to-be-deleted RocksDB WAL file '",
                  (*it).first, "' does not exist. skipping deletion");
      }
      // apparently there is a case where a file was already deleted
      // but is still in _prunable_wal_files. In this case we get an invalid
      // argument response.
      if (s.ok() || s.IsInvalidArgument()) {
        _prunable_wal_files.erase(it++);
        continue;
      } else {
        SDB_WARN(
          "xxxxx", Logger::ENGINES, "attempt to prune RocksDB WAL file '",
          (*it).first,
          "' failed with error: ", rocksutils::ConvertStatus(s).errorMessage());
      }
    }

    // cannot delete this file yet... must forward iterator to prevent an
    // endless loop
    ++it;
  }

  _metrics_prunable_wal_files.store(_prunable_wal_files.size(),
                                    std::memory_order_relaxed);

  SDB_TRACE(
    "xxxxx", Logger::ENGINES, "prune WAL files started with ", initial_size,
    " prunable WAL files, ",
    "current number of prunable WAL files: ", _prunable_wal_files.size());
}

Result RocksDBEngineCatalog::dropDatabase(ObjectId id) {
#ifdef SDB_CLUSTER
  replicationManager()->dropDatabase(id);
  dumpManager()->dropDatabase(ObjectId{id});
#endif

  auto* db = _db->GetRootDB();

  auto remove_definitions = [&](RocksDBEntryType type) {
    auto bounds = RocksDBKeyBounds::SchemaObjects(type, id, id);
    return rocksutils::RemoveLargeRange(db, bounds.start(), bounds.end(),
                                        bounds.columnFamily(), true,
                                        /*rangeDel*/ false);
  };

  auto r = remove_definitions(RocksDBEntryType::View);
  if (!r.ok()) {
    return r;
  }
  r = remove_definitions(RocksDBEntryType::Function);
  if (!r.ok()) {
    return r;
  }
  r = remove_definitions(RocksDBEntryType::Role);
  if (!r.ok()) {
    return r;
  }

  // Remove tombstone definition
  r = DeleteDefinition(
    db,
    [&] {
      RocksDBKeyWithBuffer key;
      key.constructSchemaObject(RocksDBEntryType::ScopeTombstone,
                                id::kTombstoneDatabase, id, ObjectId{});
      return key;
    },
    [] { return std::string_view{}; });
  if (!r.ok()) {
    return r;
  }

  // remove VERSION file for database. it's not a problem when this fails
  // because it will simply remain there and be ignored on subsequent starts
  // TODO(mbkkt) why ignore?
  std::ignore = SdbUnlinkFile(versionFilename(id).c_str());

  return r;
}

void RocksDBEngineCatalog::EnsureSystemDatabase() {
  bool has_system = false;
  std::ignore = VisitDatabases([&](vpack::Slice result) -> Result {
    catalog::DatabaseOptions options;

    if (auto r = vpack::ReadTupleNothrow(result, options); r.ok()) {
      has_system = options.id == id::kSystemDB;
    }

    return {ERROR_INTERNAL};  // stop iteration
  });

  if (has_system) {
    SDB_TRACE("xxxxx", Logger::STARTUP, "Found system database");
    return;
  }

  const auto database = catalog::MakeSystemDatabaseOptions();
  vpack::Builder builder;
  vpack::WriteTuple(builder, database);
  auto r = createDatabase(database.id, builder.slice());
  if (!r.ok()) {
    SDB_FATAL("xxxxx", Logger::STARTUP,
              "unable to write database marker: ", r.errorMessage());
  }

  catalog::SchemaOptions schema_options{
    .id = catalog::NextId(),
    .name = std::string{StaticStrings::kPublic},
  };
  builder.clear();
  vpack::WriteTuple(builder, schema_options);
  r = CreateSchema(database.id, schema_options.id,
                   [&](bool) { return builder.slice(); });
  if (!r.ok()) {
    SDB_FATAL("xxxxx", Logger::STARTUP,
              "unable to write schema marker: ", r.errorMessage());
  }
}

DECLARE_GAUGE(rocksdb_cache_active_tables, uint64_t,
              "rocksdb_cache_active_tables");
DECLARE_GAUGE(rocksdb_cache_allocated, uint64_t, "rocksdb_cache_allocated");
DECLARE_GAUGE(rocksdb_cache_peak_allocated, uint64_t,
              "rocksdb_cache_peak_allocated");
DECLARE_GAUGE(rocksdb_cache_hit_rate_lifetime, uint64_t,
              "rocksdb_cache_hit_rate_lifetime");
DECLARE_GAUGE(rocksdb_cache_hit_rate_recent, uint64_t,
              "rocksdb_cache_hit_rate_recent");
DECLARE_GAUGE(rocksdb_cache_limit, uint64_t, "rocksdb_cache_limit");
DECLARE_GAUGE(rocksdb_cache_unused_memory, uint64_t,
              "rocksdb_cache_unused_memory");
DECLARE_GAUGE(rocksdb_cache_unused_tables, uint64_t,
              "rocksdb_cache_unused_tables");
DECLARE_COUNTER(rocksdb_cache_migrate_tasks_total,
                "rocksdb_cache_migrate_tasks_total");
DECLARE_COUNTER(rocksdb_cache_free_memory_tasks_total,
                "rocksdb_cache_free_memory_tasks_total");
DECLARE_COUNTER(rocksdb_cache_migrate_tasks_duration_total,
                "rocksdb_cache_migrate_tasks_duration_total");
DECLARE_COUNTER(rocksdb_cache_free_memory_tasks_duration_total,
                "rocksdb_cache_free_memory_tasks_duration_total");
DECLARE_GAUGE(rocksdb_actual_delayed_write_rate, uint64_t,
              "rocksdb_actual_delayed_write_rate");
DECLARE_GAUGE(rocksdb_background_errors, uint64_t, "rocksdb_background_errors");
DECLARE_GAUGE(rocksdb_base_level, uint64_t, "rocksdb_base_level");
DECLARE_GAUGE(rocksdb_block_cache_capacity, uint64_t,
              "rocksdb_block_cache_capacity");
DECLARE_GAUGE(rocksdb_block_cache_pinned_usage, uint64_t,
              "rocksdb_block_cache_pinned_usage");
DECLARE_GAUGE(rocksdb_block_cache_usage, uint64_t, "rocksdb_block_cache_usage");
DECLARE_GAUGE(rocksdb_block_cache_entries, uint64_t,
              "rocksdb_block_cache_entries");
DECLARE_GAUGE(rocksdb_block_cache_charge_per_entry, uint64_t,
              "rocksdb_block_cache_charge_per_entry");
DECLARE_GAUGE(rocksdb_compaction_pending, uint64_t,
              "rocksdb_compaction_pending");
DECLARE_GAUGE(rocksdb_compression_ratio_at_level0, uint64_t,
              "rocksdb_compression_ratio_at_level0");
DECLARE_GAUGE(rocksdb_compression_ratio_at_level1, uint64_t,
              "rocksdb_compression_ratio_at_level1");
DECLARE_GAUGE(rocksdb_compression_ratio_at_level2, uint64_t,
              "rocksdb_compression_ratio_at_level2");
DECLARE_GAUGE(rocksdb_compression_ratio_at_level3, uint64_t,
              "rocksdb_compression_ratio_at_level3");
DECLARE_GAUGE(rocksdb_compression_ratio_at_level4, uint64_t,
              "rocksdb_compression_ratio_at_level4");
DECLARE_GAUGE(rocksdb_compression_ratio_at_level5, uint64_t,
              "rocksdb_compression_ratio_at_level5");
DECLARE_GAUGE(rocksdb_compression_ratio_at_level6, uint64_t,
              "rocksdb_compression_ratio_at_level6");
DECLARE_GAUGE(rocksdb_cur_size_active_mem_table, uint64_t,
              "rocksdb_cur_size_active_mem_table");
DECLARE_GAUGE(rocksdb_cur_size_all_mem_tables, uint64_t,
              "rocksdb_cur_size_all_mem_tables");
DECLARE_GAUGE(rocksdb_estimate_live_data_size, uint64_t,
              "rocksdb_estimate_live_data_size");
DECLARE_GAUGE(rocksdb_estimate_num_keys, uint64_t, "rocksdb_estimate_num_keys");
DECLARE_GAUGE(rocksdb_estimate_pending_compaction_bytes, uint64_t,
              "rocksdb_estimate_pending_compaction_bytes");
DECLARE_GAUGE(rocksdb_estimate_table_readers_mem, uint64_t,
              "rocksdb_estimate_table_readers_mem");
DECLARE_GAUGE(rocksdb_free_disk_space, uint64_t, "rocksdb_free_disk_space");
DECLARE_GAUGE(rocksdb_free_inodes, uint64_t, "rocksdb_free_inodes");
DECLARE_GAUGE(rocksdb_is_file_deletions_enabled, uint64_t,
              "rocksdb_is_file_deletions_enabled");
DECLARE_GAUGE(rocksdb_is_write_stopped, uint64_t, "rocksdb_is_write_stopped");
DECLARE_GAUGE(rocksdb_live_sst_files_size, uint64_t,
              "rocksdb_live_sst_files_size");
DECLARE_GAUGE(rocksdb_mem_table_flush_pending, uint64_t,
              "rocksdb_mem_table_flush_pending");
DECLARE_GAUGE(rocksdb_min_log_number_to_keep, uint64_t,
              "rocksdb_min_log_number_to_keep");
DECLARE_GAUGE(rocksdb_num_deletes_active_mem_table, uint64_t,
              "rocksdb_num_deletes_active_mem_table");
DECLARE_GAUGE(rocksdb_num_deletes_imm_mem_tables, uint64_t,
              "rocksdb_num_deletes_imm_mem_tables");
DECLARE_GAUGE(rocksdb_num_entries_active_mem_table, uint64_t,
              "rocksdb_num_entries_active_mem_table");
DECLARE_GAUGE(rocksdb_num_entries_imm_mem_tables, uint64_t,
              "rocksdb_num_entries_imm_mem_tables");
DECLARE_GAUGE(rocksdb_num_files_at_level0, uint64_t,
              "rocksdb_num_files_at_level0");
DECLARE_GAUGE(rocksdb_num_files_at_level1, uint64_t,
              "rocksdb_num_files_at_level1");
DECLARE_GAUGE(rocksdb_num_files_at_level2, uint64_t,
              "rocksdb_num_files_at_level2");
DECLARE_GAUGE(rocksdb_num_files_at_level3, uint64_t,
              "rocksdb_num_files_at_level3");
DECLARE_GAUGE(rocksdb_num_files_at_level4, uint64_t,
              "rocksdb_num_files_at_level4");
DECLARE_GAUGE(rocksdb_num_files_at_level5, uint64_t,
              "rocksdb_num_files_at_level5");
DECLARE_GAUGE(rocksdb_num_files_at_level6, uint64_t,
              "rocksdb_num_files_at_level6");
DECLARE_GAUGE(rocksdb_num_immutable_mem_table, uint64_t,
              "rocksdb_num_immutable_mem_table");
DECLARE_GAUGE(rocksdb_num_immutable_mem_table_flushed, uint64_t,
              "rocksdb_num_immutable_mem_table_flushed");
DECLARE_GAUGE(rocksdb_num_live_versions, uint64_t, "rocksdb_num_live_versions");
DECLARE_GAUGE(rocksdb_num_running_compactions, uint64_t,
              "rocksdb_num_running_compactions");
DECLARE_GAUGE(rocksdb_num_running_flushes, uint64_t,
              "rocksdb_num_running_flushes");
DECLARE_GAUGE(rocksdb_num_snapshots, uint64_t, "rocksdb_num_snapshots");
DECLARE_GAUGE(rocksdb_oldest_snapshot_time, uint64_t,
              "rocksdb_oldest_snapshot_time");
DECLARE_GAUGE(rocksdb_size_all_mem_tables, uint64_t,
              "rocksdb_size_all_mem_tables");
DECLARE_GAUGE(rocksdb_total_disk_space, uint64_t, "rocksdb_total_disk_space");
DECLARE_GAUGE(rocksdb_total_inodes, uint64_t, "rocksdb_total_inodes");
DECLARE_GAUGE(rocksdb_total_sst_files_size, uint64_t,
              "rocksdb_total_sst_files_size");
DECLARE_GAUGE(rocksdb_engine_throttle_bps, uint64_t,
              "rocksdb_engine_throttle_bps");
DECLARE_GAUGE(rocksdb_read_only, uint64_t, "rocksdb_read_only");
DECLARE_GAUGE(rocksdb_total_sst_files, uint64_t, "rocksdb_total_sst_files");
DECLARE_GAUGE(rocksdb_live_blob_file_size, uint64_t,
              "rocksdb_live_blob_file_size");
DECLARE_GAUGE(rocksdb_live_blob_file_garbage_size, uint64_t,
              "rocksdb_live_blob_file_garbage_size");
DECLARE_GAUGE(rocksdb_num_blob_files, uint64_t, "rocksdb_num_blob_files");

void RocksDBEngineCatalog::toPrometheus(std::string& result,
                                        std::string_view globals,
                                        bool ensure_whitespace) const {
  vpack::BufferUInt8 buffer;
  vpack::Builder stats(buffer);
  getStatistics(stats);
  vpack::Slice sslice = stats.slice();

  SDB_ASSERT(sslice.isObject());
  for (auto [a_key, a_value] : vpack::ObjectIterator(sslice)) {
    if (a_value.isNumber()) {
      std::string name = a_key.copyString();
      std::replace(name.begin(), name.end(), '.', '_');
      std::replace(name.begin(), name.end(), '-', '_');
      if (!name.empty() && name.front() != 'r') {
        // prepend name with "rocksdb_"
        name = absl::StrCat(kEngineName, "_", name);
      }

      metrics::Metric::addInfo(result, name, /*help*/ name,
                               name.ends_with("_total") ? "counter" : "gauge");
      metrics::Metric::addMark(result, name, globals, "");
      absl::StrAppend(&result, ensure_whitespace ? " " : "",
                      a_value.getNumber<uint64_t>(), "\n");
    }
  }
}

void RocksDBEngineCatalog::getStatistics(vpack::Builder& builder) const {
  // add int properties
  auto add_int = [&](const std::string& s) {
    std::string v;
    if (_db->GetProperty(s, &v)) {
      int64_t i = basics::string_utils::Int64(v);
      builder.add(s, i);
    }
  };

  // add string properties
  auto add_str = [&](const std::string& s) {
    std::string v;
    if (_db->GetProperty(s, &v)) {
      builder.add(s, v);
    }
  };

  // get string property from each column family and return sum;
  auto add_int_all_cf = [&](const std::string& s) {
    int64_t sum = 0;
    std::string v;
    for (auto cfh : RocksDBColumnFamilyManager::allHandles()) {
      v.clear();
      if (_db->GetProperty(cfh, s, &v)) {
        int64_t temp = basics::string_utils::Int64(v);

        // -1 returned for some things that are valid property but no value
        if (0 < temp) {
          sum += temp;
        }
      }
    }
    builder.add(s, sum);
    return sum;
  };

  // add column family properties
  auto add_cf = [&](RocksDBColumnFamilyManager::Family family) {
    std::string name = RocksDBColumnFamilyManager::name(
      family, RocksDBColumnFamilyManager::NameMode::External);
    rocksdb::ColumnFamilyHandle* c = RocksDBColumnFamilyManager::get(family);
    std::string v;
    builder.add(name, vpack::Value(vpack::ValueType::Object));
    if (_db->GetProperty(c, rocksdb::DB::Properties::kCFStats, &v)) {
      builder.add("dbstats", v);
    }

    // re-add this line to count all keys in the column family (slow!!!)
    // builder.add("keys", rocksutils::countKeys(_db, c));

    // estimate size on disk and in memtables
    uint64_t out = 0;
    rocksdb::Range r(rocksdb::Slice("\x00\x00\x00\x00\x00\x00\x00\x00", 8),
                     rocksdb::Slice("\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
                                    "\xff\xff\xff\xff\xff\xff",
                                    16));

    rocksdb::SizeApproximationOptions options{.include_memtables = true,
                                              .include_files = true};
    _db->GetApproximateSizes(options, c, &r, 1, &out);

    builder.add("memory", out);
    builder.close();
  };

  builder.openObject(/*unindexed*/ true);
  int64_t num_sst_files_on_all_levels = 0;
  for (int i = 0; i < _options_provider.getOptions().num_levels; ++i) {
    num_sst_files_on_all_levels += add_int_all_cf(
      absl::StrCat(rocksdb::DB::Properties::kNumFilesAtLevelPrefix, i));
    // ratio needs new calculation with all cf, not a simple add operation
    add_int_all_cf(
      absl::StrCat(rocksdb::DB::Properties::kCompressionRatioAtLevelPrefix, i));
  }
  builder.add("rocksdb.total-sst-files", num_sst_files_on_all_levels);
  // caution:  you must read rocksdb/db/internal_stats.cc carefully to
  //           determine if a property is for whole database or one column
  //           family
  add_int_all_cf(rocksdb::DB::Properties::kNumImmutableMemTable);
  add_int_all_cf(rocksdb::DB::Properties::kNumImmutableMemTableFlushed);
  add_int_all_cf(rocksdb::DB::Properties::kMemTableFlushPending);
  add_int_all_cf(rocksdb::DB::Properties::kCompactionPending);
  add_int(rocksdb::DB::Properties::kBackgroundErrors);
  add_int_all_cf(rocksdb::DB::Properties::kCurSizeActiveMemTable);
  add_int_all_cf(rocksdb::DB::Properties::kCurSizeAllMemTables);
  add_int_all_cf(rocksdb::DB::Properties::kSizeAllMemTables);
  add_int_all_cf(rocksdb::DB::Properties::kNumEntriesActiveMemTable);
  add_int_all_cf(rocksdb::DB::Properties::kNumEntriesImmMemTables);
  add_int_all_cf(rocksdb::DB::Properties::kNumDeletesActiveMemTable);
  add_int_all_cf(rocksdb::DB::Properties::kNumDeletesImmMemTables);
  add_int_all_cf(rocksdb::DB::Properties::kEstimateNumKeys);
  add_int_all_cf(rocksdb::DB::Properties::kEstimateTableReadersMem);
  add_int(rocksdb::DB::Properties::kNumSnapshots);
  add_int(rocksdb::DB::Properties::kOldestSnapshotTime);
  add_int_all_cf(rocksdb::DB::Properties::kNumLiveVersions);
  add_int(rocksdb::DB::Properties::kMinLogNumberToKeep);
  add_int_all_cf(rocksdb::DB::Properties::kEstimateLiveDataSize);
  add_int_all_cf(rocksdb::DB::Properties::kLiveSstFilesSize);
  add_int_all_cf(rocksdb::DB::Properties::kLiveBlobFileSize);
  add_int_all_cf(rocksdb::DB::Properties::kLiveBlobFileGarbageSize);
  add_int_all_cf(rocksdb::DB::Properties::kNumBlobFiles);
  add_str(rocksdb::DB::Properties::kDBStats);
  add_str(rocksdb::DB::Properties::kSSTables);
  add_int(rocksdb::DB::Properties::kNumRunningCompactions);
  add_int(rocksdb::DB::Properties::kNumRunningFlushes);
  add_int(rocksdb::DB::Properties::kIsFileDeletionsEnabled);
  add_int_all_cf(rocksdb::DB::Properties::kEstimatePendingCompactionBytes);
  add_int(rocksdb::DB::Properties::kBaseLevel);
  add_int(rocksdb::DB::Properties::kBlockCacheCapacity);
  add_int(rocksdb::DB::Properties::kBlockCacheUsage);
  add_int(rocksdb::DB::Properties::kBlockCachePinnedUsage);

  const auto& table_options = _options_provider.getTableOptions();
  if (table_options.block_cache != nullptr) {
    const auto& cache = table_options.block_cache;
    auto usage = cache->GetUsage();
    auto entries = cache->GetOccupancyCount();
    if (entries > 0) {
      builder.add("rocksdb.block-cache-charge-per-entry",
                  static_cast<uint64_t>(usage / entries));
    } else {
      builder.add("rocksdb.block-cache-charge-per-entry", 0);
    }
    builder.add("rocksdb.block-cache-entries", entries);
  } else {
    builder.add("rocksdb.block-cache-entries", 0);
    builder.add("rocksdb.block-cache-charge-per-entry", 0);
  }

  add_int_all_cf(rocksdb::DB::Properties::kTotalSstFilesSize);
  add_int(rocksdb::DB::Properties::kActualDelayedWriteRate);
  add_int(rocksdb::DB::Properties::kIsWriteStopped);

  if (_db_options.statistics) {
    for (const auto& stat : rocksdb::TickersNameMap) {
      builder.add(stat.second,
                  _db_options.statistics->getTickerCount(stat.first));
    }

    uint64_t wal_write, flush_write, compaction_write, user_write;
    wal_write = _db_options.statistics->getTickerCount(rocksdb::WAL_FILE_BYTES);
    flush_write =
      _db_options.statistics->getTickerCount(rocksdb::FLUSH_WRITE_BYTES);
    compaction_write =
      _db_options.statistics->getTickerCount(rocksdb::COMPACT_WRITE_BYTES);
    user_write = _db_options.statistics->getTickerCount(rocksdb::BYTES_WRITTEN);
    builder.add(
      "rocksdbengine.write.amplification.x100",
      (0 != user_write)
        ? ((wal_write + flush_write + compaction_write) * 100) / user_write
        : 100);
  }

  // print column family statistics
  //  warning: output format limits numbers to 3 digits of precision or less.
  builder.add("columnFamilies", vpack::Value(vpack::ValueType::Object));
  add_cf(RocksDBColumnFamilyManager::Family::Definitions);
  add_cf(RocksDBColumnFamilyManager::Family::Documents);
  add_cf(RocksDBColumnFamilyManager::Family::PrimaryIndex);
  add_cf(RocksDBColumnFamilyManager::Family::EdgeIndex);
  add_cf(RocksDBColumnFamilyManager::Family::VPackIndex);
  builder.close();

  {
    // total disk space in database directory
    uint64_t total_space = 0;
    // free disk space in database directory
    uint64_t free_space = 0;
    Result res =
      SdbGetDiskSpaceInfo(_base_path.c_str(), total_space, free_space);
    if (res.ok()) {
      builder.add("rocksdb.free-disk-space", free_space);
      builder.add("rocksdb.total-disk-space", total_space);
    } else {
      builder.add("rocksdb.free-disk-space",
                  vpack::Value(vpack::ValueType::Null));
      builder.add("rocksdb.total-disk-space",
                  vpack::Value(vpack::ValueType::Null));
    }
  }

  {
    // total inodes for database directory
    uint64_t total_i_nodes = 0;
    // free inodes for database directory
    uint64_t free_i_nodes = 0;
    Result res =
      SdbGetINodesInfo(_base_path.c_str(), total_i_nodes, free_i_nodes);
    if (res.ok()) {
      builder.add("rocksdb.free-inodes", free_i_nodes);
      builder.add("rocksdb.total-inodes", total_i_nodes);
    } else {
      builder.add("rocksdb.free-inodes", vpack::Value(vpack::ValueType::Null));
      builder.add("rocksdb.total-inodes", vpack::Value(vpack::ValueType::Null));
    }
  }

  if (_error_listener) {
    builder.add("rocksdb.read-only", _error_listener->called() ? 1 : 0);
  }

  auto sequence_number = _db->GetLatestSequenceNumber();
  builder.add("rocksdb.wal-sequence", sequence_number);

  builder.close();
}

Result RocksDBEngineCatalog::createTickRanges(vpack::Builder& builder) {
  rocksdb::VectorLogPtr wal_files;
  rocksdb::Status s = _db->GetSortedWalFiles(wal_files);

  Result res = rocksutils::ConvertStatus(s);
  if (res.fail()) {
    return res;
  }

  builder.openArray();
  for (auto lfile = wal_files.begin(); lfile != wal_files.end(); ++lfile) {
    auto& logfile = *lfile;
    builder.openObject();
    // filename and state are already of type string
    builder.add("datafile", logfile->PathName());
    if (logfile->Type() == rocksdb::WalFileType::kAliveLogFile) {
      builder.add("status", "open");
    } else if (logfile->Type() == rocksdb::WalFileType::kArchivedLogFile) {
      builder.add("status", "collected");
    }
    rocksdb::SequenceNumber min = logfile->StartSequence();
    builder.add("tickMin", std::to_string(min));
    rocksdb::SequenceNumber max;
    if (std::next(lfile) != wal_files.end()) {
      max = (*std::next(lfile))->StartSequence();
    } else {
      max = _db->GetLatestSequenceNumber();
    }
    builder.add("tickMax", std::to_string(max));
    builder.close();
  }
  builder.close();

  return {};
}

Result RocksDBEngineCatalog::firstTick(uint64_t& tick) {
  rocksdb::VectorLogPtr wal_files;
  rocksdb::Status s = _db->GetSortedWalFiles(wal_files);

  Result res;
  if (!s.ok()) {
    res = rocksutils::ConvertStatus(s);
  } else {
    // read minium possible tick
    if (!wal_files.empty()) {
      tick = wal_files[0]->StartSequence();
    }
  }
  return res;
}

const WalAccess* RocksDBEngineCatalog::walAccess() const {
  SDB_ASSERT(_wal_access);
  return _wal_access.get();
}

/// get compression supported by RocksDB
std::string RocksDBEngineCatalog::getCompressionSupport() const {
  std::string result;

  for (const auto& type : rocksdb::GetSupportedCompressions()) {
    std::string out;
    rocksdb::GetStringFromCompressionType(&out, type);

    if (out.empty()) {
      continue;
    }
    if (!result.empty()) {
      result.append(", ");
    }
    result.append(out);
  }
  return result;
}

// management methods for synchronizing with external persistent stores
Tick RocksDBEngineCatalog::currentTick() const {
  return _db->GetLatestSequenceNumber();
}

Tick RocksDBEngineCatalog::releasedTick() const {
  absl::ReaderMutexLock lock{&_wal_file_lock};
  return _released_tick;
}

void RocksDBEngineCatalog::releaseTick(Tick tick) {
  std::unique_lock lock{_wal_file_lock};

  if (tick > _released_tick) {
    _released_tick = tick;
    lock.unlock();

    // update metric for released tick
    _metrics_wal_released_tick_flush.store(tick, std::memory_order_relaxed);
  }
}

HealthData RocksDBEngineCatalog::healthCheck() {
  auto now = std::chrono::steady_clock::now();

  // the following checks are executed under a mutex so that different
  // threads can potentially call in here without messing up any data.
  // in addition, serializing access to this function avoids stampedes
  // with multiple threads trying to calculate the free disk space
  // capacity at the same time, which could be expensive.
  std::lock_guard guard{_health_mutex};

  SDB_IF_FAILURE("RocksDBEngineCatalog::healthCheck") {
    _health_data.res.reset(ERROR_DEBUG, "peng! 💥");
    return {static_cast<const HealthDataBase&>(_health_data),
            _health_data.res.clone()};
  }

  bool last_check_long_ago =
    (_health_data.last_check_timestamp.time_since_epoch().count() == 0) ||
    ((now - _health_data.last_check_timestamp) >= std::chrono::seconds(30));
  if (last_check_long_ago) {
    _health_data.last_check_timestamp = now;
  }

  // only log about once every 24 hours, to reduce log spam
  bool last_log_message_long_ago =
    (_last_health_log_message_timestamp.time_since_epoch().count() == 0) ||
    ((now - _last_health_log_message_timestamp) >= std::chrono::hours(24));

  _health_data.background_error = hasBackgroundError();

  if (_health_data.background_error) {
    // go into failed state
    _health_data.res.reset(
      ERROR_FAILED,
      "storage engine reports background error. please check the logs "
      "for the error reason and take action");
  } else if (_last_health_check_successful) {
    _health_data.res.reset();
  }

  if (last_check_long_ago || !_last_health_check_successful) {
    // check the amount of free disk space. this may be expensive to do, so
    // we only execute the check every once in a while, or when the last check
    // failed too (so that we don't report success only because we skipped the
    // checks)
    //
    // total disk space in database directory
    uint64_t total_space = 0;
    // free disk space in database directory
    uint64_t free_space = 0;

    if (SdbGetDiskSpaceInfo(_base_path.c_str(), total_space, free_space).ok() &&
        total_space >= 1024 * 1024) {
      // only carry out the following if we get a disk size of at least 1MB
      // back. everything else seems to be very unreasonable and not
      // trustworthy.
      double disk_free_percentage = double(free_space) / double(total_space);
      _health_data.free_disk_space_bytes = free_space;
      _health_data.free_disk_space_percent = disk_free_percentage;

      if (_health_data.res.ok() &&
          ((_options_provider._required_disk_free_percentage > 0.0 &&
            disk_free_percentage <
              _options_provider._required_disk_free_percentage) ||
           (_options_provider._required_disk_free_bytes > 0 &&
            free_space < _options_provider._required_disk_free_bytes))) {
        std::string ss_str;
        absl::strings_internal::OStringStream ss{&ss_str};
        ss << "free disk space capacity has reached critical level, "
           << "bytes free: " << free_space
           << ", % free: " << std::setprecision(1) << std::fixed
           << (disk_free_percentage * 100.0);
        // go into failed state
        _health_data.res.reset(ERROR_FAILED, ss_str);
      } else if (disk_free_percentage < 0.05 ||
                 free_space < 256 * 1024 * 1024) {
        // warnings about disk space only every 15 minutes
        bool last_log_warning_long_ago =
          (now - _last_health_log_warning_timestamp >=
           std::chrono::minutes(15));
        if (last_log_warning_long_ago) {
          SDB_WARN("xxxxx", Logger::ENGINES,
                   "free disk space capacity is low, ",
                   "bytes free: ", free_space, ", % free: ",
                   absl::StrFormat("%.1f", disk_free_percentage * 100.0));
          _last_health_log_warning_timestamp = now;
        }
        // don't go into failed state (yet)
      }
    }
  }

  _last_health_check_successful = _health_data.res.ok();

  if (_health_data.res.fail() && last_log_message_long_ago) {
    SDB_ERROR("xxxxx", Logger::ENGINES, _health_data.res.errorMessage());

    // update timestamp of last log message
    _last_health_log_message_timestamp = now;
  }

  return {static_cast<const HealthDataBase&>(_health_data),
          _health_data.res.clone()};
}

void RocksDBEngineCatalog::waitForCompactionJobsToFinish() {
  // wait for started compaction jobs to finish
  int iterations = 0;

  do {
    size_t num_running;
    {
      absl::ReaderMutexLock locker{&_pending_compactions_lock};
      num_running = _running_compactions;
    }
    if (num_running == 0) {
      return;
    }
    ++iterations;

    // Maybe a flush can help?
    if (iterations == 100) {
      Result res =
        flushWal(false /* waitForSync */, true /* flushColumnFamilies */);
      if (res.fail()) {
        SDB_WARN("xxxxx", Logger::ENGINES,
                 "Error on flushWal during waitForCompactionJobsToFinish: ",
                 res.errorMessage());
      }
    }
    // print this only every 10 seconds
    if (iterations % 200 == 0) {
      SDB_INFO("xxxxx", Logger::ENGINES, "waiting for ", num_running,
               " compaction job(s) to finish...");
    }
    // unfortunately there is not much we can do except waiting for
    // RocksDB's compaction job(s) to finish.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    // wait up to 2 minutes, and then give up
  } while (iterations <= 2400);

  SDB_WARN("xxxxx", Logger::ENGINES,
           "giving up waiting for pending compaction job(s)");
}

bool RocksDBEngineCatalog::checkExistingDB(
  const std::vector<rocksdb::ColumnFamilyDescriptor>& cf_families) {
  bool db_existed = false;

  rocksdb::Options test_options;
  test_options.create_if_missing = false;
  test_options.create_missing_column_families = false;
  test_options.avoid_flush_during_recovery = true;
  test_options.avoid_flush_during_shutdown = true;
  test_options.env = _db_options.env;

  std::vector<std::string> existing_column_families;
  rocksdb::Status status = rocksdb::DB::ListColumnFamilies(
    test_options, _path, &existing_column_families);
  if (!status.ok()) {
    // check if we have found the database directory or not
    Result res = rocksutils::ConvertStatus(status);
    if (res.isNot(ERROR_SERVER_IO_ERROR)) {
      // not an I/O error. so we better report the error and abort here
      SDB_FATAL("xxxxx", Logger::STARTUP,
                "unable to initialize RocksDB engine: ", res.errorMessage());
    }
  }

  if (status.ok()) {
    db_existed = true;
    // we were able to open the database.
    // now check which column families are present in the db
    std::string names;
    for (const auto& it : existing_column_families) {
      if (!names.empty()) {
        names.append(", ");
      }
      names.append(it);
    }

    SDB_DEBUG("xxxxx", Logger::STARTUP,
              "found existing column families: ", names);

    for (const auto& it : cf_families) {
      auto it2 = std::find(existing_column_families.begin(),
                           existing_column_families.end(), it.name);
      if (it2 == existing_column_families.end()) {
        SDB_FATAL(
          "xxxxx", Logger::STARTUP, "column family '", it.name,
          "' is missing in database",
          ". if you are upgrading from an earlier alpha or beta version "
          "of SereneDB, it is required to restart with a new database "
          "directory and "
          "re-import data");
      }
    }

    if (existing_column_families.size() <
        RocksDBColumnFamilyManager::kMinNumberOfColumnFamilies) {
      SDB_FATAL("xxxxx", Logger::STARTUP,
                "unexpected number of column families found in database (",
                existing_column_families.size(), "). expecting at least ",
                RocksDBColumnFamilyManager::kMinNumberOfColumnFamilies,
                ". if you are upgrading from an earlier alpha or beta version "
                "of SereneDB, it is required to restart with a new database "
                "directory and "
                "re-import data");
    }
  }

  return db_existed;
}

std::shared_ptr<StorageSnapshot> RocksDBEngineCatalog::currentSnapshot() {
  if (_db) [[likely]] {
    return std::make_shared<RocksDBSnapshot>(*_db);
  } else {
    return nullptr;
  }
}

std::tuple<uint64_t, uint64_t, uint64_t, uint64_t, uint64_t>
RocksDBEngineCatalog::getCacheMetrics() {
  return {_metrics_edge_cache_entries_size_initial.load(),
          _metrics_edge_cache_entries_size_effective.load(),
          _metrics_edge_cache_inserts.load(),
          _metrics_edge_cache_compressed_inserts.load(),
          _metrics_edge_cache_empty_inserts.load()};
}

void RocksDBEngineCatalog::addCacheMetrics(
  uint64_t initial, uint64_t effective, uint64_t total_inserts,
  uint64_t total_compressed_inserts, uint64_t total_empty_inserts) noexcept {
  if (total_inserts > 0) {
    _metrics_edge_cache_entries_size_initial.count(initial);
    _metrics_edge_cache_entries_size_effective.count(effective);
    _metrics_edge_cache_inserts.count(total_inserts);
    _metrics_edge_cache_compressed_inserts.count(total_compressed_inserts);
    _metrics_edge_cache_empty_inserts.count(total_empty_inserts);
  }
}

Result DeleteTableMeta(rocksdb::DB* db, uint64_t object_id) {
  rocksdb::ColumnFamilyHandle* const cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Definitions);
  rocksdb::WriteOptions wo;

  // Step 1. delete the document count
  RocksDBKeyWithBuffer key;
  key.constructCounterValue(object_id);
  rocksdb::Status s = db->Delete(wo, cf, key.string());
  if (!s.ok()) {
    SDB_ERROR("xxxxx", Logger::ENGINES,
              "could not delete counter value for collection with objectId '",
              object_id, "': ", s.ToString());
    // try to remove the key generator value regardless
  } else {
    SDB_TRACE("xxxxx", Logger::ENGINES,
              "deleted counter for collection with objectId '", object_id, "'");
  }

  key.constructKeyGeneratorValue(object_id);
  s = db->Delete(wo, cf, key.string());
  if (!s.ok() && !s.IsNotFound()) {
    SDB_ERROR("xxxxx", Logger::ENGINES,
              "could not delete key generator value: ", s.ToString());
    return rocksutils::ConvertStatus(s);
  }

  key.constructRevisionTreeValue(object_id);
  s = db->Delete(wo, cf, key.string());
  if (!s.ok() && !s.IsNotFound()) {
    SDB_ERROR("xxxxx", Logger::ENGINES,
              "could not delete revision tree value: ", s.ToString());
    return rocksutils::ConvertStatus(s);
  }

  return {};
}

Result DeleteIndexEstimate(rocksdb::DB* db, uint64_t object_id) {
  rocksdb::ColumnFamilyHandle* const cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Definitions);
  rocksdb::WriteOptions wo;

  RocksDBKeyWithBuffer key;
  key.constructIndexEstimateValue(object_id);
  rocksdb::Status s = db->Delete(wo, cf, key.string());
  if (!s.ok() && !s.IsNotFound()) {
    return rocksutils::ConvertStatus(s);
  }
  return {};
}

DocCount LoadCollectionCount(rocksdb::DB* db, uint64_t object_id) {
  auto cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Definitions);
  rocksdb::ReadOptions ro;
  // TODO ro.verify_checksums = false;
  ro.fill_cache = false;

  RocksDBKeyWithBuffer key;
  key.constructCounterValue(object_id);

  rocksdb::PinnableSlice value;
  rocksdb::Status s = db->Get(ro, cf, key.string(), &value);
  if (s.ok()) {
    vpack::Slice count_slice = RocksDBValue::data(value);
    SDB_TRACE("xxxxx", Logger::ENGINES, "loaded counter '",
              count_slice.toJson(), "' for collection with objectId '",
              object_id, "'");
    return DocCount{count_slice};
  }
  SDB_TRACE("xxxxx", Logger::ENGINES,
            "loaded default zero counter for collection with objectId '",
            object_id, "'");
  return {};
}

DocCount::DocCount(vpack::Slice slice) : DocCount{} {
  if (!slice.isArray()) {  // got a somewhat invalid slice
    // probably old data from before the key structure changes
    return;
  }

  vpack::ArrayIterator array{slice};
  if (array.valid()) {
    committed_seq = (*array).getUInt();
    // versions pre 3.4 stored only a single "count" value
    // 3.4 and higher store "added" and "removed" seperately
    added = (*(++array)).getUInt();
    if (array.size() > 3) {
      SDB_ASSERT(array.size() == 4);
      removed = (*(++array)).getUInt();
    }
    revision_id = RevisionId{(*(++array)).getUInt()};
  }
}

void DocCount::toVPack(vpack::Builder& b) const {
  b.openArray();
  b.add(committed_seq);
  b.add(added);
  b.add(removed);
  b.add(revision_id.id());
  b.close();
}

}  // namespace sdb
