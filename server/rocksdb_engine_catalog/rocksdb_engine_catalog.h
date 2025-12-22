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

#include <absl/functional/function_ref.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/snapshot.h>
#include <vpack/builder.h>
#include <vpack/slice.h>

#include <chrono>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "basics/common.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/read_write_lock.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/index_id.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/identifiers/revision_id.h"
#include "catalog/table.h"
#include "catalog/types.h"
#include "database/access_mode.h"
#include "metrics/fwd.h"
#include "rocksdb_engine_catalog/rocksdb_key_bounds.h"
#include "rocksdb_engine_catalog/rocksdb_option_feature.h"
#include "rocksdb_engine_catalog/rocksdb_recovery_manager.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "storage_engine/health_data.h"
#include "storage_engine/wal_access.h"

namespace rocksdb {
class Env;
class TransactionDB;
}  // namespace rocksdb

namespace sdb {

class TableShard;
class RocksDBBackgroundErrorListener;
class RocksDBBackgroundThread;
class RocksDBDumpManager;
class RocksDBKey;
class RocksDBLogValue;
class RocksDBRecoveryHelper;
class RocksDBReplicationManager;
class RocksDBSettingsManager;
class RocksDBSyncThread;
class RocksDBVPackComparator;
class RocksDBWalAccess;
class TransactionTable;
class TransactionState;

namespace rest {
class RestHandlerFactory;
}

class RocksDBEngineCatalog;

class StorageSnapshot {
 public:
  StorageSnapshot() = default;
  StorageSnapshot(const StorageSnapshot&) = delete;
  StorageSnapshot& operator=(const StorageSnapshot&) = delete;
  virtual ~StorageSnapshot() = default;

  virtual Tick tick() const noexcept = 0;
};

using WriteProperties = absl::FunctionRef<vpack::Slice(bool internal)>;

/// helper class to make file-purging thread-safe
/// while there is an object of this type around, it will prevent
/// purging of maybe-needed WAL files via holding a lock in the
/// RocksDB engine. if there is no object of this type around,
/// purging is allowed to happen
class RocksDBFilePurgePreventer {
 public:
  RocksDBFilePurgePreventer(const RocksDBFilePurgePreventer&) = delete;
  RocksDBFilePurgePreventer& operator=(const RocksDBFilePurgePreventer&) =
    delete;
  RocksDBFilePurgePreventer& operator=(RocksDBFilePurgePreventer&&) = delete;

  explicit RocksDBFilePurgePreventer(RocksDBEngineCatalog*);
  RocksDBFilePurgePreventer(RocksDBFilePurgePreventer&&);
  ~RocksDBFilePurgePreventer();

 private:
  RocksDBEngineCatalog* _engine;
};

/// helper class to make file-purging thread-safe
/// creating an object of this type will try to acquire a lock that rules
/// out all WAL iteration/WAL tailing while the lock is held. While this
/// is the case, we are allowed to purge any WAL file, because no other
/// thread is able to access it. Note that it is still safe to delete
/// unneeded WAL files, as they will not be accessed by any other thread.
/// however, without this object it would be unsafe to delete WAL files
/// that may still be accessed by WAL tailing etc.
class RocksDBFilePurgeEnabler {
 public:
  RocksDBFilePurgeEnabler(const RocksDBFilePurgePreventer&) = delete;
  RocksDBFilePurgeEnabler& operator=(const RocksDBFilePurgeEnabler&) = delete;
  RocksDBFilePurgeEnabler& operator=(RocksDBFilePurgeEnabler&&) = delete;

  explicit RocksDBFilePurgeEnabler(RocksDBEngineCatalog*);
  RocksDBFilePurgeEnabler(RocksDBFilePurgeEnabler&&);
  ~RocksDBFilePurgeEnabler();

  /// returns true if purging any type of WAL file is currently allowed
  bool canPurge() const { return _engine != nullptr; }

 private:
  RocksDBEngineCatalog* _engine;
};

class RocksDBSnapshot final : public StorageSnapshot {
 public:
  explicit RocksDBSnapshot(rocksdb::DB& db) : _snapshot(&db) {}

  Tick tick() const noexcept final {
    return _snapshot.snapshot()->GetSequenceNumber();
  }

  decltype(auto) getSnapshot() const { return _snapshot.snapshot(); }

 private:
  mutable rocksdb::ManagedSnapshot _snapshot;
};

class RocksDBEngineCatalog {
  friend class RocksDBFilePurgePreventer;
  friend class RocksDBFilePurgeEnabler;

 public:
  static constexpr std::string_view kEngineName = "rocksdb";

  static constexpr std::string_view name() noexcept { return "RocksDBEngine"; }

  RocksDBEngineCatalog(SerenedServer& server);
  RocksDBEngineCatalog(const RocksDBOptionFeature& options_provider,
                       metrics::MetricsFeature& metrics);
  ~RocksDBEngineCatalog();

  void prepare();
  void start();
  void beginShutdown();
  void stop();
  void unprepare();

  void flushOpenFilesIfRequired();
  HealthData healthCheck();

  void getStatistics(vpack::Builder& builder) const;
  void toPrometheus(std::string& result, std::string_view globals,
                    bool ensure_whitespace) const;

  Result VisitDatabases(
    absl::FunctionRef<Result(vpack::Slice database)> visitor);

  std::string versionFilename(ObjectId id) const;
  std::string databasePath() const { return _base_path; }
  std::string path() const { return _path; }
  std::string idxPath() const { return _idx_path; }

  void cleanupReplicationContexts();

  ErrorCode getReplicationApplierConfiguration(ObjectId database,
                                               vpack::Builder& builder);
  ErrorCode getReplicationApplierConfiguration(vpack::Builder& builder);
  ErrorCode removeReplicationApplierConfiguration(ObjectId database);
  ErrorCode removeReplicationApplierConfiguration();
  ErrorCode saveReplicationApplierConfiguration(ObjectId database,
                                                vpack::Slice slice,
                                                bool do_sync);
  ErrorCode saveReplicationApplierConfiguration(vpack::Slice slice,
                                                bool do_sync);
  Result createTickRanges(vpack::Builder& builder);
  Result firstTick(uint64_t& tick);
  const WalAccess* walAccess() const;

  // database, collection and index management

  /// flushes the RocksDB WAL.
  /// the optional parameter "waitForSync" is currently only used when the
  /// "flushColumnFamilies" parameter is also set to true. If
  /// "flushColumnFamilies" is true, all the RocksDB column family memtables are
  /// flushed, and, if "waitForSync" is set, additionally synced to disk. The
  /// only call site that uses "flushColumnFamilies" currently is backup.
  /// The function parameter name are a remainder from MMFiles times, when
  /// they made more sense. This can be refactored at any point, so that
  /// flushing column families becomes a separate API.
  Result flushWal(bool wait_for_sync = false,
                  bool flush_column_families = false);
  void waitForEstimatorSync();

  Result createDatabase(ObjectId id, vpack::Slice slice);
  Result dropDatabase(ObjectId id);

  // wal in recovery
  RecoveryState recoveryState() noexcept;

  /// current recovery tick
  Tick recoveryTick() noexcept;

  Result createTableShard(const catalog::Table& collection, bool is_new,
                          std::shared_ptr<TableShard>& physical);

  /// disallow purging of WAL files even if the archive gets too big
  /// removing WAL files does not seem to be thread-safe, so we have to track
  /// usage of WAL files ourselves
  RocksDBFilePurgePreventer disallowPurging() noexcept;

  /// whether or not purging of WAL files is currently allowed
  RocksDBFilePurgeEnabler startPurging() noexcept;

  bool inRecovery() { return recoveryState() < RecoveryState::Done; }

  void scheduleTreeRebuild(ObjectId database, ObjectId collection);
  void processTreeRebuilds();

  void compactRange(RocksDBKeyBounds bounds);
  void processCompactions();

  Result CreateFunction(ObjectId db, ObjectId schema_id, ObjectId id,
                        WriteProperties properties);

  Result DropFunction(ObjectId db, ObjectId schema_id, ObjectId id,
                      std::string_view name);

  void createTable(const catalog::Table& collection, TableShard& physical);
  Result CreateIndex(const catalog::Index& index);
  Result MarkDeleted(const catalog::Table& collection,
                     const TableShard& physical,
                     const TableTombstone& tombstone);
  Result MarkDeleted(const catalog::Index& index,
                     const IndexTombstone& tombstone);
  Result MarkDeleted(const catalog::Database& database);
  Result MarkDeleted(const catalog::Schema& schema);

  void prepareDropTable(ObjectId collection);
  Result DropIndex(IndexTombstone tombstone);
  Result DropTable(const TableTombstone& tombstone);

  void ChangeTable(const catalog::Table& collection,
                   const TableShard& physical);

  Result RenameTable(const catalog::Table& collection,
                     const TableShard& physical, std::string_view old_name);

  Result CreateSchema(ObjectId db, ObjectId id, WriteProperties properties);
  Result ChangeSchema(ObjectId db, ObjectId id, WriteProperties properties);
  Result DropSchema(ObjectId db, ObjectId id);

  Result ChangeView(ObjectId db, ObjectId schema_id, ObjectId id,
                    WriteProperties properties);

  Result CreateView(ObjectId db, ObjectId schema_id, ObjectId id,
                    WriteProperties properties);

  Result DropView(ObjectId db, ObjectId schema_id, ObjectId id,
                  std::string_view name);

  Result ChangeRole(ObjectId id, WriteProperties properties);

  Result CreateRole(const catalog::Role& role);

  Result DropRole(const catalog::Role& role);

  yaclib::Future<Result> compactAll(bool change_level,
                                    bool compact_bottom_most_level);

  // TODO(gnusi): remove
  using IndexTriple = std::tuple<ObjectId, ObjectId, IndexId>;
  IndexTriple mapObjectToIndex(uint64_t object_id) const;
  void addIndexMapping(uint64_t object_id, ObjectId db_id, ObjectId cid,
                       IndexId iid);
  void removeIndexMapping(uint64_t object_id);

  rocksdb::TransactionDB* db() const { return _db; }

  Result writeDatabaseMarker(ObjectId id, vpack::Slice slice,
                             RocksDBLogValue&& log_value);
  Result writeCreateTableMarker(ObjectId database_id, ObjectId schema_id,
                                ObjectId id, vpack::Slice slice,
                                std::string_view log_value);

  /// determine how many archived WAL files are available. this is called
  /// during the first few minutes after the instance start, when we don't
  /// want to prune any WAL files yet. this also updates the metrics for the
  /// number of available WAL files.
  void determineWalFilesInitial();

  /// determine which archived WAL files are prunable. as a side-effect,
  /// this updates the metrics for the number of available and prunable WAL
  /// files.
  void determinePrunableWalFiles(Tick min_tick_to_keep);
  void pruneWalFiles();

  double pruneWaitTimeInitial() const {
    return _options_provider.pruneWaitTimeInitial();
  }

  // management methods for synchronizing with external persistent stores
  Tick currentTick() const;
  Tick releasedTick() const;
  void releaseTick(Tick);

  /// whether or not the database existed at startup. this function
  /// provides a valid answer only after start() has successfully finished,
  /// so don't call it from other features during their start() if they are
  /// earlier in the startup sequence
  bool dbExisted() const noexcept { return _db_existed; }

  void trackRevisionTreeHibernation() noexcept;
  void trackRevisionTreeResurrection() noexcept;

  void trackRevisionTreeMemoryIncrease(uint64_t value) noexcept;
  void trackRevisionTreeMemoryDecrease(uint64_t value) noexcept;

  void trackRevisionTreeBufferedMemoryIncrease(uint64_t value) noexcept;
  void trackRevisionTreeBufferedMemoryDecrease(uint64_t value) noexcept;

  void trackIndexSelectivityMemoryIncrease(uint64_t value) noexcept;
  void trackIndexSelectivityMemoryDecrease(uint64_t value) noexcept;

  metrics::Gauge<uint64_t>& indexEstimatorMemoryUsageMetric() const noexcept {
    return _metrics_index_estimator_memory_usage;
  }

  rocksdb::Options makeOptions(bool is_new_dir);

  const rocksdb::DBOptions& rocksDBOptions() const { return _db_options; }

  /// recovery manager
  RocksDBSettingsManager* settingsManager() const {
    SDB_ASSERT(_settings_manager);
    return _settings_manager.get();
  }

  /// manages the ongoing dump clients
  RocksDBReplicationManager* replicationManager() const {
    SDB_ASSERT(_replication_manager);
    return _replication_manager.get();
  }

  RocksDBDumpManager* dumpManager() const {
    SDB_ASSERT(_dump_manager);
    return _dump_manager.get();
  }

  /// returns a pointer to the sync thread
  /// note: returns a nullptr if automatic syncing is turned off!
  RocksDBSyncThread* syncThread() const { return _sync_thread.get(); }

  bool hasBackgroundError() const;

  static Result RegisterRecoveryHelper(
    std::shared_ptr<RocksDBRecoveryHelper> helper);
  static const std::vector<std::shared_ptr<RocksDBRecoveryHelper>>&
  recoveryHelpers();

#ifdef SDB_GTEST
  uint64_t recoveryStartSequence() const noexcept {
    return _recovery_start_sequence;
  }
  void recoveryStartSequence(uint64_t value) noexcept {
    SDB_ASSERT(_recovery_start_sequence == 0);
    _recovery_start_sequence = value;
  }
#endif

  std::shared_ptr<StorageSnapshot> currentSnapshot();

  void addCacheMetrics(uint64_t initial, uint64_t effective,
                       uint64_t total_inserts,
                       uint64_t total_compressed_inserts,
                       uint64_t total_empty_inserts) noexcept;

  std::tuple<uint64_t, uint64_t, uint64_t, uint64_t, uint64_t>
  getCacheMetrics();

  Result VisitObjects(
    ObjectId database_id, RocksDBEntryType entry,
    absl::FunctionRef<Result(rocksdb::Slice, vpack::Slice)> visitor);
  Result VisitSchemaObjects(
    ObjectId database_id, ObjectId schema_id, RocksDBEntryType entry,
    absl::FunctionRef<Result(rocksdb::Slice, vpack::Slice)> visitor);

 private:
  bool UseRangeDelete(ObjectId id, uint64_t number_documents);

  Result VisitObjectsImpl(
    const RocksDBKeyBounds& bounds,
    absl::FunctionRef<Result(rocksdb::Slice, vpack::Slice)> visitor);

  Result DeleteSchemaObject(ObjectId db_id, ObjectId schema_id,
                            ObjectId object_id, std::string_view object_name,
                            RocksDBEntryType entry, RocksDBLogType log);
  Result PutSchemaObject(ObjectId db, ObjectId schema_id, ObjectId id,
                         WriteProperties properties, RocksDBEntryType entry,
                         RocksDBLogType log);

  Result PutObject(ObjectId db, ObjectId id, WriteProperties properties,
                   RocksDBEntryType entry, RocksDBLogType log);

  Result DeleteObject(ObjectId db_id, ObjectId object_id,
                      std::string_view object_name, RocksDBEntryType entry,
                      RocksDBLogType log);

  void shutdownRocksDBInstance() noexcept;
  void waitForCompactionJobsToFinish();
  ErrorCode getReplicationApplierConfiguration(const RocksDBKey& key,
                                               vpack::Builder& builder);
  ErrorCode removeReplicationApplierConfiguration(const RocksDBKey& key);
  ErrorCode saveReplicationApplierConfiguration(const RocksDBKey& key,
                                                vpack::Slice slice,
                                                bool do_sync);
  void EnsureSystemDatabase();

  std::string getCompressionSupport() const;

  [[noreturn]] void verifySstFiles() const;

  void validateJournalFiles() const;

  bool checkExistingDB(
    const std::vector<rocksdb::ColumnFamilyDescriptor>& cf_families);

  const RocksDBOptionFeature& _options_provider;

  metrics::MetricsFeature& _metrics;

  /// single rocksdb database used in this storage engine
  rocksdb::TransactionDB* _db = nullptr;
  /// default read options
  rocksdb::DBOptions _db_options;
  /// path used by rocksdb (inside _base_path)
  std::string _path;
  /// path to serenedb data dir
  std::string _base_path;
  /// path used for index creation
  std::string _idx_path;

  /// repository for replication contexts
  std::shared_ptr<RocksDBReplicationManager> _replication_manager;
  /// tracks the count of documents in collections
  std::unique_ptr<RocksDBSettingsManager> _settings_manager;
  /// Local wal access abstraction
  std::unique_ptr<RocksDBWalAccess> _wal_access;

  /// Background thread handling garbage collection etc
  std::unique_ptr<RocksDBBackgroundThread> _background_thread;

  // hook-ins for recovery process
  static inline std::vector<std::shared_ptr<RocksDBRecoveryHelper>>
    gRecoveryHelpers;

  struct Collection {
    ObjectId db;
  };

  // TODO(gnusi): remove
  mutable absl::Mutex _map_lock;
  containers::FlatHashMap<uint64_t, IndexTriple> _index_map;

  /// protects _prunable_wal_files
  mutable absl::Mutex _wal_file_lock;

  /// which WAL files can be pruned when
  /// an expiration time of <= 0.0 means the file does not have expired, but
  /// still should be purged because the WAL files archive outgrew its max
  /// configured size
  containers::FlatHashMap<std::string, double> _prunable_wal_files;

  // do not release walfiles containing writes later than this
  Tick _released_tick = 0;

  /// Background thread handling WAL syncing
  /// note: this is a nullptr if automatic syncing is turned off!
  std::unique_ptr<RocksDBSyncThread> _sync_thread;

  /// whether or not to use _released_tick when determining the WAL files
  /// to prune
  bool _use_released_tick = false;

  /// whether or not the last health check was successful.
  /// this is used to determine when to execute the potentially expensive
  /// checks for free disk space
  bool _last_health_check_successful = false;

  /// whether or not the DB existed at startup
  bool _db_existed = false;

  /// background error listener. will be invoked by rocksdb in case of
  /// a non-recoverable error
  std::shared_ptr<RocksDBBackgroundErrorListener> _error_listener;

  basics::ReadWriteLock _purge_lock;

  /// mutex that protects the storage engine health check
  absl::Mutex _health_mutex;

  /// timestamp of last health check log message. we only log health
  /// check errors every so often, in order to prevent log spamming
  std::chrono::steady_clock::time_point _last_health_log_message_timestamp;

  /// timestamp of last health check warning message. we only log health
  /// check warnings every so often, in order to prevent log spamming
  std::chrono::steady_clock::time_point _last_health_log_warning_timestamp;

  /// global health data, updated periodically
  HealthData _health_data;

  /// lock for _rebuild_collections
  absl::Mutex _rebuild_collections_lock;
  /// map of database/collection-guids for which we need to repair trees
  std::map<std::pair<ObjectId, ObjectId>, bool> _rebuild_collections;
  /// number of currently running tree rebuild jobs jobs
  size_t _running_rebuilds = 0;

  /// lock for _pending_compactions and _running_compactions
  absl::Mutex _pending_compactions_lock;
  /// bounds for compactions that we have to process
  std::deque<RocksDBKeyBounds> _pending_compactions;
  /// number of currently running compaction jobs
  size_t _running_compactions = 0;
  /// column families for which we are currently running a compaction.
  /// we track this because we want to avoid running multiple compactions on
  /// the same column family concurrently. this can help to avoid a shutdown
  /// hanger in rocksdb.
  containers::FlatHashSet<rocksdb::ColumnFamilyHandle*>
    _running_compactions_column_families;

  // sequence number from which WAL recovery was started. used only
  // for testing
#ifdef SDB_GTEST
  uint64_t _recovery_start_sequence = 0;
#endif

  // last point in time when an auto-flush happened
  std::chrono::steady_clock::time_point _auto_flush_last_executed;

  metrics::Gauge<uint64_t>& _metrics_index_estimator_memory_usage;
  metrics::Gauge<uint64_t>& _metrics_wal_released_tick_flush;
  metrics::Gauge<uint64_t>& _metrics_wal_sequence_lower_bound;
  metrics::Gauge<uint64_t>& _metrics_live_wal_files;
  metrics::Gauge<uint64_t>& _metrics_archived_wal_files;
  metrics::Gauge<uint64_t>& _metrics_live_wal_files_size;
  metrics::Gauge<uint64_t>& _metrics_archived_wal_files_size;
  metrics::Gauge<uint64_t>& _metrics_prunable_wal_files;
  metrics::Gauge<uint64_t>& _metrics_wal_pruning_active;
  metrics::Gauge<uint64_t>& _metrics_tree_memory_usage;
  metrics::Gauge<uint64_t>& _metrics_tree_buffered_memory_usage;
  metrics::Counter& _metrics_tree_rebuilds_success;
  metrics::Counter& _metrics_tree_rebuilds_failure;
  metrics::Counter& _metrics_tree_hibernations;
  metrics::Counter& _metrics_tree_resurrections;

  // total size of uncompressed values for the edge cache
  metrics::Counter& _metrics_edge_cache_entries_size_initial;
  // total size of values stored in the edge cache (can be smaller than the
  // initial size because of compression)
  metrics::Counter& _metrics_edge_cache_entries_size_effective;

  // total number of inserts into edge cache
  metrics::Counter& _metrics_edge_cache_inserts;
  // total number of inserts into edge cache that were compressed
  metrics::Counter& _metrics_edge_cache_compressed_inserts;
  // total number of inserts into edge cache that stored an empty array
  metrics::Counter& _metrics_edge_cache_empty_inserts;

  std::shared_ptr<RocksDBDumpManager> _dump_manager;
};

struct DocCount {
  rocksdb::SequenceNumber committed_seq;  /// safe sequence number for recovery
  uint64_t added;                         /// number of added documents
  uint64_t removed;                       /// number of removed documents
  RevisionId revision_id;                 /// last used revision id

  DocCount()
    : committed_seq{0}, added{0}, removed{0}, revision_id{RevisionId::none()} {}

  DocCount(rocksdb::SequenceNumber sq, uint64_t added, uint64_t removed,
           RevisionId rid)
    : committed_seq(sq), added(added), removed(removed), revision_id(rid) {}

  explicit DocCount(vpack::Slice slice);
  void toVPack(vpack::Builder& b) const;
};

Result DeleteIndexEstimate(rocksdb::DB* db, uint64_t object_id);
DocCount LoadCollectionCount(rocksdb::DB* db, uint64_t object_id);
Result DeleteTableMeta(rocksdb::DB*, uint64_t object_id);

}  // namespace sdb
