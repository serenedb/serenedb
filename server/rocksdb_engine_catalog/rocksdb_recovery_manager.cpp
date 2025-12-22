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

#include "rocksdb_recovery_manager.h"

#include <absl/cleanup/cleanup.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/write_batch.h>
#include <vpack/iterator.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <atomic>

#include "app/app_server.h"
#include "basics/application-exit.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/exitcodes.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/number_utils.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/key_generator.h"
#include "catalog/table.h"
#include "database/ticks.h"
#include "general_server/scheduler_feature.h"
#include "general_server/state.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_key.h"
#include "rocksdb_engine_catalog/rocksdb_key_bounds.h"
#include "rocksdb_engine_catalog/rocksdb_log_value.h"
#include "rocksdb_engine_catalog/rocksdb_recovery_helper.h"
#include "rocksdb_engine_catalog/rocksdb_settings_manager.h"
#include "rocksdb_engine_catalog/rocksdb_value.h"
#include "storage_engine/engine_feature.h"
#include "vpack/vpack_helper.h"

#ifdef SDB_CLUSTER
#include "replication/replication_feature.h"
#include "rocksdb_engine/rocksdb_collection.h"
#include "rocksdb_engine/rocksdb_edge_index.h"
#include "rocksdb_engine/rocksdb_vpack_index.h"
#endif

namespace sdb {

RocksDBRecoveryManager::RocksDBRecoveryManager(Server& server)
  : SerenedFeature{server, name()},
    _current_sequence_number(0),
    _recovery_state(RecoveryState::Before) {
  setOptional(true);
}

void RocksDBRecoveryManager::prepare() {
  if (ServerState::instance()->IsCoordinator()) {
    disable();
  }
}

void RocksDBRecoveryManager::start() {
  SDB_ASSERT(isEnabled());

  // synchronizes with acquire inRecovery()
  _recovery_state.store(RecoveryState::InProgress, std::memory_order_release);

  // start recovery
  runRecovery();

  // synchronizes with acquire inRecovery()
  _recovery_state.store(RecoveryState::Done, std::memory_order_release);

  // notify everyone that recovery is now done
  recoveryDone();
}

void RocksDBRecoveryManager::runRecovery() {
  auto res = parseRocksWAL();
  if (res.fail()) {
    SDB_FATAL_EXIT_CODE(
      "xxxxx", Logger::ENGINES, EXIT_RECOVERY,
      "failed during rocksdb WAL recovery: ", res.errorMessage());
  }

  // now restore collection counts into collections
}

RecoveryState RocksDBRecoveryManager::recoveryState() const noexcept {
  return _recovery_state.load(std::memory_order_acquire);
}

rocksdb::SequenceNumber RocksDBRecoveryManager::recoverySequenceNumber()
  const noexcept {
  return _current_sequence_number.load(std::memory_order_relaxed);
}

class WBReader final : public rocksdb::WriteBatch::Handler {
 private:
  WBReader(const WBReader&) = delete;
  const WBReader& operator=(const WBReader&) = delete;

  struct ProgressState {
    // sequence number from which we start recovering
    const rocksdb::SequenceNumber recovery_start_sequence;
    // latest sequence in WAL
    const rocksdb::SequenceNumber latest_sequence;

    // informational section, used only for progress reporting
    rocksdb::SequenceNumber sequence_range = 0;
    rocksdb::SequenceNumber range_begin = 0;
    int report_ticker = 0;
    int progress_value = 0;
  } _progress_state;

  // minimum server tick we are going to accept (initialized to
  // NewTickServer())
  const uint64_t _minimum_server_tick;
  // max tick value found in WAL
  uint64_t _max_tick_found;
  // max HLC value found in WAL
  uint64_t _max_hlc_found;
  // number of WAL entries scanned
  uint64_t _entries_scanned;
  // start of batch sequence number (currently only set, but not read back -
  // can be used for debugging later)
  rocksdb::SequenceNumber _batch_start_sequence;
  // current sequence number
  std::atomic<rocksdb::SequenceNumber>& _current_sequence;

  RocksDBEngineCatalog& _engine;
  // whether we are currently at the start of a batch
  bool _start_of_batch = false;

 public:
  /// seqs sequence number from which to count operations
  explicit WBReader(rocksdb::SequenceNumber recovery_start_sequence,
                    rocksdb::SequenceNumber latest_sequence,
                    std::atomic<rocksdb::SequenceNumber>& current_sequence)
    : _progress_state{recovery_start_sequence, latest_sequence},
      _minimum_server_tick(NewTickServer()),
      _max_tick_found(0),
      _max_hlc_found(0),
      _entries_scanned(0),
      _batch_start_sequence(0),
      _current_sequence(current_sequence),
      _engine(GetServerEngine()) {}

  void StartNewBatch(rocksdb::SequenceNumber start_sequence) {
    SDB_ASSERT(_current_sequence <= start_sequence);

    if (_batch_start_sequence == 0) {
      // for the first call, initialize the [from - to] recovery range values
      _progress_state.range_begin = start_sequence;
      _progress_state.sequence_range =
        _progress_state.latest_sequence - _progress_state.range_begin;
    }

    // progress reporting. only do this every 100 iterations to avoid the
    // overhead of the calculations for every new sequence number
    if (_progress_state.sequence_range > 0 &&
        ++_progress_state.report_ticker >= 100) {
      _progress_state.report_ticker = 0;

      auto progress = static_cast<int>(
        100.0 * (start_sequence - _progress_state.range_begin) /
        _progress_state.sequence_range);

      // report only every 5%, so that we don't flood the log with micro
      // progress
      if (progress >= 5 && progress >= _progress_state.progress_value + 5) {
        SDB_INFO("xxxxx", Logger::ENGINES, "Recovering from sequence number ",
                 start_sequence, " (", progress, "% of WAL)...");

        _progress_state.progress_value = progress;
      }
    }

    // starting new write batch
    _batch_start_sequence = start_sequence;
    _current_sequence = start_sequence;
    _start_of_batch = true;
  }

  Result ShutdownWbReader() {
    Result rv = basics::SafeCall([&] {
      if (_engine.dbExisted()) {
        SDB_INFO("xxxxx", Logger::ENGINES, "RocksDB recovery finished, ",
                 "WAL entries scanned: ", _entries_scanned,
                 ", recovery start sequence number: ",
                 _progress_state.recovery_start_sequence,
                 ", latest WAL sequence number: ",
                 _engine.db()->GetLatestSequenceNumber(),
                 ", max tick value found in WAL: ", _max_tick_found,
                 ", last HLC value found in WAL: ", _max_hlc_found);
      }

      // update ticks after parsing wal
      UpdateTickServer(_max_tick_found);

      NewTickHybridLogicalClock(_max_hlc_found);
    });
    return rv;
  }

 private:
  void StoreMaxHlc(uint64_t hlc) {
    if (hlc > _max_hlc_found) {
      _max_hlc_found = hlc;
    }
  }

  void StoreMaxTick(uint64_t tick) {
    if (tick > _max_tick_found) {
      _max_tick_found = tick;
    }
  }

#ifdef SDB_CLUSTER
  std::shared_ptr<RocksDBCollection> FindCollection(uint64_t object_id) {
    auto physical = GetTableShard(ObjectId{object_id});
    if (!physical) {
      return {};
    }

    return basics::downCast<RocksDBCollection>(physical);
  }

  RocksDBIndex* FindIndex(uint64_t object_id) {
    auto triple = _engine.mapObjectToIndex(object_id);
    if (!std::get<1>(triple).isSet()) {
      return nullptr;
    }

    auto coll = GetTableShard(std::get<1>(triple));
    if (coll == nullptr) {
      return nullptr;
    }

    auto index = coll->lookupIndex(std::get<2>(triple));
    if (index == nullptr) {
      return nullptr;
    }
    return static_cast<RocksDBIndex*>(index.get());
  }
#endif

  void UpdateMaxTick(uint32_t column_family_id, const rocksdb::Slice& key,
                     const rocksdb::Slice& value) {
    // RETURN (side-effect): update _max_tick_found
    //
    // extract max tick from Markers and store them as side-effect in
    // _max_tick_found member variable that can be used later (dtor) to call
    // SdbUpdateTickServer (ticks.h)
    // Markers: - collections (id,objectid) as tick and max tick in indexes
    // array
    //          - documents - _rev (revision as maxtick)
    //          - databases

    if (column_family_id == RocksDBColumnFamilyManager::get(
                              RocksDBColumnFamilyManager::Family::Documents)
                              ->GetID()) {
      StoreMaxHlc(RocksDBKey::documentId(key).id());
    } else if (column_family_id ==
               RocksDBColumnFamilyManager::get(
                 RocksDBColumnFamilyManager::Family::PrimaryIndex)
                 ->GetID()) {
      // document key
      std::string_view ref = RocksDBKey::primaryKey(key);
      SDB_ASSERT(!ref.empty());
      if (ref.empty()) {
        return;
      }
      // check if the key is numeric
      if (ref[0] >= '1' && ref[0] <= '9') {
        // numeric start byte. looks good
        bool valid;
        uint64_t tick = number_utils::Atoi<uint64_t>(
          ref.data(), ref.data() + ref.size(), valid);
        if (valid && tick > _max_tick_found) {
          uint64_t compare_tick = _max_tick_found;
          if (compare_tick == 0) {
            compare_tick = _minimum_server_tick;
          }
          // if no previous _max_tick_found set or the numeric value found is
          // "near" our previous _max_tick_found, then we update it
          if (tick > compare_tick && (tick - compare_tick) < 2048) {
            StoreMaxTick(tick);
          }
        }
        // else we got a non-numeric key. simply ignore it
      }

#ifdef SDB_CLUSTER
      auto idx = FindIndex(RocksDBKey::objectId(key));
      if (idx) {
        auto& catalog =
          SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
        auto c = catalog.Local().GetSnapshot()->GetObject<catalog::Table>(
          idx->GetMeta().id);
        SDB_ENSURE(c, ERROR_INTERNAL);

        c->keyGenerator().track(ref);
      }
#endif

    } else if (column_family_id ==
               RocksDBColumnFamilyManager::get(
                 RocksDBColumnFamilyManager::Family::Definitions)
                 ->GetID()) {
      const auto type = RocksDBKey::type(key);

      // TODO(gnusi): using tick from RocksDBKey::dataSourceId(key)
      // isn't valid on cluster as we now use cluster global identifiers
      // instead of server ticks
      auto update_tick = [is_single =
                            ServerState::instance()->IsSingle()](auto&& f) {
        if (is_single) {
          f();
        }
      };

      if (type == RocksDBEntryType::Collection) {
        update_tick([&] { StoreMaxTick(RocksDBKey::dataSourceId(key).id()); });
        auto slice = RocksDBValue::data(value);
        vpack::Slice indexes = slice.get("indexes");
        for (vpack::Slice idx : vpack::ArrayIterator(indexes)) {
          StoreMaxTick(std::max(
            basics::VPackHelper::stringUInt64(idx, StaticStrings::kObjectId),
            basics::VPackHelper::stringUInt64(idx, StaticStrings::kIndexId)));
        }
      } else if (type == RocksDBEntryType::Database) {
        StoreMaxTick(RocksDBKey::databaseId(key));
      } else if (type == RocksDBEntryType::Function ||
                 type == RocksDBEntryType::View) {
        update_tick([&] {
          StoreMaxTick(std::max(RocksDBKey::databaseId(key),
                                RocksDBKey::dataSourceId(key).id()));
        });
      }
    }
  }

  // tick function that is called before each new WAL entry
  void IncTick() {
    if (_start_of_batch) {
      // we are at the start of a batch. do NOT increase sequence number
      _start_of_batch = false;
    } else {
      // we are inside a batch already. now increase sequence number
      _current_sequence.fetch_add(1, std::memory_order_relaxed);
    }
  }

 public:
  rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice& key,
                        const rocksdb::Slice& value) override {
    ++_entries_scanned;

    IncTick();

    UpdateMaxTick(column_family_id, key, value);
    if (column_family_id == RocksDBColumnFamilyManager::get(
                              RocksDBColumnFamilyManager::Family::Documents)
                              ->GetID()) {
#ifdef SDB_CLUSTER
      if (auto coll = FindCollection(RocksDBKey::objectId(key)); coll) {
        auto revision_id = RocksDBKey::documentId(key);
        coll->meta().adjustNumberDocumentsInRecovery(_current_sequence,
                                                     revision_id, 1);

        std::vector<uint64_t> inserts;
        std::vector<uint64_t> removes;
        inserts.emplace_back(revision_id.id());
        coll->bufferUpdates(_current_sequence, std::move(inserts),
                            std::move(removes));
      }
#endif
    } else {
#ifdef SDB_CLUSTER
      // We have to adjust the estimate with an insert
      uint64_t hashval = 0;
      if (column_family_id == RocksDBColumnFamilyManager::get(
                                RocksDBColumnFamilyManager::Family::VPackIndex)
                                ->GetID()) {
        hashval = RocksDBVPackIndex::HashForKey(key);
      } else if (column_family_id ==
                 RocksDBColumnFamilyManager::get(
                   RocksDBColumnFamilyManager::Family::EdgeIndex)
                   ->GetID()) {
        hashval = RocksDBEdgeIndex::HashForKey(key);
      }

      if (hashval != 0) {
        auto* idx = FindIndex(RocksDBKey::objectId(key));
        if (idx) {
          auto* est = idx->estimator();
          if (est && est->appliedSeq() < _current_sequence) {
            // We track estimates for this index
            est->Insert(hashval);
          }
        }
      }
#endif
    }

    for (auto helper : _engine.recoveryHelpers()) {
      helper->PutCF(column_family_id, key, value, _current_sequence);
    }

    return rocksdb::Status();
  }

  void HandleDeleteCf(uint32_t cf_id, const rocksdb::Slice& key) {
    IncTick();

    if (cf_id == RocksDBColumnFamilyManager::get(
                   RocksDBColumnFamilyManager::Family::Documents)
                   ->GetID()) {
      uint64_t object_id = RocksDBKey::objectId(key);
      auto revision_id = RocksDBKey::documentId(key);

      StoreMaxHlc(revision_id.id());
      StoreMaxTick(object_id);

#ifdef SDB_CLUSTER
      if (auto coll = FindCollection(RocksDBKey::objectId(key)); coll) {
        coll->meta().adjustNumberDocumentsInRecovery(_current_sequence,
                                                     revision_id, -1);

        std::vector<uint64_t> inserts;
        std::vector<uint64_t> removes;
        removes.emplace_back(revision_id.id());
        coll->bufferUpdates(_current_sequence, std::move(inserts),
                            std::move(removes));
      }
#endif
    } else {
#ifdef SDB_CLUSTER
      // We have to adjust the estimate with an insert
      uint64_t hashval = 0;
      if (cf_id == RocksDBColumnFamilyManager::get(
                     RocksDBColumnFamilyManager::Family::VPackIndex)
                     ->GetID()) {
        hashval = RocksDBVPackIndex::HashForKey(key);
      } else if (cf_id == RocksDBColumnFamilyManager::get(
                            RocksDBColumnFamilyManager::Family::EdgeIndex)
                            ->GetID()) {
        hashval = RocksDBEdgeIndex::HashForKey(key);
      }

      if (hashval != 0) {
        auto* idx = FindIndex(RocksDBKey::objectId(key));
        if (idx) {
          RocksDBCuckooIndexEstimatorType* est = idx->estimator();
          if (est && est->appliedSeq() < _current_sequence) {
            // We track estimates for this index
            est->Remove(hashval);
          }
        }
      }
#endif
    }
  }

  rocksdb::Status DeleteCF(uint32_t column_family_id,
                           const rocksdb::Slice& key) override {
    ++_entries_scanned;

    HandleDeleteCf(column_family_id, key);
    for (auto helper : _engine.recoveryHelpers()) {
      helper->DeleteCF(column_family_id, key, _current_sequence);
    }

    return rocksdb::Status();
  }

  rocksdb::Status SingleDeleteCF(uint32_t column_family_id,
                                 const rocksdb::Slice& key) override {
    ++_entries_scanned;

    HandleDeleteCf(column_family_id, key);
    for (auto helper : _engine.recoveryHelpers()) {
      helper->SingleDeleteCF(column_family_id, key, _current_sequence);
    }

    return rocksdb::Status();
  }

  rocksdb::Status DeleteRangeCF(uint32_t column_family_id,
                                const rocksdb::Slice& begin_key,
                                const rocksdb::Slice& end_key) override {
    ++_entries_scanned;

    IncTick();
    // drop and truncate can use this, truncate is handled via a Log marker
    for (auto helper : _engine.recoveryHelpers()) {
      helper->DeleteRangeCF(column_family_id, begin_key, end_key,
                            _current_sequence);
    }

    // check for a range-delete of the primary index
    if (column_family_id == RocksDBColumnFamilyManager::get(
                              RocksDBColumnFamilyManager::Family::Documents)
                              ->GetID()) {
      uint64_t object_id = RocksDBKey::objectId(begin_key);
      SDB_ASSERT(object_id == RocksDBKey::objectId(end_key));

#ifdef SDB_CLUSTER
      auto coll = FindCollection(object_id);
      if (!coll) {
        return rocksdb::Status();
      }

      uint64_t current_count = coll->meta().numberDocuments();
      if (current_count != 0) {
        coll->meta().adjustNumberDocumentsInRecovery(
          _current_sequence, RevisionId::none(),
          -static_cast<int64_t>(current_count));
      }
      for (const auto& idx : coll->getReadyIndexes()) {
        RocksDBIndex* ridx = static_cast<RocksDBIndex*>(idx.get());
        RocksDBCuckooIndexEstimatorType* est = ridx->estimator();
        SDB_ASSERT(ridx->type() != kTypeEdgeIndex || est);
        if (est) {
          est->ClearInRecovery(_current_sequence);
        }
      }
      std::ignore = coll->bufferTruncate(_current_sequence);
#endif
    }

    return rocksdb::Status();  // make WAL iterator happy
  }

  void LogData(const rocksdb::Slice& blob) override {
    ++_entries_scanned;
    for (auto helper : _engine.recoveryHelpers()) {
      helper->LogData(blob, _current_sequence);
    }
  }

  rocksdb::Status MarkBeginPrepare(bool = false) override {
    SDB_ASSERT(false);
    return rocksdb::Status::InvalidArgument(
      "MarkBeginPrepare() handler not defined.");
  }

  rocksdb::Status MarkEndPrepare(const rocksdb::Slice& /*xid*/) override {
    SDB_ASSERT(false);
    return rocksdb::Status::InvalidArgument(
      "MarkEndPrepare() handler not defined.");
  }

  rocksdb::Status MarkNoop(bool /*empty_batch*/) override {
    return rocksdb::Status::OK();
  }

  rocksdb::Status MarkRollback(const rocksdb::Slice& /*xid*/) override {
    SDB_ASSERT(false);
    return rocksdb::Status::InvalidArgument(
      "MarkRollbackPrepare() handler not defined.");
  }

  rocksdb::Status MarkCommit(const rocksdb::Slice& /*xid*/) override {
    SDB_ASSERT(false);
    return rocksdb::Status::InvalidArgument(
      "MarkCommit() handler not defined.");
  }

  // MergeCF is not used
};

Result RocksDBRecoveryManager::parseRocksWAL() {
  Result shutdown_rv;

  Result res = basics::SafeCall([&] -> Result {
    auto& engine = GetServerEngine();

    auto db = engine.db();

    Result rv;
    for (auto& helper : engine.recoveryHelpers()) {
      helper->prepare();
    }
    absl::Cleanup helpers_cleanup = [&]() noexcept {
      for (auto& helper : engine.recoveryHelpers()) {
        helper->unprepare();
      }
    };

    rocksdb::SequenceNumber earliest =
      engine.settingsManager()->earliestSeqNeeded();
    auto recovery_start_sequence = std::min(earliest, engine.releasedTick());

#ifdef SDB_GTEST
    engine.recoveryStartSequence(recovery_start_sequence);
#endif

    auto latest_sequence_number = db->GetLatestSequenceNumber();

    if (engine.dbExisted()) {
      size_t files_active = 0;
      size_t files_in_archive = 0;
      try {
        // number of active log files
        std::string active = db->GetOptions().wal_dir;
        files_active = SdbFilesDirectory(active.c_str()).size();

        // number of log files in the archive
        std::string archive = basics::file_utils::BuildFilename(
          db->GetOptions().wal_dir, "archive");
        files_in_archive = SdbFilesDirectory(archive.c_str()).size();
      } catch (...) {
        // don't ever fail recovery because we can't get list of files in
        // archive
      }

      SDB_INFO("xxxxx", Logger::ENGINES,
               "RocksDB recovery starting, scanning WAL starting from sequence "
               "number ",
               recovery_start_sequence,
               ", latest sequence number: ", latest_sequence_number,
               ", active log files: ", files_active,
               ", files in archive: ", files_in_archive);
    }

    // Tell the WriteBatch reader the transaction markers to look for
    SDB_ASSERT(_current_sequence_number == 0);
    WBReader handler(recovery_start_sequence, latest_sequence_number,
                     _current_sequence_number);

    // prevent purging of WAL files while we are in here
    RocksDBFilePurgePreventer purge_preventer(engine.disallowPurging());

    std::unique_ptr<rocksdb::TransactionLogIterator> iterator;
    rocksdb::Status s =
      db->GetUpdatesSince(recovery_start_sequence, &iterator,
                          rocksdb::TransactionLogIterator::ReadOptions(true));

    rv = rocksutils::ConvertStatus(s);

    if (rv.ok()) {
      while (iterator->Valid()) {
        s = iterator->status();
        if (s.ok()) {
          rocksdb::BatchResult batch = iterator->GetBatch();
          handler.StartNewBatch(batch.sequence);
          s = batch.writeBatchPtr->Iterate(&handler);
        }

        if (!s.ok()) {
          rv = rocksutils::ConvertStatus(s);
          auto msg = absl::StrCat("error during WAL scan: ", rv.errorMessage());
          SDB_ERROR("xxxxx", Logger::ENGINES, msg);
          rv.reset(rv.errorNumber(), std::move(msg));  // update message
          break;
        }

        iterator->Next();
      }
    }

    shutdown_rv = handler.ShutdownWbReader();

    return rv;
  });

  if (res.ok()) {
    res = std::move(shutdown_rv);
  } else {
    if (shutdown_rv.fail()) {
      res.reset(res.errorNumber(), absl::StrCat(res.errorMessage(), " - ",
                                                shutdown_rv.errorMessage()));
    }
  }

  return res;
}

void RocksDBRecoveryManager::recoveryDone() {
  SDB_ASSERT(!ServerState::instance()->IsCoordinator());
  SDB_ASSERT(!server().getFeature<EngineFeature>().engine().inRecovery());

  // '_pending_recovery_callbacks' will not change because
  // !StorageEngine.inRecovery()
  // It's single active thread before recovery done,
  // so we could use general purpose thread pool for this
  std::vector<yaclib::Future<Result>> futures;
  futures.reserve(_pending_recovery_callbacks.size());
  for (auto& entry : _pending_recovery_callbacks) {
    futures.emplace_back(SchedulerFeature::gScheduler->queueWithFuture(
      RequestLane::ClientSlow, std::move(entry)));
  }
  _pending_recovery_callbacks.clear();
  // TODO(mbkkt) use single wait with early termination
  // when it would be available
  yaclib::Wait(futures.begin(), futures.end());
  for (auto& future : futures) {
    auto r = std::move(future).Touch().Ok();
    if (!r.ok()) {
      SDB_ERROR("xxxxx", Logger::FIXME,
                "recovery failure due to error from callback, error '",
                GetErrorStr(r.errorNumber()), "' message: ", r.errorMessage());

      SDB_THROW(std::move(r));
    }
  }

#ifdef SDB_CLUSTER
  if (auto replication_feature = server().TryGetFeature<ReplicationFeature>()) {
    for (auto [_, applier] : GetAllReplicationAppliers()) {
      replication_feature->startApplier(applier->configuration().database,
                                        *applier);
    }
  }
#endif
}

Result RocksDBRecoveryManager::registerPostRecoveryCallback(
  std::function<Result()>&& callback) {
  if (!server().getFeature<EngineFeature>().engine().inRecovery()) {
    return callback();  // if no engine then can't be in recovery
  }

  // do not need a lock since single-thread access during recovery
  _pending_recovery_callbacks.emplace_back(std::move(callback));
  return {};
}

void RocksDBRecoveryManager::unprepare() {
  _pending_recovery_callbacks.clear();
}

}  // namespace sdb
