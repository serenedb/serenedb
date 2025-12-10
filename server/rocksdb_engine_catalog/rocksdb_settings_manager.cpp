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

#include "rocksdb_settings_manager.h"

#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/write_batch.h>
#include <vpack/iterator.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include "app/app_server.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/random/random_generator.h"
#include "basics/string_utils.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "database/ticks.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_key.h"
#include "rocksdb_engine_catalog/rocksdb_key_bounds.h"
#include "rocksdb_engine_catalog/rocksdb_value.h"
#include "utils/exec_context.h"
#include "vpack/vpack_helper.h"

#ifdef SDB_CLUSTER
#include "rocksdb_engine/rocksdb_collection.h"
#endif

namespace sdb {
namespace {
void BuildSettings(sdb::StorageEngine& engine, vpack::Builder& b,
                   uint64_t seq_number) {
  b.clear();
  b.openObject();
  b.add("tick", std::to_string(GetCurrentTickServer()));
  b.add("hlc", std::to_string(NewTickHybridLogicalClock()));
  b.add("releasedTick", std::to_string(engine.releasedTick()));
  b.add("lastSync", std::to_string(seq_number));
  b.close();
}

Result WriteSettings(vpack::Slice slice, rocksdb::WriteBatch& batch) {
  SDB_DEBUG("xxxxx", Logger::ENGINES, "writing settings: ", slice.toJson());

  RocksDBKeyWithBuffer key;
  key.constructSettingsValue(RocksDBSettingsType::ServerTick);
  rocksdb::Slice value(slice.startAs<char>(), slice.byteSize());

  rocksdb::Status s =
    batch.Put(RocksDBColumnFamilyManager::get(
                RocksDBColumnFamilyManager::Family::Definitions),
              key.string(), value);
  if (!s.ok()) {
    SDB_WARN("xxxxx", Logger::ENGINES,
             "writing settings failed: ", s.ToString());
    return rocksutils::ConvertStatus(s);
  }

  return {};
}
}  // namespace

/// Constructor needs to be called synchrunously,
/// will load counts from the db and scan the WAL
RocksDBSettingsManager::RocksDBSettingsManager(RocksDBEngineCatalog& engine)
  : _engine(engine),
    _last_sync(0),
    _db(engine.db()->GetRootDB()),
    _initial_released_tick(0) {}

/// retrieve initial values from the database
void RocksDBSettingsManager::retrieveInitialValues() {
  loadSettings();
  _engine.releaseTick(_initial_released_tick);
}

// Thread-Safe force sync.
ResultOr<bool> RocksDBSettingsManager::sync(bool force) {
  std::unique_lock lock{_syncing_mutex, std::defer_lock};

  if (force) {
    lock.lock();
  } else if (!lock.try_lock()) {
    // if we can't get the lock, we need to exit here without getting
    // any work done. callers can use the force flag to indicate work
    // *must* be performed.
    return false;
  }

  SDB_ASSERT(lock.owns_lock());

  SDB_IF_FAILURE("RocksDBSettingsManagerSync") { return false; }

  try {
    // fetch the seq number prior to any writes; this guarantees that we save
    // any subsequent updates in the WAL to replay if we crash in the middle
    const auto max_seq_nr = _db->GetLatestSequenceNumber();
    auto min_seq_nr = max_seq_nr;
    SDB_ASSERT(min_seq_nr > 0);

    rocksdb::WriteOptions wo;
    rocksdb::WriteBatch batch;
    _tmp_builder.clear();  // recycle our builder

    bool did_work = false;

    // reserve a bit of scratch space to work with.
    // note: the scratch buffer is recycled, so we can start
    // small here. it will grow as needed.
    constexpr size_t kScratchBufferSize = 128 * 1024;
    _scratch.reserve(kScratchBufferSize);

#ifdef SDB_CLUSTER
    auto& catalog =
      SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
    for (auto& physical : catalog.Physical().GetTableShards()) {
      if (physical->deleted()) {
        continue;
      }

      auto coll =
        catalog.Local().GetObject<catalog::Table>(physical->GetMeta().id);
      if (!coll) {
        continue;
      }

      SDB_TRACE("xxxxx", Logger::ENGINES, "syncing metadata for collection '",
                physical->GetMeta().id, "', maxSeqNr: ", max_seq_nr);

      // clear our scratch buffers for this round
      _scratch.clear();
      _tmp_builder.clear();
      batch.Clear();

      rocksdb::SequenceNumber applied_seq = max_seq_nr;

      auto* rcoll = basics::downCast<RocksDBCollection>(physical.get());
      Result res = rcoll->meta().serializeMeta(
        batch, *rcoll, *coll, force, _tmp_builder, applied_seq, _scratch);

      if (res.ok() && batch.Count() > 0) {
        did_work = true;

        auto s = _db->Write(wo, &batch);
        if (!s.ok()) {
          res = rocksutils::ConvertStatus(s);
        }
      }

      if (!res.ok()) {
        SDB_WARN("xxxxx", Logger::ENGINES,
                 "could not sync metadata for collection '", coll->GetId(),
                 "'");
        return std::unexpected{std::move(res)};
      }

      // always log in TRACE mode
      SDB_TRACE("xxxxx", Logger::ENGINES, "synced metadata for collection '",
                coll->GetId(), "', maxSeqNr: ", max_seq_nr,
                ", minSeqNr: ", min_seq_nr, ", appliedSeq: ", applied_seq);

      if (applied_seq < min_seq_nr) {
        // in DEBUG mode only log _relevant_ information
        SDB_DEBUG("xxxxx", Logger::ENGINES, "synced metadata for collection '",
                  coll->GetId(), "', maxSeqNr: ", max_seq_nr,
                  ", minSeqNr: ", min_seq_nr, ", appliedSeq: ", applied_seq);
      }

      min_seq_nr = std::min(min_seq_nr, applied_seq);
    }
#endif

    if (_scratch.capacity() >= 32 * 1024 * 1024) {
      // much data in _scratch, let's shrink it to save memory
      SDB_ASSERT(kScratchBufferSize < 32 * 1024 * 1024);
      _scratch.resize(kScratchBufferSize);
      _scratch.shrink_to_fit();
    }
    _scratch.clear();
    SDB_ASSERT(_scratch.empty());

    const auto last_sync = _last_sync.load();

    SDB_TRACE("xxxxx", Logger::ENGINES,
              "about to store lastSync. previous value: ", last_sync,
              ", current value: ", min_seq_nr);

    if (min_seq_nr < last_sync && !force) {
      if (min_seq_nr != 0) {
        SDB_ERROR("xxxxx", Logger::ENGINES,
                  "min tick is smaller than "
                  "safe delete tick (minSeqNr: ",
                  min_seq_nr, ") < (lastSync = ", last_sync, ")");
        SDB_ASSERT(false);
      }
      return false;  // do not move backwards in time
    }

    SDB_ASSERT(last_sync <= min_seq_nr);
    if (!did_work && !force) {
      SDB_TRACE("xxxxx", Logger::ENGINES,
                "no collection data to serialize, updating lastSync to ",
                min_seq_nr);
      _last_sync.store(min_seq_nr);
      return false;  // nothing was written
    }

    SDB_IF_FAILURE("TransactionChaos::randomSleep") {
      std::this_thread::sleep_for(
        std::chrono::milliseconds(random::Interval(uint32_t(2000))));
    }

    // prepare new settings to be written out to disk
    batch.Clear();
    _tmp_builder.clear();
    auto new_last_sync = std::max(_last_sync.load(), min_seq_nr);
    BuildSettings(_engine, _tmp_builder, new_last_sync);

    SDB_ASSERT(_tmp_builder.slice().isObject());

    SDB_ASSERT(batch.Count() == 0);
    auto r = WriteSettings(_tmp_builder.slice(), batch);
    if (r.fail()) {
      SDB_WARN("xxxxx", Logger::ENGINES, "could not write metadata settings ",
               r.errorMessage());
      return std::unexpected{std::move(r)};
    }

    SDB_ASSERT(did_work || force);

    // make sure everything is synced properly when we are done
    SDB_ASSERT(batch.Count() == 1);
    wo.sync = true;
    auto s = _db->Write(wo, &batch);
    if (!s.ok()) {
      return std::unexpected{rocksutils::ConvertStatus(s)};
    }

    SDB_TRACE("xxxxx", Logger::ENGINES, "updating lastSync to ", new_last_sync);
    _last_sync.store(new_last_sync);

    // we have written the settings!
    return true;
  } catch (const basics::Exception& ex) {
    return std::unexpected<Result>(std::in_place, ex.code(), ex.what());
  } catch (const std::exception& ex) {
    return std::unexpected<Result>(std::in_place, ERROR_INTERNAL, ex.what());
  }
}

void RocksDBSettingsManager::loadSettings() {
  RocksDBKeyWithBuffer key;
  key.constructSettingsValue(RocksDBSettingsType::ServerTick);

  rocksdb::PinnableSlice result;
  rocksdb::Status status =
    _db->Get({},
             RocksDBColumnFamilyManager::get(
               RocksDBColumnFamilyManager::Family::Definitions),
             key.string(), &result);
  if (status.ok()) {
    // key may not be there, so don't fail when not found
    vpack::Slice slice =
      vpack::Slice(reinterpret_cast<const uint8_t*>(result.data()));
    SDB_ASSERT(slice.isObject());
    SDB_TRACE("xxxxx", Logger::ENGINES,
              "read initial settings: ", slice.toJson());

    if (!result.empty()) {
      try {
        if (slice.hasKey("tick")) {
          uint64_t last_tick =
            basics::VPackHelper::stringUInt64(slice.get("tick"));
          SDB_TRACE("xxxxx", Logger::ENGINES, "using last tick: ", last_tick);
          UpdateTickServer(last_tick);
        }

        if (slice.hasKey("hlc")) {
          uint64_t last_hlc =
            basics::VPackHelper::stringUInt64(slice.get("hlc"));
          SDB_TRACE("xxxxx", Logger::ENGINES, "using last hlc: ", last_hlc);
          NewTickHybridLogicalClock(last_hlc);
        }

        if (slice.hasKey("releasedTick")) {
          _initial_released_tick =
            basics::VPackHelper::stringUInt64(slice.get("releasedTick"));
          SDB_TRACE("xxxxx", Logger::ENGINES,
                    "using released tick: ", _initial_released_tick);
          _engine.releaseTick(_initial_released_tick);
        }

        if (slice.hasKey("lastSync")) {
          _last_sync = basics::VPackHelper::stringUInt64(slice.get("lastSync"));
          SDB_TRACE("xxxxx", Logger::ENGINES,
                    "last background settings sync: ", _last_sync.load());
        }
      } catch (...) {
        SDB_WARN("xxxxx", Logger::ENGINES,
                 "unable to read initial settings: invalid data");
      }
    } else {
      SDB_TRACE("xxxxx", Logger::ENGINES, "no initial settings found");
    }
  }
}

/// earliest safe sequence number to throw away from wal
rocksdb::SequenceNumber RocksDBSettingsManager::earliestSeqNeeded() const {
  return _last_sync.load();
}

}  // namespace sdb
