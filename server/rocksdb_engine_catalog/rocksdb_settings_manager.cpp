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

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>

#include "app/app_server.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/log.h"
#include "basics/random/random_generator.h"
#include "basics/serialization.h"
#include "basics/serializer.h"
#include "basics/string_utils.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "database/ticks.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_key.h"
#include "utils/exec_context.h"

namespace sdb {
namespace {

struct ServerTickSettings {
  uint64_t tick;
  uint64_t released_tick;
  uint64_t last_sync;
};

Result WriteSettings(const ServerTickSettings& settings,
                     rocksdb::WriteBatch& batch) {
  SDB_DEBUG(STORAGE, "writing settings: tick=", settings.tick,
            ", releasedTick=", settings.released_tick,
            ", lastSync=", settings.last_sync);

  RocksDBKeyWithBuffer<SettingsKey> key{RocksDBSettingsType::ServerTick};

  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream, duckdb::VersionStorageOptions()};
  basics::WriteTuple(serializer, settings);
  rocksdb::Slice value(reinterpret_cast<const char*>(stream.GetData()),
                       stream.GetPosition());

  rocksdb::Status s =
    batch.Put(RocksDBColumnFamilyManager::get(
                RocksDBColumnFamilyManager::Family::Definitions),
              key.GetBuffer(), value);
  if (!s.ok()) {
    SDB_WARN(STORAGE, "writing settings failed: ", s.ToString());
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

    bool did_work = false;

    const auto last_sync = _last_sync.load();

    SDB_TRACE(STORAGE, "about to store lastSync. previous value: ", last_sync,
              ", current value: ", min_seq_nr);

    if (min_seq_nr < last_sync && !force) {
      if (min_seq_nr != 0) {
        SDB_ERROR(STORAGE,
                  "min tick is smaller than "
                  "safe delete tick (minSeqNr: ",
                  min_seq_nr, ") < (lastSync = ", last_sync, ")");
        SDB_ASSERT(false);
      }
      return false;  // do not move backwards in time
    }

    SDB_ASSERT(last_sync <= min_seq_nr);
    if (!did_work && !force) {
      SDB_TRACE(STORAGE,
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
    auto new_last_sync = std::max(_last_sync.load(), min_seq_nr);
    ServerTickSettings settings{.tick = GetCurrentTickServer(),
                                .released_tick = _engine.releasedTick(),
                                .last_sync = new_last_sync};

    SDB_ASSERT(batch.Count() == 0);
    auto r = WriteSettings(settings, batch);
    if (r.fail()) {
      SDB_WARN(STORAGE, "could not write metadata settings ", r.errorMessage());
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

    SDB_TRACE(STORAGE, "updating lastSync to ", new_last_sync);
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
  RocksDBKeyWithBuffer<SettingsKey> key{RocksDBSettingsType::ServerTick};

  rocksdb::PinnableSlice result;
  rocksdb::Status status =
    _db->Get({},
             RocksDBColumnFamilyManager::get(
               RocksDBColumnFamilyManager::Family::Definitions),
             key.GetBuffer(), &result);
  if (status.ok()) {
    // key may not be there, so don't fail when not found
    if (!result.empty()) {
      try {
        duckdb::MemoryStream stream{
          const_cast<duckdb::data_t*>(
            reinterpret_cast<const duckdb::data_t*>(result.data())),
          result.size()};
        duckdb::BinaryDeserializer deserializer{stream};
        ServerTickSettings settings;
        basics::ReadTuple(deserializer, settings);

        SDB_TRACE(STORAGE, "read initial settings: tick=", settings.tick,
                  ", releasedTick=", settings.released_tick,
                  ", lastSync=", settings.last_sync);

        UpdateTickServer(settings.tick);

        _initial_released_tick = settings.released_tick;
        _engine.releaseTick(_initial_released_tick);

        _last_sync = settings.last_sync;
      } catch (...) {
        SDB_WARN(STORAGE, "unable to read initial settings: invalid data");
      }
    } else {
      SDB_TRACE(STORAGE, "no initial settings found");
    }
  }
}

/// earliest safe sequence number to throw away from wal
rocksdb::SequenceNumber RocksDBSettingsManager::earliestSeqNeeded() const {
  return _last_sync.load();
}

}  // namespace sdb
