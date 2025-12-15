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

#include "rocksdb_engine_catalog/rocksdb_wal_access.h"

#include <basics/buffer.h>
#include <rocksdb/utilities/transaction_db.h>
#include <vpack/builder.h>
#include <vpack/collection.h>
#include <vpack/slice.h>

#include <magic_enum/magic_enum.hpp>
#include <type_traits>
#include <utility>
#include <variant>

#include "app/app_server.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/function.h"
#include "catalog/identifiers/identifier.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/replication_common.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/types.h"
#include "catalog/view.h"
#include "rest_server/serened.h"
#include "rocksdb_engine_catalog/concat.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_log_value.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "vpack/serializer.h"

namespace sdb {
namespace {

constexpr ReplicationOperation ConvertDDLEntry(RocksDBLogType v) noexcept {
  switch (v) {
    case RocksDBLogType::DatabaseCreate:
      return kReplicationDatabaseCreate;
    case RocksDBLogType::DatabaseDrop:
      return kReplicationDatabaseDrop;
    case RocksDBLogType::FunctionCreate:
      return kReplicationFunctionCreate;
    case RocksDBLogType::FunctionDrop:
      return kReplicationFunctionDrop;
    case RocksDBLogType::TableCreate:
      return kReplicationTableCreate;
    case RocksDBLogType::TableDrop:
      return kReplicationTableDrop;
    case RocksDBLogType::TableRename:
      return kReplicationTableRename;
    case RocksDBLogType::TableChange:
      return kReplicationTableChange;
    case RocksDBLogType::TableTruncate:
      return kReplicationTableTruncate;
    case RocksDBLogType::IndexCreate:
      return kReplicationIndexCreate;
    case RocksDBLogType::IndexDrop:
      return kReplicationIndexDrop;
    case RocksDBLogType::ViewCreate:
      return kReplicationViewCreate;
    case RocksDBLogType::ViewDrop:
      return kReplicationViewDrop;
    case RocksDBLogType::ViewChange:
      return kReplicationViewChange;
    case RocksDBLogType::RoleCreate:
      return kReplicationRoleCreate;
    case RocksDBLogType::RoleDrop:
      return kReplicationRoleDrop;
    case RocksDBLogType::RoleChange:
      return kReplicationRoleChange;
    case RocksDBLogType::BeginTransaction:
      return kReplicationTransactionStart;
    case RocksDBLogType::CommitTransaction:
      return kReplicationTransactionCommit;
    default:
      SDB_ASSERT(false);
      return kReplicationInvalid;
  }
}

// NOLINTBEGIN
struct Marker {
  std::string_view db;
  std::string_view cuid;
  Identifier tick;
  std::underlying_type_t<ReplicationOperation> type = 0;
  std::variant<std::monostate, vpack::Slice, std::string_view> data;
};
// NOLINTEND

}  // namespace

RocksDBWalAccess::RocksDBWalAccess(RocksDBEngineCatalog& engine)
  : _engine(engine) {}

/// {"tickMin":"123", "tickMax":"456", "version":"3.2", "serverId":"abc"}
Result RocksDBWalAccess::tickRange(std::pair<Tick, Tick>& min_max) const {
  rocksdb::TransactionDB* tdb = _engine.db();
  rocksdb::VectorLogPtr wal_files;
  rocksdb::Status s = tdb->GetSortedWalFiles(wal_files);
  if (!s.ok()) {
    return rocksutils::ConvertStatus(s);
  }

  if (wal_files.size() > 0) {
    min_max.first = wal_files.front()->StartSequence();
  }
  min_max.second = tdb->GetLatestSequenceNumber();
  return {};
}

/// {"lastTick":"123",
///  "version":"3.2",
///  "serverId":"abc",
///  "clients": {
///    "serverId": "ass", "lastTick":"123", ...
///  }}
///
Tick RocksDBWalAccess::lastTick() const {
  std::ignore = _engine.flushWal(false, false);
  return _engine.db()->GetLatestSequenceNumber();
}

/// WAL parser. Premise of this code is that transactions
/// can potentially be batched into the same rocksdb write batch
/// but transactions can never be interleaved with operations
/// outside of the transaction
class WALDumper final : public rocksdb::WriteBatch::Handler,
                        public WalAccessContext {
  // internal WAL parser states
  enum State : char {
    kInvalid,

    kDbCreate,
    kDbDrop,

    kFunctionCreate,
    kFunctionDrop,

    kTableCreate,
    kTableRename,
    kTableChange,

    kViewCreate,
    kViewChange,

    kTransaction,
    kSingleOperation,
  };

 public:
  WALDumper(RocksDBEngineCatalog& engine, const WalAccess::Filter& filter,
            const WalAccess::MarkerCallback& f, size_t max_response_size)
    : WalAccessContext{filter, f},
      _engine{engine},
      _definitions_cf{RocksDBColumnFamilyManager::get(
                        RocksDBColumnFamilyManager::Family::Definitions)
                        ->GetID()},
      _documents_cf{RocksDBColumnFamilyManager::get(
                      RocksDBColumnFamilyManager::Family::Documents)
                      ->GetID()},
      _primary_cf{RocksDBColumnFamilyManager::get(
                    RocksDBColumnFamilyManager::Family::PrimaryIndex)
                    ->GetID()},
      _max_response_size{max_response_size} {}

  bool Continue() override {
    if (_stop_on_next) {
      return false;
    }

    if (response_size > _max_response_size) {
      // it should only be possible to be in the middle of a huge batch,
      // if and only if we are in one big transaction. We may not stop
      if (_state == kTransaction) {
        // this will make us process one more marker still
        _stop_on_next = true;
      }
    }
    return true;
  }

  void LogData(const rocksdb::Slice& blob) override {
    // TODO(gnusi): all these LoadDatabase/LoadCollection calls are wrong,
    // we have to store in WAL names instead of ids as actual objects might be
    // already deleted

#ifdef SDB_DEV
    _check_tick = true;
#endif
    // rocksdb does not count LogData towards sequence-number
    const auto type = wal::GetType(blob);
    const auto payload = blob.ToStringView().substr(1);

    auto handle_schema_object = [&](auto state, auto&& filter) {
      ResetTransientState();  // finish ongoing trx

      const auto [database_id, _, object_id, slice] =
        rocksutils::Read<wal::SchemaObjectPut>(payload);

      if (filter(database_id, object_id)) {
        _state = state;

        if (auto* database = LoadDatabase(database_id); database) {
          SDB_ASSERT(slice.get(StaticStrings::kDataSourceName).isString());

          Marker marker{
            .db = database->GetName(),
            .cuid = slice.get(StaticStrings::kDataSourceName).stringView(),
            .tick = Identifier{_current_sequence},
            .type = ConvertDDLEntry(type),
            .data = slice,
          };
          SDB_ASSERT(!marker.cuid.empty());
          vpack::WriteObject(builder, marker);

          PrintMarker();
        }
      }
    };

    auto handle_schema_object_drop = [&](auto&& filter) {
      ResetTransientState();  // finish ongoing trx

      const auto [database_id, _, object_id, uuid] =
        rocksutils::Read<wal::SchemaObjectDrop>(payload);

      if (filter(database_id, object_id)) {
        if (auto* database = LoadDatabase(database_id); database) {
          const Marker marker{
            .db = database->GetName(),
            .cuid = uuid,
            .tick = Identifier{_current_sequence + (_start_of_batch ? 0 : 1)},
            .type = ConvertDDLEntry(type),
          };
          SDB_ASSERT(!marker.cuid.empty());
          vpack::WriteObject(builder, marker);

          PrintMarker();
        }
      }
    };

    switch (type) {
      case RocksDBLogType::DatabaseCreate:
        ResetTransientState();  // finish ongoing trx
        if (const auto entry = rocksutils::Read<wal::Database>(payload);
            shouldHandleDB(entry.database_id)) {
          catalog::DatabaseOptions options;
          auto r = vpack::ReadTupleNothrow(entry.data, options);
          SDB_ENSURE(r.ok(), ERROR_SERVER_CORRUPTED_DATAFILE);

          {
            vpack::ObjectBuilder marker(&builder, true);
            marker->add("tick", std::to_string(_current_sequence));
            marker->add("type", kReplicationDatabaseCreate);
            marker->add("db", options.name);
            marker->add("data", entry.data);
          }
          PrintMarker();

          _state = kDbCreate;
        }
        // wait for marker data in Put entry
        break;
      case RocksDBLogType::DatabaseDrop: {
        ResetTransientState();  // finish ongoing trx
        const auto entry = rocksutils::Read<wal::Database>(payload);
        if (shouldHandleDB(entry.database_id)) {
          catalog::DatabaseOptions options;
          auto r = vpack::ReadTupleNothrow(entry.data, options);
          SDB_ENSURE(r.ok(), ERROR_SERVER_CORRUPTED_DATAFILE);

          {
            vpack::ObjectBuilder marker(&builder, true);
            marker->add("tick", std::to_string(_current_sequence));
            marker->add("type", kReplicationDatabaseDrop);
            marker->add("db", options.name);
          }
          PrintMarker();

          _state = kDbDrop;
        }
        // wait for marker data in Put entry
        databases.erase(entry.database_id);
      } break;
      case RocksDBLogType::TableCreate:
        handle_schema_object(kTableCreate, [&](ObjectId db, ObjectId id) {
          return shouldHandleCollection(db, id);
        });
        break;
      case RocksDBLogType::TableRename:
        handle_schema_object(kTableRename, [&](ObjectId db, ObjectId id) {
          return shouldHandleCollection(db, id);
        });
        break;
      case RocksDBLogType::TableChange:
        handle_schema_object(kTableChange, [&](ObjectId db, ObjectId id) {
          return shouldHandleCollection(db, id);
        });
        break;
      case RocksDBLogType::TableDrop:
        handle_schema_object_drop([&](ObjectId db, ObjectId id) {
          // always print drop collection marker, shouldHandleCollection will
          // always return false for dropped collections
          collections.erase(id);
          return shouldHandleDB(db);
        });
        break;
      case RocksDBLogType::TableTruncate: {
        ResetTransientState();  // finish ongoing trx

        const auto [dbid, _, cid] =
          rocksutils::Read<wal::TableTruncate>(payload);

        if (shouldHandleCollection(dbid, cid)) {  // will check database
          if (auto* database = LoadDatabase(dbid); database) {
            if (auto* c = loadCollection(dbid, cid); c) {
              const Marker marker{
                .db = database->GetName(),
                .cuid = c->GetName(),
                .tick = Identifier{_current_sequence},
                .type = ConvertDDLEntry(type),
              };
              vpack::WriteObject(builder, marker);
            }

            PrintMarker();
          }
        }
        break;
      }
      case RocksDBLogType::IndexCreate: {
        ResetTransientState();  // finish ongoing trx

        const auto [dbid, cid, index_def] =
          rocksutils::Read<wal::IndexCreate>(payload);

        // only print markers from this collection if it is set
        if (shouldHandleCollection(dbid, cid)) {  // will check database
          if (auto* database = LoadDatabase(dbid); database) {
            if (auto* c = loadCollection(dbid, cid); c) {
              auto stripped = rocksutils::StripObjectIds(index_def);

              const Marker marker{
                .db = database->GetName(),
                .cuid = c->GetName(),
                .tick =
                  Identifier{_current_sequence + (_start_of_batch ? 0 : 1)},
                .type = ConvertDDLEntry(type),
                .data = stripped.first,
              };
              vpack::WriteObject(builder, marker);

              PrintMarker();
            }
          }
        }

        break;
      }
      case RocksDBLogType::IndexDrop: {
        ResetTransientState();  // finish ongoing trx

        const auto [dbid, cid, iid] = rocksutils::Read<wal::IndexDrop>(payload);

        // only print markers from this collection if it is set
        if (shouldHandleCollection(dbid, cid)) {
          if (auto* database = LoadDatabase(dbid); database) {
            if (auto* c = loadCollection(dbid, cid); c) {
              uint64_t tick = _current_sequence + (_start_of_batch ? 0 : 1);
              {
                vpack::ObjectBuilder marker(&builder, true);
                marker->add("tick", std::to_string(tick));
                marker->add("type", ConvertDDLEntry(type));
                marker->add("db", database->GetName());
                marker->add("cuid", c->GetName());

                vpack::ObjectBuilder data(&builder, "data", true);

                data->add("id", std::to_string(iid.id()));
              }
              PrintMarker();
            }
          }
        }
        break;
      }
      case RocksDBLogType::FunctionCreate:
        handle_schema_object(kFunctionCreate, [&](ObjectId db, ObjectId id) {
          return shouldHandleFunction(db, id);
        });
        break;
      case RocksDBLogType::FunctionDrop: {
        handle_schema_object_drop([&](ObjectId db, ObjectId id) {
          return shouldHandleFunction(db, id);
        });
      } break;
      case RocksDBLogType::ViewCreate:
        handle_schema_object(kViewCreate, [&](ObjectId db, ObjectId id) {
          return shouldHandleView(db, id);
        });
        break;
      case RocksDBLogType::ViewDrop: {
        handle_schema_object_drop([&](ObjectId db, ObjectId id) {
          return shouldHandleFunction(db, id);
        });
      } break;
      case RocksDBLogType::ViewChange:
        handle_schema_object(kViewChange, [&](ObjectId db, ObjectId id) {
          return shouldHandleView(db, id);
        });
        break;
      case RocksDBLogType::RoleCreate:
      case RocksDBLogType::RoleDrop:
      case RocksDBLogType::RoleChange:
        // TODO(gnusi): fix
        SDB_WARN("xxxxx", Logger::REPLICATION, "Skipping entry ",
                 magic_enum::enum_name(type));
        break;
      case RocksDBLogType::BeginTransaction: {
        ResetTransientState();  // finish ongoing trx
        const auto [dbid, tid] = rocksutils::Read<wal::Transaction>(payload);
        if (shouldHandleDB(dbid)) {
          // note: database may be a nullptr here, if the database was already
          // deleted!
          if (auto* database = LoadDatabase(dbid); database) {
            _state = kTransaction;
            _current_trx_id = tid;
            _trx_db_id = dbid;
            builder.openObject(true);
            builder.add("tick", std::to_string(_current_sequence));
            builder.add("type", ConvertDDLEntry(type));
            builder.add("tid", std::to_string(_current_trx_id.id()));
            builder.add("db", database->GetName());
            builder.close();
            PrintMarker();
          }
        }
        break;
      }
      case RocksDBLogType::CommitTransaction: {
        if (_state == kTransaction) {
          const auto [dbid, tid] = rocksutils::Read<wal::Transaction>(payload);
          SDB_ASSERT(_current_trx_id == tid && _trx_db_id == dbid);
          if (shouldHandleDB(dbid) && _current_trx_id == tid) {
            WriteCommitMarker(dbid);
          }
        }
        ResetTransientState();
        break;
      }
      case RocksDBLogType::SingleOperation: {
        ResetTransientState();  // finish ongoing trx
        const auto [dbid, cid] = rocksutils::Read<wal::SingleOp>(payload);
        if (shouldHandleCollection(dbid, cid)) {
          _state = kSingleOperation;
        }
        break;
      }

      case RocksDBLogType::TrackedDocumentInsert:
      case RocksDBLogType::TrackedDocumentRemove:
#ifdef SDB_DEV
        _check_tick = false;
#endif
        break;  // ignore unused markers

      default:
        SDB_WARN("xxxxx", Logger::REPLICATION, "Unhandled wal log entry ",
                 magic_enum::enum_name(type));
        break;
    }
  }

  rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice& key,
                        const rocksdb::Slice& value) override {
#ifdef SDB_DEV
    _check_tick = true;
#endif
    IncTick();

    if (column_family_id == _definitions_cf) {
      // reset everything immediately after DDL operations
      ResetTransientState();
    } else if (column_family_id == _documents_cf) {
      if (_state != kTransaction && _state != kSingleOperation) {
        ResetTransientState();
        return {};
      }
      SDB_ASSERT(_state != kSingleOperation || !_current_trx_id.isSet());
      SDB_ASSERT(_state != kTransaction || _trx_db_id.isSet());

      ObjectId cid{RocksDBKey::objectId(key)};

      auto& catalog =
        SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Local();
      const auto obj = catalog.GetObject<catalog::Table>(cid);
      if (!obj) {
        return {};
      }

      const auto dbid = obj->GetDatabaseId();

      if (!shouldHandleCollection(dbid, cid)) {
        // no reset here
        return {};
      }

      SDB_ASSERT(_state != kTransaction || _trx_db_id == dbid);

      if (auto* database = LoadDatabase(dbid); database) {
        if (auto* col = loadCollection(dbid, cid); col) {
          {
            vpack::ObjectBuilder marker(&builder, true);
            marker->add("tick", std::to_string(_current_sequence));
            marker->add("type", kReplicationMarkerDocument);
            marker->add("db", database->GetName());
            marker->add("cuid", col->GetName());
            marker->add("tid", std::to_string(_current_trx_id.id()));
            marker->add("data", RocksDBValue::data(value));
          }
          PrintMarker();
        }
      }

      if (_state == kSingleOperation) {
        ResetTransientState();  // always reset after single op
      }
    }

    return {};
  }

  void HandleDeleteCf(uint32_t cf_id, const rocksdb::Slice& key) {
#ifdef SDB_DEV
    _check_tick = true;
#endif
    IncTick();

    if (cf_id != _primary_cf) {
      if (cf_id == _documents_cf) {
        _removed_doc_rid = RocksDBKey::documentId(key);
      }
      return;  // ignore all document operations
    }
    auto removed_doc_rid = std::exchange(_removed_doc_rid, {});

    if (_state != kTransaction && _state != kSingleOperation) {
      ResetTransientState();
      return;
    }
    SDB_ASSERT(_state != kSingleOperation || !_current_trx_id.isSet());
    SDB_ASSERT(_state != kTransaction || _trx_db_id.isSet());

    uint64_t object_id = RocksDBKey::objectId(key);
    auto triple = _engine.mapObjectToIndex(object_id);
    const ObjectId dbid{std::get<0>(triple)};
    const ObjectId cid = std::get<1>(triple);

    if (!shouldHandleCollection(dbid, cid) || !removed_doc_rid.isSet()) {
      return;
    }

    std::string_view doc_key = RocksDBKey::primaryKey(key);
    SDB_ASSERT(_state != kTransaction || _trx_db_id == dbid);

    auto* database = LoadDatabase(dbid);
    if (database != nullptr) {
      catalog::Table* col = loadCollection(dbid, cid);
      if (col != nullptr) {
        {
          vpack::ObjectBuilder marker(&builder, true);
          marker->add("tick", std::to_string(_current_sequence));
          marker->add("type", kReplicationMarkerRemove);
          marker->add("db", database->GetName());
          marker->add("cuid", col->GetName());
          marker->add("tid", std::to_string(_current_trx_id.id()));

          vpack::ObjectBuilder data(&builder, "data", true);
          // TODO(mbkkt) we don't really need key here
          data->add(StaticStrings::kKeyString, doc_key);
          data->add(StaticStrings::kRevString, removed_doc_rid.toHLC());
        }
        PrintMarker();
      }
    }

    if (_state == kSingleOperation) {
      ResetTransientState();
    }
  }

  rocksdb::Status DeleteCF(uint32_t column_family_id,
                           const rocksdb::Slice& key) override {
    HandleDeleteCf(column_family_id, key);
    return {};
  }

  rocksdb::Status SingleDeleteCF(uint32_t column_family_id,
                                 const rocksdb::Slice& key) override {
    HandleDeleteCf(column_family_id, key);
    return {};
  }

  rocksdb::Status DeleteRangeCF(uint32_t /*column_family_id*/,
                                const rocksdb::Slice& /*begin_key*/,
                                const rocksdb::Slice& /*end_key*/) override {
#ifdef SDB_DEV
    _check_tick = true;
#endif
    IncTick();
    return {};
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

 public:
  /// figures out from which sequence number we need to start scanning
  /// if we just use tickStart rocksdb will skip over batches we might
  /// not have completely evaluated
  uint64_t SafeBeginTick() const {
    if (filter.tick_last_scanned > 0 &&
        filter.tick_last_scanned < filter.tick_start) {
      return filter.tick_last_scanned;
    }
    return filter.tick_start;
  }

  void StartNewBatch(rocksdb::SequenceNumber start_sequence) {
    SDB_ASSERT(!_stop_on_next);
#ifdef SDB_DEV
    if (_check_tick) {
      SDB_ASSERT(_start_sequence < start_sequence);
      SDB_ASSERT(_current_sequence < start_sequence);
    }
#endif

    // starting new write batch
    _start_sequence = start_sequence;
    _current_sequence = start_sequence;
    _start_of_batch = true;
    _state = kInvalid;
    _current_trx_id = TransactionId::none();
    _trx_db_id = {};
#ifdef SDB_DEV
    _check_tick = true;
#endif
  }

  uint64_t EndBatch() {
    ResetTransientState();
    return _current_sequence;
  }

  size_t ResponseSize() const { return response_size; }

  uint64_t LastWrittenSequence() const { return _last_written_sequence; }

#ifdef SDB_DEV
  void DisableTickCheck() { _check_tick = false; }
#endif

 private:
  void WriteCommitMarker(ObjectId dbid) {
    SDB_ASSERT(_state == kTransaction);
    // note: database may be a nullptr here, if the database was already
    // deleted!
    if (auto* database = LoadDatabase(dbid); database) {
      builder.openObject(true);
      builder.add("tick", std::to_string(_current_sequence));
      builder.add("type", static_cast<uint64_t>(kReplicationTransactionCommit));
      builder.add("tid", std::to_string(_current_trx_id.id()));
      builder.add("db", database->GetName());
      builder.close();

      PrintMarker();
    }
    _state = kInvalid;
  }

  /// print maker in builder and clear it
  void PrintMarker() {
    // note: database may be a nullptr here!
    SDB_ASSERT(!builder.isEmpty());
    if (_current_sequence > filter.tick_start) {
      callback(builder.slice());
      response_size += builder.size();
      _last_written_sequence = _current_sequence;
    }
    builder.clear();
  }

  // should reset state flags which are only valid between
  // observing a specific log entry and a sequence of immediately
  // following PUT / DELETE / Log entries
  void ResetTransientState() {
    // reset all states
    _state = kInvalid;
    _current_trx_id = TransactionId::none();
    _trx_db_id = {};
  }

  // tick function that is called before each new WAL entry
  void IncTick() {
    if (_start_of_batch) {
      // we are at the start of a batch. do NOT increase sequence number
      _start_of_batch = false;
    } else {
      // we are inside a batch already. now increase sequence number
      ++_current_sequence;
    }
  }

 private:
  RocksDBEngineCatalog& _engine;
  const uint32_t _definitions_cf;
  const uint32_t _documents_cf;
  const uint32_t _primary_cf;
  const size_t _max_response_size;
  vpack::Builder _builder;

  rocksdb::SequenceNumber _start_sequence = 0;
  rocksdb::SequenceNumber _current_sequence = 0;
  rocksdb::SequenceNumber _last_written_sequence = 0;

  // Various state machine flags
  State _state = kInvalid;
  TransactionId _current_trx_id = TransactionId::none();
  ObjectId _trx_db_id;  // remove eventually
  bool _stop_on_next = false;
  bool _start_of_batch = false;
#ifdef SDB_DEV
  bool _check_tick = true;
#endif
  RevisionId _removed_doc_rid;
};

#ifdef SDB_DEV
void RocksDBWalAccess::printWal(const Filter& filter, size_t chunk_size,
                                const MarkerCallback& func) const {
  rocksdb::TransactionDB* db = _engine.db();

  if (chunk_size < 16384) {  // we need to have some sensible minimum
    chunk_size = 16384;
  }

  // pre 3.4 breaking up write batches is not supported
  size_t max_trx_chunk_size =
    filter.tick_last_scanned > 0 ? chunk_size : SIZE_MAX;

  WALDumper dumper(_engine, filter, func, max_trx_chunk_size);
  const uint64_t since = dumper.SafeBeginTick();
  SDB_ASSERT(since <= filter.tick_start);
  SDB_ASSERT(since <= filter.tick_end);

  uint64_t first_tick = UINT64_MAX;  // first tick to actually print (exclusive)
  uint64_t last_scanned_tick =
    since;  // last (begin) tick of batch we looked at
  uint64_t latest_tick = db->GetLatestSequenceNumber();

  std::unique_ptr<rocksdb::TransactionLogIterator> iterator;
  rocksdb::TransactionLogIterator::ReadOptions ro(false);
  rocksdb::Status s = db->GetUpdatesSince(since, &iterator, ro);
  SDB_ASSERT(s.ok());

  // we need to check if the builder is bigger than the chunksize,
  // only after we printed a full WriteBatch. Otherwise a client might
  // never read the full writebatch
  SDB_WARN("xxxxx", Logger::ENGINES, "WAL tailing call. Scan since: ", since,
           ", tick start: ", filter.tick_start, ", tick end: ", filter.tick_end,
           ", chunk size: ", chunk_size, ", latesttick: ", latest_tick);
  while (iterator->Valid() && last_scanned_tick <= filter.tick_end) {
    rocksdb::BatchResult batch = iterator->GetBatch();
    // record the first tick we are actually considering
    if (first_tick == UINT64_MAX) {
      first_tick = batch.sequence;
    }

    if (batch.sequence > filter.tick_end) {
      break;  // cancel out
    }

    SDB_WARN("xxxxx", Logger::REPLICATION, "found batch-seq: ", batch.sequence,
             ", count: ", batch.writeBatchPtr->Count(),
             ", last scanned: ", last_scanned_tick);
    last_scanned_tick = batch.sequence;  // start of the batch
    SDB_ASSERT(last_scanned_tick <= db->GetLatestSequenceNumber());

    if (batch.sequence < since) {
      iterator->Next();  // skip
      continue;
    }

    dumper.StartNewBatch(batch.sequence);
    s = batch.writeBatchPtr->Iterate(&dumper);
    if (batch.writeBatchPtr->Count() == 0) {
      // there can be completely empty write batches. in case we encounter
      // some, we cannot assume the tick gets increased next time
      dumper.DisableTickCheck();
    }
    SDB_ASSERT(s.ok());

    uint64_t batch_end_seq = dumper.EndBatch();  // end tick of the batch
    SDB_ASSERT(batch_end_seq >= last_scanned_tick);

    if (dumper.ResponseSize() >= chunk_size) {  // break if response gets big
      SDB_WARN("xxxxx", Logger::REPLICATION,
               "reached maximum result size. finishing tailing");
      break;
    }
    // we need to set this here again, to avoid re-scanning WriteBatches
    last_scanned_tick = batch_end_seq;  // do not remove, tailing take forever
    SDB_ASSERT(last_scanned_tick <= db->GetLatestSequenceNumber());

    iterator->Next();
  }

  latest_tick = db->GetLatestSequenceNumber();

  SDB_WARN("xxxxx", Logger::REPLICATION, "lastScannedTick: ", last_scanned_tick,
           ", latestTick: ", latest_tick);
}
#endif

WalAccessResult RocksDBWalAccess::tail(const Filter& filter, size_t chunk_size,
                                       const MarkerCallback& func) const {
  SDB_ASSERT(filter.transaction_ids.empty());  // not supported in any way

  rocksdb::TransactionDB* db = _engine.db();

  if (chunk_size < 16384) {  // we need to have some sensible minimum
    chunk_size = 16384;
  }

  // pre 3.4 breaking up write batches is not supported
  size_t max_trx_chunk_size =
    filter.tick_last_scanned > 0 ? chunk_size : SIZE_MAX;

  WALDumper dumper(_engine, filter, func, max_trx_chunk_size);
  const uint64_t since = dumper.SafeBeginTick();
  SDB_ASSERT(since <= filter.tick_start);
  SDB_ASSERT(since <= filter.tick_end);

  uint64_t first_tick = UINT64_MAX;  // first tick to actually print (exclusive)
  uint64_t last_scanned_tick =
    since;                         // last (begin) tick of batch we looked at
  uint64_t last_written_tick = 0;  // lastTick at the end of a write batch
  uint64_t latest_tick = db->GetLatestSequenceNumber();

  // prevent purging of WAL files while we are in here
  RocksDBFilePurgePreventer purge_preventer(_engine.disallowPurging());

  std::unique_ptr<rocksdb::TransactionLogIterator> iterator;
  // no need verifying the WAL contents
  rocksdb::TransactionLogIterator::ReadOptions ro(false);
  rocksdb::Status s = db->GetUpdatesSince(since, &iterator, ro);
  if (!s.ok()) {
    Result r = ConvertStatus(s, rocksutils::StatusHint::kWal);
    return WalAccessResult(r.errorNumber(), filter.tick_start == latest_tick, 0,
                           /*lastScannedTick*/ 0, latest_tick);
  }

  // we need to check if the builder is bigger than the chunksize,
  // only after we printed a full WriteBatch. Otherwise a client might
  // never read the full writebatch
  SDB_DEBUG("xxxxx", Logger::ENGINES, "WAL tailing call. Scan since: ", since,
            ", tick start: ", filter.tick_start,
            ", tick end: ", filter.tick_end, ", chunk size: ", chunk_size);
  while (iterator->Valid() && last_scanned_tick <= filter.tick_end) {
    rocksdb::BatchResult batch = iterator->GetBatch();
    // record the first tick we are actually considering
    if (first_tick == UINT64_MAX) {
      first_tick = batch.sequence;
    }

    if (batch.sequence > filter.tick_end) {
      break;  // cancel out
    }

    last_scanned_tick = batch.sequence;  // start of the batch
#ifdef SDB_DEV
    if (last_scanned_tick > db->GetLatestSequenceNumber()) {
      // this is an unexpected condition. in this case we print the WAL for
      // debug purposes and error out
      printWal(filter, chunk_size, func);
    }
#endif
    SDB_ASSERT(last_scanned_tick <= db->GetLatestSequenceNumber());

    if (batch.sequence < since) {
      iterator->Next();  // skip
      continue;
    }

    dumper.StartNewBatch(batch.sequence);
    s = batch.writeBatchPtr->Iterate(&dumper);
#ifdef SDB_DEV
    if (batch.writeBatchPtr->Count() == 0) {
      // there can be completely empty write batches. in case we encounter
      // some, we cannot assume the tick gets increased next time
      dumper.DisableTickCheck();
    }
#endif
    if (!s.ok()) {
      SDB_ERROR("xxxxx", Logger::REPLICATION,
                "error during WAL scan: ", s.ToString());
      break;  // s is considered in the end
    }

    uint64_t batch_end_seq = dumper.EndBatch();  // end tick of the batch
    last_written_tick =
      dumper.LastWrittenSequence();  // 0 if no marker was written
    SDB_ASSERT(batch_end_seq >= last_scanned_tick);

    if (dumper.ResponseSize() >= chunk_size) {  // break if response gets big
      break;
    }
    // we need to set this here again, to avoid re-scanning WriteBatches
    last_scanned_tick = batch_end_seq;  // do not remove, tailing take forever
#ifdef SDB_DEV
    if (last_scanned_tick > db->GetLatestSequenceNumber()) {
      // this is an unexpected condition. in this case we print the WAL for
      // debug purposes and error out
      printWal(filter, chunk_size, func);
    }
#endif
    SDB_ASSERT(last_scanned_tick <= db->GetLatestSequenceNumber());

    iterator->Next();
  }

  // update our latest sequence number again, because it may have been raised
  // while scanning the WAL
  latest_tick = db->GetLatestSequenceNumber();

#ifdef SDB_DEV
  if (s.ok() && last_scanned_tick > latest_tick) {
    // this is an unexpected condition. in this case we print the WAL for
    // debug purposes and error out
    printWal(filter, chunk_size, func);
  }
#endif

  SDB_ASSERT(!s.ok() || last_scanned_tick <= latest_tick);

  WalAccessResult result(ERROR_OK, first_tick <= filter.tick_start,
                         last_written_tick, last_scanned_tick, latest_tick);
  if (!s.ok()) {
    result.reset(ConvertStatus(s, rocksutils::StatusHint::kWal));
  }
  return result;
}

}  // namespace sdb
