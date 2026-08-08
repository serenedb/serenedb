////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "connector/functions/catalog_introspect.h"

#include <absl/functional/function_ref.h>
#include <absl/strings/str_cat.h>

#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <magic_enum/magic_enum.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "auth/role_closure.h"
#include "basics/serializer.h"
#include "basics/simdjson_sink.h"
#include "catalog/database.h"
#include "catalog/deferred_writes.h"
#include "catalog/duckdb_catalog.h"
#include "catalog/duckdb_dependency.h"
#include "catalog/duckdb_global_catalog.h"
#include "catalog/duckdb_schema_entry.h"
#include "catalog/entry.h"
#include "catalog/foreign_server.h"
#include "catalog/index.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/sequence.h"
#include "catalog/store/store.h"
#include "catalog/store/wal.h"
#include "catalog/tokenizer.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

using catalog::CatalogStore;

// The snapshot view's own vocabulary: duckdb's CatalogTypeToString renders
// SCREAMING_CASE and calls an open drop a DELETED_ENTRY, and this column is
// what a test reads.
constexpr std::string_view ObjectTypeName(duckdb::CatalogType type) noexcept {
  switch (type) {
    using enum duckdb::CatalogType;
    case DELETED_ENTRY:
      return "Tombstone";
    case DATABASE_ENTRY:
      return "Database";
    case ROLE_ENTRY:
      return "Role";
    case SCHEMA_ENTRY:
      return "Schema";
    case TOKENIZER_ENTRY:
      return "Tokenizer";
    case FOREIGN_SERVER_ENTRY:
      return "ForeignServer";
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      return "Function";
    case TYPE_ENTRY:
      return "Type";
    case VIEW_ENTRY:
      return "View";
    case SEQUENCE_ENTRY:
      return "Sequence";
    case TABLE_ENTRY:
      return "Table";
    case INDEX_ENTRY:
      return "Index";
    default:
      return "Invalid";
  }
}

// Runs `write` against a fresh sink; nullopt when it reports nothing to render.
std::optional<std::string> RenderJson(
  absl::FunctionRef<bool(basics::JsonSink&)> write) {
  simdjson::builder::string_builder sb{256};
  {
    basics::JsonSink sink{sb};
    if (!write(sink)) {
      return std::nullopt;
    }
  }
  std::string_view body;
  SDB_ENSURE(sb.view().get(body) == simdjson::SUCCESS,
             "catalog introspection: json render failed");
  return std::string{body};
}

// A store op renders as the DDL it is: duckdb writes the SQL for its own parse
// infos, and the two halves of the hierarchy each spell their own.
std::string RenderStoreOp(const duckdb::ParseInfo& info) {
  switch (info.info_type) {
    case duckdb::ParseInfoType::ALTER_INFO:
      return info.Cast<duckdb::AlterInfo>().ToString();
    case duckdb::ParseInfoType::CREATE_INFO:
      return info.Cast<duckdb::CreateInfo>().ToString();
    case duckdb::ParseInfoType::DROP_INFO:
      return info.Cast<duckdb::DropInfo>().ToString();
    default:
      return {};
  }
}

// The `def` column. The object renders its own tuple as named fields, so
// nothing is encoded and decoded again to display it, and there is no
// per-type render table to keep in step with the object set.
// The kinds whose record is a CreateInfo render through the same JSON sink:
// the names live in the info's own reflection, so nothing is decoded twice.
std::optional<std::string> RenderCreateInfo(duckdb::CatalogType type,
                                            const duckdb::CreateInfo& info) {
  switch (type) {
    case duckdb::CatalogType::ROLE_ENTRY:
      return RenderJson([&](basics::JsonSink& sink) {
        static_cast<const catalog::CreateRoleInfo&>(info).WriteJson(sink);
        return true;
      });
    case duckdb::CatalogType::DATABASE_ENTRY:
      return RenderJson([&](basics::JsonSink& sink) {
        static_cast<const catalog::CreateDatabaseInfo&>(info).WriteJson(sink);
        return true;
      });
    case duckdb::CatalogType::TOKENIZER_ENTRY:
      return RenderJson([&](basics::JsonSink& sink) {
        static_cast<const catalog::CreateTokenizerInfo&>(info).WriteJson(sink);
        return true;
      });
    case duckdb::CatalogType::FOREIGN_SERVER_ENTRY:
      return RenderJson([&](basics::JsonSink& sink) {
        static_cast<const catalog::CreateForeignServerInfo&>(info).WriteJson(
          sink);
        return true;
      });

    case duckdb::CatalogType::INDEX_ENTRY:
      return RenderJson([&](basics::JsonSink& sink) {
        static_cast<const catalog::CreateIndexInfoBase&>(info).WriteJson(sink);
        return true;
      });
    case duckdb::CatalogType::SCHEMA_ENTRY:
    case duckdb::CatalogType::SEQUENCE_ENTRY:
    case duckdb::CatalogType::TYPE_ENTRY:
      // duckdb's own info, so it renders as the CREATE statement it came from
      // rather than through our JSON sink.
      return info.ToString();
    default:
      return std::nullopt;
  }
}

std::optional<std::string> RenderSequenceValue(uint64_t value) {
  return RenderJson([&](basics::JsonSink& sink) {
    sink.OnObjectBegin();
    sink.OnPropertyBegin("value");
    sink.WriteValue(value);
    sink.OnObjectEnd();
    return true;
  });
}

struct EntryRow {
  uint64_t frame = 0;
  uint64_t entry = 0;
  std::string op;
  std::optional<uint64_t> parent_id;
  std::optional<std::string> type;
  std::optional<uint64_t> id;
  std::optional<std::string> def;
};

EntryRow MakeEntryRow(const catalog::wal::TaggedEntry& tagged) {
  EntryRow row;
  row.op = magic_enum::enum_name(tagged.tag);
  std::visit(
    [&](const auto& e) {
      using T = std::decay_t<decltype(e)>;
      using namespace catalog::wal;
      if constexpr (std::is_same_v<T, PutTable>) {
        row.parent_id = e.schema_id.id();
        row.type = ObjectTypeName(duckdb::CatalogType::TABLE_ENTRY);
        row.id = e.id.id();
        row.def = RenderCreateInfo(duckdb::CatalogType::TABLE_ENTRY, *e.info);
      } else if constexpr (std::is_same_v<T, catalog::wal::DropObject>) {
        row.parent_id = e.parent_id.id();
        row.type = ObjectTypeName(e.type);
        row.id = e.id.id();
      } else if constexpr (std::is_same_v<T, DropChildren>) {
        row.parent_id = e.parent_id.id();
      } else if constexpr (std::is_same_v<T, DropPrepare>) {
        row.parent_id = e.parent_id.id();
        row.type = ObjectTypeName(e.type);
        row.id = e.id.id();
      } else if constexpr (std::is_same_v<T, SetSequence> ||
                           std::is_same_v<T, BumpSequence>) {
        row.id = e.id.id();
        row.def = RenderSequenceValue(e.value);
      } else if constexpr (std::is_same_v<T, DropSequence> ||
                           std::is_same_v<T, PrepareCommit>) {
        row.id = e.id.id();
      } else if constexpr (std::is_same_v<T, PutEntry>) {
        row.parent_id = e.parent_id.id();
        row.type = ObjectTypeName(e.type);
        row.id = e.id.id();
        row.def = RenderCreateInfo(e.type, *e.info);
      } else if constexpr (std::is_same_v<T, catalog::store_op::Targeted>) {
        row.parent_id = e.database_id.id();
        row.id = e.relation_id.id();
        row.def =
          e.info == nullptr ? "materialize storage" : RenderStoreOp(*e.info);
      } else {
        // Falling through here is how two entry types once rendered blank.
        static_assert(false, "entry type has no row");
      }
    },
    tagged.entry);
  return row;
}

// The wal dump adds the frame/entry/op columns that locate a record in the
// file; the snapshot has no frames to point at.
enum class RowShape : uint8_t {
  Snapshot,
  WalRecord,
};

void PushColumns(duckdb::vector<duckdb::LogicalType>& return_types,
                 duckdb::vector<duckdb::string>& names, RowShape shape) {
  if (shape == RowShape::WalRecord) {
    return_types.push_back(duckdb::LogicalType::UBIGINT);
    names.push_back("frame");
    return_types.push_back(duckdb::LogicalType::UBIGINT);
    names.push_back("entry");
    return_types.push_back(duckdb::LogicalType::VARCHAR);
    names.push_back("op");
  }
  return_types.push_back(duckdb::LogicalType::UBIGINT);
  names.push_back("parent_id");
  return_types.push_back(duckdb::LogicalType::VARCHAR);
  names.push_back("type");
  return_types.push_back(duckdb::LogicalType::UBIGINT);
  names.push_back("id");
  return_types.push_back(duckdb::LogicalType::VARCHAR);
  names.push_back("def");
}

// The dump spans every database and includes definitions a role could never
// SELECT (role rows carry password verifiers), so it is superuser-only.
void RequireSuperuser(duckdb::ClientContext& context, std::string_view what) {
  auto& conn = GetSereneDBContext(context);
  if (!auth::ClosureFor(&context, conn.GetRoleId())->is_superuser) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("must be superuser to use ", what));
  }
}

duckdb::unique_ptr<duckdb::FunctionData> CatalogWalBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  RequireSuperuser(context, "sdb_catalog_wal()");
  PushColumns(return_types, names, RowShape::WalRecord);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

duckdb::unique_ptr<duckdb::FunctionData> CatalogSnapshotBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  RequireSuperuser(context, "sdb_catalog_snapshot()");
  PushColumns(return_types, names, RowShape::Snapshot);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

struct RowsState final : duckdb::GlobalTableFunctionState {
  std::vector<EntryRow> rows;
  size_t offset = 0;
  bool loaded = false;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<RowsState>();
  }
};

// The output vectors are freshly initialized flat vectors, so write them
// directly instead of building a duckdb::Value per cell.
void SetUnsigned(duckdb::Vector& vec, duckdb::idx_t i, uint64_t v) {
  duckdb::FlatVector::GetDataMutable<uint64_t>(vec)[i] = v;
}

void SetString(duckdb::Vector& vec, duckdb::idx_t i, std::string_view v) {
  duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[i] =
    duckdb::StringVector::AddString(vec, v);
}

void SetOptional(duckdb::Vector& vec, duckdb::idx_t i,
                 const std::optional<uint64_t>& v) {
  if (v) {
    SetUnsigned(vec, i, *v);
  } else {
    duckdb::FlatVector::SetNull(vec, i, true);
  }
}

void SetOptional(duckdb::Vector& vec, duckdb::idx_t i,
                 const std::optional<std::string>& v) {
  if (v) {
    SetString(vec, i, *v);
  } else {
    duckdb::FlatVector::SetNull(vec, i, true);
  }
}

void EmitRows(RowsState& state, duckdb::DataChunk& output, RowShape shape) {
  const auto n =
    std::min<size_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.offset);
  for (size_t i = 0; i < n; ++i) {
    const auto& row = state.rows[state.offset + i];
    duckdb::idx_t col = 0;
    if (shape == RowShape::WalRecord) {
      SetUnsigned(output.data[col++], i, row.frame);
      SetUnsigned(output.data[col++], i, row.entry);
      SetString(output.data[col++], i, row.op);
    }
    SetOptional(output.data[col++], i, row.parent_id);
    SetOptional(output.data[col++], i, row.type);
    SetOptional(output.data[col++], i, row.id);
    SetOptional(output.data[col++], i, row.def);
  }
  state.offset += n;
  output.SetChildCardinality(n);
}

void CatalogWalExecute(duckdb::ClientContext&,
                       duckdb::TableFunctionInput& input,
                       duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<RowsState>();
  if (!state.loaded) {
    state.loaded = true;
    uint64_t frame_no = 0;
    catalog::CatalogWal::Scan(
      catalog::GetCatalogStore().WalDirectory(),
      [&](std::span<const uint8_t> frame) {
        uint64_t entry_no = 0;
        for (const auto& record : CatalogStore::ParseFrame(frame).entries) {
          auto row = MakeEntryRow(record);
          row.frame = frame_no;
          row.entry = entry_no++;
          state.rows.push_back(std::move(row));
        }
        ++frame_no;
      });
  }
  EmitRows(state, output, RowShape::WalRecord);
}

void CatalogSnapshotExecute(duckdb::ClientContext&,
                            duckdb::TableFunctionInput& input,
                            duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<RowsState>();
  if (!state.loaded) {
    state.loaded = true;
    // VisitSnapshot holds the catalog store's mutex across the state it owns,
    // so rendering inside the callback would block every catalog writer for as
    // long as it takes to render the entire catalog. Take a reference to each
    // object under the lock, then render after it is released.
    struct RawDef {
      CatalogStore::Key key;
      std::shared_ptr<const duckdb::CreateInfo> info;
    };
    std::vector<RawDef> raw;
    catalog::GetCatalogStore().VisitSnapshot(
      [&](CatalogStore::Key key,
          std::shared_ptr<const duckdb::CreateInfo> info) {
        raw.push_back({key, std::move(info)});
      },
      [&](ObjectId id, uint64_t value) {
        EntryRow row;
        row.type = "SequenceValue";
        row.id = id.id();
        row.def = RenderSequenceValue(value);
        state.rows.push_back(std::move(row));
      });
    state.rows.reserve(state.rows.size() + raw.size());
    for (auto& entry : raw) {
      EntryRow row;
      row.parent_id = entry.key.parent_id.id();
      row.type = ObjectTypeName(entry.key.type);
      row.id = entry.key.id.id();
      row.def = RenderCreateInfo(entry.key.type, *entry.info);
      state.rows.push_back(std::move(row));
    }
  }
  EmitRows(state, output, RowShape::Snapshot);
}

struct CatalogSetRow {
  std::string schema;
  std::string entry_type;
  std::string name;
  uint64_t entry_oid = 0;
  bool visible = false;
};

struct CatalogSetsState final : duckdb::GlobalTableFunctionState {
  std::vector<CatalogSetRow> rows;
  size_t offset = 0;
  bool loaded = false;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<CatalogSetsState>();
  }
};

duckdb::unique_ptr<duckdb::FunctionData> CatalogSetsBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  RequireSuperuser(context, "sdb_catalog_sets()");
  names.emplace_back("schema_name");
  return_types.emplace_back(duckdb::LogicalType::VARCHAR);
  names.emplace_back("entry_type");
  return_types.emplace_back(duckdb::LogicalType::VARCHAR);
  names.emplace_back("name");
  return_types.emplace_back(duckdb::LogicalType::VARCHAR);
  // duckdb's own oid for the entry, which the port sets from the serenedb id --
  // so this column and pg_class.oid are the same number.
  names.emplace_back("entry_oid");
  return_types.emplace_back(duckdb::LogicalType::UBIGINT);
  names.emplace_back("visible");
  return_types.emplace_back(duckdb::LogicalType::BOOLEAN);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

// What is actually in the schema CatalogSets of the current database -- the one
// direct view of the entry port. `visible` says whether the version chain
// answers this transaction, so an entry another session has not committed shows
// up as present and invisible.
void CatalogSetsExecute(duckdb::ClientContext& context,
                        duckdb::TableFunctionInput& input,
                        duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<CatalogSetsState>();
  if (!state.loaded) {
    state.loaded = true;
    auto& duck_catalog = duckdb::Catalog::GetCatalog(
      context, duckdb::DatabaseManager::GetDefaultDatabase(context));
    if (duck_catalog.GetCatalogType() == catalog::kSereneDBCatalogType) {
      auto& catalog = duck_catalog.Cast<catalog::SereneDBCatalog>();
      const auto transaction = catalog.GetCatalogTransaction(context);
      catalog.VisitSchemaEntries([&](catalog::SereneDBSchemaEntry& schema) {
        // The schema entry itself, which is what owns the sets below. The two
        // static schemas have no definition of their own and are reported by
        // name alone.
        state.rows.push_back(
          {.schema = schema.name.GetIdentifierName(),
           .entry_type = duckdb::CatalogTypeToString(schema.type),
           .name = schema.name.GetIdentifierName(),
           .entry_oid = schema.oid,
           .visible = true});
        // One type per set, and the entry's own type is reported rather than
        // the set's: tables and views share a set, as do the two flavours of
        // macro.
        for (const auto type : {duckdb::CatalogType::TABLE_ENTRY,
                                duckdb::CatalogType::INDEX_ENTRY,
                                duckdb::CatalogType::SEQUENCE_ENTRY,
                                duckdb::CatalogType::MACRO_ENTRY,
                                duckdb::CatalogType::TABLE_MACRO_ENTRY,
                                duckdb::CatalogType::TYPE_ENTRY,
                                duckdb::CatalogType::TOKENIZER_ENTRY}) {
          schema.GetCatalogSet(type).Scan(
            transaction, [&](duckdb::CatalogEntry& entry) {
              state.rows.push_back(
                {.schema = schema.name.GetIdentifierName(),
                 .entry_type = duckdb::CatalogTypeToString(entry.type),
                 .name = entry.name.GetIdentifierName(),
                 .entry_oid = entry.oid,
                 .visible = true});
            });
        }
      });
      // Foreign servers are database children, so their set hangs off the
      // catalog and has no schema name to report.
      catalog.GetForeignServerSet().Scan(
        transaction, [&](duckdb::CatalogEntry& entry) {
          state.rows.push_back(
            {.schema = {},
             .entry_type = duckdb::CatalogTypeToString(entry.type),
             .name = entry.name.GetIdentifierName(),
             .entry_oid = entry.oid,
             .visible = true});
        });
    }
    // The two cluster-global sets belong to no database at all, so they are
    // reported whichever one the session is in, with no schema name.
    if (auto global = catalog::TryGlobalCatalog(context)) {
      const auto transaction = global->GetCatalogTransaction(context);
      for (const auto type : {duckdb::CatalogType::ROLE_ENTRY,
                              duckdb::CatalogType::DATABASE_ENTRY}) {
        global->TryGetCatalogSet(type)->Scan(
          transaction, [&](duckdb::CatalogEntry& entry) {
            state.rows.push_back(
              {.schema = {},
               .entry_type = duckdb::CatalogTypeToString(entry.type),
               .name = entry.name.GetIdentifierName(),
               .entry_oid = entry.oid,
               .visible = true});
          });
      }
    }
    // One row per recorded edge, from every attached manager: an edge is kept
    // by the dependent's own catalog, so no one of them holds the whole graph.
    // entry_oid is the referenced object and the name is the dependent.
    catalog::VisitAllEdges(
      context, [&](ObjectId referenced, ObjectId dependent) {
        state.rows.push_back({.schema = {},
                              .entry_type = duckdb::CatalogTypeToString(
                                duckdb::CatalogType::DEPENDENCY_ENTRY),
                              .name = std::to_string(dependent.id()),
                              .entry_oid = referenced.id(),
                              .visible = true});
      });
  }
  const auto n =
    std::min<size_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.offset);
  for (size_t i = 0; i < n; ++i) {
    const auto& row = state.rows[state.offset + i];
    SetString(output.data[0], i, row.schema);
    SetString(output.data[1], i, row.entry_type);
    SetString(output.data[2], i, row.name);
    SetUnsigned(output.data[3], i, row.entry_oid);
    duckdb::FlatVector::GetDataMutable<bool>(output.data[4])[i] = row.visible;
  }
  state.offset += n;
  output.SetChildCardinality(n);
}

duckdb::unique_ptr<duckdb::FunctionData> DeferredCatalogBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  RequireSuperuser(context, "sdb_deferred_catalog()");
  names.emplace_back("writes");
  return_types.emplace_back(duckdb::LogicalType::UBIGINT);
  names.emplace_back("catalog_version");
  return_types.emplace_back(duckdb::LogicalType::UBIGINT);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

struct OneRowState final : duckdb::GlobalTableFunctionState {
  bool emitted = false;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<OneRowState>();
  }
};

// What the running transaction has parked on itself: how many catalog
// mutations it has recorded, and the catalog identity its plans stand on.
void DeferredCatalogExecute(duckdb::ClientContext& context,
                            duckdb::TableFunctionInput& data,
                            duckdb::DataChunk& output) {
  auto& state = data.global_state->Cast<OneRowState>();
  if (std::exchange(state.emitted, true)) {
    output.SetCardinality(0);
    return;
  }
  SetUnsigned(output.data[0], 0, catalog::CatalogWriteCount(context));
  SetUnsigned(output.data[1], 0, GetSereneDBContext(context).CatalogEpoch());
  output.SetCardinality(1);
}

}  // namespace

void RegisterCatalogIntrospectFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.RegisterFunction(duckdb::TableFunction{
    "sdb_catalog_wal", {}, CatalogWalExecute, CatalogWalBind, RowsState::Init});
  loader.RegisterFunction(duckdb::TableFunction{"sdb_catalog_snapshot",
                                                {},
                                                CatalogSnapshotExecute,
                                                CatalogSnapshotBind,
                                                RowsState::Init});
  loader.RegisterFunction(duckdb::TableFunction{"sdb_deferred_catalog",
                                                {},
                                                DeferredCatalogExecute,
                                                DeferredCatalogBind,
                                                OneRowState::Init});
  loader.RegisterFunction(duckdb::TableFunction{"sdb_catalog_sets",
                                                {},
                                                CatalogSetsExecute,
                                                CatalogSetsBind,
                                                CatalogSetsState::Init});
}

}  // namespace sdb::connector
