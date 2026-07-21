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

#include <absl/strings/str_cat.h>

#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <magic_enum/magic_enum.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "basics/serializer.h"
#include "basics/simdjson_sink.h"
#include "catalog/create_info_serde.h"
#include "catalog/object.h"
#include "catalog/persistence/database.h"
#include "catalog/persistence/inverted_index.h"
#include "catalog/persistence/role.h"
#include "catalog/persistence/schema.h"
#include "catalog/persistence/secondary_index.h"
#include "catalog/persistence/sequence.h"
#include "catalog/persistence/table.h"
#include "catalog/persistence/tokenizer.h"
#include "catalog/store/store.h"
#include "catalog/store/wal.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

using catalog::CatalogStore;
using catalog::ObjectType;

constexpr std::string_view ObjectTypeName(ObjectType type) noexcept {
  switch (type) {
    using enum ObjectType;
    case Invalid:
      return "Invalid";
    case Tombstone:
      return "Tombstone";
    case PendingAlter:
      return "PendingAlter";
    case Database:
      return "Database";
    case Role:
      return "Role";
    case Schema:
      return "Schema";
    case Tokenizer:
      return "Tokenizer";
    case PgSqlFunction:
      return "PgSqlFunction";
    case PgSqlType:
      return "PgSqlType";
    case PgSqlView:
      return "PgSqlView";
    case Sequence:
      return "Sequence";
    case Table:
      return "Table";
    case SecondaryIndex:
      return "SecondaryIndex";
    case InvertedIndex:
      return "InvertedIndex";
    case Column:
      return "Column";
    case CheckConstraint:
      return "CheckConstraint";
    case Virtual:
      return "Virtual";
  }
  return "Unknown";
}

// Binary def bytes -> named-field JSON via the reflection serializer:
// ReadTuple decodes the persistence struct, WriteObject renders it through
// basics::JsonSink (expressions and create infos come out as SQL text via
// their ObjectFormat SerdeWrite overloads).
template<typename T>
std::string ToJson(const T& value) {
  simdjson::builder::string_builder sb{256};
  {
    basics::JsonSink sink{sb};
    basics::WriteObject(sink, value);
  }
  std::string_view body;
  SDB_ENSURE(sb.view().get(body) == simdjson::SUCCESS,
             "catalog introspection: json render failed");
  return std::string{body};
}

template<typename T>
std::string RenderTuple(std::string_view bytes) {
  auto stream = catalog::ReadStream(bytes);
  duckdb::BinaryDeserializer src{stream};
  T data;
  basics::ReadTuple(src, data);
  return ToJson(data);
}

template<typename Info>
std::string RenderCreateInfo(std::string_view bytes) {
  auto stream = catalog::ReadStream(bytes);
  duckdb::BinaryDeserializer src{stream};
  catalog::CreateInfoReadData<Info> data;
  basics::ReadTuple(src, data);
  return ToJson(catalog::CreateInfoWriteData<Info>{
    data.name, {data.info.info.get()}, data.perm});
}

std::string RenderDef(ObjectType type, std::string_view bytes);

std::string RenderPendingAlter(std::string_view payload) {
  const auto records = CatalogStore::ParseFrame(
    {reinterpret_cast<const uint8_t*>(payload.data()), payload.size()});
  std::string out{"["};
  for (const auto& record : records) {
    if (out.size() > 1) {
      out.push_back(',');
    }
    absl::StrAppend(&out, "{\"op\":\"", magic_enum::enum_name(record.op),
                    "\",\"parent_id\":", record.key.parent_id.id(),
                    ",\"type\":\"", ObjectTypeName(record.key.type),
                    "\",\"id\":", record.key.id.id());
    if (record.op == CatalogStore::Op::PutSequence) {
      absl::StrAppend(&out, ",\"value\":", record.sequence_value);
    }
    if (!record.def.empty()) {
      if (auto def = RenderDef(record.key.type, record.def); !def.empty()) {
        absl::StrAppend(&out, ",\"def\":", def);
      }
    }
    out.push_back('}');
  }
  out.push_back(']');
  return out;
}

std::string RenderDef(ObjectType type, std::string_view bytes) {
  switch (type) {
    using enum ObjectType;
    case Database:
      return RenderTuple<catalog::persistence::DatabaseOptions>(bytes);
    case Role:
      return RenderTuple<catalog::persistence::RoleData>(bytes);
    case Schema:
      return RenderTuple<catalog::persistence::SchemaOptions>(bytes);
    case Tokenizer:
      return RenderTuple<catalog::persistence::TokenizerData>(bytes);
    case PgSqlFunction:
      return RenderCreateInfo<duckdb::CreateMacroInfo>(bytes);
    case PgSqlType:
      return RenderCreateInfo<duckdb::CreateTypeInfo>(bytes);
    case PgSqlView:
      return RenderCreateInfo<duckdb::CreateViewInfo>(bytes);
    case Sequence:
      return RenderTuple<catalog::persistence::SequenceOptions>(bytes);
    case Table:
      return RenderTuple<catalog::persistence::TableData>(bytes);
    case SecondaryIndex:
      return RenderTuple<catalog::persistence::SecondaryIndexData>(bytes);
    case InvertedIndex:
      return RenderTuple<catalog::persistence::InvertedIndexData>(bytes);
    case PendingAlter:
      return RenderPendingAlter(bytes);
    case Invalid:
    case Tombstone:
    case Column:
    case CheckConstraint:
    case Virtual:
      return {};
  }
  return {};
}

struct RecordRow {
  uint64_t frame = 0;
  uint64_t entry = 0;
  std::string op;
  std::optional<uint64_t> parent_id;
  std::optional<std::string> type;
  std::optional<uint64_t> id;
  std::optional<std::string> def;
};

RecordRow MakeRecordRow(const CatalogStore::Entry& record) {
  RecordRow row;
  row.op = magic_enum::enum_name(record.op);
  switch (record.op) {
    using enum CatalogStore::Op;
    case PutDefinition: {
      row.parent_id = record.key.parent_id.id();
      row.type = ObjectTypeName(record.key.type);
      row.id = record.key.id.id();
      if (auto def = RenderDef(record.key.type, record.def); !def.empty()) {
        row.def = std::move(def);
      }
      break;
    }
    case DropDefinition:
      row.parent_id = record.key.parent_id.id();
      row.type = ObjectTypeName(record.key.type);
      row.id = record.key.id.id();
      break;
    case PutSequence:
    case AdvanceSequence:
      row.id = record.key.id.id();
      row.def = absl::StrCat("{\"value\":", record.sequence_value, "}");
      break;
    case DropSequence:
      row.id = record.key.id.id();
      break;
    case DropByParentType:
      row.parent_id = record.key.parent_id.id();
      row.type = ObjectTypeName(record.key.type);
      break;
    case DropByParent:
      row.parent_id = record.key.parent_id.id();
      break;
    default:
      break;
  }
  return row;
}

void PushColumns(duckdb::vector<duckdb::LogicalType>& return_types,
                 duckdb::vector<duckdb::string>& names, bool with_frame) {
  if (with_frame) {
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
  if (!conn.CatalogSnapshot()->ClosureFor(conn.GetRoleId()).is_superuser) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("must be superuser to use ", what));
  }
}

duckdb::unique_ptr<duckdb::FunctionData> CatalogWalBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  RequireSuperuser(context, "sdb_catalog_wal()");
  PushColumns(return_types, names, true);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

duckdb::unique_ptr<duckdb::FunctionData> CatalogSnapshotBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  RequireSuperuser(context, "sdb_catalog_snapshot()");
  PushColumns(return_types, names, false);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

struct RowsState final : duckdb::GlobalTableFunctionState {
  std::vector<RecordRow> rows;
  size_t offset = 0;
  bool loaded = false;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<RowsState>();
  }
};

void SetOptional(duckdb::DataChunk& output, duckdb::idx_t col, duckdb::idx_t i,
                 const std::optional<uint64_t>& v) {
  output.SetValue(col, i,
                  v ? duckdb::Value::UBIGINT(*v) : duckdb::Value{});
}

void SetOptional(duckdb::DataChunk& output, duckdb::idx_t col, duckdb::idx_t i,
                 const std::optional<std::string>& v) {
  output.SetValue(col, i, v ? duckdb::Value{*v} : duckdb::Value{});
}

void EmitRows(RowsState& state, duckdb::DataChunk& output, bool with_frame) {
  const auto n =
    std::min<size_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.offset);
  for (size_t i = 0; i < n; ++i) {
    const auto& row = state.rows[state.offset + i];
    duckdb::idx_t col = 0;
    if (with_frame) {
      output.SetValue(col++, i, duckdb::Value::UBIGINT(row.frame));
      output.SetValue(col++, i, duckdb::Value::UBIGINT(row.entry));
      output.SetValue(col++, i, duckdb::Value{row.op});
    }
    SetOptional(output, col++, i, row.parent_id);
    SetOptional(output, col++, i, row.type);
    SetOptional(output, col++, i, row.id);
    SetOptional(output, col++, i, row.def);
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
        for (const auto& record : CatalogStore::ParseFrame(frame)) {
          auto row = MakeRecordRow(record);
          row.frame = frame_no;
          row.entry = entry_no++;
          state.rows.push_back(std::move(row));
        }
        ++frame_no;
      });
  }
  EmitRows(state, output, true);
}

void CatalogSnapshotExecute(duckdb::ClientContext&,
                            duckdb::TableFunctionInput& input,
                            duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<RowsState>();
  if (!state.loaded) {
    state.loaded = true;
    catalog::GetCatalogStore().VisitSnapshot(
      [&](CatalogStore::Key key, std::string_view bytes) {
        RecordRow row;
        row.parent_id = key.parent_id.id();
        row.type = ObjectTypeName(key.type);
        row.id = key.id.id();
        if (auto def = RenderDef(key.type, bytes); !def.empty()) {
          row.def = std::move(def);
        }
        state.rows.push_back(std::move(row));
      },
      [&](ObjectId id, uint64_t value) {
        RecordRow row;
        row.type = "SequenceValue";
        row.id = id.id();
        row.def = absl::StrCat("{\"value\":", value, "}");
        state.rows.push_back(std::move(row));
      });
  }
  EmitRows(state, output, false);
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
}

}  // namespace sdb::connector
