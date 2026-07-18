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

#include <boost/pfr/core.hpp>
#include <boost/pfr/core_name.hpp>
#include <boost/pfr/tuple_size.hpp>
#include <cstdlib>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <iresearch/analysis/tokenizer_config.hpp>

#include "basics/serializer.h"
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
#include "json_serializer.hpp"
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

void AppendJsonString(std::string& out, std::string_view text) {
  out.push_back('"');
  for (const char c : text) {
    if (c == '"' || c == '\\') {
      out.push_back('\\');
      out.push_back(c);
    } else if (static_cast<uint8_t>(c) < 0x20) {
      absl::StrAppend(&out, "\\u00",
                      absl::Hex{static_cast<uint8_t>(c), absl::kZeroPad2});
    } else {
      out.push_back(c);
    }
  }
  out.push_back('"');
}

struct DocDeleter {
  void operator()(yyjson_mut_doc* doc) const noexcept {
    yyjson_mut_doc_free(doc);
  }
};

// Fallback for duckdb types whose Serialize speaks the tagged WriteProperty
// protocol JsonSerializer understands (LogicalType, parsed expressions, ...).
template<typename T>
void AppendDuckDBJson(std::string& out, const T& value) {
  std::unique_ptr<yyjson_mut_doc, DocDeleter> doc{yyjson_mut_doc_new(nullptr)};
  duckdb::JsonSerializer sink{doc.get(), false, false, false};
  value.Serialize(sink);
  size_t len = 0;
  auto* text =
    yyjson_mut_val_write_opts(sink.GetRootObject(), 0, nullptr, &len, nullptr);
  if (!text) {
    out.append("null");
    return;
  }
  out.append(text, len);
  std::free(text);
}

template<typename T>
concept HasMemberSerialize =
  requires(const T& t, duckdb::Serializer& s) { t.Serialize(s); };

template<typename T>
concept SmartNullable = requires(const T& v) {
  static_cast<bool>(v);
  *v;
} && !std::is_arithmetic_v<T>;

template<typename T>
inline constexpr bool kIsVariantV = false;
template<typename... Ts>
inline constexpr bool kIsVariantV<std::variant<Ts...>> = true;

template<typename T>
concept TupleLike = requires { std::tuple_size<T>::value; };

// Mirrors basics::WriteTuple's type dispatch, but emits self-describing JSON:
// aggregate fields get their pfr names, enums their enumerator names,
// expressions and duckdb create infos their SQL text.
template<typename T>
void JsonAppend(std::string& out, const T& value) {
  if constexpr (std::is_base_of_v<basics::Identifier, T>) {
    absl::StrAppend(&out, value.id());
  } else if constexpr (std::is_same_v<T, catalog::AclMode>) {
    absl::StrAppend(&out, std::to_underlying(value));
  } else if constexpr (std::is_same_v<T, catalog::Permissions>) {
    out.append("{\"owner\":");
    JsonAppend(out, value.owner);
    out.append(",\"acl\":");
    JsonAppend(out, value.acl);
    out.push_back('}');
  } else if constexpr (std::is_same_v<T, duckdb::LogicalType>) {
    AppendJsonString(out, value.ToString());
  } else if constexpr (std::is_same_v<T, irs::analysis::TokenizerConfig>) {
    // The 23 option variants drag in icu::Locale and analyzer internals;
    // identifying the tokenizer kind is enough for the debug view.
    std::visit(
      [&]<typename Options>(const Options&) {
        out.append("{\"tokenizer\":");
        AppendJsonString(out, Options::Owner::type_name());
        out.push_back('}');
      },
      value.config);
  } else if constexpr (std::is_same_v<T, ColumnExpr>) {
    if (value.HasExpr()) {
      AppendJsonString(out, value.GetExpr().ToString());
    } else {
      out.append("null");
    }
  } else if constexpr (std::is_same_v<T, catalog::Column>) {
    out.append("{\"id\":");
    JsonAppend(out, value.GetId());
    out.append(",\"name\":");
    AppendJsonString(out, value.GetName());
    out.append(",\"type\":");
    JsonAppend(out, value.type);
    out.append(",\"expr\":");
    JsonAppend(out, value.expr);
    out.append(",\"generated_type\":");
    JsonAppend(out, value.generated_type);
    out.append(",\"acl\":");
    JsonAppend(out, value.GetAcl());
    out.append(",\"comment\":");
    AppendJsonString(out, value.comment);
    out.push_back('}');
  } else if constexpr (std::is_same_v<T, catalog::CheckConstraint>) {
    out.append("{\"id\":");
    JsonAppend(out, value.GetId());
    out.append(",\"name\":");
    AppendJsonString(out, value.GetName());
    out.append(",\"expr\":");
    JsonAppend(out, value.expr);
    out.push_back('}');
  } else if constexpr (std::is_same_v<T, bool>) {
    out.append(value ? "true" : "false");
  } else if constexpr (std::is_arithmetic_v<T>) {
    absl::StrAppend(&out, +value);
  } else if constexpr (std::is_enum_v<T>) {
    if (const auto name = magic_enum::enum_name(value); !name.empty()) {
      AppendJsonString(out, name);
    } else {
      absl::StrAppend(&out, std::to_underlying(value));
    }
  } else if constexpr (std::convertible_to<T, std::string_view>) {
    AppendJsonString(out, std::string_view{value});
  } else if constexpr (HasMemberSerialize<T>) {
    AppendDuckDBJson(out, value);
  } else if constexpr (SmartNullable<T>) {
    if (value) {
      JsonAppend(out, *value);
    } else {
      out.append("null");
    }
  } else if constexpr (kIsVariantV<T>) {
    std::visit([&](const auto& v) { JsonAppend(out, v); }, value);
  } else if constexpr (std::ranges::range<T>) {
    out.push_back('[');
    bool first = true;
    for (const auto& v : value) {
      if (!std::exchange(first, false)) {
        out.push_back(',');
      }
      JsonAppend(out, v);
    }
    out.push_back(']');
  } else if constexpr (TupleLike<T>) {
    out.push_back('[');
    std::apply(
      [&](const auto&... args) {
        bool first = true;
        ((std::exchange(first, false) ? void() : void(out.push_back(',')),
          JsonAppend(out, args)),
         ...);
      },
      value);
    out.push_back(']');
  } else if constexpr (std::is_aggregate_v<T>) {
    out.push_back('{');
    [&]<size_t... I>(std::index_sequence<I...>) {
      bool first = true;
      ((std::exchange(first, false) ? void() : void(out.push_back(',')),
        AppendJsonString(out, boost::pfr::get_name<I, T>()),
        out.push_back(':'), JsonAppend(out, boost::pfr::get<I>(value))),
       ...);
    }(std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
    out.push_back('}');
  } else {
    static_assert(false, "type not renderable as json");
  }
}

template<typename T>
std::string RenderTuple(std::string_view bytes) {
  auto stream = catalog::ReadStream(bytes);
  duckdb::BinaryDeserializer src{stream};
  T data;
  basics::ReadTuple(src, data);
  std::string out;
  JsonAppend(out, data);
  return out;
}

template<typename Info>
std::string RenderCreateInfo(std::string_view bytes) {
  auto stream = catalog::ReadStream(bytes);
  duckdb::BinaryDeserializer src{stream};
  catalog::CreateInfoReadData<Info> data;
  basics::ReadTuple(src, data);
  std::string out{"{\"name\":"};
  AppendJsonString(out, data.name);
  out.append(",\"perm\":");
  JsonAppend(out, data.perm);
  out.append(",\"sql\":");
  AppendJsonString(out, data.info.info->ToString());
  out.push_back('}');
  return out;
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
