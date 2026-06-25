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

#include "connector/functions/es.h"

#include <absl/algorithm/container.h>
#include <absl/strings/escaping.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <cstring>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <map>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/serializer.h"
#include "basics/simdjson_sink.h"
#include "catalog/catalog.h"
#include "catalog/column_expr.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "pg/commands/create_tsdictionary.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_storage.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kEsSchema = "es";
constexpr std::string_view kIdColumn = "_id";
constexpr std::string_view kSourceColumn = "_source";
// The analyzer behind every text property, mimicking ES's standard analyzer
// (tokenize + lowercase, no stemming); frequency/position/norm make phrase
// queries and scoring possible. Created lazily in the es schema.
constexpr std::string_view kTextTokenizer = "standard";

// Field names mirror the wire JSON (boost.pfr name matching in ReadObject);
// unknown request fields are skipped, matching ES leniency. std::map keeps
// the column order deterministic and alphabetical, which is also the order
// ES reports mappings in.
struct FieldMapping {
  std::string type;
};

struct TypeMapping {
  std::map<std::string, FieldMapping> properties;
};

struct CreateIndexRequest {
  TypeMapping mappings;
};

// ES index-name rules (subset): lowercase, no spaces, not starting with
// -/_/+, none of \ / * ? " < > | , #. Stricter than ES in that the allowed
// set is a whitelist, which keeps the names safe inside double-quoted SQL
// identifiers composed by the HTTP handlers.
void ValidateIndexName(std::string_view name) {
  bool ok = !name.empty() && name.size() <= 255 && name != "." &&
            name != ".." && name.front() != '-' && name.front() != '_' &&
            name.front() != '+';
  if (ok) {
    ok = absl::c_all_of(name, [](char c) {
      return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' ||
             c == '_' || c == '+' || c == '.';
    });
  }
  if (!ok) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_NAME),
                    ERR_MSG("Invalid index name [", name,
                            "], must be lowercase alphanumerics, '-', '_', "
                            "'+' or '.', not starting with '-', '_' or '+'"));
  }
}

void ValidateFieldName(std::string_view index, std::string_view name) {
  // Leading '_' collides with ES metadata fields (_id, _source); '.' means an
  // object field path, which the flat column model doesn't support yet.
  if (name.empty() || name.front() == '_' ||
      name.find('.') != std::string_view::npos) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    ERR_MSG("Failed to parse mapping: invalid field name [",
                            name, "] for index [", index, "]"));
  }
}

duckdb::LogicalType EsTypeToLogical(std::string_view index,
                                    std::string_view field,
                                    std::string_view es_type) {
  if (es_type == "keyword" || es_type == "text") {
    return duckdb::LogicalType::VARCHAR;
  }
  if (es_type == "long") {
    return duckdb::LogicalType::BIGINT;
  }
  if (es_type == "integer") {
    return duckdb::LogicalType::INTEGER;
  }
  if (es_type == "double") {
    return duckdb::LogicalType::DOUBLE;
  }
  if (es_type == "float") {
    return duckdb::LogicalType::FLOAT;
  }
  if (es_type == "boolean") {
    return duckdb::LogicalType::BOOLEAN;
  }
  if (es_type == "date") {
    return duckdb::LogicalType::TIMESTAMP;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
    ERR_MSG("Failed to parse mapping: No handler for type [", es_type,
            "] declared on field [", field, "] for index [", index, "]"));
}

std::string_view LogicalToEsType(const duckdb::LogicalType& type,
                                 bool inverted) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::VARCHAR:
      return inverted ? "text" : "keyword";
    case duckdb::LogicalTypeId::BIGINT:
      return "long";
    case duckdb::LogicalTypeId::INTEGER:
      return "integer";
    case duckdb::LogicalTypeId::DOUBLE:
      return "double";
    case duckdb::LogicalTypeId::FLOAT:
      return "float";
    case duckdb::LogicalTypeId::BOOLEAN:
      return "boolean";
    case duckdb::LogicalTypeId::TIMESTAMP:
      return "date";
    default:
      return {};
  }
}

CreateIndexRequest ParseCreateIndexBody(std::string_view index,
                                        std::string_view body) {
  CreateIndexRequest request;
  if (body.empty()) {
    return request;
  }
  try {
    simdjson::padded_string padded{body};
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document doc;
    if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
      throw std::runtime_error{"invalid JSON"};
    }
    basics::JsonSource source{doc};
    basics::ReadObject(source, request);
  } catch (const std::exception& e) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("Failed to parse mapping for index [", index, "]: ", e.what()));
  }
  return request;
}

struct EsIndexBindData final : duckdb::TableFunctionData {
  std::string index;
  std::string body;
};

struct EsOnceState final : duckdb::GlobalTableFunctionState {
  bool done = false;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<EsOnceState>();
  }
};

duckdb::unique_ptr<duckdb::FunctionData> BindIndexArgs(
  duckdb::TableFunctionBindInput& input) {
  auto data = duckdb::make_uniq<EsIndexBindData>();
  if (input.inputs.empty() || input.inputs[0].IsNull()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("index name cannot be NULL"));
  }
  data->index = input.inputs[0].GetValue<std::string>();
  if (input.inputs.size() >= 2 && !input.inputs[1].IsNull()) {
    data->body = input.inputs[1].GetValue<std::string>();
  }
  return data;
}

duckdb::unique_ptr<duckdb::FunctionData> EsAcknowledgedBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = BindIndexArgs(input);
  return_types.push_back(duckdb::LogicalType::BOOLEAN);
  names.push_back("acknowledged");
  return data;
}

uint32_t ResolveUintSetting(duckdb::ClientContext& context,
                            std::string_view name) {
  duckdb::Value value;
  auto r = context.TryGetCurrentSetting(std::string{name}, value);
  SDB_ASSERT(r, "missing DB-level default for setting: ", name);
  return value.GetValue<uint32_t>();
}

// The backfill-free tail of CREATE INDEX ... USING inverted: the table was
// created in the same call and is empty, so after StartTasks the first
// commit only seals the meta payload.
void CreateTextIndex(duckdb::ClientContext& context, ObjectId database_id,
                     std::string_view index,
                     std::span<const std::string_view> text_columns) {
  {
    duckdb::named_parameter_map_t options;
    options["template"] = duckdb::Value{"text"};
    options["locale"] = duckdb::Value{"en_US.UTF-8"};
    options["case"] = duckdb::Value{"lower"};
    options["stemming"] = duckdb::Value::BOOLEAN(false);
    options["accent"] = duckdb::Value::BOOLEAN(false);
    options["frequency"] = duckdb::Value::BOOLEAN(true);
    options["position"] = duckdb::Value::BOOLEAN(true);
    options["norm"] = duckdb::Value::BOOLEAN(true);
    pg::CreateTokenizer(GetSereneDBContext(context), kTextTokenizer, kEsSchema,
                        /*if_not_exists=*/true, options);
  }

  auto& catalog = catalog::GetCatalog();
  auto snapshot = catalog.GetCatalogSnapshot();
  auto table = snapshot->GetTable(catalog::NoAccessCheck(), database_id, kEsSchema, index);
  SDB_ASSERT(table);

  std::vector<catalog::CreateIndexColumn> idx_columns;
  idx_columns.reserve(text_columns.size());
  for (const auto name : text_columns) {
    const auto& columns = table->Columns();
    auto it = absl::c_find_if(
      columns, [&](const auto& col) { return col.GetName() == name; });
    SDB_ASSERT(it != columns.end());
    idx_columns.push_back(catalog::CreateIndexColumn{
      .name = it->GetName(),
      .catalog_column = &*it,
      .opclass = std::string{kTextTokenizer},
    });
  }

  catalog::InvertedIndexOptions options{
    .row_group_size = ResolveUintSetting(context, "row_group_size"),
    .norm_row_group_size = ResolveUintSetting(context, "norm_row_group_size"),
    .refresh_interval_ms = ResolveUintSetting(context, "refresh_interval"),
    .compaction_interval_ms =
      ResolveUintSetting(context, "compaction_interval"),
    .cleanup_interval_step =
      ResolveUintSetting(context, "cleanup_interval_step"),
  };

  const auto index_name = absl::StrCat(index, kEsTextIndexSuffix);
  auto r = catalog.CreateInvertedIndex(
    catalog::NoAccessCheck(), context, database_id, kEsSchema, index, index_name, std::move(idx_columns),
    std::move(options), {.create_with_tombstone = true});
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  auto fresh = catalog.GetCatalogSnapshot();
  auto relation = fresh->GetRelation(catalog::NoAccessCheck(), database_id, kEsSchema, index_name);
  SDB_ASSERT(relation);
  auto storage =
    basics::downCast<const catalog::InvertedIndex>(*relation).GetData();
  SDB_ASSERT(storage);
  storage->StartTasks();
  storage->Refresh();
  storage->FinishCreation();
  r = catalog.RemoveTombstone(database_id, kEsSchema, index_name);
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
}

void EsCreateIndexExecute(duckdb::ClientContext& context,
                          duckdb::TableFunctionInput& input,
                          duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<EsOnceState>();
  if (state.done) {
    output.SetChildCardinality(0);
    return;
  }
  state.done = true;
  auto& data = input.bind_data->Cast<EsIndexBindData>();

  ValidateIndexName(data.index);
  auto request = ParseCreateIndexBody(data.index, data.body);

  auto& conn_ctx = GetSereneDBContext(context);
  const auto database_id = conn_ctx.GetDatabaseId();
  auto& catalog = catalog::GetCatalog();

  {
    auto schema = std::make_shared<catalog::Schema>(
      conn_ctx.GetRoleId(), database_id, ObjectId{}, kEsSchema);
    auto r = catalog.CreateSchema(catalog::NoAccessCheck(), database_id, std::move(schema));
    if (!r.ok() && !r.is(ERROR_SERVER_DUPLICATE_NAME)) {
      SDB_THROW(std::move(r));
    }
  }

  catalog::CreateTableOptions options;
  options.name = data.index;
  std::vector<std::string_view> text_columns;

  auto add_column = [&](std::string_view name,
                        duckdb::LogicalType type) -> catalog::Column& {
    return options.columns.emplace_back(ObjectId{}, catalog::NextId(), name,
                                        std::move(type));
  };

  auto& id_column = add_column(kIdColumn, duckdb::LogicalType::VARCHAR);
  options.pk_columns.push_back(id_column.GetId());
  {
    // PK implies NOT NULL, mirroring CREATE TABLE's constraint expansion.
    auto col_ref =
      duckdb::make_uniq<duckdb::ColumnRefExpression>(std::string{kIdColumn});
    auto is_not_null = duckdb::make_uniq<duckdb::OperatorExpression>(
      duckdb::ExpressionType::OPERATOR_IS_NOT_NULL, std::move(col_ref));
    options.check_constraints.push_back(catalog::CheckConstraint{
      ObjectId{}, catalog::NextId(),
      absl::StrCat(data.index, "_", kIdColumn, "_not_null"),
      std::make_shared<ColumnExpr>(std::move(is_not_null))});
  }
  for (const auto& [field, mapping] : request.mappings.properties) {
    ValidateFieldName(data.index, field);
    add_column(field, EsTypeToLogical(data.index, field, mapping.type));
    if (mapping.type == "text") {
      text_columns.push_back(field);
    }
  }
  add_column(kSourceColumn, duckdb::LogicalType::VARCHAR);

  auto r = catalog.CreateTable(catalog::NoAccessCheck(), database_id, kEsSchema, std::move(options), {});
  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_TABLE),
                    ERR_MSG("index [", data.index, "] already exists"));
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  if (!text_columns.empty()) {
    CreateTextIndex(context, database_id, data.index, text_columns);
  }

  output.SetChildCardinality(1);
  output.SetValue(0, 0, duckdb::Value::BOOLEAN(true));
}

void EsDropIndexExecute(duckdb::ClientContext& context,
                        duckdb::TableFunctionInput& input,
                        duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<EsOnceState>();
  if (state.done) {
    output.SetChildCardinality(0);
    return;
  }
  state.done = true;
  auto& data = input.bind_data->Cast<EsIndexBindData>();

  ValidateIndexName(data.index);

  auto& conn_ctx = GetSereneDBContext(context);
  auto& catalog = catalog::GetCatalog();
  auto r =
    catalog.DropTable(catalog::NoAccessCheck(), conn_ctx.GetDatabase(), kEsSchema, data.index, true);
  if (r.is(ERROR_SERVER_ILLEGAL_NAME) ||
      r.is(ERROR_SERVER_OBJECT_TYPE_MISMATCH)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("no such index [", data.index, "]"));
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  output.SetChildCardinality(1);
  output.SetValue(0, 0, duckdb::Value::BOOLEAN(true));
}

duckdb::unique_ptr<duckdb::FunctionData> EsMappingBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = BindIndexArgs(input);
  return_types.push_back(duckdb::LogicalType::VARCHAR);
  names.push_back("mappings");
  return data;
}

void EsMappingExecute(duckdb::ClientContext& context,
                      duckdb::TableFunctionInput& input,
                      duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<EsOnceState>();
  if (state.done) {
    output.SetChildCardinality(0);
    return;
  }
  state.done = true;
  auto& data = input.bind_data->Cast<EsIndexBindData>();

  auto& conn_ctx = GetSereneDBContext(context);
  const auto database_id = conn_ctx.GetDatabaseId();
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  auto table = snapshot->GetTable(catalog::NoAccessCheck(), database_id, kEsSchema, data.index);
  if (!table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("no such index [", data.index, "]"));
  }

  containers::FlatHashSet<catalog::Column::Id> inverted_columns;
  for (const auto& index : snapshot->GetIndexesByRelation(table->GetId())) {
    if (index->GetType() != catalog::ObjectType::InvertedIndex) {
      continue;
    }
    for (const auto id : index->GetColumns()) {
      inverted_columns.insert(id);
    }
  }

  simdjson::builder::string_builder sb;
  sb.append_raw(R"({"properties":{)");
  bool first = true;
  for (const auto& column : table->Columns()) {
    const auto name = column.GetName();
    if (name == kIdColumn || name == kSourceColumn) {
      continue;
    }
    const auto es_type =
      LogicalToEsType(column.type, inverted_columns.contains(column.GetId()));
    if (es_type.empty()) {
      continue;
    }
    if (!first) {
      sb.append_comma();
    }
    first = false;
    sb.escape_and_append_with_quotes(name);
    sb.append_raw(R"(:{"type":")");
    sb.append_raw(es_type);
    sb.append_raw(R"("})");
  }
  sb.append_raw("}}");

  output.SetChildCardinality(1);
  output.SetValue(0, 0, duckdb::Value{std::string{sb.view().value()}});
}

duckdb::unique_ptr<duckdb::FunctionData> EsCatIndicesBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  return_types.push_back(duckdb::LogicalType::VARCHAR);
  names.push_back("index");
  return_types.push_back(duckdb::LogicalType::BIGINT);
  names.push_back("docs_count");
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

struct EsCatIndicesState final : duckdb::GlobalTableFunctionState {
  std::vector<std::pair<std::string, uint64_t>> rows;
  size_t offset = 0;
  bool loaded = false;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<EsCatIndicesState>();
  }
};

void EsCatIndicesExecute(duckdb::ClientContext& context,
                         duckdb::TableFunctionInput& input,
                         duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<EsCatIndicesState>();
  if (!state.loaded) {
    state.loaded = true;
    auto& conn_ctx = GetSereneDBContext(context);
    const auto database_id = conn_ctx.GetDatabaseId();
    auto snapshot = conn_ctx.EnsureCatalogSnapshot();
    if (snapshot->GetSchema(database_id, kEsSchema)) {
      for (const auto& table : snapshot->GetTables(database_id, kEsSchema)) {
        uint64_t docs_count = 0;
        for (const auto& index :
             snapshot->GetIndexesByRelation(table->GetId())) {
          if (!index ||
              index->GetType() != catalog::ObjectType::InvertedIndex) {
            continue;
          }
          auto storage =
            basics::downCast<const catalog::InvertedIndex>(*index).GetData();
          if (auto index_snapshot =
                storage ? storage->GetInvertedIndexSnapshot() : nullptr) {
            docs_count = index_snapshot->reader.live_docs_count();
          }
          break;
        }
        state.rows.emplace_back(table->GetName(), docs_count);
      }
      absl::c_sort(state.rows);
    }
  }

  const auto n =
    std::min<size_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.offset);
  for (size_t i = 0; i < n; ++i) {
    const auto& [index, docs_count] = state.rows[state.offset + i];
    output.SetValue(0, i, duckdb::Value{index});
    output.SetValue(1, i, duckdb::Value::BIGINT(docs_count));
  }
  state.offset += n;
  output.SetChildCardinality(n);
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

// ES caps _id at 512 bytes; the empty string is rejected the same way.
void ValidateDocId(std::string_view id) {
  if (id.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("if _id is specified it must not be empty"));
  }
  if (id.size() > 512) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("id [", id,
                            "] is too long, must be no longer "
                            "than 512 bytes but was: ",
                            id.size()));
  }
}

struct EsWriteBindData final : duckdb::TableFunctionData {
  std::string index;
  std::string id;
  std::string body;
  // Keeps the column-name string_views in field_columns alive.
  std::shared_ptr<catalog::Table> table;
  containers::FlatHashMap<std::string_view, size_t> field_columns;
  size_t id_column = 0;
  size_t source_column = 0;
};

// The function's output schema IS the target table's schema, so the handler's
// `INSERT INTO "es"."<index>" SELECT * FROM es_*(...)` lines up by position.
duckdb::unique_ptr<EsWriteBindData> BindWriteTarget(
  duckdb::ClientContext& context, const duckdb::Value& index_arg,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = duckdb::make_uniq<EsWriteBindData>();
  if (index_arg.IsNull()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("index name cannot be NULL"));
  }
  data->index = index_arg.GetValue<std::string>();

  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  data->table =
    snapshot->GetTable(catalog::NoAccessCheck(), conn_ctx.GetDatabaseId(), kEsSchema, data->index);
  if (!data->table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("no such index [", data->index, "]"));
  }
  const auto& columns = data->table->Columns();
  for (size_t i = 0; i < columns.size(); ++i) {
    const auto& column = columns[i];
    return_types.push_back(column.type);
    names.push_back(std::string{column.GetName()});
    if (column.GetName() == kIdColumn) {
      data->id_column = i;
    } else if (column.GetName() == kSourceColumn) {
      data->source_column = i;
    } else {
      data->field_columns.emplace(column.GetName(), i);
    }
  }
  return data;
}

[[noreturn]] void ThrowFieldParseError(std::string_view index,
                                       std::string_view field,
                                       std::string_view expected) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                  ERR_MSG("failed to parse field [", field, "] for index [",
                          index, "]: expected ", expected));
}

// ES default coercion: numeric fields accept JSON strings ("42") and
// truncate floating points; real corpora (e.g. rally's pmc) depend on it.
int64_t CoerceInt64(simdjson::ondemand::value value, std::string_view index,
                    std::string_view field) {
  int64_t v = 0;
  if (value.get_int64().get(v) == simdjson::SUCCESS) {
    return v;
  }
  if (double d = 0; value.get_double().get(d) == simdjson::SUCCESS) {
    return static_cast<int64_t>(d);
  }
  if (std::string_view text;
      value.get_string().get(text) == simdjson::SUCCESS) {
    if (absl::SimpleAtoi(text, &v)) {
      return v;
    }
    if (double d = 0; absl::SimpleAtod(text, &d)) {
      return static_cast<int64_t>(d);
    }
  }
  ThrowFieldParseError(index, field, "an integer");
}

double CoerceDouble(simdjson::ondemand::value value, std::string_view index,
                    std::string_view field) {
  double v = 0;
  if (value.get_double().get(v) == simdjson::SUCCESS) {
    return v;
  }
  if (std::string_view text;
      value.get_string().get(text) == simdjson::SUCCESS &&
      absl::SimpleAtod(text, &v)) {
    return v;
  }
  ThrowFieldParseError(index, field, "a number");
}

// false = the value reduces to NULL (ES treats an empty string as null for
// every non-string field type).
bool WriteDocField(duckdb::Vector& vec, duckdb::idx_t row,
                   simdjson::ondemand::value value, std::string_view index,
                   std::string_view field) {
  if (vec.GetType().id() != duckdb::LogicalTypeId::VARCHAR) {
    if (std::string_view text;
        value.get_string().get(text) == simdjson::SUCCESS && text.empty()) {
      return false;
    }
  }
  switch (vec.GetType().id()) {
    case duckdb::LogicalTypeId::VARCHAR: {
      std::string_view text;
      if (value.get_string().get(text) != simdjson::SUCCESS) {
        ThrowFieldParseError(index, field, "a string");
      }
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[row] =
        duckdb::StringVector::AddString(vec, text.data(), text.size());
      return true;
    }
    case duckdb::LogicalTypeId::BIGINT: {
      duckdb::FlatVector::GetDataMutable<int64_t>(vec)[row] =
        CoerceInt64(value, index, field);
      return true;
    }
    case duckdb::LogicalTypeId::INTEGER: {
      const int64_t v = CoerceInt64(value, index, field);
      if (v < std::numeric_limits<int32_t>::min() ||
          v > std::numeric_limits<int32_t>::max()) {
        ThrowFieldParseError(index, field, "a 32-bit integer");
      }
      duckdb::FlatVector::GetDataMutable<int32_t>(vec)[row] =
        static_cast<int32_t>(v);
      return true;
    }
    case duckdb::LogicalTypeId::DOUBLE: {
      duckdb::FlatVector::GetDataMutable<double>(vec)[row] =
        CoerceDouble(value, index, field);
      return true;
    }
    case duckdb::LogicalTypeId::FLOAT: {
      duckdb::FlatVector::GetDataMutable<float>(vec)[row] =
        static_cast<float>(CoerceDouble(value, index, field));
      return true;
    }
    case duckdb::LogicalTypeId::BOOLEAN: {
      bool v = false;
      if (value.get_bool().get(v) == simdjson::SUCCESS) {
        duckdb::FlatVector::GetDataMutable<bool>(vec)[row] = v;
        return true;
      }
      if (std::string_view text;
          value.get_string().get(text) == simdjson::SUCCESS &&
          (text == "true" || text == "false")) {
        duckdb::FlatVector::GetDataMutable<bool>(vec)[row] = text == "true";
        return true;
      }
      ThrowFieldParseError(index, field, "a boolean");
    }
    case duckdb::LogicalTypeId::TIMESTAMP: {
      // ES default date leniency: ISO-8601 (offsets applied, named zones
      // rejected) or epoch milliseconds.
      duckdb::timestamp_t ts;
      if (simdjson::ondemand::json_type t;
          value.type().get(t) == simdjson::SUCCESS &&
          t == simdjson::ondemand::json_type::number) {
        int64_t ms = 0;
        if (value.get_int64().get(ms) != simdjson::SUCCESS) {
          ThrowFieldParseError(index, field, "epoch milliseconds");
        }
        ts = duckdb::Timestamp::FromEpochMsPossiblyInfinite(ms);
      } else {
        std::string_view text;
        bool has_offset = false;
        duckdb::string_t tz{nullptr, 0};
        if (value.get_string().get(text) != simdjson::SUCCESS ||
            duckdb::Timestamp::TryConvertTimestampTZ(
              text.data(), text.size(), ts, /*use_offset=*/true, has_offset,
              tz) != duckdb::TimestampCastResult::SUCCESS ||
            tz.GetSize() != 0) {
          ThrowFieldParseError(index, field,
                               "an ISO-8601 date or epoch milliseconds");
        }
      }
      duckdb::FlatVector::GetDataMutable<duckdb::timestamp_t>(vec)[row] = ts;
      return true;
    }
    default:
      ThrowFieldParseError(index, field, "a supported type");
  }
}

void WriteDocRow(const EsWriteBindData& bind, simdjson::ondemand::document& doc,
                 std::string_view id, std::string_view source,
                 duckdb::DataChunk& output, duckdb::idx_t row) {
  auto& id_vec = output.data[bind.id_column];
  duckdb::FlatVector::GetDataMutable<duckdb::string_t>(id_vec)[row] =
    duckdb::StringVector::AddString(id_vec, id.data(), id.size());
  auto& source_vec = output.data[bind.source_column];
  duckdb::FlatVector::GetDataMutable<duckdb::string_t>(source_vec)[row] =
    duckdb::StringVector::AddString(source_vec, source.data(), source.size());

  // Unmapped-in-doc columns stay NULL; matched fields flip back to valid.
  for (const auto& [name, column] : bind.field_columns) {
    duckdb::FlatVector::ValidityMutable(output.data[column]).SetInvalid(row);
  }

  simdjson::ondemand::object object;
  if (doc.get_object().get(object) != simdjson::SUCCESS) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("document for index [", bind.index, "] must be a JSON object"));
  }
  for (auto field : object) {
    std::string_view key;
    if (field.unescaped_key().get(key) != simdjson::SUCCESS) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
        ERR_MSG("malformed document for index [", bind.index, "]"));
    }
    const auto it = bind.field_columns.find(key);
    if (it == bind.field_columns.end()) {
      continue;  // unmapped field: lives only in _source
    }
    simdjson::ondemand::value value;
    if (field.value().get(value) != simdjson::SUCCESS) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
        ERR_MSG("malformed document for index [", bind.index, "]"));
    }
    if (bool is_null = false;
        value.is_null().get(is_null) == simdjson::SUCCESS && is_null) {
      continue;
    }
    if (WriteDocField(output.data[it->second], row, value, bind.index, key)) {
      duckdb::FlatVector::ValidityMutable(output.data[it->second])
        .SetValid(row);
    }
  }
  // Trailing content would make the stored _source invalid JSON (and corrupt
  // the GET _doc envelope it gets embedded into verbatim).
  if (!doc.at_end()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("document for index [", bind.index, "] has trailing content"));
  }
}

// --- es_doc(index, id, body): one document as one row ----------------------

duckdb::unique_ptr<duckdb::FunctionData> EsDocBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = BindWriteTarget(context, input.inputs[0], return_types, names);
  if (input.inputs[1].IsNull() || input.inputs[2].IsNull()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("es_doc id and body cannot be NULL"));
  }
  data->id = input.inputs[1].GetValue<std::string>();
  data->body = input.inputs[2].GetValue<std::string>();
  ValidateDocId(data->id);
  if (data->body.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("request body is required"));
  }
  return data;
}

void EsDocExecute(duckdb::ClientContext& context,
                  duckdb::TableFunctionInput& input,
                  duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<EsOnceState>();
  if (state.done) {
    output.SetChildCardinality(0);
    return;
  }
  state.done = true;
  auto& data = input.bind_data->Cast<EsWriteBindData>();

  simdjson::padded_string padded{data.body};
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("failed to parse document for index [", data.index, "]"));
  }
  WriteDocRow(data, doc, data.id, data.body, output, 0);
  output.SetChildCardinality(1);
}

// --- es_bulk(index, ndjson): action/document line pairs as rows ------------

struct EsBulkState final : duckdb::GlobalTableFunctionState {
  size_t pos = 0;
  size_t line = 0;
  bool finished = false;
  simdjson::ondemand::parser parser;
  // Reused line copy with simdjson padding; _id is copied out before the
  // buffer is reused for the document line.
  std::string padded;
  std::string id;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<EsBulkState>();
  }
};

std::string_view NextBulkLine(std::string_view body, size_t& pos) {
  const size_t start = pos;
  const size_t nl = body.find('\n', start);
  size_t end = body.size();
  if (nl == std::string_view::npos) {
    pos = body.size();
  } else {
    end = nl;
    pos = nl + 1;
  }
  auto out = body.substr(start, end - start);
  if (!out.empty() && out.back() == '\r') {
    out.remove_suffix(1);
  }
  return out;
}

[[noreturn]] void ThrowMalformedAction(size_t line, std::string_view detail) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("Malformed action/metadata line [", line, "]: ", detail));
}

duckdb::unique_ptr<duckdb::FunctionData> EsBulkBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = BindWriteTarget(context, input.inputs[0], return_types, names);
  if (input.inputs[1].IsNull()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("es_bulk body cannot be NULL"));
  }
  data->body = input.inputs[1].GetValue<std::string>();
  if (data->body.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("no requests added"));
  }
  return data;
}

void EsBulkExecute(duckdb::ClientContext& context,
                   duckdb::TableFunctionInput& input,
                   duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<EsBulkState>();
  auto& data = input.bind_data->Cast<EsWriteBindData>();
  const std::string_view body = data.body;
  std::string* sink = GetSereneDBContext(context).GetResponseSink();

  auto parse_line = [&](std::string_view text,
                        simdjson::ondemand::document& doc) {
    state.padded.assign(text);
    state.padded.append(simdjson::SIMDJSON_PADDING, ' ');
    return state.parser
             .iterate(state.padded.data(), text.size(), state.padded.size())
             .get(doc) == simdjson::SUCCESS;
  };

  duckdb::idx_t row = 0;
  while (row < STANDARD_VECTOR_SIZE && !state.finished) {
    if (state.pos >= body.size()) {
      state.finished = true;
      break;
    }
    const auto action_line = NextBulkLine(body, state.pos);
    ++state.line;
    simdjson::ondemand::document action_doc;
    simdjson::ondemand::object action;
    if (!parse_line(action_line, action_doc) ||
        action_doc.get_object().get(action) != simdjson::SUCCESS) {
      ThrowMalformedAction(state.line, "expected a JSON object");
    }

    std::string_view op;
    state.id.clear();
    for (auto field : action) {
      std::string_view key;
      if (field.unescaped_key().get(key) != simdjson::SUCCESS || !op.empty()) {
        ThrowMalformedAction(state.line, "expected a single action");
      }
      if (key == "index") {
        op = "index";
      } else if (key == "create") {
        op = "create";
      } else {
        ThrowMalformedAction(state.line,
                             absl::StrCat("expected one of [create, index] but "
                                          "found [",
                                          key, "]"));
      }
      simdjson::ondemand::object params;
      if (field.value().get_object().get(params) != simdjson::SUCCESS) {
        ThrowMalformedAction(state.line, "expected an object value");
      }
      for (auto param : params) {
        std::string_view param_key;
        if (param.unescaped_key().get(param_key) != simdjson::SUCCESS) {
          ThrowMalformedAction(state.line, "malformed parameters");
        }
        if (param_key == "_id") {
          std::string_view id;
          if (param.value().get_string().get(id) != simdjson::SUCCESS) {
            ThrowMalformedAction(state.line, "_id must be a string");
          }
          ValidateDocId(id);
          state.id.assign(id);
        } else if (param_key == "_index") {
          std::string_view explicit_index;
          if (param.value().get_string().get(explicit_index) !=
                simdjson::SUCCESS ||
              explicit_index != data.index) {
            ThrowMalformedAction(state.line,
                                 absl::StrCat("_index must match the request "
                                              "index [",
                                              data.index, "]"));
          }
        }
        // routing/version/pipeline/...: accepted and ignored.
      }
    }
    if (op.empty()) {
      ThrowMalformedAction(state.line, "expected FIELD_NAME");
    }

    if (state.pos >= body.size()) {
      ThrowMalformedAction(state.line + 1, "document is missing");
    }
    const auto doc_line = NextBulkLine(body, state.pos);
    ++state.line;
    simdjson::ondemand::document doc;
    if (!parse_line(doc_line, doc)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                      ERR_MSG("failed to parse document on line [", state.line,
                              "] for index [", data.index, "]"));
    }
    if (state.id.empty()) {
      state.id = GenerateEsDocId();
    }
    // doc_line views data.body, so _source survives the padded-buffer reuse.
    WriteDocRow(data, doc, state.id, doc_line, output, row);
    ++row;

    if (sink) {
      if (!sink->empty()) {
        sink->push_back(',');
      }
      absl::StrAppend(sink, "{\"", op, "\":{\"_index\":");
      AppendJsonString(*sink, data.index);
      absl::StrAppend(sink, ",\"_id\":");
      AppendJsonString(*sink, state.id);
      absl::StrAppend(
        sink, R"(,"_version":1,"result":"created","_shards":{"total":1,)"
              R"("successful":1,"failed":0},"_seq_no":0,"_primary_term":1,)"
              R"("status":201}})");
    }
  }
  output.SetChildCardinality(row);
}

// --- es_refresh(index): commit inverted shards so writes become searchable -

void EsRefreshExecute(duckdb::ClientContext& context,
                      duckdb::TableFunctionInput& input,
                      duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<EsOnceState>();
  if (state.done) {
    output.SetChildCardinality(0);
    return;
  }
  state.done = true;
  auto& data = input.bind_data->Cast<EsIndexBindData>();

  auto& conn_ctx = GetSereneDBContext(context);
  const auto database_id = conn_ctx.GetDatabaseId();
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  auto refresh_table = [&](const catalog::Table& table) {
    for (const auto& index : snapshot->GetIndexesByRelation(table.GetId())) {
      if (!index || index->GetType() != catalog::ObjectType::InvertedIndex) {
        continue;
      }
      if (auto storage =
            basics::downCast<const catalog::InvertedIndex>(*index).GetData()) {
        storage->Refresh();
      }
    }
  };

  if (data.index.empty()) {
    if (snapshot->GetSchema(database_id, kEsSchema)) {
      for (const auto& table : snapshot->GetTables(database_id, kEsSchema)) {
        refresh_table(*table);
      }
    }
  } else {
    auto table = snapshot->GetTable(catalog::NoAccessCheck(), database_id, kEsSchema, data.index);
    if (!table) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                      ERR_MSG("no such index [", data.index, "]"));
    }
    refresh_table(*table);
  }

  output.SetChildCardinality(1);
  output.SetValue(0, 0, duckdb::Value::BOOLEAN(true));
}

duckdb::unique_ptr<duckdb::FunctionData> EsRefreshBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = duckdb::make_uniq<EsIndexBindData>();
  if (!input.inputs.empty() && !input.inputs[0].IsNull()) {
    data->index = input.inputs[0].GetValue<std::string>();
  }
  return_types.push_back(duckdb::LogicalType::BOOLEAN);
  names.push_back("acknowledged");
  return data;
}

}  // namespace

std::string GenerateEsDocId() {
  // ES-style ids: 20 chars of unpadded base64url. 15 bytes from a v4 UUID
  // (the few fixed version bits are an acceptable entropy loss).
  const auto uuid = duckdb::UUID::GenerateRandomUUID();
  char bytes[15];
  std::memcpy(bytes, &uuid.lower, 8);
  std::memcpy(bytes + 8, &uuid.upper, 7);
  return absl::WebSafeBase64Escape(absl::string_view{bytes, sizeof bytes});
}

void RegisterEsFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  const auto kVarchar = duckdb::LogicalType::VARCHAR;

  loader.RegisterFunction(duckdb::TableFunction{"es_create_index",
                                                {kVarchar, kVarchar},
                                                EsCreateIndexExecute,
                                                EsAcknowledgedBind,
                                                EsOnceState::Init});
  loader.RegisterFunction(duckdb::TableFunction{"es_drop_index",
                                                {kVarchar},
                                                EsDropIndexExecute,
                                                EsAcknowledgedBind,
                                                EsOnceState::Init});
  loader.RegisterFunction(duckdb::TableFunction{"es_mapping",
                                                {kVarchar},
                                                EsMappingExecute,
                                                EsMappingBind,
                                                EsOnceState::Init});
  loader.RegisterFunction(duckdb::TableFunction{"es_cat_indices",
                                                {},
                                                EsCatIndicesExecute,
                                                EsCatIndicesBind,
                                                EsCatIndicesState::Init});
  loader.RegisterFunction(duckdb::TableFunction{"es_doc",
                                                {kVarchar, kVarchar, kVarchar},
                                                EsDocExecute,
                                                EsDocBind,
                                                EsOnceState::Init});
  loader.RegisterFunction(duckdb::TableFunction{"es_bulk",
                                                {kVarchar, kVarchar},
                                                EsBulkExecute,
                                                EsBulkBind,
                                                EsBulkState::Init});
  loader.RegisterFunction(duckdb::TableFunction{"es_refresh",
                                                {kVarchar},
                                                EsRefreshExecute,
                                                EsRefreshBind,
                                                EsOnceState::Init});
}

}  // namespace sdb::connector
