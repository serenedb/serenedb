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
#include <absl/strings/str_cat.h>

#include <simdjson.h>

#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>

#include <map>

#include "basics/assert.h"
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
#include "basics/containers/flat_hash_set.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kEsSchema = "es";
constexpr std::string_view kIdColumn = "_id";
constexpr std::string_view kSourceColumn = "_source";
// "$" is rejected by index-name validation, so the derived name can never
// collide with another ES index's table.
constexpr std::string_view kTextIndexSuffix = "$text";

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
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Failed to parse mapping: invalid field name [", name,
              "] for index [", index, "]"));
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
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("Failed to parse mapping: No handler for type [",
                          es_type, "] declared on field [", field,
                          "] for index [", index, "]"));
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
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    ERR_MSG("Failed to parse mapping for index [", index,
                            "]: ", e.what()));
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
    throw duckdb::BinderException("index name cannot be NULL");
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
  auto& catalog = catalog::CatalogFeature::instance().Global();
  auto snapshot = catalog.GetCatalogSnapshot();
  auto table = snapshot->GetTable(database_id, kEsSchema, index);
  SDB_ASSERT(table);

  std::vector<catalog::CreateIndexColumn> idx_columns;
  idx_columns.reserve(text_columns.size());
  for (const auto name : text_columns) {
    const auto& columns = table->Columns();
    auto it = absl::c_find_if(
      columns, [&](const auto& col) { return col.GetName() == name; });
    SDB_ASSERT(it != columns.end());
    idx_columns.push_back(catalog::CreateIndexColumn{
      .catalog_column = &*it,
      .name = it->GetName(),
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

  const auto index_name = absl::StrCat(index, kTextIndexSuffix);
  auto r = catalog.CreateInvertedIndex(database_id, kEsSchema, index,
                                       index_name, std::move(idx_columns),
                                       std::move(options),
                                       {.create_with_tombstone = true});
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  auto fresh = catalog.GetCatalogSnapshot();
  auto relation = fresh->GetRelation(database_id, kEsSchema, index_name);
  SDB_ASSERT(relation);
  auto shard = fresh->GetIndexShard(relation->GetId());
  SDB_ASSERT(shard);
  auto& inverted = basics::downCast<search::InvertedIndexShard>(*shard);
  inverted.StartTasks();
  std::ignore = inverted.CommitWait().Get().Ok();
  inverted.FinishCreation();
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
  auto& catalog = catalog::CatalogFeature::instance().Global();

  {
    auto schema = std::make_shared<catalog::Schema>(
      database_id, catalog::SchemaOptions{.name = std::string{kEsSchema}});
    auto r = catalog.CreateSchema(database_id, std::move(schema));
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

  auto r = catalog.CreateTable(database_id, kEsSchema, std::move(options), {});
  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_TABLE),
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
  auto& catalog = catalog::CatalogFeature::instance().Global();
  auto r =
    catalog.DropTable(conn_ctx.GetDatabase(), kEsSchema, data.index, true);
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

  auto table = snapshot->GetTable(database_id, kEsSchema, data.index);
  if (!table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("no such index [", data.index, "]"));
  }

  containers::FlatHashSet<catalog::Column::Id> inverted_columns;
  for (const auto& index : snapshot->GetIndexesByRelation(table->GetId())) {
    if (index->GetType() != catalog::ObjectType::InvertedIndex) {
      continue;
    }
    for (const auto id : index->GetColumnIds()) {
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
        auto shard = snapshot->GetTableShard(table->GetId());
        state.rows.emplace_back(
          table->GetName(), shard ? shard->GetTableStats().num_rows : 0);
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

}  // namespace

void RegisterEsFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  const auto kVarchar = duckdb::LogicalType::VARCHAR;

  loader.RegisterFunction(duckdb::TableFunction{
    "es_create_index", {kVarchar, kVarchar}, EsCreateIndexExecute,
    EsAcknowledgedBind, EsOnceState::Init});
  loader.RegisterFunction(duckdb::TableFunction{"es_drop_index",
                                                {kVarchar},
                                                EsDropIndexExecute,
                                                EsAcknowledgedBind,
                                                EsOnceState::Init});
  loader.RegisterFunction(duckdb::TableFunction{
    "es_mapping", {kVarchar}, EsMappingExecute, EsMappingBind,
    EsOnceState::Init});
  loader.RegisterFunction(duckdb::TableFunction{"es_cat_indices",
                                                {},
                                                EsCatIndicesExecute,
                                                EsCatIndicesBind,
                                                EsCatIndicesState::Init});
}

}  // namespace sdb::connector
