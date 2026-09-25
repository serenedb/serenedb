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
#include <absl/functional/function_ref.h>
#include <absl/strings/escaping.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <cstring>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/enums/statement_type.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/serializer.hpp>
#include <map>
#include <string_view>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "connector/duckdb_client_state.h"
#include "connector/inverted_index_bind.h"
#include "connector/inverted_store_index.h"
#include "pg/commands/create_tsdictionary.h"
#include "pg/connection_context.h"
#include "search/inverted_index_storage.h"
#include "server/utils/simdjson_sink.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kEsSchema = "es";
constexpr std::string_view kIdColumn = "_id";
constexpr std::string_view kSourceColumn = "_source";
// The analyzer behind every text property, mimicking ES's standard analyzer
// (tokenize + lowercase, no stemming); frequency/position/norm make phrase
// queries and scoring possible. Created lazily in the es schema.
constexpr std::string_view kTextTokenizer = "standard";

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> EsSchema(
  duckdb::ClientContext& context) {
  auto& db_catalog = duckdb::Catalog::GetCatalog(
    context, duckdb::Identifier{GetSereneDBContext(context).GetDatabase()});
  return db_catalog.GetSchema(context, duckdb::Identifier{kEsSchema},
                              duckdb::OnEntryNotFound::RETURN_NULL);
}

duckdb::optional_ptr<duckdb::TableCatalogEntry> FindEsTable(
  duckdb::ClientContext& context, std::string_view index) {
  return duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
    context,
    duckdb::QualifiedName{
      duckdb::Identifier{GetSereneDBContext(context).GetDatabase()},
      duckdb::Identifier{kEsSchema}, duckdb::Identifier{index}},
    duckdb::OnEntryNotFound::RETURN_NULL);
}

// Every inverted index over `table`, taken off the schema entry the table
// already names: the index set is the table's own sibling.
void VisitInvertedIndexes(
  duckdb::ClientContext& context, duckdb::TableCatalogEntry& table,
  absl::FunctionRef<void(duckdb::DuckIndexEntry&)> visitor) {
  std::vector<duckdb::reference<duckdb::DuckIndexEntry>> indexes;
  table.ParentSchema(context).Scan(
    context, duckdb::CatalogType::INDEX_ENTRY,
    [&](duckdb::CatalogEntry& entry) {
      auto& index = entry.Cast<duckdb::DuckIndexEntry>();
      if (index.GetTableName() == table.name &&
          index.index_type == InvertedStoreIndex::kTypeName) {
        indexes.emplace_back(index);
      }
    });
  for (auto& index : indexes) {
    visitor(index.get());
  }
}

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
  simdjson::padded_string padded{body};
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  if (const auto ec = parser.iterate(padded).get(doc);
      ec != simdjson::SUCCESS) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    ERR_MSG("Failed to parse mapping for index [", index,
                            "]: ", simdjson::error_message(ec)));
  }
  try {
    utils::JsonSource source{doc};
    irs::utils::ReadObject(source, request);
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
  if (input.inputs[0].IsNull()) {
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
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = BindIndexArgs(input);
  if (input.binder) {
    input.binder->GetStatementProperties().RegisterDBModify(
      duckdb::Catalog::GetCatalog(
        context, duckdb::Identifier{GetSereneDBContext(context).GetDatabase()}),
      context,
      duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY |
        duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  }
  return_types.push_back(duckdb::LogicalType::BOOLEAN);
  names.push_back("acknowledged");
  return data;
}

// The backfill-free tail of CREATE INDEX ... USING inverted: the table was
// created in the same call and is empty, so after StartTasks the first
// commit only seals the meta payload.
void CreateTextIndex(duckdb::ClientContext& context,
                     duckdb::TableCatalogEntry& table,
                     std::span<const std::string_view> text_columns) {
  {
    duckdb::named_parameter_map_t features;
    features["frequency"] = duckdb::Value::BOOLEAN(true);
    features["position"] = duckdb::Value::BOOLEAN(true);
    features["norm"] = duckdb::Value::BOOLEAN(true);
    pg::CreateTokenizer(
      GetSereneDBContext(context),
      duckdb::QualifiedName{duckdb::Identifier{}, duckdb::Identifier{kEsSchema},
                            duckdb::Identifier{kTextTokenizer}},
      /*if_not_exists=*/true, features,
      "split_text(case := 'lower') | "
      "normalize_tokens('en_US.UTF-8', accent := false)");
  }

  const auto index_name =
    absl::StrCat(table.name.GetIdentifierName(), kEsTextIndexSuffix);
  duckdb::CreateIndexInfo info;
  info.SetSchema(duckdb::Identifier{kEsSchema});
  info.SetIndexName(duckdb::Identifier{index_name});
  info.table = table.name;
  info.index_type = InvertedStoreIndex::kTypeName;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> bound_expressions;
  for (const auto name : text_columns) {
    const duckdb::Identifier column{std::string{name}};
    SDB_ASSERT(table.GetColumns().ColumnExists(column));
    const auto& definition = table.GetColumns().GetColumn(column);
    bound_expressions.emplace_back(
      duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
        definition.Type(), duckdb::ColumnBinding{
                             duckdb::TableIndex{0},
                             duckdb::ProjectionIndex{info.column_ids.size()}}));
    info.column_ids.emplace_back(definition.Physical().index);
    info.parsed_expressions.emplace_back(
      duckdb::make_uniq<duckdb::ColumnRefExpression>(column));
    info.column_opclasses.emplace_back(kTextTokenizer);
    info.column_opclass_options.emplace_back(std::nullopt);
  }
  catalog::BindInvertedIndexOptions(context, info.options, false);
  auto& schema = table.ParentSchema(context);
  auto entry = schema.CreateIndex(
    schema.ParentCatalog().GetCatalogTransaction(context), info, table);
  SDB_ASSERT(entry);
  auto& index_entry = entry->Cast<catalog::InvertedIndexEntry>();
  index_entry.SetConfig(BindInvertedIndexConfig(context, index_entry, table,
                                                bound_expressions,
                                                duckdb::LogicalType::INVALID));
  PublishInvertedIndex(context, index_entry, table, bound_expressions);
  const auto& storage = index_entry.Storage();
  SDB_ASSERT(storage);
  storage->StartTasks();
  storage->Refresh();
  storage->FinishCreation();
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

  // Through the database's own catalog: CREATE SCHEMA and CREATE TABLE are
  // duckdb's operations, and serenedb's are the same ones.
  auto& db_catalog = duckdb::Catalog::GetCatalog(
    context, duckdb::Identifier{conn_ctx.GetDatabase()});
  {
    duckdb::CreateSchemaInfo info;
    info.SetSchema(duckdb::Identifier{std::string{kEsSchema}});
    info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
    db_catalog.CreateSchema(db_catalog.GetCatalogTransaction(context), info);
  }

  auto options = duckdb::make_uniq<duckdb::CreateTableInfo>();
  options->SetTableName(duckdb::Identifier{data.index});
  options->SetSchema(duckdb::Identifier{kEsSchema});
  options->on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  std::vector<std::string_view> text_columns;

  auto add_column = [&](std::string_view name, duckdb::LogicalType type) {
    options->columns.AddColumn(
      duckdb::ColumnDefinition{duckdb::Identifier{name}, std::move(type)});
  };

  add_column(kIdColumn, duckdb::LogicalType::VARCHAR);
  options->constraints.emplace_back(duckdb::make_uniq<duckdb::UniqueConstraint>(
    duckdb::vector<duckdb::Identifier>{duckdb::Identifier{kIdColumn}},
    /*is_primary_key=*/true));
  for (const auto& [field, mapping] : request.mappings.properties) {
    ValidateFieldName(data.index, field);
    add_column(field, EsTypeToLogical(data.index, field, mapping.type));
    if (mapping.type == "text") {
      text_columns.push_back(field);
    }
  }
  add_column(kSourceColumn, duckdb::LogicalType::VARCHAR);

  auto table = db_catalog.CreateTable(context, std::move(options));
  if (!table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_TABLE),
                    ERR_MSG("index [", data.index, "] already exists"));
  }

  if (!text_columns.empty()) {
    CreateTextIndex(context, table->Cast<duckdb::TableCatalogEntry>(),
                    text_columns);
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
  auto& db_catalog = duckdb::Catalog::GetCatalog(
    context, duckdb::Identifier{conn_ctx.GetDatabase()});
  const duckdb::QualifiedName qname{db_catalog.GetName(),
                                    duckdb::Identifier{kEsSchema},
                                    duckdb::Identifier{data.index}};
  // ES "no such index" covers both a missing name and a name that resolves
  // to a non-table relation, so gate the drop on an actual table existing.
  if (!duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
        context, qname, duckdb::OnEntryNotFound::RETURN_NULL)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("no such index [", data.index, "]"));
  }
  duckdb::DropInfo drop;
  drop.type = duckdb::CatalogType::TABLE_ENTRY;
  drop.SetQualifiedName(qname);
  drop.cascade = true;
  drop.if_not_found = duckdb::OnEntryNotFound::RETURN_NULL;
  db_catalog.DropEntry(context, drop);

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

  auto table = FindEsTable(context, data.index);
  if (!table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("no such index [", data.index, "]"));
  }

  irs::containers::FlatHashSet<ColumnId> inverted_columns;
  VisitInvertedIndexes(context, *table, [&](duckdb::DuckIndexEntry& index) {
    for (const auto id : index.column_ids) {
      inverted_columns.insert(ColumnId{id});
    }
  });

  simdjson::builder::string_builder sb;
  sb.append_raw(R"({"properties":{)");
  bool first = true;
  for (const auto& column : table->GetColumns().Logical()) {
    const auto name = column.Name().GetIdentifierName();
    if (name == kIdColumn || name == kSourceColumn) {
      continue;
    }
    const auto es_type =
      LogicalToEsType(column.Type(), inverted_columns.contains(column.Oid()));
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
    // Collect-then-resolve: reading a table's indexes opens the schema's
    // index set from inside the relation set's own walk.
    std::vector<duckdb::reference<duckdb::TableCatalogEntry>> tables;
    if (auto schema = EsSchema(context)) {
      schema->Scan(
        context, duckdb::CatalogType::TABLE_ENTRY,
        [&](duckdb::CatalogEntry& entry) {
          if (entry.type == duckdb::CatalogType::TABLE_ENTRY) {
            tables.emplace_back(entry.Cast<duckdb::TableCatalogEntry>());
          }
        });
    }
    for (auto table : tables) {
      uint64_t docs_count = 0;
      bool first = true;
      VisitInvertedIndexes(
        context, table.get(), [&](duckdb::DuckIndexEntry& index) {
          if (!first) {
            return;
          }
          first = false;
          const auto& storage =
            index.Cast<catalog::InvertedIndexEntry>().Storage();
          if (auto snapshot =
                storage ? storage->GetInvertedIndexSnapshot() : nullptr) {
            docs_count = snapshot->reader.live_docs_count();
          }
        });
      state.rows.emplace_back(table.get().name.GetIdentifierName(), docs_count);
    }
    absl::c_sort(state.rows);
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
  irs::containers::FlatHashMap<std::string, size_t> field_columns;
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

  auto table = FindEsTable(context, data->index);
  if (!table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("no such index [", data->index, "]"));
  }
  size_t i = 0;
  for (const auto& column : table->GetColumns().Logical()) {
    const auto name = column.Name().GetIdentifierName();
    return_types.push_back(column.Type());
    names.emplace_back(name);
    if (name == kIdColumn) {
      data->id_column = i;
    } else if (name == kSourceColumn) {
      data->source_column = i;
    } else {
      data->field_columns.emplace(name, i);
    }
    ++i;
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

// A document field, read exactly once. An ondemand value cannot be re-read:
// every get_string() unescapes into the parser's string buffer again, so
// asking twice walks it off the end.
struct FieldValue {
  simdjson::ondemand::json_type type{};
  // Set when type is string; the only unescape of this value.
  std::string_view text;
};

FieldValue ReadFieldValue(simdjson::ondemand::value& value,
                          std::string_view index, std::string_view field) {
  FieldValue out;
  if (value.type().get(out.type) != simdjson::SUCCESS) {
    ThrowFieldParseError(index, field, "a JSON value");
  }
  if (out.type == simdjson::ondemand::json_type::string &&
      value.get_string().get(out.text) != simdjson::SUCCESS) {
    ThrowFieldParseError(index, field, "a string");
  }
  return out;
}

// ES default coercion: numeric fields accept JSON strings ("42") and
// truncate floating points; real corpora (e.g. rally's pmc) depend on it.
int64_t CoerceInt64(simdjson::ondemand::value& value, const FieldValue& read,
                    std::string_view index, std::string_view field) {
  if (read.type == simdjson::ondemand::json_type::number) {
    // One scan of the number, whichever of the three shapes it turns out to be.
    if (simdjson::ondemand::number parsed;
        value.get_number().get(parsed) == simdjson::SUCCESS) {
      if (parsed.is_int64()) {
        return parsed.get_int64();
      }
      if (parsed.is_uint64()) {
        return static_cast<int64_t>(parsed.get_uint64());
      }
      return static_cast<int64_t>(parsed.get_double());
    }
  } else if (read.type == simdjson::ondemand::json_type::string) {
    if (int64_t v = 0; absl::SimpleAtoi(read.text, &v)) {
      return v;
    }
    if (double d = 0; absl::SimpleAtod(read.text, &d)) {
      return static_cast<int64_t>(d);
    }
  }
  ThrowFieldParseError(index, field, "an integer");
}

double CoerceDouble(simdjson::ondemand::value& value, const FieldValue& read,
                    std::string_view index, std::string_view field) {
  if (read.type == simdjson::ondemand::json_type::number) {
    if (double v = 0; value.get_double().get(v) == simdjson::SUCCESS) {
      return v;
    }
  } else if (read.type == simdjson::ondemand::json_type::string) {
    if (double v = 0; absl::SimpleAtod(read.text, &v)) {
      return v;
    }
  }
  ThrowFieldParseError(index, field, "a number");
}

// false = the value reduces to NULL (ES treats an empty string as null for
// every non-string field type).
bool WriteDocField(duckdb::Vector& vec, duckdb::idx_t row,
                   simdjson::ondemand::value value, std::string_view index,
                   std::string_view field) {
  using JsonType = simdjson::ondemand::json_type;
  const auto read = ReadFieldValue(value, index, field);
  const auto id = vec.GetType().id();

  // ES treats an empty string as null for every non-string field type.
  if (id != duckdb::LogicalTypeId::VARCHAR && read.type == JsonType::string &&
      read.text.empty()) {
    return false;
  }

  switch (id) {
    case duckdb::LogicalTypeId::VARCHAR: {
      if (read.type != JsonType::string) {
        ThrowFieldParseError(index, field, "a string");
      }
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[row] =
        duckdb::StringVector::AddString(vec, read.text.data(),
                                        read.text.size());
      return true;
    }
    case duckdb::LogicalTypeId::BIGINT: {
      duckdb::FlatVector::GetDataMutable<int64_t>(vec)[row] =
        CoerceInt64(value, read, index, field);
      return true;
    }
    case duckdb::LogicalTypeId::INTEGER: {
      const int64_t v = CoerceInt64(value, read, index, field);
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
        CoerceDouble(value, read, index, field);
      return true;
    }
    case duckdb::LogicalTypeId::FLOAT: {
      duckdb::FlatVector::GetDataMutable<float>(vec)[row] =
        static_cast<float>(CoerceDouble(value, read, index, field));
      return true;
    }
    case duckdb::LogicalTypeId::BOOLEAN: {
      if (read.type == JsonType::boolean) {
        bool v = false;
        if (value.get_bool().get(v) != simdjson::SUCCESS) {
          ThrowFieldParseError(index, field, "a boolean");
        }
        duckdb::FlatVector::GetDataMutable<bool>(vec)[row] = v;
        return true;
      }
      if (read.type == JsonType::string &&
          (read.text == "true" || read.text == "false")) {
        duckdb::FlatVector::GetDataMutable<bool>(vec)[row] =
          read.text == "true";
        return true;
      }
      ThrowFieldParseError(index, field, "a boolean");
    }
    case duckdb::LogicalTypeId::TIMESTAMP: {
      // ES default date leniency: ISO-8601 (offsets applied, named zones
      // rejected) or epoch milliseconds.
      duckdb::timestamp_t ts;
      if (read.type == JsonType::number) {
        int64_t ms = 0;
        if (value.get_int64().get(ms) != simdjson::SUCCESS) {
          ThrowFieldParseError(index, field, "epoch milliseconds");
        }
        ts = duckdb::Timestamp::FromEpochMsPossiblyInfinite(ms);
      } else {
        bool has_offset = false;
        duckdb::string_t tz{nullptr, 0};
        if (read.type != JsonType::string ||
            duckdb::Timestamp::TryConvertTimestampTZ(
              read.text.data(), read.text.size(), ts, /*use_offset=*/true,
              has_offset, tz) != duckdb::TimestampCastResult::SUCCESS ||
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
  if (const auto ec = parser.iterate(padded).get(doc);
      ec != simdjson::SUCCESS) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    ERR_MSG("failed to parse document for index [", data.index,
                            "]: ", simdjson::error_message(ec)));
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

  auto refresh_table = [&](duckdb::TableCatalogEntry& table) {
    VisitInvertedIndexes(context, table, [&](duckdb::DuckIndexEntry& index) {
      if (const auto& storage =
            irs::utils::downCast<catalog::InvertedIndexEntry>(index)
              .Storage()) {
        storage->Refresh();
      }
    });
  };

  if (data.index.empty()) {
    // Collect-then-resolve: reading a table's indexes opens the schema's
    // index set from inside the relation set's own walk.
    std::vector<duckdb::reference<duckdb::TableCatalogEntry>> tables;
    if (auto schema = EsSchema(context)) {
      schema->Scan(
        context, duckdb::CatalogType::TABLE_ENTRY,
        [&](duckdb::CatalogEntry& entry) {
          if (entry.type == duckdb::CatalogType::TABLE_ENTRY) {
            tables.emplace_back(entry.Cast<duckdb::TableCatalogEntry>());
          }
        });
    }
    for (auto table : tables) {
      refresh_table(table.get());
    }
  } else {
    auto table = FindEsTable(context, data.index);
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
  if (!input.inputs[0].IsNull()) {
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
  return absl::WebSafeBase64Escape(std::string_view{bytes, sizeof bytes});
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
