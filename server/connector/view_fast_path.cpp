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

#include "connector/view_fast_path.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/common/enums/file_compression_type.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/function/function_binder.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/comparison_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/result_modifier.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression_binder/table_function_binder.hpp>
#include <duckdb/planner/tableref/bound_at_clause.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <iterator>

#include "connector/pg_logical_types.h"
#include "planning/iceberg_multi_file_list.hpp"

namespace duckdb {

TableFunction MakeParquetLookupTableFunction();
TableFunction MakeCSVLookupTableFunction();
TableFunction MakeJSONLookupTableFunction();
TableFunction MakeJSONObjectsLookupTableFunction();
TableFunction MakeTextLookupTableFunction();
TableFunction MakeDuckDBLookupTableFunction();

}  // namespace duckdb
namespace sdb::connector {
namespace {

struct RegistryEntry {
  std::string_view function_name;
  PkSpec single_pk_spec;
  PkSpec glob_pk_spec;
  duckdb::TableFunction (*make_lookup)();
  // Whether the reader's lookup applies pushed table filters. csv/json/text
  // only fetch rows by offset and ignore filters, so filters on their lookup
  // columns must NOT be pushed (they'd be silently dropped) -- see
  // supports_pushdown.
  bool supports_filters = false;
};

const RegistryEntry kRegistry[] = {
  {
    .function_name = "read_parquet",
    .single_pk_spec = PkSpec::FileRowNumber,
    .glob_pk_spec = PkSpec::FileIndexPlusRowNumber,
    .make_lookup = duckdb::MakeParquetLookupTableFunction,
    .supports_filters = true,
  },
  {
    .function_name = "read_csv",
    .single_pk_spec = PkSpec::FileOffset,
    .glob_pk_spec = PkSpec::FileIndexPlusOffset,
    .make_lookup = duckdb::MakeCSVLookupTableFunction,
  },
  {
    .function_name = "read_json",
    .single_pk_spec = PkSpec::FileOffset,
    .glob_pk_spec = PkSpec::FileIndexPlusOffset,
    .make_lookup = duckdb::MakeJSONLookupTableFunction,
  },
  {
    .function_name = "read_ndjson",
    .single_pk_spec = PkSpec::FileOffset,
    .glob_pk_spec = PkSpec::FileIndexPlusOffset,
    .make_lookup = duckdb::MakeJSONLookupTableFunction,
  },
  {
    .function_name = "read_json_objects",
    .single_pk_spec = PkSpec::FileOffset,
    .glob_pk_spec = PkSpec::FileIndexPlusOffset,
    .make_lookup = duckdb::MakeJSONObjectsLookupTableFunction,
  },
  {
    .function_name = "read_ndjson_objects",
    .single_pk_spec = PkSpec::FileOffset,
    .glob_pk_spec = PkSpec::FileIndexPlusOffset,
    .make_lookup = duckdb::MakeJSONObjectsLookupTableFunction,
  },
  // Iceberg data files are parquet; reuse the parquet lookup TF.
  {
    .function_name = "iceberg_scan",
    .single_pk_spec = PkSpec::FileIndexPlusRowNumber,
    .glob_pk_spec = PkSpec::FileIndexPlusRowNumber,
    .make_lookup = duckdb::MakeParquetLookupTableFunction,
    .supports_filters = true,
  },
  // read_text emits one row per file; PK is (file_index, 0) in glob mode.
  {
    .function_name = "read_text",
    .single_pk_spec = PkSpec::FileRowNumber,
    .glob_pk_spec = PkSpec::FileIndexPlusRowNumber,
    .make_lookup = duckdb::MakeTextLookupTableFunction,
  },
  {
    .function_name = "read_duckdb",
    .single_pk_spec = PkSpec::DuckDBRowId,
    .glob_pk_spec = PkSpec::FileIndexPlusDuckDBRowId,
    .make_lookup = duckdb::MakeDuckDBLookupTableFunction,
    .supports_filters = true,
  },
  // TODO: read_avro, postgres_scan / postgres_query.
};

constexpr struct {
  std::string_view alias;
  std::string_view canonical;
} kFunctionAliases[] = {
  {"parquet_scan", "read_parquet"},
  {"read_csv_auto", "read_csv"},
  {"read_json_auto", "read_json"},
  {"read_json_objects_auto", "read_json_objects"},
  {"read_ndjson_auto", "read_ndjson"},
};

const RegistryEntry* LookupRegistry(std::string_view function_name) {
  const auto* it = absl::c_find_if(kRegistry, [&](const RegistryEntry& entry) {
    return entry.function_name == function_name;
  });
  return it == std::end(kRegistry) ? nullptr : it;
}

// Resolve key column names against the source table, in order. Empty means no
// key: either `names` was empty or one of them is not a column of the table --
// both leave nothing to key on, so callers reject the same way. Takes the
// user's WITH (key_columns = ...) as std::string and the engine's PK metadata
// as duckdb::Identifier.
template<typename Names>
std::vector<ExternalKeyColumn> FindKeyColumns(
  const duckdb::TableCatalogEntry& entry, const Names& names) {
  const auto& columns = entry.GetColumns();
  std::vector<ExternalKeyColumn> cols;
  cols.reserve(names.size());
  for (const auto& name : names) {
    const duckdb::Identifier id{name};
    if (!columns.ColumnExists(id)) {
      return {};
    }
    const auto& col = columns.GetColumn(id);
    cols.emplace_back(id.GetIdentifierName(), col.Logical().index,
                      col.GetType());
  }
  return cols;
}

// The stored pk column's type for an ExternalColumnKey index: the key
// columns packed in resolution order, each field under its own column name.
duckdb::LogicalType ExternalKeyStructType(
  std::span<const ExternalKeyColumn> keys) {
  duckdb::child_list_t<duckdb::LogicalType> fields;
  fields.reserve(keys.size());
  for (const auto& key : keys) {
    fields.emplace_back(key.name, key.type);
  }
  return duckdb::LogicalType::STRUCT(std::move(fields));
}

struct ViewBody {
  const duckdb::TableRef& source;
  std::vector<std::string> projection_columns;
  bool has_limit = false;
};

std::optional<ViewBody> GetViewBody(const duckdb::CreateViewInfo& view) {
  if (view.query->node->type != duckdb::QueryNodeType::SELECT_NODE) {
    return std::nullopt;
  }
  const auto& select_node = view.query->node->Cast<duckdb::SelectNode>();
  if (select_node.select_list.empty() || select_node.having ||
      select_node.qualify || select_node.sample ||
      !select_node.groups.group_expressions.empty() ||
      !select_node.cte_map.map.empty()) {
    return std::nullopt;
  }
  // DISTINCT would collapse base rows -- we'd lose dedupe at materialisation.
  bool has_limit = false;
  for (const auto& mod : select_node.modifiers) {
    switch (mod->type) {
      case duckdb::ResultModifierType::ORDER_MODIFIER:
        break;
      case duckdb::ResultModifierType::LIMIT_MODIFIER:
      case duckdb::ResultModifierType::LEGACY_LIMIT_PERCENT_MODIFIER:
        has_limit = true;
        break;
      case duckdb::ResultModifierType::DISTINCT_MODIFIER:
        return std::nullopt;
    }
  }
  std::vector<std::string> projection_columns;
  if (select_node.select_list.size() != 1 ||
      select_node.select_list[0]->GetExpressionClass() !=
        duckdb::ExpressionClass::STAR) {
    for (const auto& item : select_node.select_list) {
      const duckdb::ParsedExpression* cur = item.get();
      while (cur->GetExpressionClass() == duckdb::ExpressionClass::CAST) {
        cur = &cur->Cast<duckdb::CastExpression>().Child();
      }
      if (cur->GetExpressionClass() != duckdb::ExpressionClass::COLUMN_REF) {
        return std::nullopt;
      }
      const auto& colref = cur->Cast<duckdb::ColumnRefExpression>();
      if (colref.IsQualified()) {
        return std::nullopt;
      }
      projection_columns.emplace_back(
        colref.GetColumnName().GetIdentifierName());
    }
  }
  return ViewBody{.source = *select_node.from_table,
                  .projection_columns = std::move(projection_columns),
                  .has_limit = has_limit};
}

ViewFastPath CatalogFastPath(const duckdb::Catalog& catalog,
                             const duckdb::Identifier& schema,
                             const duckdb::Identifier& table,
                             std::vector<std::string> projection_columns) {
  ViewFastPath out;
  out.catalog_ref =
    CatalogTableRef{.catalog = catalog.GetName().GetIdentifierName(),
                    .schema = schema.GetIdentifierName(),
                    .table = table.GetIdentifierName()};
  out.projection_columns = std::move(projection_columns);
  return out;
}

ViewFastPath IcebergFastPath(ViewFastPath out, bool has_limit) {
  const auto* registry_entry = LookupRegistry("iceberg_scan");
  SDB_ASSERT(registry_entry);
  out.function_name = registry_entry->function_name;
  out.is_glob = true;
  out.pk_spec = registry_entry->glob_pk_spec;
  out.supports_filters = registry_entry->supports_filters;
  out.supports_delta = IsGlobPK(out.pk_spec) && !has_limit;
  return out;
}

std::optional<ViewFastPath> AttachedDuckDBFastPath(duckdb::Catalog& catalog,
                                                   ViewFastPath out) {
  if (catalog.IsSystemCatalog() || catalog.IsTemporaryCatalog() ||
      catalog.InMemory()) {
    return std::nullopt;
  }
  out.function_name = "read_duckdb";
  out.pk_spec = PkSpec::DuckDBRowId;
  // Attached duckdb table: materialized via DataTable::LookupScan, which
  // applies pushed filters in-scan.
  out.supports_filters = true;
  return out;
}

// Views over a serenedb table ride the same rowid-keyed machinery as views
// over an attached database.
std::optional<ViewFastPath> SereneDBFastPath(
  const duckdb::TableCatalogEntry& table, ViewFastPath out) {
  // Only the check that every projected name is one of the relation's
  // columns: a name that belongs to none is not this fast path's to serve.
  if (!absl::c_all_of(out.projection_columns, [&](const std::string& name) {
        return table.GetColumns().ColumnExists(duckdb::Identifier{name});
      })) {
    return std::nullopt;
  }
  out.pk_spec = PkSpec::DuckDBRowId;
  out.supports_filters = true;
  return out;
}

std::optional<ViewFastPath> ExternalColumnKeyFastPath(
  ViewFastPath out, std::vector<ExternalKeyColumn> keys) {
  if (keys.empty()) {
    return std::nullopt;
  }
  out.pk_spec = PkSpec::ExternalColumnKey;
  out.key_columns = std::move(keys);
  return out;
}

std::optional<ViewFastPath> ExternalFastPath(
  duckdb::TableCatalogEntry& table, ViewFastPath out,
  std::span<const std::string> key_columns) {
  if (!key_columns.empty()) {
    // WITH (key_columns = '...') takes precedence over the connector default.
    // Unknown column -> no fast path.
    return ExternalColumnKeyFastPath(std::move(out),
                                     FindKeyColumns(table, key_columns));
  }
  if (table.ParentCatalog().GetCatalogType() == "postgres") {
    // Postgres: key on ctid (the duckdb rowid) -- universal, no PRIMARY KEY
    // needed, unique within the index snapshot. The lookup's `rowid IN (...)`
    // is pushed down as a `ctid IN (...)` TID scan.
    out.pk_spec = PkSpec::ExternalPostgresCtid;
    return out;
  }
  // ClickHouse: part+offset ids die on merges, so key on the engine's PK
  // metadata -- the whole MergeTree key in order, whatever its arity and
  // types, since composite keys are the norm there. That key is a sorting
  // prefix and not a uniqueness constraint, so duplicate keys each index
  // their own document and a re-fetch returns every row sharing a key.
  const auto pk = table.GetPrimaryKey();
  if (!pk) {
    return std::nullopt;
  }
  return ExternalColumnKeyFastPath(
    std::move(out),
    FindKeyColumns(table,
                   pk->Cast<duckdb::UniqueConstraint>().GetColumnNames()));
}

std::optional<ViewFastPath> ResolveTableSource(
  duckdb::Binder& binder, ViewBody body,
  std::span<const std::string> key_columns) {
  auto& retriever = binder.EntryRetriever();
  auto name = body.source.Cast<duckdb::BaseTableRef>().GetQualifiedName();
  duckdb::Binder::BindSchemaOrCatalog(binder.context, name);
  if (!name.Catalog().empty() && !name.Schema().empty()) {
    auto catalog = duckdb::Catalog::GetCatalogEntry(retriever, name.Catalog());
    if (catalog && catalog->GetCatalogType() == "iceberg") {
      return IcebergFastPath(
        CatalogFastPath(*catalog, name.Schema(), name.Name(),
                        std::move(body.projection_columns)),
        body.has_limit);
    }
  }
  SDB_IF_FAILURE("view_index_source_lookup") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  auto entry = retriever.GetEntry(
    duckdb::EntryLookupInfo{duckdb::CatalogType::TABLE_ENTRY, name},
    duckdb::OnEntryNotFound::RETURN_NULL);
  if (!entry || entry->type != duckdb::CatalogType::TABLE_ENTRY) {
    return std::nullopt;
  }
  auto& table = entry->Cast<duckdb::TableCatalogEntry>();
  auto& catalog = table.ParentCatalog();
  auto out = CatalogFastPath(catalog, table.ParentSchema(binder.context).name,
                             table.name, std::move(body.projection_columns));
  const auto cat_type = catalog.GetCatalogType();
  if (cat_type == "iceberg") {
    return IcebergFastPath(std::move(out), body.has_limit);
  }
  if (cat_type == "duckdb") {
    return AttachedDuckDBFastPath(catalog, std::move(out));
  }
  if (cat_type == "serenedb") {
    return SereneDBFastPath(table, std::move(out));
  }
  if (cat_type == "postgres" || cat_type == "clickhouse") {
    return ExternalFastPath(table, std::move(out), key_columns);
  }
  return std::nullopt;
}

bool IsCompressedJson(const std::string& path,
                      const duckdb::named_parameter_map_t& named_params) {
  auto compression = duckdb::FileCompressionType::AUTO_DETECT;
  if (auto it = named_params.find("compression"); it != named_params.end()) {
    compression = duckdb::FileCompressionType{it->second.ToString()};
  }
  if (compression.IsAutoDetect()) {
    return duckdb::IsFileCompressed(path, duckdb::FileCompressionType::GZIP) ||
           duckdb::IsFileCompressed(path, duckdb::FileCompressionType::ZSTD);
  }
  return compression != duckdb::FileCompressionType::UNCOMPRESSED;
}

std::optional<ViewFastPath> ResolveFunctionSource(duckdb::Binder& binder,
                                                  ViewBody body) {
  const auto& fn_expr = body.source.Cast<duckdb::TableFunctionRef>()
                          .function->Cast<duckdb::FunctionExpression>();
  std::string_view name = fn_expr.FunctionName().GetIdentifierName();
  if (const auto* alias = absl::c_find_if(
        kFunctionAliases, [&](const auto& a) { return a.alias == name; });
      alias != std::end(kFunctionAliases)) {
    name = alias->canonical;
  }
  const auto* entry = LookupRegistry(name);
  if (!entry) {
    return std::nullopt;
  }
  duckdb::vector<duckdb::Value> args;
  duckdb::named_parameter_map_t named_params;
  for (const auto& arg : fn_expr.GetArguments()) {
    auto child = arg.GetExpression().Copy();
    duckdb::Identifier param_name;
    if (child->GetExpressionType() == duckdb::ExpressionType::COMPARE_EQUAL) {
      auto& comp = child->Cast<duckdb::ComparisonExpression>();
      if (comp.Left().GetExpressionType() ==
          duckdb::ExpressionType::COLUMN_REF) {
        const auto& colref = comp.Left().Cast<duckdb::ColumnRefExpression>();
        if (!colref.IsQualified()) {
          param_name = colref.GetColumnName();
          child = std::move(comp.RightMutable());
        }
      }
    } else if (arg.HasName()) {
      param_name = arg.GetName();
    }
    duckdb::TableFunctionBinder arg_binder(binder, binder.context,
                                           std::string{entry->function_name});
    auto bound = arg_binder.Bind(child);
    if (bound->HasParameter() || !bound->IsScalar()) {
      return std::nullopt;
    }
    auto value =
      duckdb::ExpressionExecutor::EvaluateScalar(binder.context, *bound, true);
    if (param_name.empty()) {
      args.emplace_back(std::move(value));
    } else {
      named_params[param_name] = std::move(value);
    }
  }
  if (args.size() != 1 ||
      args[0].type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return std::nullopt;
  }
  static constexpr std::string_view kJsonReaders[] = {
    "read_json", "read_ndjson", "read_json_objects", "read_ndjson_objects"};
  if (absl::c_contains(kJsonReaders, entry->function_name) &&
      IsCompressedJson(args[0].GetValue<std::string>(), named_params)) {
    return std::nullopt;
  }
  ViewFastPath out;
  out.function_name = entry->function_name;
  out.args = std::move(args);
  out.named_params = std::move(named_params);
  out.is_glob =
    duckdb::FileSystem::HasGlob(out.args[0].GetValue<std::string>());
  out.projection_columns = std::move(body.projection_columns);
  out.pk_spec = out.is_glob ? entry->glob_pk_spec : entry->single_pk_spec;
  out.supports_filters = entry->supports_filters;
  out.supports_delta = IsGlobPK(out.pk_spec) &&
                       !out.named_params.contains("union_by_name") &&
                       !body.has_limit;
  return out;
}

}  // namespace

std::optional<ViewFastPath> ResolveViewFastPath(
  duckdb::ClientContext& context, duckdb::Catalog& view_catalog,
  const duckdb::CreateViewInfo& view,
  std::span<const std::string> key_columns) {
  auto body = GetViewBody(view);
  if (!body) {
    return std::nullopt;
  }
  auto binder = duckdb::Binder::CreateBinder(context);
  binder->SetSearchPath(view_catalog, view.GetQualifiedName().Schema());
  if (body->source.type == duckdb::TableReferenceType::BASE_TABLE) {
    return ResolveTableSource(*binder, std::move(*body), key_columns);
  }
  if (body->source.type == duckdb::TableReferenceType::TABLE_FUNCTION) {
    return ResolveFunctionSource(*binder, std::move(*body));
  }
  return std::nullopt;
}

duckdb::LogicalType ViewFastPath::GeneratedPkType() const {
  switch (pk_spec) {
    case PkSpec::FileIndexPlusRowNumber:
    case PkSpec::FileIndexPlusOffset:
    case PkSpec::FileIndexPlusDuckDBRowId:
      return FileIndexRowNumberStructType();
    case PkSpec::FileRowNumber:
    case PkSpec::FileOffset:
    case PkSpec::DuckDBRowId:
      return duckdb::LogicalType::BIGINT;
    case PkSpec::ExternalPostgresCtid:
      return pg::CTID();
    case PkSpec::ExternalColumnKey:
      return ExternalKeyStructType(key_columns);
  }
  SDB_UNREACHABLE();
}

std::vector<duckdb::column_t> BackfillPkVirtualColumns(const ViewFastPath& fp) {
  switch (fp.pk_spec) {
    // Postgres ctid: the key is the virtual rowid, not a real column.
    case PkSpec::ExternalPostgresCtid:
    case PkSpec::DuckDBRowId:
      return {duckdb::COLUMN_IDENTIFIER_ROW_ID};
    // Project the key columns in resolution order: the sink packs them into one
    // struct in that order, and the re-fetch matches on it positionally.
    case PkSpec::ExternalColumnKey: {
      std::vector<duckdb::column_t> columns;
      absl::c_transform(
        fp.key_columns, std::back_inserter(columns),
        [](const ExternalKeyColumn& key) { return key.source_index; });
      return columns;
    }
    case PkSpec::FileIndexPlusDuckDBRowId:
      return {duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX,
              duckdb::COLUMN_IDENTIFIER_ROW_ID};
    case PkSpec::FileIndexPlusRowNumber:
    case PkSpec::FileIndexPlusOffset:
      return {duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX,
              duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER};
    case PkSpec::FileRowNumber:
    case PkSpec::FileOffset:
      return {duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER};
  }
  SDB_UNREACHABLE();
}

duckdb::TableFunction MakeFastPathLookupFunction(const ViewFastPath& fp) {
  const auto* entry = LookupRegistry(fp.function_name);
  SDB_ASSERT(entry);
  return entry->make_lookup();
}

void EnableIcebergSort(duckdb::FunctionData* bind_data) noexcept {
  auto* multi_bd = dynamic_cast<duckdb::MultiFileBindData*>(bind_data);
  if (!multi_bd) {
    return;
  }
  if (auto* iceberg_list = dynamic_cast<duckdb::IcebergMultiFileList*>(
        multi_bd->file_list.get())) {
    iceberg_list->GetScanPlanner().SortFilesByPath();
  }
}

namespace {

duckdb::unique_ptr<duckdb::FunctionData> BindCatalogSource(
  duckdb::ClientContext& context, const ViewFastPath& fp) {
  SDB_IF_FAILURE("view_index_source_lookup") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  auto& entry =
    duckdb::Catalog::GetEntry(
      context, duckdb::CatalogType::TABLE_ENTRY,
      duckdb::QualifiedName(duckdb::Identifier{fp.catalog_ref->catalog},
                            duckdb::Identifier{fp.catalog_ref->schema},
                            duckdb::Identifier{fp.catalog_ref->table}))
      .Cast<duckdb::TableCatalogEntry>();
  std::optional<duckdb::BoundAtClause> at_clause;
  if (fp.pinned_iceberg_snapshot_id != 0) {
    at_clause.emplace("version",
                      duckdb::Value::BIGINT(fp.pinned_iceberg_snapshot_id));
  }
  duckdb::EntryLookupInfo lookup(
    duckdb::CatalogType::TABLE_ENTRY,
    duckdb::QualifiedName(duckdb::Identifier{fp.catalog_ref->table}),
    at_clause ? &*at_clause : nullptr, duckdb::QueryErrorContext{});
  duckdb::unique_ptr<duckdb::FunctionData> bind_data;
  auto fn = entry.GetScanFunction(context, bind_data, lookup);
  if (fn.get_virtual_columns) {
    fn.get_virtual_columns(context, bind_data.get());
  }
  return bind_data;
}

duckdb::unique_ptr<duckdb::FunctionData> BindReaderSource(
  duckdb::ClientContext& context, const ViewFastPath& fp) {
  SDB_ASSERT(!fp.args.empty());
  auto& reader =
    duckdb::Catalog::GetEntry(
      context,
      duckdb::EntryLookupInfo{
        duckdb::CatalogType::TABLE_FUNCTION_ENTRY,
        duckdb::QualifiedName(SYSTEM_CATALOG, DEFAULT_SCHEMA,
                              duckdb::Identifier{fp.function_name})})
      .Cast<duckdb::TableFunctionCatalogEntry>();
  const bool pin_snapshot =
    fp.pinned_iceberg_snapshot_id != 0 && fp.function_name == "iceberg_scan";
  const duckdb::Identifier snapshot_param{"snapshot_from_id"};
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> positional;
  positional.reserve(fp.args.size());
  for (const auto& arg : fp.args) {
    positional.push_back(
      duckdb::make_uniq<duckdb::BoundConstantExpression>(arg));
  }
  duckdb::vector<
    std::pair<duckdb::Identifier, duckdb::unique_ptr<duckdb::Expression>>>
    named;
  named.reserve(fp.named_params.size() + 1);
  for (const auto& [name, value] : fp.named_params) {
    if (pin_snapshot && name == snapshot_param) {
      continue;
    }
    named.emplace_back(
      name, duckdb::make_uniq<duckdb::BoundConstantExpression>(value));
  }
  if (pin_snapshot) {
    named.emplace_back(
      snapshot_param,
      duckdb::make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::UBIGINT(
        static_cast<uint64_t>(fp.pinned_iceberg_snapshot_id))));
  }
  duckdb::FunctionBinder function_binder{context};
  duckdb::vector<duckdb::Value> parameters;
  duckdb::named_argument_map_t named_parameters;
  duckdb::ErrorData error;
  const auto index =
    function_binder.BindFunction(reader.name, reader.functions, positional,
                                 named, parameters, named_parameters, error);
  if (!index.IsValid()) {
    error.Throw();
  }
  duckdb::BoundTableFunction function{
    reader.functions.GetFunctionByOffset(index.GetIndex())};
  function.SetCallArguments(parameters, named_parameters);
  duckdb::vector<duckdb::LogicalType> input_table_types;
  duckdb::vector<duckdb::Identifier> input_table_names;
  duckdb::TableFunctionRef ref;
  duckdb::TableFunctionBindInput input(
    parameters, named_parameters, input_table_types, input_table_names,
    function.function_info.get(), nullptr, function, ref);
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::Identifier> names;
  auto bind_data = function.bind(context, input, types, names);
  if (function.get_virtual_columns) {
    function.get_virtual_columns(context, bind_data.get());
  }
  return bind_data;
}

}  // namespace

duckdb::unique_ptr<duckdb::FunctionData> BindFastPathSource(
  duckdb::ClientContext& context, const ViewFastPath& fp) {
  auto bind_data = fp.catalog_ref ? BindCatalogSource(context, fp)
                                  : BindReaderSource(context, fp);
  if (fp.function_name == "iceberg_scan") {
    EnableIcebergSort(bind_data.get());
  }
  return bind_data;
}

std::string FormatLookupLabel(const ViewFastPath& fp) {
  if (fp.function_name == "iceberg_scan") {
    return "iceberg";
  }
  std::string_view name = fp.function_name;
  absl::ConsumePrefix(&name, "read_");
  if (name == "ndjson") {
    name = "json";
  }
  if (fp.is_glob) {
    return absl::StrCat("glob ", name);
  }
  return std::string{name};
}

}  // namespace sdb::connector
