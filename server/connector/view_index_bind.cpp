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

#include "connector/view_index_bind.h"

#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/planner/expression_binder/index_binder.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "connector/view_fast_path.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

// The view columns the index build reads, as positions in the view's own column
// list. A fast path that names its projection keeps only those, in that order;
// otherwise the whole view is read as declared.
std::vector<duckdb::idx_t> KeptViewPositions(
  const ViewFastPath& fp, const duckdb::CreateViewInfo& view) {
  std::vector<duckdb::idx_t> kept;
  if (fp.projection_columns.empty()) {
    kept.reserve(view.names.size());
    for (duckdb::idx_t i = 0; i < view.names.size(); ++i) {
      kept.push_back(i);
    }
    return kept;
  }
  kept.reserve(fp.projection_columns.size());
  for (const auto& name : fp.projection_columns) {
    bool found = false;
    for (duckdb::idx_t i = 0; i < view.names.size(); ++i) {
      if (absl::EqualsIgnoreCase(view.names[i].GetIdentifierName(), name)) {
        kept.push_back(i);
        found = true;
        break;
      }
    }
    if (!found) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                      ERR_MSG("view \"", view.GetViewName().GetIdentifierName(),
                              "\" has no column \"", name,
                              "\" for its indexed source"));
    }
  }
  return kept;
}

// Where each kept view column sits in the source the reader returns. The view
// names the column; the reader orders it.
duckdb::idx_t SourceColumnFor(const FastPathScan& scan,
                              std::string_view view_column) {
  for (duckdb::idx_t i = 0; i < scan.names.size(); ++i) {
    if (absl::EqualsIgnoreCase(scan.names[i].GetIdentifierName(),
                               view_column)) {
      return i;
    }
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
    ERR_MSG("indexed source does not expose column \"", view_column, "\""));
}

// How the source identifies a row, in the vocabulary ResolveInvertedIndexOptions
// reads it back in. A composite (file, position) key indexes per source file,
// which is what lets a refresh revisit one file at a time.
std::string_view FastPathPkKind(PkSpec spec) {
  switch (spec) {
    case PkSpec::ExternalPostgresCtid:
    case PkSpec::ExternalColumnKey:
      return "external_struct_key";
    case PkSpec::FileIndexPlusRowNumber:
      return "file_index_plus_row_number";
    case PkSpec::FileIndexPlusOffset:
      return "file_index_plus_offset";
    case PkSpec::FileIndexPlusDuckDBRowId:
      return "file_index_plus_duckdb_rowid";
    case PkSpec::DuckDBRowId:
    case PkSpec::FileRowNumber:
    case PkSpec::FileOffset:
      return "single";
  }
  return "single";
}

}  // namespace

duckdb::unique_ptr<duckdb::LogicalOperator> BindCreateIndexOnView(
  duckdb::Binder& binder, duckdb::CreateStatement& stmt,
  duckdb::ViewCatalogEntry& view) {
  auto& context = binder.context;
  auto info = duckdb::unique_ptr_cast<duckdb::CreateInfo,
                                      duckdb::CreateIndexInfo>(
    std::move(stmt.info));

  auto view_info = view.GetInfo();
  auto& view_base = view_info->Cast<duckdb::CreateViewInfo>();

  const auto key_columns = KeyColumnsFromOptions(info->options);
  auto fp = ResolveViewFastPath(context, view_base, key_columns);
  if (!fp) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("cannot index view \"", view.name.GetIdentifierName(),
              "\": its body must be a simple `SELECT ... FROM <reader>("
              "literal_args)` over a recognised fast-path source "
              "(read_parquet/csv/json/...)"));
  }

  auto scan = BindFastPathScan(context, *fp);
  if (!scan) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("cannot index view \"",
                            view.name.GetIdentifierName(),
                            "\": its source could not be bound"));
  }

  info->options["_sdb_view_fast_path_pk"] =
    duckdb::Value{std::string{FastPathPkKind(fp->pk_spec)}};

  const auto kept = KeptViewPositions(*fp, view_base);

  auto table_index = binder.GenerateTableIndex();
  auto get = duckdb::make_uniq<duckdb::LogicalGet>(
    table_index, scan->function, std::move(scan->bind_data), scan->types,
    scan->names, scan->virtual_columns);

  // The build reads the kept view columns in view order, then the pk virtual
  // columns the source identifies its rows by. Both ride the same chunk, so the
  // order fixed here is the order the sink unpacks.
  duckdb::vector<duckdb::Identifier> bound_names;
  duckdb::vector<duckdb::LogicalType> bound_types;
  bound_names.reserve(kept.size());
  bound_types.reserve(kept.size());
  for (const auto position : kept) {
    const auto& view_column = view_base.names[position].GetIdentifierName();
    const auto source_column = SourceColumnFor(*scan, view_column);
    get->AddColumnId(source_column);
    bound_names.push_back(view_base.names[position]);
    bound_types.push_back(scan->types[source_column]);
    info->column_ids.push_back(position);
    info->scan_types.push_back(scan->types[source_column]);
  }
  for (const auto virtual_column : BackfillPkVirtualColumns(*fp)) {
    get->AddColumnId(virtual_column);
  }
  info->scan_types.emplace_back(duckdb::LogicalType::ROW_TYPE);
  info->names = bound_names;

  // ColumnBindingResolver rewrites a CREATE INDEX's expressions against
  // (TableIndex(0), logical column position) -- for a view, a position in its
  // whole declared column list. So they bind against that, not against the
  // source scan, whose column order and width are its own. The caller already
  // bound the view body, so a fresh context is needed for the name too.
  duckdb::vector<duckdb::ColumnIndex> view_column_ids;
  view_column_ids.reserve(view_base.names.size());
  for (duckdb::idx_t i = 0; i < view_base.names.size(); ++i) {
    view_column_ids.emplace_back(i);
  }
  auto index_binder_owner = duckdb::Binder::CreateBinder(context, &binder);
  index_binder_owner->bind_context.AddTableFunction(
    duckdb::TableIndex(0), view.name, view_base.names, view_base.types,
    view_column_ids, nullptr, duckdb::virtual_column_map_t{});

  auto& dependencies = info->dependencies;
  auto& catalog =
    duckdb::Catalog::GetCatalog(context, info->GetQualifiedName().Catalog());
  duckdb::catalog_entry_callback_t lookup_callback =
    [&dependencies, &catalog](duckdb::CatalogEntry& entry) {
      if (&catalog != &entry.ParentCatalog()) {
        return;
      }
      dependencies.AddDependency(entry);
    };

  duckdb::IndexBinder index_binder(*index_binder_owner, context);
  index_binder.SetCatalogLookupCallback(lookup_callback);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions;
  expressions.reserve(info->expressions.size());
  for (auto& expr : info->expressions) {
    expressions.push_back(index_binder.Bind(expr));
  }

  duckdb::unique_ptr<duckdb::Expression> bound_where;
  if (info->where_clause) {
    duckdb::IndexBinder where_binder(*index_binder_owner, context);
    where_binder.target_type = duckdb::LogicalType::BOOLEAN;
    where_binder.SetCatalogLookupCallback(lookup_callback);
    auto where_copy = info->where_clause->Copy();
    bound_where = where_binder.Bind(where_copy);
  }

  info->SetQualifiedName(duckdb::QualifiedName(
    view.ParentCatalog().GetName(), view.ParentSchema().name,
    info->GetQualifiedName().Name()));

  duckdb::unique_ptr<duckdb::LogicalOperator> plan = std::move(get);
  if (bound_where) {
    auto filter = duckdb::make_uniq<duckdb::LogicalFilter>(
      std::move(bound_where));
    filter->AddChild(std::move(plan));
    plan = std::move(filter);
  }

  auto result = duckdb::make_uniq<duckdb::LogicalCreateIndex>(
    std::move(info), std::move(expressions), view, nullptr);
  result->children.push_back(std::move(plan));
  return std::move(result);
}

}  // namespace sdb::connector
