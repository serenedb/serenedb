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

#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/common/types/hash.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression_binder/index_binder.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <duckdb/planner/operator/logical_empty_result.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/pg_logical_types.h"
#include "connector/source_file.h"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "planning/iceberg_multi_file_list.hpp"
#include "query/config_variable_names.h"
#include "search/search_table.h"
#include "search/search_table_transaction.h"

namespace sdb::connector {
namespace {

duckdb::unique_ptr<SereneDBCreateIndexInfo> TakeInfo(
  duckdb::CreateStatement& stmt) {
  auto info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateIndexInfo>(
      std::move(stmt.info));
  if (dynamic_cast<SereneDBCreateIndexInfo*>(info.get())) {
    return duckdb::unique_ptr_cast<duckdb::CreateIndexInfo,
                                   SereneDBCreateIndexInfo>(std::move(info));
  }
  return duckdb::make_uniq<SereneDBCreateIndexInfo>(std::move(*info));
}

duckdb::ProjectionIndex LeafSlot(duckdb::LogicalGet& leaf,
                                 duckdb::column_t column) {
  const auto& ids = leaf.GetColumnIds();
  for (duckdb::idx_t i = 0; i < ids.size(); ++i) {
    if (ids[i] == duckdb::ColumnIndex(column)) {
      return duckdb::ProjectionIndex(i);
    }
  }
  return leaf.AddColumnId(column);
}

const duckdb::LogicalType& ColumnType(duckdb::LogicalGet& leaf,
                                      duckdb::ProjectionIndex slot) {
  const auto& column = leaf.GetColumnIds()[slot];
  if (column.IsVirtualColumn() &&
      column.GetPrimaryIndex() ==
        duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX &&
      !leaf.virtual_columns.contains(column.GetPrimaryIndex())) {
    leaf.virtual_columns.emplace(
      column.GetPrimaryIndex(),
      duckdb::TableColumn("file_index", duckdb::LogicalType::UBIGINT));
  }
  return leaf.GetColumnType(column);
}

duckdb::ColumnBinding CarryUp(duckdb::LogicalOperator& op,
                              duckdb::ColumnBinding binding,
                              const duckdb::LogicalType& type) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    return binding;
  }
  SDB_ASSERT(op.children.size() == 1);
  auto below = CarryUp(*op.children[0], binding, type);
  if (op.type != duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    return below;
  }
  auto& projection = op.Cast<duckdb::LogicalProjection>();
  projection.expressions.emplace_back(
    duckdb::make_uniq<duckdb::BoundColumnRefExpression>(type, below));
  return {projection.table_index,
          duckdb::ProjectionIndex(projection.expressions.size() - 1)};
}

void SelectPassFiles(duckdb::ClientContext& context,
                     SereneDBCreateIndexInfo& info,
                     duckdb::MultiFileBindData& bind) {
  irs::containers::FlatHashMap<std::string_view, const std::string*> by_path;
  by_path.reserve(info.pass_terms.size());
  for (const auto& term : info.pass_terms) {
    by_path.emplace(
      ParseSourceFileTerm(irs::ViewCast<irs::byte_type>(std::string_view{term}))
        .path,
      &term);
  }
  if (const auto* iceberg_list =
        dynamic_cast<const duckdb::IcebergMultiFileList*>(
          bind.file_list.get())) {
    auto paths = duckdb::make_shared_ptr<duckdb::unordered_set<std::string>>();
    paths->reserve(by_path.size());
    for (const auto& [path, term] : by_path) {
      paths->emplace(path);
    }
    bind.file_list = iceberg_list->SelectDataFiles(std::move(paths));
  }
  const auto listing =
    ListSource(context, *bind.file_list, /*versioned=*/false);
  duckdb::vector<duckdb::OpenFileInfo> selected;
  selected.reserve(by_path.size());
  for (const auto& file : listing.files) {
    if (const auto it = by_path.find(file.path); it != by_path.end()) {
      selected.push_back(file);
      info.file_terms.push_back(*it->second);
    }
  }
  if (selected.size() != listing.files.size()) {
    bind.file_list =
      duckdb::make_shared_ptr<duckdb::SimpleMultiFileList>(std::move(selected));
  }
  duckdb::MultiFileReader::PruneReaders(bind, *bind.file_list);
}

void PrepareSourceFiles(duckdb::ClientContext& context,
                        SereneDBCreateIndexInfo& info, const ViewFastPath& fp,
                        duckdb::MultiFileBindData& bind) {
  if (info.IsPass()) {
    SelectPassFiles(context, info, bind);
    return;
  }
  SDB_IF_FAILURE("legacy_view_index_payload") { return; }
  const bool versioned = fp.refresh_source == RefreshSource::Files;
  const auto listing = ListSource(context, *bind.file_list, versioned);
  info.file_terms = SourceFileTerms(listing);
  if (versioned) {
    info.position.listing = listing.digest;
  }
  if (const auto* iceberg_list =
        dynamic_cast<const duckdb::IcebergMultiFileList*>(
          bind.file_list.get())) {
    if (const auto& snapshot =
          iceberg_list->GetScanPlanner().GetSnapshot().snapshot) {
      info.position.snapshot_id = snapshot->snapshot_id.value_or(0);
    }
  }
}

}  // namespace

duckdb::LogicalGet& LeafScan(duckdb::LogicalOperator& op) {
  auto* current = &op;
  while (current->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    SDB_ASSERT(current->children.size() == 1);
    current = current->children[0].get();
  }
  return current->Cast<duckdb::LogicalGet>();
}

uint64_t ViewIndexDefinition(const duckdb::CreateViewInfo& view,
                             std::span<const duckdb::column_t> column_ids,
                             const duckdb::ParsedExpression* where) {
  const auto query = view.query->ToString();
  auto hash = duckdb::Hash(query.data(), query.size());
  for (const auto column : column_ids) {
    hash = duckdb::CombineHash(hash, duckdb::Hash(column));
  }
  if (where) {
    const auto predicate = where->ToString();
    hash = duckdb::CombineHash(
      hash, duckdb::Hash(predicate.data(), predicate.size()));
  }
  return hash;
}

duckdb::unique_ptr<duckdb::LogicalOperator> BindCreateIndexOnView(
  duckdb::Binder& binder, duckdb::CreateStatement& stmt,
  duckdb::ViewCatalogEntry& view,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
  auto& context = binder.context;
  auto info = TakeInfo(stmt);
  if (info->index_type != catalog::kInvertedIndexTypeName) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("plain indexes on views are not supported; use an "
                            "inverted index instead"));
  }
  auto view_info = view.GetInfo();
  auto& view_base = view_info->Cast<duckdb::CreateViewInfo>();
  const auto fp = ResolveViewFastPath(context, view.ParentCatalog(), view_base,
                                      catalog::ParseKeyColumns(info->options));
  std::vector<duckdb::unique_ptr<duckdb::Expression>> pk_refs;
  if (fp) {
    auto& leaf = LeafScan(*plan);
    EnableIcebergSort(leaf.bind_data.get());
    if (IsFilePkSpec(fp->pk_spec)) {
      PrepareSourceFiles(context, *info, *fp,
                         leaf.bind_data->Cast<duckdb::MultiFileBindData>());
    }
    const auto vcols = BackfillPkVirtualColumns(*fp);
    pk_refs.reserve(vcols.size());
    for (const auto column : vcols) {
      const auto slot = LeafSlot(leaf, column);
      if (fp->pk_spec == PkSpec::ExternalPostgresCtid) {
        leaf.virtual_columns.insert_or_assign(
          column, duckdb::TableColumn("rowid", pg::CTID()));
      }
      const auto& type = ColumnType(leaf, slot);
      pk_refs.emplace_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
        type,
        CarryUp(*plan, duckdb::ColumnBinding(leaf.table_index, slot), type)));
    }
    info->generated_pk_type = fp->GeneratedPkType();
  }

  const auto kept_index = binder.GenerateTableIndex();
  duckdb::vector<duckdb::ColumnIndex> kept;
  auto kept_binder = duckdb::Binder::CreateBinder(context, &binder);
  kept_binder->bind_context.AddTableFunction(
    kept_index, view.name, view_base.names, view_base.types, kept, nullptr,
    duckdb::virtual_column_map_t{});

  auto& dependencies = info->dependencies;
  auto& catalog = view.ParentCatalog();
  duckdb::catalog_entry_callback_t lookup_callback =
    [&dependencies, &catalog](duckdb::CatalogEntry& entry) {
      if (&catalog == &entry.ParentCatalog()) {
        dependencies.AddDependency(entry);
      }
    };
  duckdb::IndexBinder index_binder(*kept_binder, context);
  index_binder.SetCatalogLookupCallback(lookup_callback);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions;
  expressions.reserve(info->expressions.size());
  for (auto& expr : info->expressions) {
    expressions.emplace_back(index_binder.Bind(expr));
  }
  duckdb::unique_ptr<duckdb::Expression> bound_where;
  if (info->where_clause) {
    duckdb::IndexBinder where_binder(*kept_binder, context);
    where_binder.target_type = duckdb::LogicalType::BOOLEAN;
    where_binder.SetCatalogLookupCallback(lookup_callback);
    auto where_copy = info->where_clause->Copy();
    bound_where = where_binder.Bind(where_copy);
  }

  const auto top = plan->GetColumnBindings();
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> select_list;
  select_list.reserve(kept.size() + pk_refs.size());
  for (const auto& column : kept) {
    const auto position = column.GetPrimaryIndex();
    info->column_ids.emplace_back(position);
    info->scan_types.emplace_back(view_base.types[position]);
    select_list.emplace_back(
      duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
        view_base.types[position], top[position]));
  }
  for (auto& ref : pk_refs) {
    select_list.emplace_back(std::move(ref));
  }
  info->scan_types.emplace_back(duckdb::LogicalType::ROW_TYPE);
  info->names = view_base.names;
  auto qualification =
    view.ParentSchemaPath(view.ParentCatalog().GetCatalogTransaction(context));
  qualification.insert(qualification.begin(), view.ParentCatalog().GetName());
  info->SetQualifiedName(
    info->GetQualifiedName().WithQualification(std::move(qualification)));
  info->position.definition =
    ViewIndexDefinition(view_base, info->column_ids, info->where_clause.get());

  auto projection = duckdb::make_uniq<duckdb::LogicalProjection>(
    kept_index, std::move(select_list));
  projection->AddChild(std::move(plan));
  duckdb::unique_ptr<duckdb::LogicalOperator> input = std::move(projection);
  if (bound_where) {
    auto filter =
      duckdb::make_uniq<duckdb::LogicalFilter>(std::move(bound_where));
    filter->AddChild(std::move(input));
    input = std::move(filter);
  }

  for (auto& expr : expressions) {
    duckdb::ExpressionIterator::EnumerateExpression(
      expr, [](duckdb::Expression& node) {
        if (node.GetExpressionClass() ==
            duckdb::ExpressionClass::BOUND_COLUMN_REF) {
          node.Cast<duckdb::BoundColumnRefExpression>()
            .BindingMutable()
            .table_index = duckdb::TableIndex(0);
        }
      });
  }

  auto result = duckdb::make_uniq<duckdb::LogicalCreateIndex>(
    std::move(info), std::move(expressions), view, nullptr);
  result->children.emplace_back(std::move(input));
  return std::move(result);
}

duckdb::unique_ptr<duckdb::LogicalOperator> BindCreateIndexOnSearchTable(
  duckdb::Binder& binder, duckdb::CreateStatement& stmt,
  catalog::SearchTableEntry& table,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
  auto& context = binder.context;
  auto info = TakeInfo(stmt);
  if (info->index_type != catalog::kInvertedIndexTypeName) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("only inverted indexes are supported on a search-backed table"));
  }
  if (info->options.contains(kOptimizeTopKSetting)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG(kOptimizeTopKSetting,
              " is a table option on a search-backed table"),
      ERR_HINT("Set it in CREATE TABLE ... WITH (storage = 'search', ",
               kOptimizeTopKSetting,
               " = '...'); the table's store keeps the score bounds every "
               "index on it prunes with."));
  }
  if (info->where_clause) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("partial indexes are not supported on a search-backed table"));
  }
  if (GetSereneDBContext(context).SearchTxn().HasWritesFor(table.oid)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
      ERR_MSG("CREATE INDEX on a search-backed table cannot run in a "
              "transaction that has already written to \"",
              table.name.GetIdentifierName(), "\""));
  }

  auto& dependencies = info->dependencies;
  auto& catalog = table.ParentCatalog();
  duckdb::catalog_entry_callback_t lookup_callback =
    [&dependencies, &catalog](duckdb::CatalogEntry& entry) {
      if (&catalog == &entry.ParentCatalog()) {
        dependencies.AddDependency(entry);
      }
    };
  duckdb::IndexBinder index_binder(binder, context, &table, info.get());
  index_binder.SetCatalogLookupCallback(lookup_callback);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions;
  expressions.reserve(info->expressions.size());
  for (auto& expr : info->expressions) {
    expressions.emplace_back(index_binder.Bind(expr));
  }

  auto& get = plan->Cast<duckdb::LogicalGet>();
  for (const auto& column_id : get.GetColumnIds()) {
    const auto position = column_id.GetPrimaryIndex();
    info->column_ids.emplace_back(position);
    info->scan_types.emplace_back(get.returned_types[position]);
  }
  info->scan_types.emplace_back(duckdb::LogicalType::ROW_TYPE);
  info->names = get.names;
  auto qualification = table.ParentSchemaPath(
    table.ParentCatalog().GetCatalogTransaction(context));
  qualification.insert(qualification.begin(), table.ParentCatalog().GetName());
  info->SetQualifiedName(
    info->GetQualifiedName().WithQualification(std::move(qualification)));
  plan = duckdb::make_uniq<duckdb::LogicalEmptyResult>(std::move(plan));
  auto result = duckdb::make_uniq<duckdb::LogicalCreateIndex>(
    std::move(info), std::move(expressions), table, nullptr);
  result->children.emplace_back(std::move(plan));
  return std::move(result);
}

}  // namespace sdb::connector
