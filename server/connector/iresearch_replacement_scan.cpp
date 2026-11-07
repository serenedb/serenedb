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

#include "connector/iresearch_replacement_scan.h"

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry_retriever.hpp>
#include <duckdb/function/replacement_scan.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression_binder/where_binder.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

#include "catalog/entry/inverted_index.h"
#include "connector/scan/scan_bind.h"

namespace sdb::connector {
namespace {

duckdb::unique_ptr<duckdb::TableRef> IResearchReplacementScan(
  duckdb::ClientContext& context, duckdb::ReplacementScanInput& input,
  duckdb::optional_ptr<duckdb::ReplacementScanData> /*data*/) {
  duckdb::CatalogEntryRetriever retriever{context};
  auto index = duckdb::Catalog::GetEntry<duckdb::IndexCatalogEntry>(
    context, duckdb::Binder::BindTableName(retriever, input.name),
    duckdb::OnEntryNotFound::RETURN_NULL);
  if (!index || !catalog::IsInvertedIndex(*index)) {
    return nullptr;
  }
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> path;
  path.emplace_back(duckdb::ConstantExpression::String(
    index->ParentCatalog().GetName().GetIdentifierName()));
  for (const auto& schema :
       index->ParentSchemaPath(index->catalog.GetCatalogTransaction(context))) {
    path.emplace_back(
      duckdb::ConstantExpression::String(schema.GetIdentifierName()));
  }
  path.emplace_back(
    duckdb::ConstantExpression::String(index->name.GetIdentifierName()));
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arguments;
  arguments.emplace_back(duckdb::make_uniq<duckdb::FunctionExpression>(
    "list_value", std::move(path)));

  auto ref = duckdb::make_uniq<duckdb::TableFunctionRef>();
  ref->function = duckdb::make_uniq<duckdb::FunctionExpression>(
    "iresearch_scan", std::move(arguments));
  return std::move(ref);
}

void FilterPartialIndexScan(duckdb::ClientContext& context,
                            duckdb::unique_ptr<duckdb::LogicalOperator>& op) {
  auto& get = op->Cast<duckdb::LogicalGet>();
  if (get.function.GetName() != "iresearch_scan") {
    return;
  }
  const auto& bind = get.bind_data->Cast<ScanBindData>();
  if (bind.relation.kind != ScanEntryKind::SearchTableIndex ||
      !bind.relation.inverted_index->where_clause) {
    return;
  }
  const auto& index = *bind.relation.inverted_index;
  auto binder = duckdb::Binder::CreateBinder(context);
  binder->bind_context.AddTableFunction(get.table_index, index.name, get.names,
                                        get.returned_types,
                                        get.GetMutableColumnIds(), nullptr, {});
  duckdb::WhereBinder where_binder(*binder, context);
  auto predicate = index.where_clause->Copy();
  auto filter =
    duckdb::make_uniq<duckdb::LogicalFilter>(where_binder.Bind(predicate));
  filter->AddChild(std::move(op));
  op = std::move(filter);
}

}  // namespace

void RegisterIResearchReplacementScan(duckdb::DBConfig& config) {
  config.replacement_scans.emplace_back(IResearchReplacementScan);
}

void FilterPartialIndexScans(duckdb::ClientContext& context,
                             duckdb::LogicalOperator& plan) {
  for (auto& child : plan.children) {
    FilterPartialIndexScans(context, *child);
    if (child->type == duckdb::LogicalOperatorType::LOGICAL_GET) {
      FilterPartialIndexScan(context, child);
    }
  }
}

}  // namespace sdb::connector
