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
#include <duckdb/function/replacement_scan.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>

#include "connector/inverted_store_index.h"

namespace sdb::connector {
namespace {

duckdb::unique_ptr<duckdb::ParsedExpression> NameArgument(
  const duckdb::Identifier& value) {
  return duckdb::make_uniq<duckdb::ConstantExpression>(
    duckdb::Value{value.GetIdentifierName()});
}

duckdb::unique_ptr<duckdb::TableRef> IResearchReplacementScan(
  duckdb::ClientContext& context, duckdb::ReplacementScanInput& input,
  duckdb::optional_ptr<duckdb::ReplacementScanData> /*data*/) {
  auto index = duckdb::Catalog::GetEntry<duckdb::IndexCatalogEntry>(
    context, input.name, duckdb::OnEntryNotFound::RETURN_NULL);
  if (!index || !IsInvertedIndex(*index)) {
    return nullptr;
  }
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arguments;
  arguments.push_back(NameArgument(index->ParentCatalog().GetName()));
  arguments.push_back(NameArgument(index->ParentSchema().name));
  arguments.push_back(NameArgument(index->name));

  auto ref = duckdb::make_uniq<duckdb::TableFunctionRef>();
  ref->function = duckdb::make_uniq<duckdb::FunctionExpression>(
    "iresearch_scan", std::move(arguments));
  return std::move(ref);
}

}  // namespace

void RegisterIResearchReplacementScan(duckdb::DBConfig& config) {
  config.replacement_scans.emplace_back(IResearchReplacementScan);
}

}  // namespace sdb::connector
