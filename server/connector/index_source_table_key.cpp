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

#include "connector/index_source_table_key.h"

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parallel/thread_context.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/transaction/transaction.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

namespace sdb::connector {

TableKeyIndexSource::TableKeyIndexSource(
  duckdb::ClientContext& context, ViewFastPath fast_path,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const ColumnId> bind_column_ids)
  : ViewIndexSourceBase{std::move(fast_path)} {
  SDB_ASSERT(_fast_path.catalog_ref);
  const auto& ref = *_fast_path.catalog_ref;
  _table = &duckdb::Catalog::GetEntry(
              context, duckdb::CatalogType::TABLE_ENTRY,
              duckdb::QualifiedName(duckdb::Identifier{ref.catalog},
                                    duckdb::Identifier{ref.schema},
                                    duckdb::Identifier{ref.table}))
              .Cast<duckdb::TableCatalogEntry>();
  duckdb::Transaction::Get(context, _table->ParentCatalog());
  const auto& columns = _table->GetColumns();
  const auto names = columns.GetColumnNames();
  InitProjection(context, projected_columns, projected_types, bind_column_ids,
                 SourceColumns{names}, [&](duckdb::idx_t column) {
                   _projected_slots.push_back(ScanColumn(column));
                   return columns.GetColumn(duckdb::LogicalIndex{column}).Type();
                 });
  for (const auto& key : _fast_path.key_columns) {
    _scan_keys.push_back(
      {.input_col_idx = ScanColumn(key.source_index), .type = key.type});
    _input_keys.push_back({.input_col_idx = _input_keys.size(), .type = key.type});
  }
  duckdb::vector<duckdb::LogicalType> types;
  types.reserve(_scan_columns.size());
  for (const auto column : _scan_columns) {
    types.push_back(columns.GetColumn(duckdb::LogicalIndex{column}).Type());
  }
  _scan.Initialize(context, types);
  _matched.Initialize(STANDARD_VECTOR_SIZE);
}

duckdb::idx_t TableKeyIndexSource::ScanColumn(duckdb::idx_t column) {
  if (const auto it = absl::c_find(_scan_columns, column);
      it != _scan_columns.end()) {
    return static_cast<duckdb::idx_t>(it - _scan_columns.begin());
  }
  const auto& definition = _table->GetColumn(duckdb::LogicalIndex{column});
  if (definition.Category() == duckdb::TableColumnType::GENERATED_VIRTUAL) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("cannot materialise generated column \"",
              definition.Name().GetIdentifierName(), "\" of \"",
              _table->name.GetIdentifierName(), "\" through an index lookup"));
  }
  _scan_columns.push_back(column);
  return _scan_columns.size() - 1;
}

void TableKeyIndexSource::IndexKeys(duckdb::Vector& pk, duckdb::idx_t count) {
  _input.data.clear();
  for (auto& child : duckdb::StructVector::GetEntries(pk)) {
    _input.data.emplace_back(duckdb::Vector::Ref(child));
  }
  _input.SetCardinality(count);
  primary_key::PreparePKFormats(_input, _input_keys, _formats);
  _pending.clear();
  _next.assign(count, duckdb::DConstants::INVALID_INDEX);
  for (auto row = count; row-- > 0;) {
    _key.clear();
    primary_key::Create(_formats, _input_keys, row, _key);
    const auto [it, inserted] = _pending.try_emplace(_key, row);
    if (!inserted) {
      _next[row] = it->second;
      it->second = row;
    }
  }
}

duckdb::unique_ptr<duckdb::Expression> TableKeyIndexSource::KeyFilter(
  const duckdb::LogicalGet& get, duckdb::Vector& pk) const {
  auto& children = duckdb::StructVector::GetEntries(pk);
  const auto column = [&](size_t key) {
    return duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      _scan_keys[key].type,
      duckdb::ColumnBinding(
        get.table_index, duckdb::ProjectionIndex(_scan_keys[key].input_col_idx)));
  };
  const auto value = [&](size_t key, duckdb::idx_t row) {
    return duckdb::make_uniq<duckdb::BoundConstantExpression>(
      children[key].GetValue(row));
  };
  if (_scan_keys.size() == 1) {
    auto in = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
      duckdb::ExpressionType::COMPARE_IN, duckdb::LogicalType::BOOLEAN);
    auto& arguments = in->GetChildrenMutable();
    arguments.reserve(_pending.size() + 1);
    arguments.push_back(column(0));
    for (const auto& [key, row] : _pending) {
      arguments.push_back(value(0, row));
    }
    return in;
  }
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> rows;
  rows.reserve(_pending.size());
  for (const auto& [key, row] : _pending) {
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> equalities;
    equalities.reserve(_scan_keys.size());
    for (size_t k = 0; k < _scan_keys.size(); ++k) {
      equalities.push_back(duckdb::BoundComparisonExpression::Create(
        duckdb::ExpressionType::COMPARE_EQUAL, column(k), value(k, row)));
    }
    rows.push_back(duckdb::BoundConjunctionExpression::Create(
      duckdb::ExpressionType::CONJUNCTION_AND, std::move(equalities)));
  }
  return duckdb::BoundConjunctionExpression::Create(
    duckdb::ExpressionType::CONJUNCTION_OR, std::move(rows));
}

duckdb::unique_ptr<duckdb::LogicalGet> TableKeyIndexSource::BindSource(
  duckdb::ClientContext& context, duckdb::Vector& pk) {
  auto get = BindTableScan(
    context, *_table,
    duckdb::EntryLookupInfo{duckdb::CatalogType::TABLE_ENTRY,
                            duckdb::QualifiedName(_table->name), nullptr,
                            duckdb::QueryErrorContext{}});
  auto& column_ids = get->GetMutableColumnIds();
  for (const auto column : _scan_columns) {
    column_ids.emplace_back(column);
  }
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> filters;
  filters.push_back(KeyFilter(*get, pk));
  get->function.pushdown_complex_filter(context, *get, get->bind_data.get(),
                                        filters);
  return get;
}

duckdb::idx_t TableKeyIndexSource::Collect(duckdb::idx_t found) {
  primary_key::PreparePKFormats(_scan, _scan_keys, _formats);
  duckdb::idx_t matched = 0;
  for (duckdb::idx_t row = 0;
       row < _scan.size() && found + matched < _survivor_idx.size(); ++row) {
    _key.clear();
    primary_key::Create(_formats, _scan_keys, row, _key);
    const auto it = _pending.find(_key);
    if (it == _pending.end() ||
        it->second == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    _survivor_idx[found + matched] = it->second;
    it->second = _next[it->second];
    _matched.set_index(matched++, row);
  }
  for (size_t c = 0; c < _projected_slots.size(); ++c) {
    duckdb::VectorOperations::Copy(_scan.data[_projected_slots[c]],
                                   _tf_target.data[c], _matched, matched, 0,
                                   found);
  }
  return found + matched;
}

duckdb::idx_t TableKeyIndexSource::Materialize(duckdb::ClientContext& context,
                                               duckdb::Vector& pk,
                                               duckdb::idx_t count,
                                               duckdb::DataChunk& output) {
  if (count == 0) {
    return 0;
  }
  SDB_ASSERT(pk.GetType().id() == duckdb::LogicalTypeId::STRUCT);
  SDB_ASSERT(count <= STANDARD_VECTOR_SIZE);
  AliasOutput(output);
  _survivor_idx.resize(count);
  IndexKeys(pk, count);
  auto get = BindSource(context, pk);
  duckdb::TableFunctionInitInput init(get->bind_data.get(),
                                      get->GetColumnIds(), {}, nullptr);
  auto global = get->function.init_global(context, init);
  duckdb::ThreadContext thread{context};
  duckdb::ExecutionContext exec{context, thread, nullptr};
  duckdb::unique_ptr<duckdb::LocalTableFunctionState> local;
  if (get->function.init_local) {
    local = get->function.init_local(exec, init, global.get());
  }
  duckdb::TableFunctionInput in(get->bind_data.get(), local.get(),
                                global.get());
  duckdb::idx_t found = 0;
  while (found < count) {
    _scan.Reset();
    get->function.function(context, in, _scan);
    if (_scan.size() == 0) {
      break;
    }
    found = Collect(found);
  }
  _tf_target.SetCardinality(found);
  RunCastPass(output, found);
  GatherNonLookupColumns(output, found);
  return found;
}

}  // namespace sdb::connector
