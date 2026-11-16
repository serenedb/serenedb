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

#include "connector/optimizer/plan_index.h"

#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/operator/logical_join.hpp>

namespace sdb::optimizer {
namespace {

bool PadsWithNulls(const duckdb::LogicalOperator& op, size_t child) {
  switch (op.type) {
    case duckdb::LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
      return true;
    case duckdb::LogicalOperatorType::LOGICAL_JOIN:
    case duckdb::LogicalOperatorType::LOGICAL_DELIM_JOIN:
    case duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
    case duckdb::LogicalOperatorType::LOGICAL_ANY_JOIN:
    case duckdb::LogicalOperatorType::LOGICAL_ASOF_JOIN:
    case duckdb::LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
      break;
    default:
      return false;
  }
  const auto join_type = op.Cast<duckdb::LogicalJoin>().join_type;
  return child == 0 ? duckdb::IsRightOuterJoin(join_type)
                    : duckdb::IsLeftOuterJoin(join_type) ||
                        join_type == duckdb::JoinType::SINGLE;
}

bool BelowNullPadding(const duckdb::LogicalOperator& op,
                      duckdb::TableIndex target, bool padded) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    return padded && op.Cast<duckdb::LogicalGet>().table_index == target;
  }
  for (size_t i = 0; i != op.children.size(); ++i) {
    if (BelowNullPadding(*op.children[i], target,
                         padded || PadsWithNulls(op, i))) {
      return true;
    }
  }
  return false;
}

}  // namespace

duckdb::column_t PrimaryColumn(duckdb::ColumnBinding binding,
                               const duckdb::LogicalGet& get) {
  if (binding.table_index != get.table_index) {
    return duckdb::DConstants::INVALID_INDEX;
  }
  const auto col_idx = binding.column_index.GetIndex();
  const auto& column_ids = get.GetColumnIds();
  if (col_idx >= column_ids.size() || !column_ids[col_idx].HasPrimaryIndex()) {
    return duckdb::DConstants::INVALID_INDEX;
  }
  return column_ids[col_idx].GetPrimaryIndex();
}

connector::ScanBindData* SearchBindOf(duckdb::LogicalOperator& op) {
  if (op.type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    return nullptr;
  }
  auto& get = op.Cast<duckdb::LogicalGet>();
  return connector::IsSereneDBScan(get)
           ? &get.bind_data->Cast<connector::ScanBindData>()
           : nullptr;
}

PlannedScan::PlannedScan(duckdb::LogicalGet& get)
  : _get{&get}, _bind{&get.bind_data->Cast<connector::ScanBindData>()} {}

connector::ColumnId PlannedScan::IdAt(duckdb::idx_t get_column) const {
  return IdOf({_get->table_index, duckdb::ProjectionIndex{get_column}});
}

connector::ColumnId PlannedScan::IdOf(duckdb::ColumnBinding binding) const {
  const auto bind_column = PrimaryColumn(binding, *_get);
  return bind_column < _bind->columns.ids.size()
           ? _bind->columns.ids[bind_column]
           : connector::kInvalidColumnId;
}

std::span<const connector::ColumnId> PlannedScan::Ids() {
  Index();
  return _ids;
}

duckdb::idx_t PlannedScan::GetColumnOf(duckdb::idx_t bind_column) {
  Index();
  return bind_column < _get_columns.size() ? _get_columns[bind_column]
                                           : duckdb::DConstants::INVALID_INDEX;
}

AppendedColumn PlannedScan::Append(connector::ColumnId id,
                                   const duckdb::LogicalType& type,
                                   std::string_view name) {
  const auto column = AppendColumn(id, type, name);
  if (_indexed) {
    _columns_by_id.try_emplace(id, column.get_column);
  }
  return column;
}

duckdb::idx_t PlannedScan::Ensure(connector::ColumnId id,
                                  const duckdb::LogicalType& type,
                                  std::string_view name) {
  SDB_ASSERT(id != connector::kInvalidColumnId);
  Index();
  const auto [it, inserted] = _columns_by_id.try_emplace(id);
  if (inserted) {
    it->second = AppendColumn(id, type, name).get_column;
  }
  return it->second;
}

AppendedColumn PlannedScan::AppendColumn(connector::ColumnId id,
                                         const duckdb::LogicalType& type,
                                         std::string_view name) {
  auto& get = *_get;
  auto& columns = _bind->columns;
  const auto bind_column = columns.ids.size();
  columns.ids.push_back(id);
  columns.types.push_back(type);
  get.returned_types.push_back(type);
  get.names.emplace_back(name);
  const auto get_column = get.GetColumnIds().size();
  const auto projection = get.AddColumnId(bind_column);
  if (!get.projection_ids.empty()) {
    get.projection_ids.push_back(projection);
  }
  get.types.push_back(type);
  if (_indexed) {
    SDB_ASSERT(_get_columns.size() == bind_column);
    _ids.push_back(id);
    _get_columns.push_back(get_column);
  }
  return {.bind_column = bind_column, .get_column = get_column};
}

void PlannedScan::Index() {
  if (_indexed) {
    return;
  }
  _indexed = true;
  const auto& columns = _get->GetColumnIds();
  _ids.reserve(columns.size());
  _get_columns.assign(_bind->columns.ids.size(),
                      duckdb::DConstants::INVALID_INDEX);
  for (duckdb::idx_t get_column = 0; get_column != columns.size();
       ++get_column) {
    const auto id = IdAt(get_column);
    _ids.push_back(id);
    if (id == connector::kInvalidColumnId) {
      continue;
    }
    auto& first = _get_columns[columns[get_column].GetPrimaryIndex()];
    if (first == duckdb::DConstants::INVALID_INDEX) {
      first = get_column;
    }
    _columns_by_id.try_emplace(id, get_column);
  }
}

PlanIndex::PlanIndex(duckdb::LogicalOperator& root) { Visit(root, false); }

PlannedScan* PlanIndex::Scan(duckdb::TableIndex index) {
  const auto it = _producers.find(index.index);
  return it == _producers.end() ? nullptr : it->second.scan;
}

const duckdb::LogicalProjection* PlanIndex::Projection(
  duckdb::TableIndex index) const {
  const auto it = _producers.find(index.index);
  return it == _producers.end() ? nullptr : it->second.projection;
}

ForwardedBinding PlanIndex::Walk(duckdb::ColumnBinding binding) const {
  while (const auto* projection = Projection(binding.table_index)) {
    const auto column = binding.column_index.GetIndex();
    if (column >= projection->expressions.size()) {
      break;
    }
    const auto& forwarded = *projection->expressions[column];
    if (forwarded.GetExpressionType() !=
        duckdb::ExpressionType::BOUND_COLUMN_REF) {
      return {binding, &forwarded};
    }
    binding = forwarded.Cast<duckdb::BoundColumnRefExpression>().Binding();
  }
  return {binding, nullptr};
}

ScanColumn PlanIndex::Column(duckdb::ColumnBinding binding) {
  const auto resolved = Walk(binding).binding;
  return {Scan(resolved.table_index), resolved};
}

bool PlanIndex::NullPadded(const duckdb::LogicalOperator& op,
                           const PlannedScan& scan) const {
  const auto index = scan.Get().table_index;
  const auto it = _producers.find(index.index);
  SDB_ASSERT(it != _producers.end() && it->second.scan == &scan);
  return it->second.padded && BelowNullPadding(op, index, false);
}

duckdb::ColumnBinding PlanIndex::Expose(duckdb::TableIndex anchor,
                                        const PlannedScan& scan,
                                        duckdb::idx_t get_column,
                                        std::string_view name,
                                        const duckdb::LogicalType& type) {
  const auto target = scan.Get().table_index;
  if (anchor == target) {
    return {target, duckdb::ProjectionIndex{get_column}};
  }
  const auto it = _producers.find(anchor.index);
  SDB_ASSERT(it != _producers.end() && it->second.projection);
  auto* projection = it->second.projection;
  auto& expressions = projection->expressions;
  for (duckdb::idx_t i = 0; i != expressions.size(); ++i) {
    const auto& e = *expressions[i];
    if (e.GetExpressionType() != duckdb::ExpressionType::BOUND_COLUMN_REF) {
      continue;
    }
    const auto binding = e.Cast<duckdb::BoundColumnRefExpression>().Binding();
    if (binding.table_index == target &&
        binding.column_index.GetIndex() == get_column) {
      return {anchor, duckdb::ProjectionIndex{i}};
    }
  }
  expressions.push_back(duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    duckdb::Identifier{name}, type,
    duckdb::ColumnBinding{target, duckdb::ProjectionIndex{get_column}}));
  if (!projection->types.empty()) {
    projection->types.push_back(type);
  }
  return {anchor, duckdb::ProjectionIndex{expressions.size() - 1}};
}

void PlanIndex::Add(duckdb::LogicalOperator& op) { Register(op, true); }

void PlanIndex::AddTree(duckdb::LogicalOperator& op) { Visit(op, true); }

void PlanIndex::Visit(duckdb::LogicalOperator& op, bool padded) {
  Register(op, padded);
  for (size_t i = 0; i != op.children.size(); ++i) {
    Visit(*op.children[i], padded || PadsWithNulls(op, i));
  }
}

void PlanIndex::Register(duckdb::LogicalOperator& op, bool padded) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    AddScan(op.Cast<duckdb::LogicalGet>(), padded);
    return;
  }
  if (op.type != duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    return;
  }
  auto& projection = op.Cast<duckdb::LogicalProjection>();
  PlannedScan* scan = nullptr;
  if (projection.children.size() == 1 &&
      projection.children[0]->type ==
        duckdb::LogicalOperatorType::LOGICAL_GET) {
    scan = AddScan(projection.children[0]->Cast<duckdb::LogicalGet>(), padded);
  }
  _producers.try_emplace(projection.table_index.index,
                         Producer{.projection = &projection, .scan = scan});
}

PlannedScan* PlanIndex::AddScan(duckdb::LogicalGet& get, bool padded) {
  if (!connector::IsSereneDBScan(get)) {
    return nullptr;
  }
  const auto [it, inserted] = _producers.try_emplace(get.table_index.index);
  if (inserted) {
    auto& scan = _scans.emplace_back(get);
    _stores_expressions =
      _stores_expressions ||
      scan.Bind().relation.ScannedIndex().StoresExpressions();
    it->second = {.scan = &scan, .padded = padded};
  }
  SDB_ASSERT(it->second.scan && &it->second.scan->Get() == &get);
  return it->second.scan;
}

}  // namespace sdb::optimizer
