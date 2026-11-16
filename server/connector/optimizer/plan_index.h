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

#pragma once

#include <deque>
#include <duckdb/planner/column_binding.hpp>
#include <duckdb/planner/expression.hpp>
#include <duckdb/planner/logical_operator.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <span>
#include <string_view>
#include <vector>

#include "connector/column_id.h"
#include "connector/scan/scan_bind.h"

namespace sdb::optimizer {

duckdb::column_t PrimaryColumn(duckdb::ColumnBinding binding,
                               const duckdb::LogicalGet& get);

connector::ScanBindData* SearchBindOf(duckdb::LogicalOperator& op);

struct AppendedColumn {
  duckdb::idx_t bind_column;
  duckdb::idx_t get_column;
};

class PlannedScan {
 public:
  explicit PlannedScan(duckdb::LogicalGet& get);

  duckdb::LogicalGet& Get() const noexcept { return *_get; }
  connector::ScanBindData& Bind() const noexcept { return *_bind; }

  connector::ColumnId IdAt(duckdb::idx_t get_column) const;
  connector::ColumnId IdOf(duckdb::ColumnBinding binding) const;
  std::span<const connector::ColumnId> Ids();
  duckdb::idx_t GetColumnOf(duckdb::idx_t bind_column);

  AppendedColumn Append(connector::ColumnId id, const duckdb::LogicalType& type,
                        std::string_view name);
  duckdb::idx_t Ensure(connector::ColumnId id, const duckdb::LogicalType& type,
                       std::string_view name);

 private:
  void Index();
  AppendedColumn AppendColumn(connector::ColumnId id,
                              const duckdb::LogicalType& type,
                              std::string_view name);

  duckdb::LogicalGet* _get;
  connector::ScanBindData* _bind;
  std::vector<connector::ColumnId> _ids;
  std::vector<duckdb::idx_t> _get_columns;
  irs::containers::FlatHashMap<connector::ColumnId, duckdb::idx_t>
    _columns_by_id;
  bool _indexed = false;
};

struct ScanColumn {
  PlannedScan* scan = nullptr;
  duckdb::ColumnBinding binding;

  explicit operator bool() const noexcept { return scan; }
};

struct ForwardedBinding {
  duckdb::ColumnBinding binding;
  const duckdb::Expression* stop_expr = nullptr;
};

class PlanIndex {
 public:
  explicit PlanIndex(duckdb::LogicalOperator& root);

  PlannedScan* Scan(duckdb::TableIndex index);
  const duckdb::LogicalProjection* Projection(duckdb::TableIndex index) const;
  ForwardedBinding Walk(duckdb::ColumnBinding binding) const;
  ScanColumn Column(duckdb::ColumnBinding binding);
  bool NullPadded(const duckdb::LogicalOperator& op,
                  const PlannedScan& scan) const;
  bool AnyScan() const noexcept { return !_scans.empty(); }
  bool StoresExpressions() const noexcept { return _stores_expressions; }

  duckdb::ColumnBinding Expose(duckdb::TableIndex anchor,
                               const PlannedScan& scan,
                               duckdb::idx_t get_column, std::string_view name,
                               const duckdb::LogicalType& type);

  void Add(duckdb::LogicalOperator& op);
  void AddTree(duckdb::LogicalOperator& op);

 private:
  struct Producer {
    duckdb::LogicalProjection* projection = nullptr;
    PlannedScan* scan = nullptr;
    bool padded = false;
  };

  void Visit(duckdb::LogicalOperator& op, bool padded);
  void Register(duckdb::LogicalOperator& op, bool padded);
  PlannedScan* AddScan(duckdb::LogicalGet& get, bool padded);

  irs::containers::FlatHashMap<duckdb::idx_t, Producer> _producers;
  std::deque<PlannedScan> _scans;
  bool _stores_expressions = false;
};

}  // namespace sdb::optimizer
