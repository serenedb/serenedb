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

#include "iresearch/index/flush_order.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/enums/order_type.hpp>
#include <duckdb/common/sorting/sort.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/parallel/interrupt.hpp>
#include <duckdb/parallel/thread_context.hpp>
#include <duckdb/planner/bound_result_modifier.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <memory>
#include <numeric>
#include <utility>

#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/index/merge/sort_spec.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

std::vector<uint32_t> FlushOrder(duckdb::DatabaseInstance& db,
                                 const SortSpec& spec, ColWriter& columns,
                                 uint64_t rows) {
  std::vector<uint32_t> new2old(rows);
  std::iota(new2old.begin(), new2old.end(), uint32_t{0});
  std::vector<const ColumnWriter*> keys;
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::BoundOrderByNode> orders;
  for (const auto& key : spec.keys) {
    auto* column = columns.StagedColumn(key.column);
    if (column == nullptr) {
      continue;
    }
    column->PadNullsTo(rows);
    orders.emplace_back(key.descending ? duckdb::OrderType::DESCENDING
                                       : duckdb::OrderType::ASCENDING,
                        key.nulls_first ? duckdb::OrderByNullType::NULLS_FIRST
                                        : duckdb::OrderByNullType::NULLS_LAST,
                        duckdb::make_uniq<duckdb::BoundReferenceExpression>(
                          column->Type(), duckdb::storage_t{types.size()}));
    types.push_back(column->Type());
    keys.push_back(column);
  }
  if (keys.empty() || rows < 2) {
    return new2old;
  }
  const auto ordinal = types.size();
  types.push_back(duckdb::LogicalType::UINTEGER);

  duckdb::Connection connection{db};
  auto& context = *connection.context;
  std::unique_ptr<duckdb::Sort> sort;
  context.RunFunctionInTransaction([&] {
    sort = std::make_unique<duckdb::Sort>(
      context, orders, types, duckdb::vector<duckdb::idx_t>{ordinal});
  });
  auto global = sort->GetGlobalSinkState(context);
  duckdb::InterruptState interrupt;
  duckdb::ThreadContext thread{context};
  duckdb::ExecutionContext execution{context, thread, nullptr};
  auto local = sort->GetLocalSinkState(execution);
  duckdb::DataChunk chunk;
  chunk.Initialize(context, types);
  for (uint64_t first = 0, c = 0; first < rows; ++c) {
    const auto n = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(STANDARD_VECTOR_SIZE, rows - first));
    chunk.Reset();
    for (size_t k = 0; k != keys.size(); ++k) {
      const auto& staged = keys[k]->Staged()[c];
      SDB_ASSERT(staged.count == n);
      chunk.data[k].Reference(staged.data);
    }
    auto* ordinals =
      duckdb::FlatVector::GetDataMutable<uint32_t>(chunk.data[ordinal]);
    for (duckdb::idx_t i = 0; i != n; ++i) {
      ordinals[i] = static_cast<uint32_t>(first + i);
    }
    chunk.SetChildCardinality(n);
    duckdb::OperatorSinkInput input{*global, *local, interrupt};
    sort->Sink(execution, chunk, input);
    first += n;
  }
  duckdb::OperatorSinkCombineInput combine{*global, *local, interrupt};
  sort->Combine(execution, combine);
  duckdb::OperatorSinkFinalizeInput finalize{*global, interrupt};
  sort->Finalize(context, finalize);
  auto source = sort->GetGlobalSourceState(context, *global);
  auto reader = sort->GetLocalSourceState(execution, *source);
  duckdb::DataChunk out;
  out.Initialize(context, {duckdb::LogicalType::UINTEGER});
  uint64_t at = 0;
  while (true) {
    out.Reset();
    duckdb::OperatorSourceInput input{*source, *reader, interrupt};
    const auto result = sort->GetData(execution, out, input);
    const auto* sorted = duckdb::FlatVector::GetData<uint32_t>(out.data[0]);
    SDB_ASSERT(at + out.size() <= rows);
    std::copy_n(sorted, out.size(), new2old.begin() + at);
    at += out.size();
    if (result == duckdb::SourceResultType::FINISHED) {
      break;
    }
  }
  SDB_ASSERT(at == rows);
  return new2old;
}

DocOrder MakeDocOrder(std::vector<uint32_t> new2old) {
  DocOrder order;
  if (absl::c_is_sorted(new2old)) {
    return order;
  }
  order.old2new.resize(new2old.size());
  for (uint32_t row = 0; const auto old : new2old) {
    order.old2new[old] = row++;
  }
  order.new2old = std::move(new2old);
  return order;
}

}  // namespace irs
