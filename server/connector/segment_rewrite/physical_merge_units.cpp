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

#include "connector/segment_rewrite/physical_merge_units.h"

#include <duckdb/common/mutex.hpp>
#include <optional>
#include <utility>

#include "search/compaction/merge_unit_run.h"

namespace sdb::connector {
namespace {

using Guard = duckdb::annotated_lock_guard<duckdb::annotated_mutex>;

struct MergeUnitsSourceState final : duckdb::GlobalSourceState {
  explicit MergeUnitsSourceState(duckdb::idx_t threads) noexcept
    : threads{threads} {}

  duckdb::idx_t MaxThreads() final { return threads; }

  duckdb::idx_t threads;
};

}  // namespace

PhysicalMergeUnits::PhysicalMergeUnits(
  duckdb::PhysicalPlan& physical_plan,
  std::shared_ptr<search::MergeUnitRun> run, duckdb::idx_t threads)
  : duckdb::PhysicalOperator(physical_plan,
                             duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT}, 0),
    _run{std::move(run)},
    _threads{threads} {}

duckdb::unique_ptr<duckdb::GlobalSourceState>
PhysicalMergeUnits::GetGlobalSourceState(duckdb::ClientContext&) const {
  return duckdb::make_uniq<MergeUnitsSourceState>(_threads);
}

duckdb::SourceResultType PhysicalMergeUnits::GetDataInternal(
  duckdb::ExecutionContext&, duckdb::DataChunk&,
  duckdb::OperatorSourceInput& input) const {
  auto& state = input.global_state.Cast<MergeUnitsSourceState>();
  std::optional<size_t> unit;
  {
    Guard guard{state.lock};
    if (_run->schedule.Done()) {
      return duckdb::SourceResultType::FINISHED;
    }
    unit = _run->schedule.Claim();
    if (!unit) {
      return state.BlockSource(input.interrupt_state);
    }
  }
  _run->Run(*unit);
  Guard guard{state.lock};
  state.UnblockTasks();
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

duckdb::string PhysicalMergeUnits::GetName() const { return "MERGE_UNITS"; }

}  // namespace sdb::connector
