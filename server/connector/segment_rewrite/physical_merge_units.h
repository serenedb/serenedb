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

#include <duckdb/execution/physical_operator.hpp>
#include <memory>

namespace sdb::search {

struct MergeUnitRun;

}  // namespace sdb::search
namespace sdb::connector {

class PhysicalMergeUnits final : public duckdb::PhysicalOperator {
 public:
  PhysicalMergeUnits(duckdb::PhysicalPlan& physical_plan,
                     std::shared_ptr<search::MergeUnitRun> run,
                     duckdb::idx_t threads);

  bool IsSource() const final { return true; }
  bool ParallelSource() const final { return true; }
  duckdb::OrderPreservationType SourceOrder() const final {
    return duckdb::OrderPreservationType::NO_ORDER;
  }
  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const final;
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;

  duckdb::string GetName() const final;

 private:
  std::shared_ptr<search::MergeUnitRun> _run;
  duckdb::idx_t _threads;
};

}  // namespace sdb::connector
