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
#include <functional>

namespace irs {

class TermRangeMerge;

}  // namespace irs
namespace sdb::connector {

class PhysicalPostingsMerge final : public duckdb::PhysicalOperator {
 public:
  PhysicalPostingsMerge(duckdb::PhysicalPlan& physical_plan,
                        irs::TermRangeMerge& merge,
                        std::function<void()> on_part = {},
                        std::function<void()> on_done = {});

  bool IsSink() const final { return true; }
  bool ParallelSink() const final { return true; }
  duckdb::unique_ptr<duckdb::LocalSinkState> GetLocalSinkState(
    duckdb::ExecutionContext& context) const final;
  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const final;
  duckdb::SinkCombineResultType Combine(
    duckdb::ExecutionContext& context,
    duckdb::OperatorSinkCombineInput& input) const final;
  duckdb::SinkFinalizeType Finalize(
    duckdb::Pipeline& pipeline, duckdb::Event& event,
    duckdb::ClientContext& context,
    duckdb::OperatorSinkFinalizeInput& input) const final;

  bool IsSource() const final { return true; }
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;

  duckdb::string GetName() const final;
  duckdb::InsertionOrderPreservingMap<duckdb::string> ParamsToString()
    const final;

 private:
  void FinishPartition(duckdb::LocalSinkState& local) const;

  irs::TermRangeMerge& _merge;
  std::function<void()> _on_part;
  std::function<void()> _on_done;
};

}  // namespace sdb::connector
