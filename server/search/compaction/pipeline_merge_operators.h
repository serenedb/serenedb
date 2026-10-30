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

#include <duckdb/common/sorting/presorted_merge.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <duckdb/planner/operator/logical_extension_operator.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/merge/term_ranges.hpp>
#include <memory>
#include <span>

#include "search/compaction/merge_unit_run.h"

namespace sdb::search {

class LogicalPresortedMerge final : public duckdb::LogicalExtensionOperator {
 public:
  explicit LogicalPresortedMerge(
    duckdb::shared_ptr<duckdb::PresortedMerge> merge,
    duckdb::TableIndex table_index = duckdb::TableIndex(0));

  duckdb::PhysicalOperator& CreatePlan(
    duckdb::ClientContext& context,
    duckdb::PhysicalPlanGenerator& planner) final;
  duckdb::vector<duckdb::ColumnBinding> GetColumnBindings() final;
  duckdb::vector<duckdb::TableIndex> GetTableIndex() const final;
  duckdb::string GetName() const final;
  bool RequireOptimizer() const final { return false; }
  bool SupportSerialization() const final { return false; }

 protected:
  void ResolveTypes() final;

 private:
  duckdb::shared_ptr<duckdb::PresortedMerge> _merge;
  duckdb::TableIndex _table_index;
};

class LogicalMergeOrder final : public duckdb::LogicalExtensionOperator {
 public:
  explicit LogicalMergeOrder(irs::MergePlan& plan);

  duckdb::PhysicalOperator& CreatePlan(
    duckdb::ClientContext& context,
    duckdb::PhysicalPlanGenerator& planner) final;
  duckdb::vector<duckdb::ColumnBinding> GetColumnBindings() final;
  duckdb::vector<duckdb::TableIndex> GetTableIndex() const final;
  duckdb::string GetName() const final;
  bool RequireOptimizer() const final { return false; }
  bool SupportSerialization() const final { return false; }

 protected:
  void ResolveTypes() final;

 private:
  irs::MergePlan& _plan;
};

class LogicalPostingsMerge final : public duckdb::LogicalExtensionOperator {
 public:
  LogicalPostingsMerge(irs::TermRangeMerge& merge,
                       duckdb::TableIndex table_index,
                       std::shared_ptr<MergeUnitRun> run);

  duckdb::PhysicalOperator& CreatePlan(
    duckdb::ClientContext& context,
    duckdb::PhysicalPlanGenerator& planner) final;
  duckdb::vector<duckdb::ColumnBinding> GetColumnBindings() final;
  duckdb::vector<duckdb::TableIndex> GetTableIndex() const final;
  duckdb::string GetName() const final;
  bool RequireOptimizer() const final { return false; }
  bool SupportSerialization() const final { return false; }

 protected:
  void ResolveTypes() final;

 private:
  irs::TermRangeMerge& _merge;
  duckdb::TableIndex _table_index;
  std::shared_ptr<MergeUnitRun> _run;
};

class LogicalMergeUnits final : public duckdb::LogicalExtensionOperator {
 public:
  LogicalMergeUnits(
    std::shared_ptr<MergeUnitRun> run, duckdb::idx_t threads,
    std::span<const std::unique_ptr<irs::TermRangeMerge>> ranges);

  duckdb::PhysicalOperator& CreatePlan(
    duckdb::ClientContext& context,
    duckdb::PhysicalPlanGenerator& planner) final;
  duckdb::vector<duckdb::ColumnBinding> GetColumnBindings() final;
  duckdb::vector<duckdb::TableIndex> GetTableIndex() const final;
  duckdb::string GetName() const final;
  bool RequireOptimizer() const final { return false; }
  bool SupportSerialization() const final { return false; }

 protected:
  void ResolveTypes() final;

 private:
  std::shared_ptr<MergeUnitRun> _run;
  duckdb::idx_t _threads;
};

class PhysicalMergeOrder final : public duckdb::PhysicalOperator {
 public:
  PhysicalMergeOrder(duckdb::PhysicalPlan& physical_plan, irs::MergePlan& plan);

  bool IsSink() const final { return true; }
  bool ParallelSink() const final { return true; }
  duckdb::unique_ptr<duckdb::LocalSinkState> GetLocalSinkState(
    duckdb::ExecutionContext& context) const final;
  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const final;

  bool IsSource() const final { return true; }
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;

  duckdb::string GetName() const final;

 private:
  irs::MergePlan& _plan;
};

class PhysicalPostingsMerge final : public duckdb::PhysicalOperator {
 public:
  PhysicalPostingsMerge(duckdb::PhysicalPlan& physical_plan,
                        irs::TermRangeMerge& merge,
                        std::shared_ptr<MergeUnitRun> run);

  bool IsSink() const final { return true; }
  bool ParallelSink() const final { return true; }
  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext& context) const final;
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
  void FinishPartition(duckdb::GlobalSinkState& global,
                       duckdb::LocalSinkState& local) const;

  irs::TermRangeMerge& _merge;
  std::shared_ptr<MergeUnitRun> _run;
};

class PhysicalMergeUnits final : public duckdb::PhysicalOperator {
 public:
  PhysicalMergeUnits(duckdb::PhysicalPlan& physical_plan,
                     std::shared_ptr<MergeUnitRun> run, duckdb::idx_t threads);

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
  std::shared_ptr<MergeUnitRun> _run;
  duckdb::idx_t _threads;
};

}  // namespace sdb::search
