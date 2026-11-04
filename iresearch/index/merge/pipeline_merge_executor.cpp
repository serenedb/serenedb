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

#include "iresearch/index/merge/pipeline_merge_executor.hpp"

#include <absl/cleanup/cleanup.h>

#include <algorithm>
#include <duckdb/common/enums/set_scope.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/query_result.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>
#include <duckdb/planner/logical_plan_verifier.hpp>
#include <duckdb/planner/operator/logical_extension_operator.hpp>
#include <memory>
#include <string>
#include <utility>

#include "iresearch/index/merge/merge_schedule.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {
namespace {

class MergeUnitsSourceState final : public duckdb::GlobalSourceState {
 public:
  explicit MergeUnitsSourceState(duckdb::idx_t threads) : _threads{threads} {}

  duckdb::idx_t MaxThreads() final { return _threads; }

 private:
  duckdb::idx_t _threads;
};

class PhysicalMergeUnits final : public duckdb::PhysicalOperator {
 public:
  PhysicalMergeUnits(duckdb::PhysicalPlan& physical_plan,
                     std::shared_ptr<MergeSchedule> schedule,
                     duckdb::idx_t threads)
    : duckdb::PhysicalOperator(physical_plan,
                               duckdb::PhysicalOperatorType::EXTENSION,
                               {duckdb::LogicalType::BIGINT}, 0),
      _schedule{std::move(schedule)},
      _threads{threads} {}

  bool IsSource() const final { return true; }
  bool ParallelSource() const final { return true; }
  duckdb::OrderPreservationType SourceOrder() const final {
    return duckdb::OrderPreservationType::NO_ORDER;
  }
  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext&) const final {
    return duckdb::make_uniq<MergeUnitsSourceState>(_threads);
  }
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk&,
    duckdb::OperatorSourceInput& input) const final {
    const auto unit = _schedule->Claim(input.interrupt_state);
    if (unit == MergeSchedule::kFinished) {
      return duckdb::SourceResultType::FINISHED;
    }
    if (unit == MergeSchedule::kNoUnit) {
      return duckdb::SourceResultType::BLOCKED;
    }
    if (!_schedule->Run(unit)) {
      context.client.Interrupt();
    }
    return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
  }
  void SourceFinished(duckdb::ClientContext&,
                      duckdb::GlobalSourceState&) const final {
    _schedule->Stop();
  }

  duckdb::string GetName() const final { return "MERGE_UNITS"; }

 private:
  std::shared_ptr<MergeSchedule> _schedule;
  duckdb::idx_t _threads;
};

class LogicalMergeUnits final : public duckdb::LogicalExtensionOperator {
 public:
  LogicalMergeUnits(std::shared_ptr<MergeSchedule> schedule,
                    duckdb::idx_t threads)
    : _schedule{std::move(schedule)}, _threads{threads} {}

  duckdb::PhysicalOperator& CreatePlan(
    duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) final {
    return planner.Make<PhysicalMergeUnits>(_schedule, _threads);
  }
  duckdb::vector<duckdb::ColumnBinding> GetColumnBindings() final {
    return GenerateColumnBindings(duckdb::TableIndex(0), 1);
  }
  duckdb::vector<duckdb::TableIndex> GetTableIndex() const final {
    return {duckdb::TableIndex(0)};
  }
  duckdb::string GetName() const final { return "MERGE_UNITS"; }
  bool RequireOptimizer() const final { return false; }
  bool SupportSerialization() const final { return false; }

 protected:
  void ResolveTypes() final { types = {duckdb::LogicalType::BIGINT}; }

 private:
  std::shared_ptr<MergeSchedule> _schedule;
  duckdb::idx_t _threads;
};

bool Execute(duckdb::Connection& connection,
             duckdb::unique_ptr<duckdb::LogicalOperator> root) {
  root->ResolveOperatorTypes();
  const auto result = connection.Query(
    duckdb::make_uniq<duckdb::LogicalPlanStatement>(std::move(root)));
  if (result->HasError() &&
      result->GetErrorType() != duckdb::ExceptionType::INTERRUPT) {
    result->ThrowError();
  }
  return !result->HasError();
}

}  // namespace

bool PipelineMergeExecutor::Run(MergePlan& plan) const {
  SDB_IF_FAILURE("irs::PipelineMergeExecutor::Run") { return false; }
  const auto threads = std::max<size_t>(_threads, 1);
  duckdb::Connection connection{_db};
  auto& context = *connection.context;
  context.SetSchedulerAccount(
    duckdb::TaskScheduler::GetScheduler(context).CreateAccount(
      duckdb::SchedulerClass::BACKGROUND));
  context.config.threads = threads;
  duckdb::Settings::Set<duckdb::MaxExecutionTimeSetting>(
    context, duckdb::SetScope::SESSION, duckdb::Value::BIGINT(0));
  const auto schedule = std::make_shared<MergeSchedule>(plan);
  auto units = duckdb::make_uniq<LogicalMergeUnits>(schedule, threads);
  SDB_ASSERT(!duckdb::LogicalPlanVerifier::VerifyAlways(*units).HasError());
  const bool completed = Execute(connection, std::move(units));
  return schedule->Result() && completed;
}

bool MergeSegments(Directory& dir, MergeDocMap& doc_map, SegmentMeta& segment,
                   const SegmentWriterOptions& options,
                   const PipelineMergeExecutor& executor,
                   const MergeProgress& progress, const AnnBuildEnv* env,
                   MergeProfile* profile) {
  SDB_ASSERT(options.db);
  bool merged = false;
  absl::Cleanup invalidate = [&merged, &segment]() noexcept {
    if (!merged) [[unlikely]] {
      segment.files.clear();
      static_cast<SegmentInfo&>(segment) = SegmentInfo{};
    }
  };
  MergePlan plan{dir,          doc_map,
                 segment,      options.scorer,
                 *options.db,  options.field_options,
                 progress,     env,
                 options.sort, options.hnsw_bases};
  absl::Cleanup report = [&plan, profile]() noexcept {
    if (profile) {
      *profile = plan.Profile();
    }
  };
  merged = plan.Prepare(executor.Threads()) && executor.Run(plan);
  return merged;
}

}  // namespace irs
