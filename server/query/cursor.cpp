////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "query/cursor.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_split.h>
#include <velox/vector/VariantToVector.h>

#include "basics/down_cast.h"
#include "basics/string_utils.h"
#include "query/config.h"
#include "query/query.h"
#include "utils.h"

namespace sdb::query {
namespace {

std::string ProcessPlan(std::string plan, bool clean_column_names) {
  // column_name<separator>unique_id -> column_name
  if (clean_column_names) {
    plan = CleanColumnNames(std::move(plan));
  }
  return plan;
}

}  // namespace

void Cursor::RequestCancel() {
  if (_runner) {
    SDB_ASSERT(!_query.HasExternal());
    _runner.RequestCancel();
  } else if (_query.HasExternal()) {
    std::ignore = _query.GetExternalExecutor().RequestCancel();
  }
}

Cursor::Process Cursor::Next(velox::RowVectorPtr& batch) {
  const auto& query_ctx = _query.GetContext();
  auto output_type = _query.GetOutputType();

  if (query_ctx.command_type.Has(CommandType::External)) {
    return ExecuteStmt();
  }

  if (query_ctx.command_type.Has(CommandType::Show)) {
    // SHOW name has only 1 column
    if (output_type->children().size() == 1) {
      return ExecuteShow(batch);
    }
    // SHOW ALL has 3 columns
    if (output_type->children().size() == 3) {
      return ExecuteShowAll(batch);
    }
    SDB_UNREACHABLE();
  }

  Process process = Process::Done;
  if (query_ctx.command_type.Has(CommandType::Query)) {
    process = ExecuteVelox(batch);
  }

  if (!query_ctx.command_type.Has(CommandType::Explain)) {
    return process;
  }

  batch = nullptr;
  if (process != Process::Done) {
    return process;
  }
  std::vector<std::string> data;
  const bool clean_column_names =
    !query_ctx.explain_params.Has(ExplainWith::Registers);

  auto post_process_plan = [&](std::string_view plan) {
    for (auto line : absl::StrSplit(plan, '\n')) {
      line = basics::string_utils::RTrim(line);
      if (!line.empty()) {
        data.emplace_back(line);
      }
    }
  };

  if (query_ctx.explain_params.Has(ExplainWith::Logical)) {
    data.emplace_back("LOGICAL PLAN:");
    post_process_plan(ProcessPlan(_query.GetLogicalPlan(), clean_column_names));
  }

  if (query_ctx.explain_params.Has(ExplainWith::InitialQueryGraph)) {
    data.emplace_back("INITIAL QUERY GRAPH:");
    post_process_plan(
      ProcessPlan(_query.GetInitialQueryGraphPlan(), clean_column_names));
  }

  if (query_ctx.explain_params.Has(ExplainWith::FinalQueryGraph)) {
    data.emplace_back("FINAL QUERY GRAPH:");
    post_process_plan(
      ProcessPlan(_query.GetFinalQueryGraphPlan(), clean_column_names));
  }

  if (query_ctx.explain_params.Has(ExplainWith::Physical)) {
    data.emplace_back("PHYSICAL PLAN:");
    post_process_plan(
      ProcessPlan(_query.GetPhysicalPlan(), clean_column_names));
  }

  if (query_ctx.explain_params.Has(ExplainWith::Execution)) {
    if (query_ctx.explain_params.Has(ExplainWith::Stats)) {
      data.emplace_back("EXECUTION PLAN WITH STATS:");
      post_process_plan(
        ProcessPlan(_runner.PrintPlanWithStats(), clean_column_names));
    } else {
      data.emplace_back("EXECUTION PLAN:");
      post_process_plan(
        ProcessPlan(_query.GetExecutionPlan(), clean_column_names));
    }
  }

  BuildBatch(batch, {std::move(data)});
  return Process::Done;
}

Cursor::Process Cursor::ExecuteVelox(velox::RowVectorPtr& batch) {
  SDB_ASSERT(!batch);
  SDB_ASSERT(_runner);
  yaclib::Future<> wait;
  batch = _runner.Next(wait);
  if (wait.Valid()) {
    SDB_ASSERT(!batch);
    std::move(wait).DetachInline(
      [user_task = _user_task](auto&&) { user_task(); });
    return Process::Wait;
  }
  if (batch) {
    return Process::More;
  }
  return Process::Done;
}

Cursor::Process Cursor::ExecuteStmt() {
  {
    std::lock_guard lock{_stmt_result_mutex};
    if (_stmt_result.State() != yaclib::ResultState::Empty) {
      auto r = std::move(_stmt_result).Ok();
      if (!r.ok()) {
        SDB_THROW(std::move(r));
      }
      return Process::Done;
    }
  }
  auto f = _query.GetExternalExecutor().Execute();
  if (!f.Valid()) {
    return Process::Done;
  }
  if (f.Ready()) {
    auto r = std::move(f).Touch().Ok();
    if (!r.ok()) {
      SDB_THROW(std::move(r));
    }
    return Process::Done;
  }

  std::move(f).DetachInline(
    [&, user_task = _user_task](yaclib::Result<Result>&& r) {
      {
        std::lock_guard lock{_stmt_result_mutex};
        _stmt_result = std::move(r);
      }
      user_task();
    });
  return Process::Wait;
}

Cursor::Process Cursor::ExecuteShowAll(velox::RowVectorPtr& batch) {
  SDB_ASSERT(_query.GetOutputType()->equivalent(
    *velox::ROW({velox::VARCHAR(), velox::VARCHAR(), velox::VARCHAR()})));
  const auto& query_config = _query.GetContext().velox_query_ctx->queryConfig();
  const auto& config = basics::downCast<Config>(*query_config.config());

  std::vector<std::string> names, values, descriptions;

  config.VisitFullDescription([&](std::string_view name, std::string_view value,
                                  std::string_view description) {
    names.emplace_back(name);
    values.push_back(std::string{value});
    descriptions.emplace_back(description);
  });

  BuildBatch(batch, {
                      std::move(names),
                      std::move(values),
                      std::move(descriptions),
                    });
  return Process::Done;
}

Cursor::Process Cursor::ExecuteShow(velox::RowVectorPtr& batch) {
  SDB_ASSERT(
    _query.GetOutputType()->equivalent(*velox::ROW({velox::VARCHAR()})));
  const auto& config = _query.GetContext().velox_query_ctx->queryConfig();
  const std::string& name = _query.GetOutputType()->nameOf(0);

#ifdef SDB_FAULT_INJECTION
  if (name.starts_with(kFailPointPrefix)) {
    std::string_view point = name;
    point.remove_prefix(kFailPointPrefix.size());
    if (point == "s") {
      auto column = GetFailurePointsDebugging();
      BuildBatch(batch, {std::move(column)});
      return Process::Done;
    }
    if (!point.starts_with('_')) {
      SDB_THROW(ERROR_FAILED,
                "failure point configuration parameter must start with '",
                kFailPointPrefix, "_'");
    }
    point.remove_prefix(1);
    std::vector<std::string> column{ShouldFailDebugging(point) ? "on" : "off"};
    BuildBatch(batch, {std::move(column)});
    return Process::Done;
  }
#endif

  auto value = config.get<std::string>(name);
  if (!value) {
    // TODO(codeworse) PG error code?
    SDB_THROW(ERROR_FAILED, "unrecognized configuration parameter \"", name,
              "\"");
  }
  std::vector<std::string> column{*value};
  BuildBatch(batch, {std::move(column)});
  return Process::Done;
}

void Cursor::BuildBatch(velox::RowVectorPtr& batch,
                        std::span<const std::vector<std::string>> columns) {
  SDB_ASSERT(_query.GetOutputType()->isRow());
  SDB_ASSERT(
    absl::c_all_of(_query.GetOutputType()->children(),
                   [](const auto& ptr) { return ptr == velox::VARCHAR(); }));
  std::vector<velox::VectorPtr> vectors;
  vectors.reserve(columns.size());
  size_t batch_rows = 0;
  for (size_t i = 0; i < columns.size(); ++i) {
    auto vector =
      velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
        _query.GetOutputType()->children()[i], columns[i].size(),
        _data_memory_pool.get());
    for (size_t j = 0; j < columns[i].size(); ++j) {
      vector->set(j, velox::StringView(columns[i][j]));
    }
    batch_rows = std::max(batch_rows, columns[i].size());
    vectors.push_back(std::move(vector));
  }

  batch = std::make_shared<velox::RowVector>(_data_memory_pool.get(),
                                             _query.GetOutputType(), nullptr,
                                             batch_rows, std::move(vectors));
}

Cursor::Cursor(std::function<void()>&& user_task, const Query& query)
  : _data_memory_pool{query.GetContext().velox_query_ctx->pool()->addLeafChild(
      "data_memory_pool")},
    _user_task{std::move(user_task)},
    _query{query} {
  if (_query.GetContext().command_type.Has(query::CommandType::Query)) {
    _runner = _query.MakeRunner();
  }
}

Cursor::~Cursor() {
  if (_query.HasExternal()) {
    std::ignore = _query.GetExternalExecutor().RequestCancel().Get();
  }
}

}  // namespace sdb::query
