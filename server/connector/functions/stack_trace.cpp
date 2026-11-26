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

#include "connector/functions/stack_trace.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "auth/enforce.h"
#include "query/config.h"
#include "server/utils/stack_trace.h"

namespace sdb::connector {
namespace {

constexpr char kSdbStackTrace[] = "sdb_stack_trace";

struct StackTraceState final : duckdb::GlobalTableFunctionState {
  std::vector<ThreadStack> stacks;
  size_t offset = 0;
};

duckdb::unique_ptr<duckdb::FunctionData> StackTraceBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::Identifier>& names) {
  auth::RequireSuperuser(context, kSdbStackTrace);
  const auto add = [&](std::string_view name, duckdb::LogicalType type) {
    names.emplace_back(name);
    return_types.emplace_back(std::move(type));
  };
  add("tid", duckdb::LogicalType::INTEGER);
  add("thread_name", duckdb::LogicalType::VARCHAR);
  add("frames", duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT));
  add("symbols", duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR));
  return nullptr;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> StackTraceInit(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto result = duckdb::make_uniq<StackTraceState>();
  {
    duckdb::TaskScheduler::BlockingScope blocking;
    result->stacks = CollectStacks(std::chrono::milliseconds{GetIntSetting(
                                     context, "sdb_stack_trace_timeout")},
                                   [&] { return context.IsInterrupted(); });
  }
  context.InterruptCheck();
  return result;
}

void StackTraceExecute(duckdb::ClientContext& context,
                       duckdb::TableFunctionInput& input,
                       duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<StackTraceState>();
  const auto count =
    std::min<size_t>(state.stacks.size() - state.offset, STANDARD_VECTOR_SIZE);
  auto tids = duckdb::FlatVector::Writer<int32_t>(output.data[0], count);
  auto names =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[1], count);
  auto frames = duckdb::FlatVector::Writer<duckdb::VectorListType<int64_t>>(
    output.data[2], count);
  auto symbols =
    duckdb::FlatVector::Writer<duckdb::VectorListType<duckdb::string_t>>(
      output.data[3], count);
  for (const auto& stack :
       std::span{state.stacks}.subspan(state.offset, count)) {
    tids.WriteValue(stack.tid);
    names.WriteValue(duckdb::string_t{
      stack.name.data(), static_cast<uint32_t>(stack.name.size())});
    size_t i = 0;
    for (auto& frame : frames.WriteList(stack.frames.size())) {
      frame.WriteValue(static_cast<int64_t>(stack.frames[i++]));
    }
    symbols.WriteNull();
  }
  state.offset += count;
  output.CheckCardinality(count);
}

}  // namespace

void RegisterStackTraceFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.RegisterFunction(duckdb::TableFunction{
    kSdbStackTrace, {}, StackTraceExecute, StackTraceBind, StackTraceInit});
}

}  // namespace sdb::connector
