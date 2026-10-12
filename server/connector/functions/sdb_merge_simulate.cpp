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

#include "connector/functions/sdb_merge_simulate.h"

#include <absl/strings/str_cat.h>
#include <absl/time/time.h>

#include <algorithm>
#include <array>
#include <duckdb/common/types/value.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/operator/logical_extension_operator.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/persistence/search_table.h"
#include "search/compaction/merge_policies.h"
#include "search/compaction/merge_simulator.h"
#include "search/maintenance.h"
#include "storage_engine/search_engine.h"

namespace sdb::connector {
namespace {

struct TraceColumn {
  std::string_view name;
  duckdb::LogicalTypeId type;
  bool required;
};

enum TraceField : size_t {
  kFinished = 0,
  kRelation,
  kDocs,
  kLiveDocs,
  kBytes,
  kRowidMin,
  kRowidMax,
  kType,
};

constexpr std::array<TraceColumn, 8> kTraceColumns{{
  {"finished", duckdb::LogicalTypeId::TIMESTAMP_TZ, true},
  {"relation_id", duckdb::LogicalTypeId::UBIGINT, false},
  {"output_docs", duckdb::LogicalTypeId::UBIGINT, true},
  {"output_live_docs", duckdb::LogicalTypeId::UBIGINT, false},
  {"output_bytes", duckdb::LogicalTypeId::UBIGINT, true},
  {"rowid_min", duckdb::LogicalTypeId::BIGINT, false},
  {"rowid_max", duckdb::LogicalTypeId::BIGINT, false},
  {"type", duckdb::LogicalTypeId::VARCHAR, false},
}};

struct ResultColumn {
  std::string_view name;
  duckdb::LogicalTypeId type;
};

constexpr std::array<ResultColumn, 22> kResultColumns{{
  {"event", duckdb::LogicalTypeId::BIGINT},
  {"time_s", duckdb::LogicalTypeId::DOUBLE},
  {"relation_id", duckdb::LogicalTypeId::BIGINT},
  {"type", duckdb::LogicalTypeId::VARCHAR},
  {"segment", duckdb::LogicalTypeId::VARCHAR},
  {"inputs", duckdb::LogicalTypeId::LIST},
  {"docs", duckdb::LogicalTypeId::BIGINT},
  {"live_docs", duckdb::LogicalTypeId::BIGINT},
  {"bytes", duckdb::LogicalTypeId::BIGINT},
  {"input_bytes", duckdb::LogicalTypeId::BIGINT},
  {"rowid_min", duckdb::LogicalTypeId::BIGINT},
  {"rowid_max", duckdb::LogicalTypeId::BIGINT},
  {"queued_s", duckdb::LogicalTypeId::DOUBLE},
  {"started_s", duckdb::LogicalTypeId::DOUBLE},
  {"threads", duckdb::LogicalTypeId::BIGINT},
  {"score", duckdb::LogicalTypeId::DOUBLE},
  {"lane", duckdb::LogicalTypeId::VARCHAR},
  {"segments", duckdb::LogicalTypeId::BIGINT},
  {"segment_bytes", duckdb::LogicalTypeId::BIGINT},
  {"overlap", duckdb::LogicalTypeId::DOUBLE},
  {"merges_queued", duckdb::LogicalTypeId::BIGINT},
  {"merges_running", duckdb::LogicalTypeId::BIGINT},
}};

duckdb::vector<duckdb::LogicalType> ResultTypes() {
  duckdb::vector<duckdb::LogicalType> types;
  types.reserve(kResultColumns.size());
  for (const auto& column : kResultColumns) {
    types.emplace_back(
      column.type == duckdb::LogicalTypeId::LIST
        ? duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)
        : duckdb::LogicalType{column.type});
  }
  return types;
}

duckdb::vector<duckdb::LogicalType> TraceTypes() {
  duckdb::vector<duckdb::LogicalType> types;
  types.reserve(kTraceColumns.size());
  for (const auto& column : kTraceColumns) {
    types.emplace_back(column.type);
  }
  return types;
}

struct SimulateGlobalState final : duckdb::GlobalSinkState {
  std::vector<search::SimulatedFlush> trace;
  std::vector<search::SimulatedEvent> events;
};

struct SimulateLocalState final : duckdb::LocalSinkState {
  SimulateLocalState(
    duckdb::ClientContext& context,
    const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& inputs)
    : executor{context, inputs} {
    columns.Initialize(context, TraceTypes());
  }

  duckdb::ExpressionExecutor executor;
  duckdb::DataChunk columns;
};

struct SimulateSourceState final : duckdb::GlobalSourceState {
  size_t next = 0;
};

[[noreturn]] void ThrowInvalid(std::string_view message) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("sdb_merge_simulate: ", message));
}

void ReadTrace(duckdb::DataChunk& columns,
               std::vector<search::SimulatedFlush>& trace) {
  auto finished = columns.data[kFinished].Values<duckdb::timestamp_tz_t>();
  auto relations = columns.data[kRelation].Values<uint64_t>();
  auto docs = columns.data[kDocs].Values<uint64_t>();
  auto live_docs = columns.data[kLiveDocs].Values<uint64_t>();
  auto bytes = columns.data[kBytes].Values<uint64_t>();
  auto rowid_min = columns.data[kRowidMin].Values<int64_t>();
  auto rowid_max = columns.data[kRowidMax].Values<int64_t>();
  auto types = columns.data[kType].Values<duckdb::string_t>();
  for (duckdb::idx_t row = 0; row < columns.size(); ++row) {
    const auto type = types[row];
    if (type.IsValid() && type.GetValue().GetString() != "flush") {
      continue;
    }
    const auto time = finished[row];
    const auto doc_count = docs[row];
    const auto byte_count = bytes[row];
    if (!time.IsValid() || !doc_count.IsValid() || !byte_count.IsValid()) {
      ThrowInvalid("finished, output_docs and output_bytes must not be NULL");
    }
    const auto live = live_docs[row];
    auto& flush = trace.emplace_back(search::SimulatedFlush{
      .time = absl::FromUnixMicros(time.GetValue().value),
      .relation_id = relations[row].IsValid() ? relations[row].GetValue() : 0,
      .docs = doc_count.GetValue(),
      .live_docs = live.IsValid() ? live.GetValue() : doc_count.GetValue(),
      .bytes = byte_count.GetValue(),
    });
    if (flush.live_docs > flush.docs) {
      ThrowInvalid("output_live_docs must not exceed output_docs");
    }
    const auto low = rowid_min[row];
    const auto high = rowid_max[row];
    if (low.IsValid() != high.IsValid()) {
      ThrowInvalid("rowid_min and rowid_max must be both NULL or both set");
    }
    if (!low.IsValid()) {
      continue;
    }
    if (high.GetValue() < low.GetValue()) {
      ThrowInvalid("rowid_max must not be below rowid_min");
    }
    flush.rowid_min = low.GetValue();
    flush.rowid_max = high.GetValue();
  }
}

duckdb::Value MergeOnly(bool merge, duckdb::Value value) {
  return merge ? std::move(value) : duckdb::Value{value.type()};
}

duckdb::Value Bigint(uint64_t value) {
  return duckdb::Value::BIGINT(static_cast<int64_t>(value));
}

duckdb::Value OptionalBigint(const std::optional<int64_t>& value) {
  return value ? duckdb::Value::BIGINT(*value)
               : duckdb::Value{duckdb::LogicalType::BIGINT};
}

void AppendEvent(duckdb::DataChunk& chunk, size_t number,
                 const search::SimulatedEvent& event) {
  const bool merge = event.type == search::SegmentEvent::Merge;
  duckdb::vector<duckdb::Value> inputs;
  inputs.reserve(event.inputs.size());
  for (const auto& input : event.inputs) {
    inputs.emplace_back(input);
  }
  size_t column = 0;
  auto append = [&](duckdb::Value value) {
    chunk.data[column++].Append(std::move(value));
  };
  append(Bigint(number));
  append(duckdb::Value::DOUBLE(absl::ToDoubleSeconds(event.at)));
  append(Bigint(event.relation_id));
  append(duckdb::Value{std::string{search::SegmentEventName(event.type)}});
  append(duckdb::Value{event.segment});
  append(MergeOnly(merge, duckdb::Value::LIST(duckdb::LogicalType::VARCHAR,
                                              std::move(inputs))));
  append(Bigint(event.docs));
  append(Bigint(event.live_docs));
  append(Bigint(event.bytes));
  append(MergeOnly(merge, Bigint(event.input_bytes)));
  append(OptionalBigint(event.rowid_min));
  append(OptionalBigint(event.rowid_max));
  append(MergeOnly(merge,
                   duckdb::Value::DOUBLE(absl::ToDoubleSeconds(event.queued))));
  append(MergeOnly(
    merge, duckdb::Value::DOUBLE(absl::ToDoubleSeconds(event.started))));
  append(MergeOnly(merge, Bigint(event.threads)));
  append(MergeOnly(merge, duckdb::Value::DOUBLE(event.score)));
  append(MergeOnly(
    merge, duckdb::Value{std::string{search::MergeLaneName(event.lane)}}));
  append(Bigint(event.segments));
  append(Bigint(event.segment_bytes));
  append(event.overlap ? duckdb::Value::DOUBLE(*event.overlap)
                       : duckdb::Value{duckdb::LogicalType::DOUBLE});
  append(Bigint(event.merges_queued));
  append(Bigint(event.merges_running));
}

class PhysicalMergeSimulate final : public duckdb::PhysicalOperator {
 public:
  PhysicalMergeSimulate(
    duckdb::PhysicalPlan& physical_plan,
    std::shared_ptr<const search::MergeSimulation> options,
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> inputs,
    duckdb::idx_t estimated_cardinality)
    : duckdb::PhysicalOperator(physical_plan,
                               duckdb::PhysicalOperatorType::EXTENSION,
                               ResultTypes(), estimated_cardinality),
      _options{std::move(options)},
      _inputs{std::move(inputs)} {}

  bool IsSink() const final { return true; }
  bool ParallelSink() const final { return false; }

  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext&) const final {
    return duckdb::make_uniq<SimulateGlobalState>();
  }

  duckdb::unique_ptr<duckdb::LocalSinkState> GetLocalSinkState(
    duckdb::ExecutionContext& context) const final {
    return duckdb::make_uniq<SimulateLocalState>(context.client, _inputs);
  }

  duckdb::SinkResultType Sink(duckdb::ExecutionContext&,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const final {
    auto& local = input.local_state.Cast<SimulateLocalState>();
    local.columns.Reset();
    local.executor.Execute(chunk, local.columns);
    ReadTrace(local.columns,
              input.global_state.Cast<SimulateGlobalState>().trace);
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  duckdb::SinkFinalizeType Finalize(
    duckdb::Pipeline&, duckdb::Event&, duckdb::ClientContext& context,
    duckdb::OperatorSinkFinalizeInput& input) const final {
    auto& state = input.global_state.Cast<SimulateGlobalState>();
    state.events =
      search::SimulateMerges(std::move(state.trace), *_options, [&context] {
        context.InterruptCheck();
        context.DeadlineCheck();
      });
    return duckdb::SinkFinalizeType::READY;
  }

  bool IsSource() const final { return true; }

  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext&) const final {
    return duckdb::make_uniq<SimulateSourceState>();
  }

  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext&, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final {
    const auto& events = sink_state->Cast<SimulateGlobalState>().events;
    auto& state = input.global_state.Cast<SimulateSourceState>();
    const auto end =
      std::min(events.size(), state.next + size_t{STANDARD_VECTOR_SIZE});
    for (; state.next != end; ++state.next) {
      AppendEvent(chunk, state.next + 1, events[state.next]);
    }
    return state.next == events.size()
             ? duckdb::SourceResultType::FINISHED
             : duckdb::SourceResultType::HAVE_MORE_OUTPUT;
  }

  duckdb::string GetName() const final { return "MERGE_SIMULATE"; }

 private:
  std::shared_ptr<const search::MergeSimulation> _options;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> _inputs;
};

class LogicalMergeSimulate final : public duckdb::LogicalExtensionOperator {
 public:
  LogicalMergeSimulate(
    duckdb::idx_t bind_index,
    std::shared_ptr<const search::MergeSimulation> options,
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> inputs,
    duckdb::unique_ptr<duckdb::LogicalOperator> trace)
    : duckdb::LogicalExtensionOperator(std::move(inputs)),
      _bind_index{bind_index},
      _options{std::move(options)} {
    children.emplace_back(std::move(trace));
  }

  duckdb::PhysicalOperator& CreatePlan(
    duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner) final {
    auto& trace = planner.CreatePlan(*children[0]);
    auto& simulate = planner.Make<PhysicalMergeSimulate>(
      _options, std::move(expressions), estimated_cardinality);
    simulate.children.push_back(trace);
    return simulate;
  }

  duckdb::vector<duckdb::ColumnBinding> GetColumnBindings() final {
    return GenerateColumnBindings(duckdb::TableIndex(_bind_index),
                                  kResultColumns.size());
  }

  duckdb::vector<duckdb::TableIndex> GetTableIndex() const final {
    return {duckdb::TableIndex(_bind_index)};
  }

  duckdb::string GetName() const final { return "MERGE_SIMULATE"; }
  bool SupportSerialization() const final { return false; }

 protected:
  void ResolveTypes() final { types = ResultTypes(); }

 private:
  duckdb::idx_t _bind_index;
  std::shared_ptr<const search::MergeSimulation> _options;
};

const duckdb::Value& NamedParameter(const duckdb::TableFunctionBindInput& input,
                                    std::string_view name) {
  const auto entry = input.named_parameters.find(duckdb::Identifier{name});
  SDB_ASSERT(entry != input.named_parameters.end());
  return entry->second;
}

uint64_t ReadPositive(const duckdb::TableFunctionBindInput& input,
                      std::string_view name, uint64_t fallback) {
  const auto& value = NamedParameter(input, name);
  if (value.IsNull()) {
    return fallback;
  }
  const auto number = value.GetValue<int64_t>();
  if (number < 1) {
    ThrowInvalid(absl::StrCat(name, " must be positive"));
  }
  return static_cast<uint64_t>(number);
}

std::shared_ptr<const search::MergeSimulation> ReadOptions(
  const duckdb::TableFunctionBindInput& input) {
  const auto& policy = NamedParameter(input, "policy");
  if (!policy.IsNull() && policy.GetValue<std::string>() != "tier") {
    ThrowInvalid("policy must be 'tier'");
  }
  const catalog::persistence::SearchTableOptions defaults;
  search::TasksSettings settings;
  settings.compaction_max_segments = ReadPositive(
    input, "compaction_max_segments", defaults.compaction_max_segments);
  settings.compaction_max_segments_bytes =
    ReadPositive(input, "compaction_max_segments_bytes",
                 defaults.compaction_max_segments_bytes);
  settings.compaction_floor_segment_bytes =
    ReadPositive(input, "compaction_floor_segment_bytes",
                 defaults.compaction_floor_segment_bytes);
  const auto threads = ReadPositive(
    input, "threads",
    static_cast<uint64_t>(search::SearchEngine::MaxConcurrentCompactions()));
  auto options = std::make_shared<search::MergeSimulation>();
  options->policy = search::MakeTierPolicy(settings);
  options->threads = threads;
  options->helpers_per_merge = threads / 2;
  options->bytes_per_second = static_cast<double>(ReadPositive(
    input, "merge_bytes_per_second",
    static_cast<uint64_t>(search::MergeSimulation{}.bytes_per_second)));
  return options;
}

duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> BindTrace(
  duckdb::ClientContext& context, const duckdb::TableFunctionBindInput& input,
  duckdb::LogicalOperator& trace) {
  const auto bindings = trace.GetColumnBindings();
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> inputs;
  inputs.reserve(kTraceColumns.size());
  for (const auto& column : kTraceColumns) {
    const auto& names = input.input_table_names;
    const auto it = std::ranges::find_if(
      names,
      [&](const duckdb::Identifier& name) { return name == column.name; });
    if (it == names.end()) {
      if (column.required) {
        ThrowInvalid(absl::StrCat("the trace has no ", column.name, " column"));
      }
      inputs.emplace_back(duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value{duckdb::LogicalType{column.type}}));
      continue;
    }
    const auto index = static_cast<size_t>(it - names.begin());
    inputs.emplace_back(duckdb::BoundCastExpression::AddCastToType(
      context,
      duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
        input.input_table_types[index], bindings[index]),
      duckdb::LogicalType{column.type}));
  }
  return inputs;
}

duckdb::unique_ptr<duckdb::LogicalOperator> SimulateBindOperator(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::TableIndex bind_index, duckdb::vector<duckdb::Identifier>& names) {
  SDB_ASSERT(input.input_plan && *input.input_plan);
  auto options = ReadOptions(input);
  auto trace = std::move(*input.input_plan);
  auto inputs = BindTrace(context, input, *trace);
  for (const auto& column : kResultColumns) {
    names.emplace_back(duckdb::Identifier{column.name});
  }
  return duckdb::make_uniq<LogicalMergeSimulate>(
    bind_index.index, std::move(options), std::move(inputs), std::move(trace));
}

}  // namespace

void RegisterSdbMergeSimulateFunction(duckdb::DatabaseInstance& db) {
  duckdb::TableFunction function{"sdb_merge_simulate",
                                 duckdb::FunctionSignature().AddPositionalOnly(
                                   "trace", duckdb::LogicalType::TABLE),
                                 nullptr};
  function.bind_operator = SimulateBindOperator;
  auto& signature = function.GetSignature();
  signature.AddKeywordOnly("policy", duckdb::LogicalType::VARCHAR,
                           duckdb::Value{duckdb::LogicalType::VARCHAR});
  for (const auto* name :
       {"threads", "merge_bytes_per_second", "compaction_max_segments",
        "compaction_max_segments_bytes", "compaction_floor_segment_bytes"}) {
    signature.AddKeywordOnly(name, duckdb::LogicalType::BIGINT,
                             duckdb::Value{duckdb::LogicalType::BIGINT});
  }
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.RegisterFunction(std::move(function));
}

}  // namespace sdb::connector
