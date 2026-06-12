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

#include "network/pg/wire_collector.h"

#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/execution/operator/helper/physical_result_collector.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <duckdb/main/prepared_statement_data.hpp>

#include "connector/duckdb_client_state.h"
#include "network/connection.h"
#include "network/pg/wire_frames.h"

namespace sdb::network::pg {
namespace {

// The settings template is cloned per lstate (SerializationContext owns a
// per-lstate types cache and buffer pointer, so it is not copyable as-is).
// The types cache stays null: record serialization lazily creates it
// (GetSerializersCache), so flat-typed results skip the allocation.
sdb::pg::SerializationContext CloneProto(
  const sdb::pg::SerializationContext& proto) {
  sdb::pg::SerializationContext context;
  context.extra_float_digits = proto.extra_float_digits;
  context.bytea_output = proto.bytea_output;
  context.snapshot = proto.snapshot;
  context.quote_seq = proto.quote_seq;
  context.backslash_count = proto.backslash_count;
  return context;
}

class PgWireCollectorGlobalState : public duckdb::GlobalSinkState {
 public:
  std::shared_ptr<WireSinkContext> ctx;
};

class PgWireCollectorLocalState : public duckdb::LocalSinkState {
 public:
  // Parallel modes encode into this; direct mode points sctx at the session's
  // _send instead and never touches it. Starts tiny: short queries seal one
  // small chain (or none in direct mode).
  message::Buffer buffer{1024, kBufferMaxGrowth};
  sdb::pg::SerializationContext sctx;
  std::vector<sdb::pg::SerializationFunction> serializers;
  size_t sealed_total = 0;
  uint64_t current_batch = 0;
  bool initialized = false;
};

class PhysicalPgWireCollector : public duckdb::PhysicalResultCollector {
 public:
  PhysicalPgWireCollector(duckdb::PhysicalPlan& physical_plan,
                          duckdb::PreparedStatementData& data,
                          std::shared_ptr<WireSinkContext> ctx)
    : PhysicalResultCollector(physical_plan, data), _ctx{std::move(ctx)} {}

  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const override {
    auto& ctx = *input.global_state.Cast<PgWireCollectorGlobalState>().ctx;
    auto& lstate = input.local_state.Cast<PgWireCollectorLocalState>();
    if (!lstate.initialized) {
      lstate.sctx = CloneProto(ctx.proto);
      lstate.sctx.buffer =
        ctx.mode == WireSinkContext::Mode::Direct ? ctx.send : &lstate.buffer;
      lstate.serializers.reserve(types.size());
      for (size_t column = 0; column < types.size(); ++column) {
        lstate.serializers.push_back(sdb::pg::GetSerialization(
          types[column], FormatFor(ctx.formats, column), lstate.sctx));
      }
      lstate.current_batch = lstate.partition_info.batch_index.IsValid()
                               ? lstate.partition_info.batch_index.GetIndex()
                               : 0;
      lstate.initialized = true;
    }

    if (ctx.mode == WireSinkContext::Mode::Ordered) {
      lstate.current_batch = lstate.partition_info.batch_index.GetIndex();
      ctx.UpdateMinBatch(lstate.partition_info.min_batch_index.GetIndex());
    }

    if (OverHighWater(ctx, lstate)) {
      ctx.BlockSink(
        input.interrupt_state,
        ctx.mode == WireSinkContext::Mode::Ordered ? lstate.current_batch : 0);
      return duckdb::SinkResultType::BLOCKED;
    }

    auto& out = *lstate.sctx.buffer;
    WriteDataChunk(out, chunk, lstate.serializers, lstate.sctx);
    ctx.rows.fetch_add(chunk.size(), std::memory_order_relaxed);

    if (ctx.mode == WireSinkContext::Mode::Direct) {
      // Publish progress for the session's backpressure/unblock checks; the
      // send buffer's own threshold auto-flush streams the bytes out.
      ctx.direct_committed.store(out.TotalCommitted(),
                                 std::memory_order_release);
    } else if (out.TotalCommitted() - lstate.sealed_total >= kSendFlushSize) {
      Seal(ctx, lstate);
    }
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  duckdb::SinkNextBatchType NextBatch(
    duckdb::ExecutionContext& context,
    duckdb::OperatorSinkNextBatchInput& input) const override {
    auto& ctx = *input.global_state.Cast<PgWireCollectorGlobalState>().ctx;
    auto& lstate = input.local_state.Cast<PgWireCollectorLocalState>();
    if (lstate.initialized) {
      Seal(ctx, lstate);
    }
    lstate.current_batch = lstate.partition_info.batch_index.GetIndex();
    ctx.UpdateMinBatch(lstate.partition_info.min_batch_index.GetIndex());
    return duckdb::SinkNextBatchType::READY;
  }

  duckdb::SinkCombineResultType Combine(
    duckdb::ExecutionContext& context,
    duckdb::OperatorSinkCombineInput& input) const override {
    auto& ctx = *input.global_state.Cast<PgWireCollectorGlobalState>().ctx;
    auto& lstate = input.local_state.Cast<PgWireCollectorLocalState>();
    if (lstate.initialized) {
      Seal(ctx, lstate);
      if (ctx.mode == WireSinkContext::Mode::Ordered) {
        ctx.UpdateMinBatch(lstate.partition_info.min_batch_index.GetIndex());
      }
    }
    return duckdb::SinkCombineResultType::FINISHED;
  }

  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext& context) const override {
    auto state = duckdb::make_uniq<PgWireCollectorGlobalState>();
    state->ctx = _ctx;
    return std::move(state);
  }

  duckdb::unique_ptr<duckdb::LocalSinkState> GetLocalSinkState(
    duckdb::ExecutionContext& context) const override {
    return duckdb::make_uniq<PgWireCollectorLocalState>();
  }

  // The rows already went to the wire; the session only needs a valid result
  // object for the standard fetch/cleanup path.
  duckdb::unique_ptr<duckdb::QueryResult> GetResult(
    duckdb::GlobalSinkState& state) const override {
    auto& gstate = state.Cast<PgWireCollectorGlobalState>();
    auto cc = gstate.ctx->context.lock();
    auto collection = duckdb::make_uniq<duckdb::ColumnDataCollection>(
      duckdb::Allocator::DefaultAllocator(), types);
    return duckdb::make_uniq<duckdb::MaterializedQueryResult>(
      statement_type, properties, names, std::move(collection),
      cc->GetClientProperties());
  }

  bool ParallelSink() const override {
    return _ctx->mode != WireSinkContext::Mode::Direct;
  }

  // Without this the pipeline never populates partition_info and the ordered
  // Sink reads an unset batch_index.
  duckdb::OperatorPartitionInfo RequiredPartitionInfo() const override {
    return _ctx->mode == WireSinkContext::Mode::Ordered
             ? duckdb::OperatorPartitionInfo::BatchIndex()
             : duckdb::OperatorPartitionInfo::NoPartitionInfo();
  }

  bool SinkOrderDependent() const override { return true; }

 private:
  bool OverHighWater(const WireSinkContext& ctx,
                     const PgWireCollectorLocalState& lstate) const {
    if (ctx.mode == WireSinkContext::Mode::Direct) {
      return ctx.send->TotalCommitted() -
               ctx.send_written->load(std::memory_order_acquire) >
             kSendHighWater;
    }
    if (ctx.mode == WireSinkContext::Mode::Ordered) {
      const auto min_batch = ctx.MinBatch();
      const auto pinned = ctx.pinned_bytes.load(std::memory_order_relaxed);
      if (lstate.current_batch <= min_batch) {
        // The emit cursor: its output drains at socket speed, so it only
        // pauses for a slow client (and is released by writer progress --
        // the cursor must otherwise always advance).
        return ctx.queued_bytes.load(std::memory_order_relaxed) - pinned >
               kWireQueuedHighWater;
      }
      return lstate.current_batch > min_batch + kWireOrderedLookahead ||
             pinned > kWirePinnedHighWater;
    }
    return ctx.queued_bytes.load(std::memory_order_relaxed) >
           kWireQueuedHighWater;
  }

  void Seal(WireSinkContext& ctx, PgWireCollectorLocalState& lstate) const {
    lstate.sealed_total = lstate.buffer.TotalCommitted();
    auto chain = lstate.buffer.ReleaseChain();
    if (chain.head != nullptr) {
      ctx.PushChain(std::move(chain), ctx.mode == WireSinkContext::Mode::Ordered
                                        ? lstate.current_batch
                                        : 0);
    }
  }

  std::shared_ptr<WireSinkContext> _ctx;
};

}  // namespace

duckdb::unique_ptr<duckdb::PhysicalOperator> MakeWireCollector(
  duckdb::ClientContext& context, duckdb::PreparedStatementData& data) {
  auto state = context.registered_state->Get<connector::SereneDBClientState>(
    connector::kSereneDBClientStateKey);
  auto ctx = state ? state->wire_sink : nullptr;
  if (!ctx || data.properties.return_type !=
                duckdb::StatementReturnType::QUERY_RESULT) {
    return duckdb::PhysicalResultCollector::GetResultCollector(context, data);
  }
  ctx->context = context.shared_from_this();
  auto& physical_plan = *data.physical_plan;
  auto& root = physical_plan.Root();
  if (!duckdb::PhysicalPlanGenerator::PreserveInsertionOrder(context, root)) {
    ctx->mode = WireSinkContext::Mode::Unordered;
  } else if (duckdb::PhysicalPlanGenerator::UseBatchIndex(context, root)) {
    ctx->mode = WireSinkContext::Mode::Ordered;
  } else {
    ctx->mode = WireSinkContext::Mode::Direct;
  }
  return duckdb::make_uniq<PhysicalPgWireCollector>(physical_plan, data, ctx);
}

}  // namespace sdb::network::pg
