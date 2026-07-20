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
#include <duckdb/parallel/task_scheduler.hpp>

#include "basics/debugging.h"
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
  if (proto.time_zone) {
    context.time_zone.reset(proto.time_zone->clone());
  }
  context.snapshot = proto.snapshot;
  context.quote_seq = proto.quote_seq;
  context.backslash_count = proto.backslash_count;
  context.copy_text = proto.copy_text;
  context.copy_delim = proto.copy_delim;
  context.copy_null = proto.copy_null;
  return context;
}

class PgWireCollectorGlobalState : public duckdb::GlobalSinkState {
 public:
  std::shared_ptr<WireSinkContext> ctx;
};

class PgWireCollectorLocalState : public duckdb::LocalSinkState {
 public:
  // Parallel mode encodes into this; direct mode points sctx at the session's
  // _send instead and never touches it. Starts tiny: short queries seal one
  // small chain (or none in direct mode).
  message::Buffer buffer{kWireChunkMin, kWireChunkMax};
  sdb::pg::SerializationContext sctx;
  std::vector<sdb::pg::SerializationFunction> serializers;
  // Committed-bytes watermark at the last Seal; the gap to the buffer's
  // TotalCommitted() drives the next seal. Direct mode reads ctx.send instead.
  size_t sealed_total = 0;
  // Armed by the session only for COPY ... TO STDOUT (binary/text); the
  // collector then feeds pg_stat_progress_copy per chunk. Null on every other
  // statement.
  sdb::pg::ProgressMetrics* progress = nullptr;
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
      lstate.serializers.reserve(types.size());
      for (size_t column = 0; column < types.size(); ++column) {
        lstate.serializers.push_back(sdb::pg::GetSerialization(
          types[column], FormatFor(ctx.formats, column), lstate.sctx));
      }
      if (ctx.row_encoding != RowEncoding::DataRow) {
        if (auto state = context.client.registered_state
                           ->Get<connector::SereneDBClientState>(
                             connector::kSereneDBClientStateKey)) {
          lstate.progress = &state->Progress();
        }
      }
      lstate.initialized = true;
    }

    if (OverHighWater(ctx)) {
      ctx.BlockSink(input.interrupt_state);
      return duckdb::SinkResultType::BLOCKED;
    }

    auto& out =
      ctx.mode == WireSinkContext::Mode::Direct ? *ctx.send : lstate.buffer;
    if (ctx.paged) {
      // Protocol max_rows: emit up to the row budget on the single Direct sink,
      // then suspend. DuckDB re-delivers this chunk after BLOCKED, so
      // page_offset resumes a partially emitted chunk on the next Execute.
      const auto sent = ctx.rows.load(std::memory_order_relaxed);
      const auto budget = ctx.row_budget.load(std::memory_order_acquire);
      if (sent >= budget) {
        ctx.BlockSink(input.interrupt_state);
        return duckdb::SinkResultType::BLOCKED;
      }
      const duckdb::idx_t start = ctx.page_offset;
      const duckdb::idx_t take =
        std::min<duckdb::idx_t>(chunk.size() - start, budget - sent);
      WriteDataChunk(out, chunk, lstate.serializers, lstate.sctx, start,
                     start + take);
      ctx.rows.fetch_add(take, std::memory_order_relaxed);
      ctx.direct_committed.store(out.TotalCommitted(),
                                 std::memory_order_release);
      if (start + take < chunk.size()) {
        ctx.page_offset = start + take;
        ctx.BlockSink(input.interrupt_state);
        return duckdb::SinkResultType::BLOCKED;
      }
      ctx.page_offset = 0;
      return duckdb::SinkResultType::NEED_MORE_INPUT;
    }

    switch (ctx.row_encoding) {
      case RowEncoding::DataRow:
        WriteDataChunk(out, chunk, lstate.serializers, lstate.sctx, 0,
                       chunk.size());
        break;
      case RowEncoding::CopyBinary:
        WriteCopyChunk<RowEncoding::CopyBinary>(out, chunk, lstate.serializers,
                                                lstate.sctx);
        break;
      case RowEncoding::CopyText:
        WriteCopyChunk<RowEncoding::CopyText>(out, chunk, lstate.serializers,
                                              lstate.sctx);
        break;
    }
    ctx.rows.fetch_add(chunk.size(), std::memory_order_relaxed);
    if (lstate.progress) {
      sdb::pg::ProgressMetrics::Add(lstate.progress->tuples_processed,
                                    static_cast<int64_t>(chunk.size()));
      sdb::pg::ProgressMetrics::Add(
        lstate.progress->bytes_processed,
        static_cast<int64_t>(chunk.GetAllocationSize()));
      SDB_WAIT_ON_FAILURE("pause_copy_to_mid_stream");
    }

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

  duckdb::SinkCombineResultType Combine(
    duckdb::ExecutionContext& context,
    duckdb::OperatorSinkCombineInput& input) const override {
    auto& ctx = *input.global_state.Cast<PgWireCollectorGlobalState>().ctx;
    auto& lstate = input.local_state.Cast<PgWireCollectorLocalState>();
    if (lstate.initialized) {
      Seal(ctx, lstate);
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
    auto collection = duckdb::make_uniq<duckdb::ColumnDataCollection>(
      duckdb::Allocator::DefaultAllocator(), types);
    return duckdb::make_uniq<duckdb::MaterializedQueryResult>(
      statement_type, properties, duckdb::IdentifiersToStrings(names),
      std::move(collection), duckdb::ClientProperties{});
  }

  bool ParallelSink() const override {
    return _ctx->mode != WireSinkContext::Mode::Direct;
  }

  // Direct depends on receiving chunks in pipeline order (it streams straight
  // to the wire); ParallelSink()==false already serializes it, so this is the
  // matching intent. Parallel does not care -- letting the pipeline
  // cache/reorder is free throughput.
  bool SinkOrderDependent() const override {
    return _ctx->mode == WireSinkContext::Mode::Direct;
  }

 private:
  bool OverHighWater(const WireSinkContext& ctx) const {
    if (ctx.mode == WireSinkContext::Mode::Direct) {
      return ctx.send->TotalCommitted() -
               ctx.send_written->load(std::memory_order_acquire) >
             kSendHighWater;
    }
    return ctx.queued_bytes.load(std::memory_order_relaxed) > kSendHighWater;
  }

  void Seal(WireSinkContext& ctx, PgWireCollectorLocalState& lstate) const {
    lstate.sealed_total = lstate.buffer.TotalCommitted();
    auto chain = lstate.buffer.ReleaseChain();
    if (chain.head != nullptr) {
      ctx.PushChain(std::move(chain));
    }
  }

  std::shared_ptr<WireSinkContext> _ctx;
};

// Whether the collector's pipeline will actually run on more than one thread:
// multiple threads available AND every pipeline source supports parallel
// execution. A single-thread pool, or a non-parallel source like an in-out
// table function (generate_series/range -- one input row, one local state),
// runs the sink single-threaded, so Parallel mode would only pay its
// queue/splice overhead for a lone encoder. Source-only, mirroring DuckDB's own
// AllSourcesSupportBatchIndex idiom (GetSources walks to the pipeline sources,
// flattening set operations).
bool PlanRunsParallel(duckdb::ClientContext& context,
                      duckdb::PhysicalOperator& root) {
  if (duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads() <= 1) {
    return false;
  }
  for (auto& source : root.GetSources()) {
    if (!source.get().ParallelSource()) {
      return false;
    }
  }
  return true;
}

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
  ctx->engaged = true;
  if (ctx->announce_rowdesc) {
    WriteRowDescription(*ctx->send, data.types, data.names, ctx->formats);
  }
  auto& physical_plan = *data.physical_plan;
  auto& root = physical_plan.Root();
  // Parallel (workers encode into a completion-order queue the session splices)
  // only when it pays: the plan actually runs multi-threaded AND its output
  // order is not client-observable. Everything else is Direct (a single sink
  // encoding straight into _send) -- a paged portal, a plan that won't
  // parallelize (single-thread pool or a non-parallel source), or any plan
  // whose order DuckDB preserves: preserve_insertion_order on, or an ORDER BY /
  // TOP N (PreserveInsertionOrder returns true on FIXED_ORDER regardless of the
  // setting). Flipping preserve_insertion_order off then routes plain scans to
  // Parallel with no wire-code change; ORDER BY stays Direct.
  ctx->mode =
    !ctx->paged && PlanRunsParallel(context, root) &&
        !duckdb::PhysicalPlanGenerator::PreserveInsertionOrder(context, root)
      ? WireSinkContext::Mode::Parallel
      : WireSinkContext::Mode::Direct;
  return duckdb::make_uniq<PhysicalPgWireCollector>(physical_plan, data, ctx);
}

}  // namespace sdb::network::pg
