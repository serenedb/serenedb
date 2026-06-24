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

#include <atomic>
#include <duckdb/execution/physical_operator.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parallel/interrupt.hpp>
#include <mutex>
#include <optional>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "basics/message_buffer.h"
#include "network/cpu_resumer.h"
#include "network/pg/wire_frames.h"
#include "pg/serialize.h"

namespace duckdb {

class PreparedStatementData;
}

namespace sdb::network::pg {

// Per-execution contract between the session and the wire collector: the
// session arms a pointer to this in the SereneDBClientState before PendingQuery
// (the get_result_collector hook reads it while the executor is initialized,
// synchronously inside PendingQuery) and disarms right after.
//
// Two modes, decided by MakeWireCollector from the plan and the thread count:
// - kDirect: a single sink thread encodes straight into the session's _send
//   buffer -- no chains, no queue. Used whenever the result order is
//   client-observable (PreserveInsertionOrder: preserve_insertion_order on, or
//   any ORDER BY / TOP N), for single-threaded plans, and for paged Execute.
//   The single sink is the only _send producer for the drive (RowDescription
//   goes out before, CommandComplete after), so its bytes are already in order;
//   the completion wake hands the producer role back.
// - kParallel: worker threads each encode into their own lstate buffer and seal
//   byte chains into one shared completion-order queue; the session splices
//   them into _send as they arrive. No ordering, so only chosen when the result
//   order is not client-observable. This is the throughput path.
//
// Backpressure mirrors SimpleBufferedData. Parallel: a worker over
// kSendHighWater queued bytes parks via SinkResultType::BLOCKED +
// InterruptState; the session (awake on writer wakes during the drive) drains
// the queue into _send and fires the unblocks once back under the cap. Direct
// rides _send's own committed-vs-written window (OverSendHighWater) instead of
// a queue.
class WireSinkContext {
 public:
  enum class Mode : uint8_t {
    Direct,
    Parallel,
  };

  // Set by the hook; GetResult builds the placeholder result through it.
  duckdb::weak_ptr<duckdb::ClientContext> context;

  // --- set by the session before PendingQuery ---
  // _send, the direct-mode target; also the splice destination for chains.
  message::Buffer* send = nullptr;
  // Writer progress, paired with the send buffer's TotalCommitted() for
  // backpressure.
  const std::atomic<size_t>* send_written = nullptr;
  // The session's cpu resumer; woken (RequestRun) on chain ready / sink
  // registration.
  CpuResumer* task = nullptr;
  // Output formats for this portal (empty = all text) and the settings
  // template the lstates clone (buffer/types_cache filled per lstate).
  std::vector<sdb::pg::VarFormat> formats;
  sdb::pg::SerializationContext proto;
  // Normal SELECT emits DataRow; COPY ... TO STDOUT emits one CopyData frame
  // per chunk holding PGCOPY rows (CopyBinary) or PG-text rows (CopyText). The
  // session brackets a COPY stream -- CopyOutResponse (+ PGCOPY header for
  // binary) before, trailer (binary) + CopyDone after -- while mode selection /
  // chain splice stay shared with SELECT, so COPY inherits Direct/Parallel +
  // zero-copy.
  RowEncoding row_encoding = RowEncoding::DataRow;

  // Paged Execute (protocol max_rows>0): force single-threaded Direct mode and
  // cap emitted rows at row_budget. The sink suspends (BLOCKED) when the budget
  // is reached; a re-Execute raises the budget and unblocks. page_offset is the
  // row offset within the chunk DuckDB re-delivers after a mid-chunk suspend.
  bool paged = false;
  std::atomic<uint64_t> row_budget{0};
  idx_t page_offset = 0;

  // --- filled by the collector ---
  Mode mode = Mode::Direct;
  std::atomic<uint64_t> rows{0};
  // Direct mode: committed bytes mirrored to an atomic after each chunk so
  // the session can evaluate backpressure (_send.TotalCommitted is plain and
  // owned by the producing sink during the drive).
  std::atomic<size_t> direct_committed{0};
  // Parallel mode: aggregate size of the sealed-but-unspliced chains in the
  // queue; the worker backpressure bound.
  std::atomic<size_t> queued_bytes{0};

  // Prepares the context for the session's next query (the context is reused
  // per session: a fresh one per query costs ~9% on prepared select1 at c8).
  // Leftover state is only possible after an error/cancel teardown: the stray
  // chain just frees; stray blocked entries hold weak task refs the executor
  // already cancelled, so dropping them without firing is safe.
  void Reset() {
    context.reset();
    proto = sdb::pg::SerializationContext{};
    row_encoding = RowEncoding::DataRow;
    mode = Mode::Direct;
    paged = false;
    row_budget.store(0, std::memory_order_relaxed);
    page_offset = 0;
    rows.store(0, std::memory_order_relaxed);
    direct_committed.store(0, std::memory_order_relaxed);
    std::lock_guard lock{_lock};
    _pending = {};
    _blocked.clear();
    queued_bytes.store(0, std::memory_order_relaxed);
  }

  // Parallel: a worker seals its buffer and appends the chain to the shared
  // queue (Chain::Append concats in O(1)), then wakes the session to splice.
  void PushChain(message::Chain&& chain) {
    const auto bytes = chain.bytes;
    {
      std::lock_guard lock{_lock};
      _pending.Append(std::move(chain));
      queued_bytes.fetch_add(bytes, std::memory_order_relaxed);
    }
    task->RequestRun();
  }

  // Session side: the whole accumulated chain, or nullopt. Order does not
  // matter in Parallel mode, so the splice takes everything queued in one move.
  std::optional<message::Chain> PopChain() {
    std::lock_guard lock{_lock};
    if (_pending.head == nullptr) {
      return std::nullopt;
    }
    auto chain = std::move(_pending);  // move-ctor leaves _pending empty
    queued_bytes.fetch_sub(chain.bytes, std::memory_order_relaxed);
    return chain;
  }

  // Registering wakes the session so a drain pass is guaranteed to follow every
  // registration -- otherwise a sink registering right after the session's last
  // drain would sleep forever. Sinks must never fire the callbacks themselves:
  // a callback for a task that does not end up descheduled spins
  // Executor::RescheduleTask indefinitely.
  void BlockSink(const duckdb::InterruptState& state) {
    {
      std::lock_guard lock{_lock};
      _blocked.push_back(state);
    }
    task->RequestRun();
  }

  // Session side. Releases every blocked worker once the queue is back under
  // the cap (`under_cap` from the caller); `force_all` is the broken-connection
  // path.
  void UnblockSinks(bool under_cap, bool force_all = false) {
    absl::InlinedVector<duckdb::InterruptState, 8> ready;
    {
      std::lock_guard lock{_lock};
      if (!force_all && !under_cap) {
        return;
      }
      ready.assign(_blocked.begin(), _blocked.end());
      _blocked.clear();
    }
    for (auto& state : ready) {
      state.Callback();
    }
  }

 private:
  absl::Mutex _lock;
  // The single completion-order accumulator (Parallel mode); pushes concat in
  // O(1), the session pops the whole thing.
  message::Chain _pending;
  std::vector<duckdb::InterruptState> _blocked;
};

// get_result_collector hook: returns the wire collector when a WireSinkContext
// is armed in the SereneDBClientState and the statement returns rows; falls
// back to DuckDB's stock collector choice otherwise.
duckdb::unique_ptr<duckdb::PhysicalOperator> MakeWireCollector(
  duckdb::ClientContext& context, duckdb::PreparedStatementData& data);

}  // namespace sdb::network::pg
