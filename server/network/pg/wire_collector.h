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
#include <map>
#include <mutex>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "basics/message_buffer.h"
#include "network/connection.h"
#include "network/task_runner.h"
#include "pg/serialize.h"

namespace duckdb {

class PreparedStatementData;
}

namespace sdb::network::pg {

// Per-execution contract between the session and the wire collector: the
// session arms a pointer to this in the SereneDBClientState before
// PendingQuery (the get_result_collector hook reads it while the executor is
// initialized, synchronously inside PendingQuery) and disarms right after.
//
// Three modes, decided by the same plan predicates DuckDB uses to pick its
// stock collectors:
// - kDirect (order-preserving, no batch index => single-threaded sink): the
//   sink encodes straight into the session's _send buffer. Safe by the
//   producer-role argument: the single-threaded sink is the only _send
//   producer for the duration of the drive (RowDescription goes out before,
//   CommandComplete after), and the completion wake hands the role back.
// - kOrdered (order-preserving + batch index => parallel sink): per-lstate
//   buffers; sealed chains are queued per batch_index and the session splices
//   them in batch order once the executor-wide min_batch_index passes them
//   (the BatchedBufferedData discipline).
// - kUnordered (not order-preserving => parallel sink): per-lstate buffers;
//   chains splice in completion order.
//
// Backpressure mirrors SimpleBufferedData: a sink over the byte cap parks via
// SinkResultType::BLOCKED + InterruptState; the session (awake on writer
// wakes during the drive) drains and fires the unblocks. In kOrdered the
// min-batch sink is never blocked, so the emit cursor always advances.
class WireSinkContext {
 public:
  enum class Mode : uint8_t {
    Direct,
    Ordered,
    Unordered,
  };

  // Set by the hook; GetResult builds the placeholder result through it.
  duckdb::weak_ptr<duckdb::ClientContext> context;

  // --- set by the session before PendingQuery ---
  // _send, the direct-mode target; also the splice destination for chains.
  message::Buffer* send = nullptr;
  // Writer progress, paired with committed counters for backpressure.
  const std::atomic<size_t>* send_written = nullptr;
  // The session's task runner; woken (RequestRun) on chain ready / cursor
  // advance / sink registration.
  TaskRunner* task = nullptr;
  // Output formats for this portal (empty = all text) and the settings
  // template the lstates clone (buffer/types_cache filled per lstate).
  std::vector<sdb::pg::VarFormat> formats;
  sdb::pg::SerializationContext proto;
  // COPY ... TO STDOUT (FORMAT binary): the sink emits PGCOPY rows wrapped in
  // CopyData frames (one frame per chunk) instead of pg DataRow messages. The
  // session brackets the stream with CopyOutResponse + header before, and the
  // PGCOPY trailer + CopyDone after; mode selection / chain splice are shared
  // with SELECT, so COPY inherits Direct/Ordered/Unordered + zero-copy.
  bool copy_binary = false;

  // --- filled by the collector ---
  Mode mode = Mode::Direct;
  std::atomic<uint64_t> rows{0};
  // Direct mode: committed bytes mirrored to an atomic after each chunk so
  // the session can evaluate backpressure (_send.TotalCommitted is plain and
  // owned by the producing sink during the drive).
  std::atomic<size_t> direct_committed{0};

  // Parallel modes: sealed chains and their aggregate size. `pinned_bytes` is
  // the ordered-mode subset queued for batches beyond the emit cursor: it
  // cannot drain until the cursor reaches it, so it is bounded separately
  // (emittable bytes drain at socket speed and bound the slow-client case).
  std::atomic<size_t> queued_bytes{0};
  std::atomic<size_t> pinned_bytes{0};

  // Prepares the context for the session's next query (the context is reused
  // per session: a fresh one per query costs ~9% on prepared select1 at c8).
  // Leftover state is only possible after an error/cancel teardown: stray
  // chains just free; stray blocked entries hold weak task refs the executor
  // already cancelled, so dropping them without firing is safe.
  void Reset() {
    context.reset();
    proto = sdb::pg::SerializationContext{};
    copy_binary = false;
    mode = Mode::Direct;
    rows.store(0, std::memory_order_relaxed);
    direct_committed.store(0, std::memory_order_relaxed);
    std::lock_guard lock{_lock};
    _batches.clear();
    _blocked.clear();
    queued_bytes.store(0, std::memory_order_relaxed);
    pinned_bytes.store(0, std::memory_order_relaxed);
    _min_batch.store(0, std::memory_order_relaxed);
  }

  void PushChain(message::Buffer::Chain&& chain, uint64_t batch) {
    const auto bytes = chain.bytes;
    {
      std::lock_guard lock{_lock};
      _batches[batch].Append(std::move(chain));
      queued_bytes.fetch_add(bytes, std::memory_order_relaxed);
      if (mode == Mode::Ordered &&
          batch > _min_batch.load(std::memory_order_relaxed)) {
        pinned_bytes.fetch_add(bytes, std::memory_order_relaxed);
      }
    }
    task->RequestRun();
  }

  // Ordered mode: the min batch is the emit cursor -- its chains stream live,
  // batches below it are complete. A raise unpins the bytes the cursor passed
  // and wakes the session so newly admitted followers get released.
  void UpdateMinBatch(uint64_t min_batch) {
    if (min_batch <= _min_batch.load(std::memory_order_relaxed)) {
      return;
    }
    {
      std::lock_guard lock{_lock};
      const auto current = _min_batch.load(std::memory_order_relaxed);
      if (min_batch <= current) {
        return;
      }
      for (auto it = _batches.upper_bound(current);
           it != _batches.end() && it->first <= min_batch; ++it) {
        pinned_bytes.fetch_sub(it->second.bytes, std::memory_order_relaxed);
      }
      _min_batch.store(min_batch, std::memory_order_relaxed);
    }
    task->RequestRun();
  }

  // Session side: the lowest batch's accumulated chain, or nullopt. Within a
  // batch a single sink thread produces in order, so the min batch is
  // emittable while still in progress. `finished` lifts the gate entirely.
  std::optional<message::Buffer::Chain> PopChain(bool finished) {
    std::lock_guard lock{_lock};
    if (_batches.empty()) {
      return std::nullopt;
    }
    auto front = _batches.begin();
    if (mode == Mode::Ordered && !finished &&
        front->first > _min_batch.load(std::memory_order_relaxed)) {
      return std::nullopt;
    }
    auto chain = std::move(front->second);
    if (mode == Mode::Ordered &&
        front->first > _min_batch.load(std::memory_order_relaxed)) {
      // Only reachable with `finished`: the gate above bars pinned batches.
      pinned_bytes.fetch_sub(chain.bytes, std::memory_order_relaxed);
    }
    _batches.erase(front);
    queued_bytes.fetch_sub(chain.bytes, std::memory_order_relaxed);
    return chain;
  }

  // Registering wakes the session so a drain pass is guaranteed to follow
  // every registration -- otherwise a sink registering right after the
  // session's last drain would sleep forever. Sinks must never fire the
  // callbacks themselves: a callback for a task that does not end up
  // descheduled spins Executor::RescheduleTask indefinitely.
  void BlockSink(const duckdb::InterruptState& state, uint64_t batch) {
    {
      std::lock_guard lock{_lock};
      _blocked.emplace_back(state, batch);
    }
    task->RequestRun();
  }

  // Session side; mirrors the OverHighWater conditions. `force_all` is the
  // direct-mode/broken-connection path. Unordered: release everything once
  // the queue is under the cap (`under_cap` from the caller). Ordered:
  // releases follow window admission -- sinks whose batch entered the
  // lookahead window, and cursor sinks once emittable bytes drained.
  void UnblockSinks(bool under_cap, bool force_all = false) {
    absl::InlinedVector<duckdb::InterruptState, 8> ready;
    {
      std::lock_guard lock{_lock};
      if (force_all || mode != Mode::Ordered) {
        if (!force_all && !under_cap) {
          return;
        }
        for (auto& [state, batch] : _blocked) {
          ready.push_back(state);
        }
        _blocked.clear();
      } else {
        const auto min_batch = _min_batch.load(std::memory_order_relaxed);
        const auto pinned = pinned_bytes.load(std::memory_order_relaxed);
        const auto emittable =
          queued_bytes.load(std::memory_order_relaxed) - pinned;
        const bool cursor_ok = emittable <= kWireQueuedHighWater;
        const bool follower_ok = pinned <= kWirePinnedHighWater;
        std::erase_if(_blocked, [&](auto& entry) {
          const bool admit =
            entry.second <= min_batch
              ? cursor_ok
              : follower_ok &&
                  entry.second <= min_batch + kWireOrderedLookahead;
          if (admit) {
            ready.push_back(entry.first);
          }
          return admit;
        });
      }
    }
    for (auto& state : ready) {
      state.Callback();
    }
  }

  bool HasBlockedSinks() {
    std::lock_guard lock{_lock};
    return !_blocked.empty();
  }

  uint64_t MinBatch() const {
    return _min_batch.load(std::memory_order_relaxed);
  }

 private:
  std::mutex _lock;
  // One accumulated chain per batch index (pushes concat in O(1)). Unordered
  // mode uses a single pseudo-batch 0.
  std::map<uint64_t, message::Buffer::Chain> _batches;
  std::vector<std::pair<duckdb::InterruptState, uint64_t>> _blocked;
  std::atomic<uint64_t> _min_batch{0};
};

// get_result_collector hook: returns the wire collector when a
// WireSinkContext is armed in the SereneDBClientState and the statement
// returns rows; falls back to DuckDB's stock collector choice otherwise.
duckdb::unique_ptr<duckdb::PhysicalOperator> MakeWireCollector(
  duckdb::ClientContext& context, duckdb::PreparedStatementData& data);

}  // namespace sdb::network::pg
