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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "basics/asio_ns.h"
#include "basics/message_buffer.h"
#include "network/gate.h"
#include "network/io_context.h"
#include "network/pg/task_runner.h"
#include "network/socket.h"

namespace sdb::network {

inline constexpr size_t kReadBlock = 16 * 1024;
inline constexpr size_t kBufferMaxGrowth = 1u << 20;

// Send-side write-behind: committed bytes auto-start an async socket write at
// this threshold, so encoding the next rows overlaps the write of earlier
// ones.
inline constexpr size_t kSendFlushSize = 64 * 1024;
// Backpressure high-water mark: a producer encoding rows pauses while more
// than this many committed bytes are not yet written to the socket. Bounds
// per-connection memory for results a slow client doesn't drain.
inline constexpr size_t kSendHighWater = 4u << 20;
// Caps for parallel wire serialization (encoded-but-unsent chains). Emittable
// bytes (batches at or below the ordered emit cursor, or everything in
// unordered mode) drain at socket speed; this cap is the slow-client
// backpressure bound.
inline constexpr size_t kWireQueuedHighWater = 64u << 20;
// Ordered-mode lookahead window, in batches beyond the emit cursor. Admission
// must be per batch, not by a byte budget: pinned bytes only unpin when the
// cursor reaches them, so any global byte cap fills once and from then on
// admits a single follower per cursor advance -- serializing the encode.
// Window admission keeps W encoders busy; memory is W x batch wire bytes,
// backstopped by the pinned cap below.
inline constexpr uint64_t kWireOrderedLookahead = 16;
inline constexpr size_t kWirePinnedHighWater = 256u << 20;

// Largest single pg-wire message accepted -- a distinct concept from the
// buffer's chunk-growth ceiling above (they previously shared one constant).
// Bounds per-connection peak memory; bulk data goes through COPY, which streams
// per CopyData frame and is not subject to this. Default for PgServerContext;
// overridable via --network_pg_max_message_bytes.
inline constexpr uint32_t kDefaultMaxMessageBytes = 64u * 1024 * 1024;

// Per-read HTTP inactivity timeouts: bound how long one async socket read may
// stall so a slow/idle client cannot pin an io thread (slow-loris). Re-armed
// around each read; the keep-alive value applies while waiting for the first
// byte of the next request.
inline constexpr auto kHttpHeaderReadTimeout = std::chrono::seconds{10};
inline constexpr auto kHttpKeepAliveIdleTimeout = std::chrono::seconds{75};
inline constexpr auto kHttpBodyReadTimeout = std::chrono::seconds{30};

// The full-duplex byte transport shared by every protocol session (pg-wire,
// HTTP). It owns the socket, the io->duck recv channel, the write-behind send
// pipe, the single SessionTask host, and the send backpressure -- everything
// below the protocol's framing. A session derives via CRTP, supplies its own
// framing + SessionMain body on top, and provides one hook (OnSendPoison) for
// any extra wake it owes when the writer dies.
//
// The two perf seams live here, once: (1) write-behind -- _send auto-flushes at
// kSendFlushSize into a single-slot view the io-pinned SendWriter drains, so
// encoding overlaps the socket write; (2) the lazy writer-progress wake -- a
// recv-parked session pays no per-flush worker wake, and SendWriter only
// RequestRun()s the task when it armed interest (the _send_waiter Dekker pair,
// seq_cst-fenced against ArmSendWaiter). Touch these only with both invariants
// in mind.
template<SocketKind Kind, typename Session>
class Transport {
 public:
  explicit Transport(IoExecutor& exec)
    requires(Kind == SocketKind::Tcp)
    : _socket{exec.Context()}, _ioexec{&exec} {}

  Transport(IoExecutor& exec, asio_ns::ssl::context& ssl)
    requires(Kind != SocketKind::Tcp)
    : _socket{exec.Context(), ssl}, _ioexec{&exec} {}

  void Close() noexcept { _socket.Close(); }
  asio_ns::ip::tcp::socket& Lowest() noexcept { return _socket.Lowest(); }

 protected:
  // Kicks the send writer without waiting -- the hot path. Response bytes drain
  // to the socket concurrently with whatever the session does next.
  void KickSend() {
    if (_send.GetUncommittedSize() != 0 || HasUnsentBytes()) {
      _send.Commit(true);
    }
  }
  bool HasUnsentBytes() const {
    return _send.TotalCommitted() !=
           _send_written.load(std::memory_order_acquire);
  }
  bool SendBroken() const { return _io_broken.load(std::memory_order_acquire); }
  // Session-side producer fullness check; only valid while the session owns the
  // _send producer role (a direct-mode wire drive publishes its own counter).
  bool OverSendHighWater() const {
    return _send.TotalCommitted() -
             _send_written.load(std::memory_order_acquire) >
           kSendHighWater;
  }

  // The committed bytes auto-flushed by _send arm the single writer slot and
  // wake the io-pinned SendWriter.
  void OnSendViewReady(message::SequenceView view) {
    _write_view = view;
    _write_armed.store(true, std::memory_order_release);
    _write_gate.Kick();
  }

  // The fence orders the waiter-store before the caller's condition re-check
  // loads (SendWriter fences between its progress-store and the waiter-load);
  // one side always sees the other, so parking after a final re-check is safe.
  void ArmSendWaiter() {
    _send_waiter.store(_send_written.load(std::memory_order_relaxed),
                       std::memory_order_relaxed);
    std::atomic_thread_fence(std::memory_order_seq_cst);
  }
  void DisarmSendWaiter() {
    _send_waiter.store(kSendWaiterIdle, std::memory_order_relaxed);
  }

  // Duck-side parks of the SessionTask: pause while the client is slower than
  // the serializer / until everything committed reached the socket.
  yaclib::Future<> AwaitSendBelowHighWater() {
    ArmSendWaiter();
    while (_send.TotalCommitted() -
               _send_written.load(std::memory_order_acquire) >
             kSendHighWater &&
           !SendBroken()) {
      co_await _task->Park();
    }
    DisarmSendWaiter();
    co_return {};
  }
  yaclib::Future<> DrainSendOnTask() {
    KickSend();
    ArmSendWaiter();
    while (HasUnsentBytes() && !SendBroken()) {
      co_await _task->Park();
    }
    DisarmSendWaiter();
    co_return {};
  }
  // io-side drain (the pre-handoff phase, before the SessionTask exists): forces
  // a flush and awaits until the socket accepted everything committed.
  yaclib::Future<> Flush() {
    KickSend();
    while (HasUnsentBytes() && !SendBroken()) {
      co_await _producer_gate.Wait(*_ioexec);
    }
    co_return {};
  }

  // Parks the SessionTask until the recv producer commits bytes beyond `seen`
  // (false = connection broken). The watermark refresh pairs with the pump's
  // CommitWrite + RequestRun.
  yaclib::Future<bool> AwaitMoreBytes(size_t seen) {
    while (_recv.ReadableSize() <= seen) {
      if (SendBroken()) {
        co_return false;
      }
      co_await _task->Park();
    }
    co_return true;
  }

  // io-pinned writer: parked until a committed flush arms a view, then drives
  // async_write + FlushDone, overlapping the SessionTask's encoding/execution.
  // All socket writes happen here (TLS-safe). Call as
  // `SendWriter().Detach()` from the session's Start().
  yaclib::Future<> SendWriter() {
    auto self = static_cast<Session*>(this)->shared_from_this();
    for (;;) {
      co_await _write_gate.Wait(*_ioexec);
      if (_write_armed.exchange(false, std::memory_order_acq_rel)) {
        const auto view = _write_view;
        size_t bytes = 0;
        for (const auto buffer : view) {
          bytes += buffer.size();
        }
        try {
          co_await _socket.Write(view);
        } catch (const std::exception&) {
          // Client is gone. Poison the send side, wake every parked waiter, and
          // close so the read side notices.
          _io_broken.store(true, std::memory_order_release);
          _producer_gate.Kick();
          static_cast<Session*>(this)->OnSendPoison();
          if (_task_spawned) {
            _task->RequestRun();
          }
          _socket.Close();
          co_return {};
        }
        _send_written.fetch_add(bytes, std::memory_order_release);
        // May immediately re-arm via the send callback -- the pending kick is
        // consumed by the next Wait.
        _send.FlushDone();
        _producer_gate.Kick();
        // Wake the SessionTask only when it declared interest (ArmSendWaiter):
        // the common request/response flow parks on recv and an unconditional
        // wake here would cost a full no-op worker frame per flush. Dekker pair
        // with ArmSendWaiter -- progress-store then waiter-load here,
        // waiter-store then progress-load there -- so a wake is never lost.
        std::atomic_thread_fence(std::memory_order_seq_cst);
        const auto seen = _send_waiter.load(std::memory_order_relaxed);
        if (seen != kSendWaiterIdle && _task_spawned &&
            _send_written.load(std::memory_order_relaxed) > seen) {
          _task->RequestRun();
        }
        continue;
      }
      if (_writer_stop) {
        co_return {};
      }
    }
  }

  Socket<Kind> _socket;
  // Non-owning: this session's io worker, which IS the IoExecutor.
  IoExecutor* _ioexec;

  message::Buffer _recv{kReadBlock, kBufferMaxGrowth};
  message::Buffer _send{
    kReadBlock, kBufferMaxGrowth, kSendFlushSize,
    [this](message::SequenceView view) { OnSendViewReady(view); }};

  message::SequenceView _write_view;
  std::atomic<bool> _write_armed{false};
  Gate _write_gate;
  std::atomic<size_t> _send_written{0};
  // Writer-progress wake handshake: the _send_written value the SessionTask had
  // seen when it armed interest; kSendWaiterIdle = not interested (the default,
  // so recv-parked sessions never pay a wake per flush).
  static constexpr size_t kSendWaiterIdle = std::numeric_limits<size_t>::max();
  std::atomic<size_t> _send_waiter{kSendWaiterIdle};
  std::atomic<bool> _io_broken{false};
  bool _writer_stop = false;
  // io-side drain waker for the pre-handoff phase (Flush); also kicked by
  // SendWriter so a waiting Flush wakes.
  Gate _producer_gate;

  // Hosts the session's SessionMain coroutine as a duckdb::Task; emplaced by the
  // session right before spawning. By value -- no per-connection heap alloc.
  std::optional<pg::TaskRunner> _task;
  // Set once SessionMain is detached and _task->Begin() captured the job; gates
  // RequestRun wakes from io-side code.
  bool _task_spawned = false;
};

}  // namespace sdb::network
