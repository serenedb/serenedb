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

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <yaclib/async/future.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/task.hpp>

#include "basics/asio_ns.h"
#include "basics/message_buffer.h"
#include "network/cancel_registry.h"
#include "network/cpu_resumer.h"
#include "network/gate.h"
#include "network/io_context.h"
#include "network/listen_spec.h"
#include "network/proxy_protocol.h"
#include "network/socket.h"

namespace sdb::network {

// Socket read size: bytes each recv asks the kernel for (the Reserve
// granularity). Distinct from the buffer chunk knobs below -- it bounds the
// syscall count for bulk recv, not buffering memory.
inline constexpr size_t kReadBlock = 16 * 1024;

// Send-side write-behind: committed bytes auto-start an async socket write once
// this many accumulate since the last write, so encoding the next rows overlaps
// the write of the earlier ones. 64K measured best across narrow / wide /
// nested results (drift-cancelled interleaved sweep): near-peak everywhere,
// while 256K only gains wide results ~2% and costs narrow scans ~9% plus the
// most memory.
inline constexpr size_t kSendFlushSize = 64 * 1024;

// Backpressure bound: the most encoded-but-unsent bytes one connection may
// hold. The Direct sink / session producer pause while _send carries more than
// this committed-but-unwritten; Parallel workers pause while more than this is
// queued for the splice. Bounds per-connection serialize memory (and a slow
// client's buffered bytes). Measured minimum that doesn't regress: Direct needs
// ~4 flushes of run-ahead and plateaus at 256K (64K->256K is +21%, 256K->1M
// only +1.6%); Parallel is flat. Must stay >= the flush.
inline constexpr size_t kSendHighWater = 256 * 1024;

// Wire-buffer chunk growth. Max (== the flush size, shared by recv and send) is
// the doubling ceiling: the send buffer seals at the flush, so a chunk never
// needs to carry more. Min seeds the first chunk -- send-side encode buffers
// start small (a tiny result needn't allocate a full chunk); recv buffers seed
// at kReadBlock instead, since their first read is already that size.
inline const size_t kWireChunkMin = 1024;
inline const size_t kWireChunkMax = kSendFlushSize;

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
// Mutable so --http_*_timeout / --idle_session_timeout can override them once
// at startup (Server::start), before any acceptor runs.
inline std::chrono::steady_clock::duration gHttpHeaderTimeout =
  std::chrono::seconds{10};
inline std::chrono::steady_clock::duration gHttpKeepAliveTimeout =
  std::chrono::seconds{75};
inline std::chrono::steady_clock::duration gHttpBodyTimeout =
  std::chrono::seconds{30};

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
class Transport : public TransportBase {
 public:
  explicit Transport(IoExecutor& exec)
    requires(Kind == SocketKind::Tcp || Kind == SocketKind::Unix)
    : _socket{exec.Context()}, _ioexec{&exec} {}

  Transport(IoExecutor& exec, asio_ns::ssl::context& ssl)
    requires(Kind == SocketKind::Ssl || Kind == SocketKind::MaybeTls)
    : _socket{exec.Context(), ssl}, _ioexec{&exec} {}

  void Close() noexcept { _socket.Close(); }
  auto& Lowest() noexcept { return _socket.Lowest(); }

  // Begin teardown. Any of the three coroutines calls this on its terminal
  // condition (recv EOF/error, send error, cpu side finished), and
  // CancelToken::Terminate at pg_terminate_backend / server shutdown;
  // idempotent and cross-thread safe. Marks stopping once and wakes all three
  // so they unwind. The socket is closed by SendWriter on the io worker when
  // it sees the stop -- never cross-thread, never via a side-channel post.
  void Stop() final {
    if (_stopping.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    _write_gate.Kick();
    _producer_gate.Kick();
    if (_task) {
      _task->RequestRun();
    }
    static_cast<Session*>(this)->OnStop();
  }

 protected:
  // Kicks the send writer without waiting -- the hot path. Response bytes drain
  // to the socket concurrently with whatever the session does next.
  void KickSend() {
    if (HasUnsentBytes()) {
      _send.Flush();
    }
  }
  bool HasUnsentBytes() const {
    return _send.TotalCommitted() !=
           _send_written.load(std::memory_order_acquire);
  }
  bool SendBroken() const { return _stopping.load(std::memory_order_acquire); }

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
  yaclib::Task<> AwaitSendBelowHighWater() {
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
  yaclib::Task<> DrainSendOnTask() {
    KickSend();
    ArmSendWaiter();
    while (HasUnsentBytes() && !SendBroken()) {
      co_await _task->Park();
    }
    DisarmSendWaiter();
    co_return {};
  }
  // io-side drain (the pre-handoff phase, before the SessionTask exists):
  // forces a flush and awaits until the socket accepted everything committed.
  yaclib::Task<> Flush() {
    KickSend();
    while (HasUnsentBytes() && !SendBroken()) {
      co_await _producer_gate.Wait(*_ioexec);
    }
    co_return {};
  }

  // Parks the SessionTask until the recv producer commits bytes beyond `seen`
  // (false = connection broken). The watermark refresh pairs with the pump's
  // CommitWrite + RequestRun.
  yaclib::Task<bool> AwaitMoreBytes(size_t seen) {
    while (_recv.ReadableSize() <= seen) {
      if (SendBroken()) {
        co_return false;
      }
      co_await _task->Park();
    }
    co_return true;
  }

  // HAProxy PROXY-protocol preface (v1/v2), consumed raw before any
  // TLS/framing; bytes read past the header are injected back into _recv for
  // the normal read path. Off is a no-op. Returns false to reject
  // (require-but-absent, malformed, or EOF before the header). Not used on Ssl
  // listeners (header precedes TLS).
  yaclib::Task<bool> ReadProxyPreface(ProxyMode mode) {
    if (mode == ProxyMode::Off) {
      co_return true;
    }
    std::array<uint8_t, 256> tmp;
    size_t have = 0;
    for (;;) {
      auto [ec, n] =
        co_await _socket
          .ReadSome(std::span{tmp.data() + have, tmp.size() - have})
          .NoThrow();
      if (ec || n == 0) {
        co_return false;
      }
      have += n;
      const auto r =
        ParseProxyHeader(std::span<const uint8_t>{tmp.data(), have});
      if (r.status == ProxyParse::NeedMore) {
        if (have == tmp.size()) {
          co_return false;  // header longer than any valid PROXY header
        }
        continue;
      }
      if (r.status == ProxyParse::Invalid) {
        co_return false;
      }
      if (r.status == ProxyParse::NotProxy) {
        if (mode == ProxyMode::Require) {
          co_return false;
        }
        InjectRecv(tmp.data(), have);  // no header: all bytes are protocol
        co_return true;
      }
      // r.source_addr is proxy source here, can be saved if needed
      if (have > r.length) {
        InjectRecv(tmp.data() + r.length, have - r.length);
      }
      co_return true;
    }
  }

  void InjectRecv(const uint8_t* data, size_t len) {
    if (len == 0) {
      return;
    }
    auto dst = _recv.Reserve(len);
    std::memcpy(dst.data(), data, len);
    _recv.CommitWrite(len);
  }

  // io-pinned writer: parked until a committed flush arms a view, then drives
  // async_write + FlushDone, overlapping the cpu coroutine's encoding. All
  // socket writes happen here (TLS-safe). Started eagerly by Run, which holds
  // the returned future and joins it at teardown -- so it runs on a raw `this`
  // kept alive by Run's owning self.
  yaclib::Future<> SendWriter() {
    for (;;) {
      co_await _write_gate.Wait(*_ioexec);
      if (_write_armed.exchange(false, std::memory_order_acq_rel)) {
        auto [ec, bytes] = co_await _socket.Write(_write_view).NoThrow();
        if (ec) [[unlikely]] {
          Stop();  // client gone
          break;
        }
        _send_written.fetch_add(bytes, std::memory_order_release);
        // May immediately re-arm via the send callback -- the pending kick is
        // consumed by the next Wait.
        _send.FlushDone();
        // _producer_gate has a waiter only pre-handoff (Flush, before the
        // SessionTask exists). Once _task is set the steady-state drive parks
        // on the task, not here, so skip the seq_cst-fenced Kick per flush.
        if (!_task) {
          _producer_gate.Kick();
        }
        // Wake the cpu task only when it declared interest (ArmSendWaiter): the
        // common request/response flow parks on recv and an unconditional wake
        // here would cost a full no-op worker frame per flush. Dekker pair with
        // ArmSendWaiter, seq_cst-fenced, so a wake is never lost.
        std::atomic_thread_fence(std::memory_order_seq_cst);
        const auto seen = _send_waiter.load(std::memory_order_relaxed);
        if (seen != kSendWaiterIdle && _task &&
            _send_written.load(std::memory_order_relaxed) > seen) {
          _task->RequestRun();
        }
        continue;
      }
      if (_stopping.load(std::memory_order_acquire)) {
        break;
      }
    }
    // io-side close: cancels RecvLoop's pending read so it unwinds.
    _socket.Close();
    co_return {};
  }

  Socket<Kind> _socket;
  // Non-owning: this session's io worker, which IS the IoExecutor.
  IoExecutor* _ioexec;

  message::Buffer _recv{kReadBlock, kWireChunkMax};
  message::Buffer _send{
    kWireChunkMin, kWireChunkMax, kSendFlushSize,
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
  // Set once by Stop() from any coroutine; every park-loop checks it to unwind.
  std::atomic<bool> _stopping{false};
  // io-side drain waker for the pre-handoff phase (Flush); also kicked by
  // SendWriter so a waiting Flush wakes.
  Gate _producer_gate;

  // Hosts the cpu coroutine as a duckdb::Task; created just before it spawns.
  // Null == not spawned yet, so it also gates RequestRun wakes from io-side
  // code. Standalone shared_ptr, co-owned with the DuckDB scheduler.
  duckdb::shared_ptr<CpuResumer> _task;
};

}  // namespace sdb::network
