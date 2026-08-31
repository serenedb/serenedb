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

#include <absl/container/flat_hash_set.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>
#include <yaclib/async/future.hpp>
#include <yaclib/async/promise.hpp>
#include <yaclib/async/run.hpp>
#include <yaclib/exe/executor.hpp>
#include <yaclib/runtime/fair_thread_pool.hpp>
#include <yaclib/util/intrusive_ptr.hpp>

#include "basics/asio_ns.h"

namespace sdb {

// The single background work pool: drop tasks today, and (later) search
// refresh/compaction/cleanup and object-store prefetch. One fair yaclib thread
// pool so blocking / latency-tolerant work stays off the io threads (which only
// do socket IO) and off the DuckDB cpu pool (which runs queries). Sized by
// --server_background_threads. Eventually merges into DuckDB's async pool.
//
// Delays reuse the network io workers' asio timers: arming a steady_timer there
// is free and the fire callback only re-enqueues onto this pool, so io threads
// never run background work. The io pool is absent in two very different
// windows, and Delay treats them differently: before OpenDelays() (boot, the
// pool has never been up) a waiter parks, because a retry loop co_awaiting an
// already-satisfied Delay is a busy spin; after CancelDelays() (shutdown) it
// completes immediately, because that is exactly how a loop learns to look at
// its stop flag and exit.
class BackgroundScheduler final {
 public:
  using clock = std::chrono::steady_clock;

  inline static BackgroundScheduler* gInstance = nullptr;
  static BackgroundScheduler& instance() noexcept { return *gInstance; }

  BackgroundScheduler();
  ~BackgroundScheduler();

  void start();
  void stop();

  // Run func on the background pool; co_awaitable. A func returning a
  // yaclib::Future is flattened (Future unwrap), so callers co_await the inner
  // value -- the queueWithFuture contract the old scheduler exposed.
  template<typename Func>
  auto Run(Func&& func) {
    return yaclib::Run(*_pool, std::forward<Func>(func));
  }

  yaclib::IExecutor& executor() noexcept { return *_pool; }

  // Completes after `d` (best-effort; immediate once CancelDelays() has run,
  // parked until OpenDelays() while the io pool has never been up).
  yaclib::Future<> Delay(clock::duration d);

  // Startup: the io pool is up, so Delay can arm real timers. Releases every
  // waiter parked during boot. Called once, after Server::StartIoPool().
  void OpenDelays();

  // Shutdown: wake every armed Delay and complete future ones immediately.
  // Stop flags are only checked when a sleeper wakes, so without this a loop
  // parked on a long timer (e.g. a stretched refresh delay) holds up its join
  // for the timer's remainder.
  void CancelDelays();

  // True once CancelDelays() has run. From here on Delay never waits, so a
  // retry loop that keeps going burns a core instead of backing off: every
  // such loop must poll this and bail out.
  bool IsStopping() const noexcept {
    return _delays_cancelled.load(std::memory_order_acquire);
  }

 private:
  std::uint64_t _threads;
  yaclib::IntrusivePtr<yaclib::FairThreadPool> _pool;
  std::atomic_bool _delays_cancelled = false;
  absl::Mutex _delays_mutex;
  absl::flat_hash_set<std::shared_ptr<asio_ns::steady_timer>> _delays;
  // Waiters that arrived before the io pool existed, released by OpenDelays()
  // (boot finished) or CancelDelays() (boot was aborted).
  std::vector<yaclib::Promise<>> _parked;
  bool _delays_open = false;
};

}  // namespace sdb
