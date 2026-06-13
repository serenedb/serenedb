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

#include <chrono>
#include <cstdint>

#include <yaclib/async/future.hpp>
#include <yaclib/async/run.hpp>
#include <yaclib/exe/executor.hpp>
#include <yaclib/runtime/fair_thread_pool.hpp>
#include <yaclib/util/intrusive_ptr.hpp>

namespace sdb {

// The single background work pool: drop tasks today, and (later) search
// refresh/compaction/cleanup and object-store prefetch. One fair yaclib thread
// pool so blocking / latency-tolerant work stays off the io threads (which only
// do socket IO) and off the DuckDB cpu pool (which runs queries). Sized by
// --server_background_threads. Eventually merges into DuckDB's async pool.
//
// Delays reuse the network io workers' asio timers: arming a steady_timer there
// is free and the fire callback only re-enqueues onto this pool, so io threads
// never run background work. If the io pool is not up (no endpoints / shutdown)
// Delay completes immediately -- the delay is best-effort backoff, not a
// correctness requirement.
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

  // Completes after `d` (best-effort; immediate if the io pool is unavailable).
  yaclib::Future<> Delay(clock::duration d);

 private:
  std::uint64_t _threads;
  yaclib::IntrusivePtr<yaclib::FairThreadPool> _pool;
};

}  // namespace sdb
