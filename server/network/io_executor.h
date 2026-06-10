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

#include <duckdb/common/helper.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <yaclib/exe/executor.hpp>
#include <yaclib/exe/job.hpp>

#include "basics/asio_ns.h"

namespace sdb::network {

// One per io worker thread (IoThreadPool::Worker inherits this), owning that
// worker's io_context. Sessions on the worker hold a non-owning IoExecutor*, so
// there is no per-connection executor. Submit is `final` so that
// `co_await yaclib::On(*_ioexec)` (templated On over the concrete type)
// devirtualizes the dispatch.
class IoExecutor : public yaclib::IExecutor {
 public:
  Type Tag() const noexcept override { return Type::Custom; }

  bool Alive() const noexcept override { return true; }

  void Submit(yaclib::Job& job) noexcept final {
    asio_ns::post(_ctx, [&job] { job.Call(); });
  }

  asio_ns::io_context& Context() noexcept { return _ctx; }

  // The one producer used to enqueue onto DuckDB's scheduler for every session
  // bound to this io worker. DuckDB's ProducerToken is internally
  // mutex-guarded, so it's safe to enqueue through from this io thread (the
  // hop) and from a duck-worker thread (a reschedule). Lazily created on this
  // io thread's first hop (which always precedes any reschedule of a task it
  // enqueued), so the creation is single-threaded and visible to workers via
  // the queue's own happens-before.
  duckdb::ProducerToken& DuckProducer(duckdb::TaskScheduler& scheduler) {
    if (!_duck_producer) {
      _duck_producer = scheduler.CreateProducer();
    }
    return *_duck_producer;
  }

 protected:
  asio_ns::io_context _ctx{1};

 private:
  duckdb::unique_ptr<duckdb::ProducerToken> _duck_producer;
};

}  // namespace sdb::network
