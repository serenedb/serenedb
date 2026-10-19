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

#include <yaclib/exe/executor.hpp>
#include <yaclib/exe/job.hpp>

#include "server/utils/asio_ns.h"

namespace sdb::network {

// One per io worker thread (IoThreadPool::Worker inherits this), owning that
// worker's io_context. Sessions on the worker hold a non-owning IoExecutor*, so
// there is no per-connection executor. Submit is `final` so that
// `co_await yaclib::On(*_ioexec)` (templated On over the concrete type)
// devirtualizes the dispatch.
class IoExecutor : public yaclib::IExecutor {
 public:
  Type Tag() const noexcept final { return Type::Custom; }

  bool Alive() const noexcept final { return true; }

  void Submit(yaclib::Job& job) noexcept final {
    asio_ns::post(_ctx, [&job] { job.Call(); });
  }

  asio_ns::io_context& Context() noexcept { return _ctx; }

 protected:
  asio_ns::io_context _ctx{1};
};

}  // namespace sdb::network
