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

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <memory>
#include <utility>
#include <yaclib/async/future.hpp>
#include <yaclib/async/run.hpp>

#include "network/io_context.h"
#include "runtime/duck_executor.h"
#include "server/utils/asio_ns.h"

namespace sdb {

std::uint32_t ReadIoThreads();

class Runtime final {
 public:
  static Runtime& Get() noexcept {
    SDB_ASSERT(gInstance);
    return *gInstance;
  }

  explicit Runtime(std::uint32_t io_threads);
  ~Runtime();

  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;

  network::IoThreadPool& IoPool() noexcept { return _io; }
  DuckExecutor& Background() noexcept { return _background; }

  template<typename Func>
  auto Run(Func&& func) {
    return yaclib::Run(_background, std::forward<Func>(func));
  }

  yaclib::Future<> Delay(std::chrono::steady_clock::duration duration);

  void RequestStop() noexcept;
  bool IsStopping() const noexcept {
    return _stopping.load(std::memory_order_acquire);
  }

 private:
  inline static Runtime* gInstance = nullptr;

  network::IoThreadPool _io;
  DuckExecutor _background;
  std::atomic_bool _stopping = false;
  absl::Mutex _mutex;
  irs::containers::FlatHashSet<std::shared_ptr<asio_ns::steady_timer>> _delays
    ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb
