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
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "basics/asio_ns.h"
#include "network/io_executor.h"

namespace sdb::network {

class IoThreadPool final {
 public:
  explicit IoThreadPool(std::uint32_t threads);
  ~IoThreadPool();

  IoThreadPool(const IoThreadPool&) = delete;
  IoThreadPool& operator=(const IoThreadPool&) = delete;

  // The worker IS its io thread's executor; a session assigned to it holds a
  // non-owning IoExecutor* and posts back via On(*_ioexec).
  IoExecutor& Next() noexcept;

  std::uint32_t Size() const noexcept {
    return static_cast<std::uint32_t>(_workers.size());
  }

  void Start();
  void Stop() noexcept;

 private:
  struct Worker final : IoExecutor {
    asio_ns::executor_work_guard<asio_ns::io_context::executor_type> guard{
      Context().get_executor()};
    std::jthread thread;
  };

  std::vector<std::unique_ptr<Worker>> _workers;
  std::atomic<std::uint32_t> _next{0};
};

}  // namespace sdb::network
