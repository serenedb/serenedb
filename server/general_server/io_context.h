////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <atomic>
#include <thread>

#include "basics/asio_ns.h"
#include "basics/common.h"

namespace sdb {
namespace rest {

class IoContext {
 public:
  asio_ns::io_context io_context;

 private:
  asio_ns::executor_work_guard<asio_ns::io_context::executor_type> _work;
  std::atomic<unsigned> _clients;
  // jthread is declared last so it is destroyed first: this joins the IO
  // thread before _work / io_context get torn down.
  std::jthread _io_thread;

 public:
  IoContext();
  ~IoContext();

  unsigned clients() const noexcept {
    return _clients.load(std::memory_order_acquire);
  }

  void incClients() noexcept {
    _clients.fetch_add(1, std::memory_order_release);
  }

  void decClients() noexcept {
    _clients.fetch_sub(1, std::memory_order_release);
  }

  void stop();
};

}  // namespace rest
}  // namespace sdb
