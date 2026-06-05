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

#include "network/io_context.h"

namespace sdb::network {

IoThreadPool::IoThreadPool(std::uint32_t threads) {
  if (threads == 0) {
    threads = 1;
  }
  _workers.reserve(threads);
  for (std::uint32_t i = 0; i < threads; ++i) {
    _workers.push_back(std::make_unique<Worker>());
  }
}

IoThreadPool::~IoThreadPool() { Stop(); }

asio_ns::io_context& IoThreadPool::Next() noexcept {
  const auto index =
    _next.fetch_add(1, std::memory_order_relaxed) % _workers.size();
  return _workers[index]->ctx;
}

void IoThreadPool::Start() {
  for (auto& worker : _workers) {
    worker->thread = std::jthread{[w = worker.get()] { w->ctx.run(); }};
  }
}

void IoThreadPool::Stop() noexcept {
  for (auto& worker : _workers) {
    worker->guard.reset();
    worker->ctx.stop();
  }
  for (auto& worker : _workers) {
    if (worker->thread.joinable()) {
      worker->thread.join();
    }
  }
}

}  // namespace sdb::network
