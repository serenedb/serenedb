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

namespace sdb::catalog {

struct WalkCounters {
  std::atomic<uint32_t> parity[2];
};

class [[nodiscard]] WalkGuard final {
 public:
  explicit WalkGuard(WalkCounters& walks) noexcept;
  ~WalkGuard() { _counter->fetch_sub(1, std::memory_order_release); }
  WalkGuard(const WalkGuard&) = delete;
  WalkGuard& operator=(const WalkGuard&) = delete;

 private:
  std::atomic<uint32_t>* _counter;
};

}  // namespace sdb::catalog
