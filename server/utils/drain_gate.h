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
#include <duckdb/main/client_context.hpp>
#include <memory>

#include "iresearch/utils/assert.hpp"

namespace sdb::utils {

class DrainGate final {
  struct Leaver {
    void operator()(DrainGate* gate) const noexcept {
      SDB_ASSERT(gate);
      gate->Leave();
    }
  };
  struct Opener {
    void operator()(DrainGate* gate) const noexcept {
      SDB_ASSERT(gate);
      gate->Open();
    }
  };

 public:
  using Pass = std::unique_ptr<DrainGate, Leaver>;
  using Closed = std::unique_ptr<DrainGate, Opener>;

  [[nodiscard]] Pass Enter(duckdb::ClientContext& context);
  [[nodiscard]] Pass TryEnter() noexcept;
  [[nodiscard]] Closed Close(duckdb::ClientContext& context);
  bool IsClosed() const noexcept {
    return _state.load(std::memory_order_relaxed) & kClosed;
  }

 private:
  static constexpr uint32_t kClosed = 1;
  static constexpr uint32_t kPass = 2;

  void Leave() noexcept;
  void Open() noexcept;

  std::atomic<uint32_t> _state{0};
};

}  // namespace sdb::utils
