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

#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstdint>
#include <utility>

namespace sdb::utils {

class DrainGate final {
 public:
  class [[nodiscard]] Pass {
   public:
    Pass() = default;
    explicit Pass(DrainGate* gate) noexcept : _gate{gate} {}
    Pass(Pass&& other) noexcept : _gate{std::exchange(other._gate, nullptr)} {}
    Pass& operator=(Pass&& other) noexcept {
      if (this != &other) {
        Release();
        _gate = std::exchange(other._gate, nullptr);
      }
      return *this;
    }
    ~Pass() { Release(); }

    explicit operator bool() const noexcept { return _gate != nullptr; }
    void Release() noexcept {
      if (auto* gate = std::exchange(_gate, nullptr)) {
        gate->Leave();
      }
    }

   private:
    DrainGate* _gate = nullptr;
  };

  Pass Enter();
  Pass TryEnter() noexcept;
  bool Close(absl::FunctionRef<bool()> cancelled) noexcept;
  void Open() noexcept;
  bool WaitOpen(absl::FunctionRef<bool()> cancelled);
  bool IsClosed() const noexcept { return _closed.load(); }

 private:
  void Leave() noexcept;

  absl::Mutex _mutex;
  std::atomic<bool> _closed{false};
  std::atomic<uint32_t> _in_flight{0};
};

}  // namespace sdb::utils
