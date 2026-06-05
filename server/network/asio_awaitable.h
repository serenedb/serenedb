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

#include <coroutine>
#include <cstddef>
#include <type_traits>
#include <utility>

#include "basics/asio_ns.h"

namespace sdb::network {

template<typename Value, typename Initiate>
class [[nodiscard]] AsioAwaitable final {
  struct Empty {};
  static constexpr bool kHasValue = !std::is_void_v<Value>;
  using Stored = std::conditional_t<kHasValue, Value, Empty>;

 public:
  explicit AsioAwaitable(Initiate initiate) noexcept
    : _initiate{std::move(initiate)} {}

  AsioAwaitable(const AsioAwaitable&) = delete;
  AsioAwaitable& operator=(const AsioAwaitable&) = delete;

  bool await_ready() const noexcept { return false; }

  void await_suspend(std::coroutine_handle<> handle) {
    _handle = handle;
    if constexpr (kHasValue) {
      std::move(_initiate)([this](const asio_ns::error_code& ec, Value value) {
        _ec = ec;
        _value = std::move(value);
        _handle.resume();
      });
    } else {
      std::move(_initiate)([this](const asio_ns::error_code& ec) {
        _ec = ec;
        _handle.resume();
      });
    }
  }

  Value await_resume() {
    if (_ec) [[unlikely]] {
      throw asio_ns::system_error{_ec};
    }
    if constexpr (kHasValue) {
      return std::move(_value);
    }
  }

 private:
  Initiate _initiate;
  std::coroutine_handle<> _handle{};
  asio_ns::error_code _ec{};
  [[no_unique_address]] Stored _value{};
};

template<typename Value, typename Initiate>
AsioAwaitable<Value, std::decay_t<Initiate>> Async(
  Initiate&& initiate) noexcept {
  return AsioAwaitable<Value, std::decay_t<Initiate>>{
    std::forward<Initiate>(initiate)};
}

}  // namespace sdb::network
