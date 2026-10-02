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

#include <concepts>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

namespace sdb {

template<typename T>
concept PoolState = requires(T& state) {
  { state.Reset() } noexcept -> std::same_as<bool>;
};

template<PoolState T, size_t kMaxIdle = 2>
class ThreadLocalPool {
 public:
  static std::unique_ptr<T> Acquire() {
    auto& idle = Idle();
    if (idle.empty()) {
      return std::make_unique<T>();
    }
    auto state = std::move(idle.back());
    idle.pop_back();
    return state;
  }

  static void Release(std::unique_ptr<T> state) noexcept {
    if (state == nullptr) {
      return;
    }
    auto& idle = Idle();
    if (idle.size() < kMaxIdle && state->Reset()) {
      idle.push_back(std::move(state));
    }
  }

  static size_t IdleCount() noexcept { return Idle().size(); }

 private:
  static std::vector<std::unique_ptr<T>>& Idle() noexcept {
    thread_local std::vector<std::unique_ptr<T>> gIdle = [] {
      std::vector<std::unique_ptr<T>> reserved;
      reserved.reserve(kMaxIdle);
      return reserved;
    }();
    return gIdle;
  }
};

template<PoolState T, size_t KMaxIdle = 2>
class Pooled {
 public:
  using Pool = ThreadLocalPool<T, KMaxIdle>;

  Pooled() : _state{Pool::Acquire()} {}
  ~Pooled() { Pool::Release(std::move(_state)); }

  Pooled(const Pooled&) = delete;
  Pooled& operator=(const Pooled&) = delete;

  T* operator->() const noexcept { return _state.get(); }
  T& operator*() const noexcept { return *_state; }

 private:
  std::unique_ptr<T> _state;
};

}  // namespace sdb
