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
#include <iresearch/utils/object_pool.hpp>
#include <memory>

#include "server/utils/number_of_cores.h"

namespace sdb::network::http {

template<typename T>
concept PoolState = requires(T& state) {
  { state.Reset() } noexcept -> std::same_as<bool>;
};

template<PoolState T>
class Pooled {
 public:
  static size_t Capacity() { return Pool().size(); }

  Pooled() : _state{Pool().emplace()} {}

  ~Pooled() {
    if (!_state->Reset()) {
      std::default_delete<T>{}(_state.release());
    }
  }

  Pooled(const Pooled&) = delete;
  Pooled& operator=(const Pooled&) = delete;

  T* operator->() const noexcept { return _state.get(); }
  T& operator*() const noexcept { return *_state; }

 private:
  struct Builder {
    using ptr = std::unique_ptr<T>;  // NOLINT

    static ptr make() { return std::make_unique<T>(); }  // NOLINT
  };

  using Objects = irs::UnboundedObjectPool<Builder>;

  static Objects& Pool() {
    static auto* const kPool =
      new Objects{static_cast<size_t>(2 * CountPhysicalCores())};
    return *kPool;
  }

  typename Objects::ptr _state;
};

}  // namespace sdb::network::http
