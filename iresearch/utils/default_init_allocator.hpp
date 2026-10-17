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

#include <memory>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

namespace irs {

template<typename T>
struct DefaultInitAllocator : std::allocator<T> {
  // NOLINTBEGIN
  template<typename U>
  struct rebind {
    using other = DefaultInitAllocator<U>;
  };

  using std::allocator<T>::allocator;

  template<typename U>
  void construct(U* p) noexcept(std::is_nothrow_default_constructible_v<U>) {
    ::new (static_cast<void*>(p)) U;
  }

  template<typename U, typename... Args>
  void construct(U* p, Args&&... args) {
    std::construct_at(p, std::forward<Args>(args)...);
  }
  // NOLINTEND
};

template<typename T>
using ScratchVector = std::vector<T, DefaultInitAllocator<T>>;

}  // namespace irs
