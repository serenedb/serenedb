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

#include <cstddef>
#include <memory>
#include <new>
#include <vector>

namespace irs {

#ifdef __linux__

void* AllocateHugePages(size_t bytes);
void FreeHugePages(void* p, size_t bytes) noexcept;

template<typename T>
struct HugePageAllocator {
  static_assert(alignof(T) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);

  using value_type = T;

  HugePageAllocator() = default;

  template<typename U>
  HugePageAllocator(const HugePageAllocator<U>&) noexcept {}

  T* allocate(size_t n) {
    return static_cast<T*>(AllocateHugePages(n * sizeof(T)));
  }

  void deallocate(T* p, size_t n) noexcept { FreeHugePages(p, n * sizeof(T)); }

  template<typename U>
  void construct(U* p) {
    ::new (static_cast<void*>(p)) U;
  }

  friend bool operator==(const HugePageAllocator&,
                         const HugePageAllocator&) = default;
};

#else

template<typename T>
using HugePageAllocator = std::allocator<T>;

#endif

template<typename T>
using HugePageVector = std::vector<T, HugePageAllocator<T>>;

}  // namespace irs
