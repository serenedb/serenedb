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

#include <bit>
#include <cstddef>
#include <cstdint>
#include <duckdb/storage/arena_allocator.hpp>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs {

#if defined(__has_feature)
#if __has_feature(address_sanitizer)
inline constexpr bool kArenaRewindFrees = true;
#else
inline constexpr bool kArenaRewindFrees = false;
#endif
#elif defined(__SANITIZE_ADDRESS__)
inline constexpr bool kArenaRewindFrees = true;
#else
inline constexpr bool kArenaRewindFrees = false;
#endif

class MemoryArena {
 public:
  static constexpr size_t kAlignment = 8;

  MemoryArena() : _arena{TrackedDuckAllocator()} {}

  MemoryArena(const MemoryArena&) = delete;
  MemoryArena& operator=(const MemoryArena&) = delete;

  void* Allocate(size_t bytes, size_t alignment) {
    SDB_ASSERT(std::has_single_bit(alignment));
    if (alignment <= kAlignment) {
      return _arena.AllocateAligned(bytes);
    }
    auto* p = _arena.AllocateAligned(bytes + alignment - kAlignment);
    return reinterpret_cast<void*>(
      memory::AlignUp(reinterpret_cast<uintptr_t>(p), alignment));
  }

  template<typename T>
  T* Allocate(size_t count) {
    return static_cast<T*>(Allocate(count * sizeof(T), alignof(T)));
  }

  template<typename Base, typename Derived = Base, typename... Args>
  memory::managed_ptr<Base> Make(Args&&... args) {
    using Object = memory::OnArena<Derived>;
    return memory::make_managed_at<Base, Derived>(
      Allocate(sizeof(Object), alignof(Object)), std::forward<Args>(args)...);
  }

  void Rewind() noexcept {
    if constexpr (kArenaRewindFrees) {
      _arena.FreeAll();
    } else {
      _arena.Reset();
    }
  }

 private:
  duckdb::ArenaAllocator _arena;
};

// NOLINTBEGIN
template<typename T>
class ArenaStlAllocator {
 public:
  using value_type = T;
  using propagate_on_container_copy_assignment = std::true_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;
  using is_always_equal = std::false_type;

  ArenaStlAllocator(MemoryArena& arena) noexcept : _arena{&arena} {}

  template<typename U>
  ArenaStlAllocator(const ArenaStlAllocator<U>& other) noexcept
    : _arena{&other.Arena()} {}

  T* allocate(size_t n) { return _arena->Allocate<T>(n); }

  void deallocate(T*, size_t) noexcept {}

  MemoryArena& Arena() const noexcept { return *_arena; }

  friend bool operator==(const ArenaStlAllocator& lhs,
                         const ArenaStlAllocator& rhs) noexcept {
    return lhs._arena == rhs._arena;
  }

 private:
  MemoryArena* _arena;
};
// NOLINTEND

namespace memory {

template<typename T>
using ArenaVector = std::vector<T, ArenaStlAllocator<T>>;

}  // namespace memory

class QueryMemory {
 public:
  QueryMemory() = default;

  QueryMemory(const QueryMemory&) = delete;
  QueryMemory& operator=(const QueryMemory&) = delete;

  template<typename Base, typename Derived = Base, typename... Args>
  memory::managed_ptr<Base> Make(Args&&... args) {
    return _queries.Make<Base, Derived>(std::forward<Args>(args)...);
  }

  MemoryArena& Arena() noexcept { return _queries; }

  MemoryArena& Scratch() noexcept { return _scratch; }

  void Rewind() noexcept {
    _queries.Rewind();
    _scratch.Rewind();
  }

 private:
  MemoryArena _queries;
  MemoryArena _scratch;
};

class PlanMemory {
 public:
  PlanMemory() = default;

  PlanMemory(const PlanMemory&) = delete;
  PlanMemory& operator=(const PlanMemory&) = delete;

  template<typename Base, typename Derived = Base, typename... Args>
  memory::managed_ptr<Base> Make(Args&&... args) {
    return _nodes.Make<Base, Derived>(std::forward<Args>(args)...);
  }

  MemoryArena& Arena() noexcept { return _nodes; }

  MemoryArena& Scratch() noexcept { return _scratch; }

  void Rewind() noexcept {
    _nodes.Rewind();
    _scratch.Rewind();
  }

 private:
  MemoryArena _nodes;
  MemoryArena _scratch;
};

}  // namespace irs
