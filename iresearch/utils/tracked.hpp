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

#include <absl/cleanup/cleanup.h>

#include <duckdb/common/allocator.hpp>
#include <duckdb/common/enums/memory_tag.hpp>
#include <duckdb/storage/buffer/buffer_pool_reservation.hpp>
#include <memory>
#include <new>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

inline constexpr duckdb::MemoryTag kMemoryTag = duckdb::MemoryTag::ALLOCATOR;

[[nodiscard]] duckdb::TempBufferPoolReservation ReserveMemory(
  duckdb::MemoryTag tag, size_t size);
void FreeReservedMemory(duckdb::MemoryTag tag, size_t size) noexcept;

void ChargeRoot(duckdb::MemoryTag tag, size_t size);

class MemoryBudget;

class BudgetNode {
 public:
  BudgetNode(const BudgetNode&) = delete;
  BudgetNode& operator=(const BudgetNode&) = delete;

  void Reserve(size_t bytes);

  void Free(size_t bytes) noexcept {
    SDB_ASSERT(bytes <= _used);
    _used -= bytes;
  }

  void Trim() noexcept;

  size_t Held() const noexcept { return _held; }
  size_t Used() const noexcept { return _used; }
  size_t Available() const noexcept { return _held - _used; }

 protected:
  BudgetNode(MemoryBudget* parent, size_t reserve);
  ~BudgetNode();

  void Take(size_t bytes);
  void Use(size_t bytes) noexcept { _used += bytes; }
  void Detach(size_t bytes) noexcept;

 private:
  void Give(size_t bytes) noexcept;

  MemoryBudget* _parent;
  size_t _held = 0;
  size_t _used = 0;
};

class MemoryBudget final : public BudgetNode {
 public:
  explicit MemoryBudget(MemoryBudget* parent, size_t reserve = 0)
    : BudgetNode{parent, reserve} {}

  void Charge(size_t bytes) {
    if (bytes > Available()) [[unlikely]] {
      Take(bytes - Available());
    }
    Use(bytes);
  }

  [[nodiscard]] duckdb::TempBufferPoolReservation Lend(duckdb::MemoryTag tag,
                                                       size_t bytes);
};

class FixedBudget final : public BudgetNode {
 public:
  FixedBudget(MemoryBudget* parent, size_t reserve)
    : BudgetNode{parent, reserve} {}

  void Charge(size_t bytes) {
    if (bytes > Available()) [[unlikely]] {
      Exhausted(bytes);
    }
    Use(bytes);
  }

 private:
  [[noreturn]] void Exhausted(size_t bytes) const;
};

template<typename Budget>
void ChargeMemory(Budget* budget, duckdb::MemoryTag tag, size_t size) {
  if (!budget) {
    ChargeRoot(tag, size);
    return;
  }
  SDB_ASSERT(tag == kMemoryTag);
  budget->Charge(size);
}

template<typename Budget>
void FreeCharge(Budget* budget, duckdb::MemoryTag tag, size_t size) noexcept {
  if (!budget) {
    FreeReservedMemory(tag, size);
    return;
  }
  SDB_ASSERT(tag == kMemoryTag);
  budget->Free(size);
}

duckdb::Allocator& TrackedDuckAllocator();
duckdb::Allocator BudgetAllocator(MemoryBudget* budget);

void* TrackedMalloc(MemoryBudget* budget, size_t size) noexcept;
void TrackedFree(MemoryBudget* budget, void* p) noexcept;

// NOLINTBEGIN
template<typename T, duckdb::MemoryTag Tag = kMemoryTag,
         typename Base = std::allocator<T>, typename Budget = MemoryBudget>
struct TrackedAllocator {
  static_assert(std::is_empty_v<Base>);

  using value_type = T;
  using propagate_on_container_copy_assignment = std::false_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;
  using is_always_equal = std::false_type;

  template<typename U>
  struct rebind {
    using other = TrackedAllocator<
      U, Tag, typename std::allocator_traits<Base>::template rebind_alloc<U>,
      Budget>;
  };

  TrackedAllocator() = default;
  explicit TrackedAllocator(Budget* budget) noexcept : budget{budget} {}

  template<typename U, typename B>
  TrackedAllocator(const TrackedAllocator<U, Tag, B, Budget>& other) noexcept
    : budget{other.budget} {}

  TrackedAllocator select_on_container_copy_construction() const noexcept {
    return {};
  }

  T* allocate(size_t n) {
    ChargeMemory(budget, Tag, n * sizeof(T));
    absl::Cleanup release = [&] { FreeCharge(budget, Tag, n * sizeof(T)); };
    auto* p = Base{}.allocate(n);
    std::move(release).Cancel();
    return p;
  }

  void deallocate(T* p, size_t n) noexcept {
    Base{}.deallocate(p, n);
    FreeCharge(budget, Tag, n * sizeof(T));
  }

  template<typename U, typename... Args>
  void construct(U* p, Args&&... args) {
    using UBase =
      typename std::allocator_traits<Base>::template rebind_alloc<U>;
    UBase base;
    std::allocator_traits<UBase>::construct(base, p,
                                            std::forward<Args>(args)...);
  }

  friend bool operator==(const TrackedAllocator& lhs,
                         const TrackedAllocator& rhs) noexcept {
    return lhs.budget == rhs.budget;
  }

  Budget* budget = nullptr;
};
// NOLINTEND

template<typename T>
using FixedAllocator =
  TrackedAllocator<T, kMemoryTag, std::allocator<T>, FixedBudget>;

struct Tracked {
  static void* operator new(size_t size) {
    auto charge = ReserveMemory(kMemoryTag, size);
    auto* p = ::operator new(size);
    charge.size = 0;
    return p;
  }

  static void* operator new(size_t size, std::align_val_t alignment) {
    auto charge = ReserveMemory(kMemoryTag, size);
    auto* p = ::operator new(size, alignment);
    charge.size = 0;
    return p;
  }

  static void* operator new(size_t, void* p) noexcept { return p; }

  static void operator delete(void* p, size_t size) noexcept {
    ::operator delete(p, size);
    FreeReservedMemory(kMemoryTag, size);
  }

  static void operator delete(void* p, size_t size,
                              std::align_val_t alignment) noexcept {
    ::operator delete(p, size, alignment);
    FreeReservedMemory(kMemoryTag, size);
  }
};

class MemoryCharge {
 public:
  MemoryCharge() = default;
  explicit MemoryCharge(size_t bytes) { Resize(bytes); }
  MemoryCharge(MemoryBudget* budget, size_t bytes) : _budget{budget} {
    Resize(bytes);
  }
  MemoryCharge(MemoryCharge&& other) noexcept
    : _budget{other._budget}, _bytes{std::exchange(other._bytes, 0)} {}
  MemoryCharge& operator=(MemoryCharge&& other) noexcept {
    if (this != &other) {
      Release();
      _budget = other._budget;
      _bytes = std::exchange(other._bytes, 0);
    }
    return *this;
  }
  ~MemoryCharge() { Release(); }

  void Resize(size_t bytes) {
    if (bytes > _bytes) {
      ChargeMemory(_budget, kMemoryTag, bytes - _bytes);
    } else if (bytes < _bytes) {
      FreeCharge(_budget, kMemoryTag, _bytes - bytes);
    }
    _bytes = bytes;
  }

  size_t Bytes() const noexcept { return _bytes; }

 private:
  void Release() noexcept {
    if (_bytes != 0) {
      FreeCharge(_budget, kMemoryTag, std::exchange(_bytes, 0));
    }
  }

  MemoryBudget* _budget = nullptr;
  size_t _bytes = 0;
};

template<typename T>
using TrackedVector = std::vector<T, TrackedAllocator<T>>;

template<typename T>
using FixedVector = std::vector<T, FixedAllocator<T>>;

using TrackedString =
  std::basic_string<char, std::char_traits<char>, TrackedAllocator<char>>;

using TrackedBstring = basic_string<byte_type, TrackedAllocator<byte_type>>;

template<typename T>
struct TrackedArrayDelete {
  size_t size = 0;
  MemoryBudget* budget = nullptr;

  void operator()(T* p) const noexcept {
    std::destroy_n(p, size);
    TrackedAllocator<T>{budget}.deallocate(p, size);
  }
};

template<typename T>
using TrackedArray = std::unique_ptr<T[], TrackedArrayDelete<T>>;

template<typename T>
TrackedArray<T> MakeTrackedArray(size_t size, MemoryBudget* budget = nullptr) {
  TrackedAllocator<T> allocator{budget};
  auto* p = allocator.allocate(size);
  absl::Cleanup release = [&] { allocator.deallocate(p, size); };
  std::uninitialized_value_construct_n(p, size);
  std::move(release).Cancel();
  return TrackedArray<T>{p, TrackedArrayDelete<T>{size, budget}};
}

template<typename Budget>
struct TrackedDelete {
  Budget* budget = nullptr;
  size_t size = 0;

  template<typename T>
  void operator()(T* p) const noexcept {
    std::destroy_at(p);
    TrackedAllocator<std::byte, kMemoryTag, std::allocator<std::byte>, Budget>{
      budget}
      .deallocate(reinterpret_cast<std::byte*>(p), size);
  }
};

template<typename T, typename Budget = MemoryBudget>
using TrackedPtr = std::unique_ptr<T, TrackedDelete<Budget>>;

template<typename T, typename Budget, typename... Args>
TrackedPtr<T, Budget> MakeTracked(Budget* budget, Args&&... args) {
  static_assert(alignof(T) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);
  TrackedAllocator<std::byte, kMemoryTag, std::allocator<std::byte>, Budget>
    allocator{budget};
  auto* p = allocator.allocate(sizeof(T));
  absl::Cleanup release = [&] { allocator.deallocate(p, sizeof(T)); };
  auto* object =
    std::construct_at(reinterpret_cast<T*>(p), std::forward<Args>(args)...);
  std::move(release).Cancel();
  return TrackedPtr<T, Budget>{object,
                               TrackedDelete<Budget>{budget, sizeof(T)}};
}

}  // namespace irs
