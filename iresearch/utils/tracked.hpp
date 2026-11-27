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

#include <cstddef>
#include <cstdint>
#include <deque>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/enums/memory_tag.hpp>
#include <duckdb/storage/buffer/buffer_pool_reservation.hpp>
#include <duckdb/storage/buffer/memory_account.hpp>
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

void ChargeRoot(size_t size);

class MemoryBudget final : public duckdb::MemoryAccount {
 public:
  static constexpr size_t kUnlimited = duckdb::MemoryAccount::UNLIMITED;

  constexpr MemoryBudget() noexcept = default;
  explicit MemoryBudget(size_t limit) noexcept : MemoryAccount{limit} {}

  void Charge(size_t bytes);
  void Free(size_t bytes) noexcept;
  void Adopt(size_t bytes);
  void Admit(size_t bytes) const;

 private:
  [[noreturn]] void Exceeded(size_t bytes, size_t used) const;
};

inline constinit MemoryBudget gRootBudget;

class LocalBudget {
 public:
  constexpr explicit LocalBudget(MemoryBudget& parent) noexcept
    : _parent{&parent} {}
  ~LocalBudget();

  LocalBudget(const LocalBudget&) = delete;
  LocalBudget& operator=(const LocalBudget&) = delete;

  void Charge(size_t bytes) {
    if (bytes >= Available()) [[unlikely]] {
      Grow(bytes);
      return;
    }
    _used += bytes;
  }

  void Free(size_t bytes) noexcept {
    SDB_ASSERT(bytes <= _used);
    _used -= bytes;
  }

  void Reserve(size_t bytes);
  void Trim() noexcept;
  void Rebind(MemoryBudget& parent);
  void Return() noexcept;

  MemoryBudget& Parent() const noexcept { return *_parent; }
  size_t Held() const noexcept { return _held; }
  size_t Used() const noexcept { return _used; }
  size_t Available() const noexcept { return _held - _used; }

  [[nodiscard]] duckdb::TempBufferPoolReservation Lend(duckdb::MemoryTag tag,
                                                       size_t bytes);

 private:
  void Grow(size_t bytes);
  void Take(size_t bytes);

  MemoryBudget* _parent;
  size_t _held = 0;
  size_t _used = 0;
};

inline void* AllocateRaw(size_t bytes, size_t alignment) {
  if (alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
    return ::operator new(bytes, std::align_val_t{alignment});
  }
  return ::operator new(bytes);
}

inline void DeallocateRaw(void* p, size_t bytes, size_t alignment) noexcept {
  if (alignment > __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
    ::operator delete(p, bytes, std::align_val_t{alignment});
    return;
  }
  ::operator delete(p, bytes);
}

void* AllocateHugePages(size_t bytes);
void FreeHugePages(void* p, size_t bytes) noexcept;

enum class TrackedPolicy : uint8_t {
  Value,
  DefaultInit,
  HugePages,
};

// NOLINTBEGIN
template<typename T = void, TrackedPolicy kPolicy = TrackedPolicy::Value>
class TrackedAllocator {
 public:
  using value_type = T;
  using propagate_on_container_copy_assignment = std::false_type;
  using propagate_on_container_move_assignment = std::false_type;
  using propagate_on_container_swap = std::false_type;
  using is_always_equal = std::false_type;

  template<typename U>
  struct rebind {
    using other = TrackedAllocator<U, kPolicy>;
  };

  explicit TrackedAllocator(LocalBudget& budget) noexcept : _budget{&budget} {}

  template<typename U, TrackedPolicy kOther>
  TrackedAllocator(const TrackedAllocator<U, kOther>& other) noexcept
    : _budget{&other.Budget()} {}

  T* allocate(size_t n) {
    static_assert(!std::is_void_v<T>);
    const auto bytes = n * sizeof(T);
    _budget->Charge(bytes);
    absl::Cleanup release = [&] { _budget->Free(bytes); };
    auto* p = static_cast<T*>(Obtain(bytes));
    std::move(release).Cancel();
    return p;
  }

  void deallocate(T* p, size_t n) noexcept {
    const auto bytes = n * sizeof(T);
    Release(p, bytes);
    _budget->Free(bytes);
  }

  template<typename U, typename... Args>
  void construct(U* p, Args&&... args) {
    if constexpr (kPolicy != TrackedPolicy::Value && sizeof...(Args) == 0) {
      ::new (static_cast<void*>(p)) U;
    } else {
      std::construct_at(p, std::forward<Args>(args)...);
    }
  }

  LocalBudget& Budget() const noexcept {
    SDB_ASSERT(_budget);
    return *_budget;
  }

  friend bool operator==(const TrackedAllocator& lhs,
                         const TrackedAllocator& rhs) noexcept {
    return lhs._budget == rhs._budget;
  }

 private:
  static void* Obtain(size_t bytes) {
    if constexpr (kPolicy == TrackedPolicy::HugePages) {
      static_assert(alignof(T) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);
      return AllocateHugePages(bytes);
    } else {
      return AllocateRaw(bytes, alignof(T));
    }
  }

  static void Release(T* p, size_t bytes) noexcept {
    if constexpr (kPolicy == TrackedPolicy::HugePages) {
      FreeHugePages(p, bytes);
    } else {
      DeallocateRaw(p, bytes, alignof(T));
    }
  }

  LocalBudget* _budget;
};
// NOLINTEND

duckdb::Allocator DuckAllocator(TrackedAllocator<> alloc);

void* TrackedMalloc(TrackedAllocator<> alloc, size_t size) noexcept;
void TrackedFree(TrackedAllocator<> alloc, void* p) noexcept;

class MemoryCharge {
 public:
  explicit MemoryCharge(TrackedAllocator<> alloc, size_t bytes = 0)
    : _budget{&alloc.Budget()} {
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
      _budget->Charge(bytes - _bytes);
    } else if (bytes < _bytes) {
      _budget->Free(_bytes - bytes);
    }
    _bytes = bytes;
  }

  size_t Bytes() const noexcept { return _bytes; }

 private:
  void Release() noexcept {
    if (_bytes != 0) {
      _budget->Free(std::exchange(_bytes, 0));
    }
  }

  LocalBudget* _budget;
  size_t _bytes = 0;
};

template<typename T>
using TrackedVector = std::vector<T, TrackedAllocator<T>>;

template<typename T>
using TrackedDeque = std::deque<T, TrackedAllocator<T>>;

using TrackedString =
  std::basic_string<char, std::char_traits<char>, TrackedAllocator<char>>;

using TrackedBstring = basic_string<byte_type, TrackedAllocator<byte_type>>;

struct TrackedDelete {
  LocalBudget* budget = nullptr;
  size_t size = 0;

  template<typename T>
  void operator()(T* p) const noexcept {
    std::destroy_at(p);
    TrackedAllocator<std::byte>{*budget}.deallocate(
      reinterpret_cast<std::byte*>(p), size);
  }
};

template<typename T>
using TrackedUniquePtr = std::unique_ptr<T, TrackedDelete>;

template<typename T, typename... Args>
TrackedUniquePtr<T> MakeTrackedUnique(TrackedAllocator<> alloc,
                                      Args&&... args) {
  static_assert(alignof(T) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);
  TrackedAllocator<std::byte> bytes{alloc};
  auto* p = bytes.allocate(sizeof(T));
  absl::Cleanup release = [&] { bytes.deallocate(p, sizeof(T)); };
  auto* object =
    std::construct_at(reinterpret_cast<T*>(p), std::forward<Args>(args)...);
  std::move(release).Cancel();
  return TrackedUniquePtr<T>{object, TrackedDelete{&alloc.Budget(), sizeof(T)}};
}

template<typename T>
TrackedUniquePtr<T> MakeTrackedUniqueForOverwrite(TrackedAllocator<> alloc) {
  static_assert(alignof(T) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);
  TrackedAllocator<std::byte> bytes{alloc};
  auto* p = bytes.allocate(sizeof(T));
  absl::Cleanup release = [&] { bytes.deallocate(p, sizeof(T)); };
  auto* object = ::new (static_cast<void*>(p)) T;
  std::move(release).Cancel();
  return TrackedUniquePtr<T>{object, TrackedDelete{&alloc.Budget(), sizeof(T)}};
}

}  // namespace irs
