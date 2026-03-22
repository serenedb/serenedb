////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <utility>

#include "basics/assert.h"
#include "basics/common.h"
#include "basics/error_code.h"
#include "basics/number_utils.h"

namespace sdb {

class GlobalResourceMonitor;

/// a ResourceMonitor to track and limit memory usage for allocations
/// in a certain area.
struct alignas(64) ResourceMonitor final {
  /// @brief: granularity of allocations that we track. this should be a
  /// power of 2, so dividing by it is efficient!
  /// note: whatever this value is, it will also dictate the minimum granularity
  /// of the global memory usage counter, plus the granularity for each query's
  /// peak memory usage value.
  /// note: if you adjust this value, keep in mind that making the chunk size
  /// smaller will lead to better granularity, but also will increase the number
  /// of atomic updates we need to make inside increaseMemoryUsage() and
  /// decreaseMemoryUsage().
  static constexpr uint64_t kChunkSize = 32768;
  static_assert(std::has_single_bit(kChunkSize));

  ResourceMonitor(const ResourceMonitor&) = delete;
  ResourceMonitor& operator=(const ResourceMonitor&) = delete;

  explicit ResourceMonitor(GlobalResourceMonitor& global) noexcept
    : _global{global} {}
  ~ResourceMonitor();

  /// sets a memory limit
  void memoryLimit(uint64_t value) noexcept { _limit = value; }

  /// returns the current memory limit
  uint64_t memoryLimit() const noexcept { return _limit; }

  /// increase memory usage by <value> bytes. may throw!
  const char* increaseMemoryUsageNT(uint64_t value) noexcept;
  void increaseMemoryUsage(uint64_t value);

  /// decrease memory usage by <value> bytes. will not throw
  void decreaseMemoryUsage(uint64_t value) noexcept;

  /// return the current memory usage of the instance
  uint64_t current() const noexcept;

  /// return the peak memory usage of the instance
  uint64_t peak() const noexcept;

  /// reset counters for the local instance
  void clear() noexcept {
    _current = 0;
    _peak = 0;
  }

  /// calculate the "number of chunks" used by an allocation size.
  /// for this, we simply divide the size by a constant value, which is large
  /// enough so that many subsequent small allocations mostly fall into the
  /// same chunk.
  static constexpr int64_t numChunks(uint64_t value) noexcept {
    // this is intentionally an integer division, which truncates any
    // remainders. we want this to be fast, so chunkSize should be a power of 2
    // and the div operation can be substituted by a bit shift operation.
    static_assert(std::has_single_bit(kChunkSize));
    return static_cast<int64_t>(value / kChunkSize);
  }

 private:
  std::atomic<uint64_t> _current{0};
  std::atomic<uint64_t> _peak{0};
  uint64_t _limit{0};
  GlobalResourceMonitor& _global;
};

/// RAII object for temporary resource tracking
/// will track the resource usage on creation, and untrack it
/// on destruction, unless the responsibility is stolen
/// from it.
class ResourceUsageScope {
 public:
  ResourceUsageScope(const ResourceUsageScope&) = delete;
  ResourceUsageScope& operator=(const ResourceUsageScope&) = delete;

  explicit ResourceUsageScope(ResourceMonitor& resource_monitor) noexcept
    : _resource_monitor{resource_monitor} {}
  ResourceUsageScope(ResourceUsageScope&& other) noexcept
    : _resource_monitor{other._resource_monitor},
      _value{std::exchange(other._value, 0)} {}
  explicit ResourceUsageScope(ResourceMonitor& resource_monitor, uint64_t value)
    : ResourceUsageScope(resource_monitor) {
    // may throw
    increase(value);
  }

  ~ResourceUsageScope() { revert(); }

  /// steal responsibility for decreasing the memory
  /// usage on destruction
  void steal() noexcept { _value = 0; }

  /// revert all memory usage tracking operations in this scope
  void revert() noexcept { decrease(_value); }

  void increase(uint64_t value) {
    if (value > 0) {
      // may throw
      _resource_monitor.increaseMemoryUsage(value);
      _value += value;
    }
  }

  void decrease(uint64_t value) noexcept {
    if (value > 0) {
      SDB_ASSERT(_value >= value);
      _resource_monitor.decreaseMemoryUsage(value);
      _value -= value;
    }
  }

  // memory tracked by this particlular scope instance
  uint64_t tracked() const noexcept { return _value; }

  uint64_t trackedAndSteal() noexcept {
    uint64_t value = _value;
    _value = 0;
    return value;
  }

  ResourceMonitor& monitor() noexcept { return _resource_monitor; }

 private:
  ResourceMonitor& _resource_monitor;
  uint64_t _value{0};
};

/// an std::allocator-like specialization that uses a ResourceMonitor
/// underneath.
template<typename Allocator, typename ResourceMonitor>
class ResourceUsageAllocatorBase : private Allocator {
 public:
  using difference_type =
    typename std::allocator_traits<Allocator>::difference_type;
  using propagate_on_container_move_assignment = typename std::allocator_traits<
    Allocator>::propagate_on_container_move_assignment;
  using size_type = typename std::allocator_traits<Allocator>::size_type;
  using value_type = typename std::allocator_traits<Allocator>::value_type;

  ResourceUsageAllocatorBase() = delete;

  template<typename... Args>
  ResourceUsageAllocatorBase(ResourceMonitor& resource_monitor, Args&&... args)
    : Allocator(std::forward<Args>(args)...),
      _resource_monitor(&resource_monitor) {}

  ResourceUsageAllocatorBase(ResourceUsageAllocatorBase&& other) noexcept
    : Allocator(other.rawAllocator()),
      _resource_monitor(other._resource_monitor) {}

  ResourceUsageAllocatorBase(const ResourceUsageAllocatorBase& other) noexcept
    : Allocator(other.rawAllocator()),
      _resource_monitor(other._resource_monitor) {}

  ResourceUsageAllocatorBase& operator=(
    ResourceUsageAllocatorBase&& other) noexcept {
    static_cast<Allocator&>(*this) = std::move(static_cast<Allocator&>(other));
    _resource_monitor = other._resource_monitor;
    return *this;
  }

  ResourceUsageAllocatorBase& operator=(
    const ResourceUsageAllocatorBase& other) noexcept {
    static_cast<Allocator&>(*this) = static_cast<const Allocator&>(other);
    _resource_monitor = other._resource_monitor;
    return *this;
  }

  template<typename A>
  ResourceUsageAllocatorBase(
    const ResourceUsageAllocatorBase<A, ResourceMonitor>& other) noexcept
    : Allocator(other.rawAllocator()),
      _resource_monitor(other.resourceMonitor()) {}

  value_type* allocate(size_t n) {
    _resource_monitor->increaseMemoryUsage(sizeof(value_type) * n);
    try {
      return Allocator::allocate(n);
    } catch (...) {
      _resource_monitor->decreaseMemoryUsage(sizeof(value_type) * n);
      throw;
    }
  }

  void deallocate(value_type* p, size_t n) {
    Allocator::deallocate(p, n);
    _resource_monitor->decreaseMemoryUsage(sizeof(value_type) * n);
  }

  const Allocator& rawAllocator() const noexcept {
    return static_cast<const Allocator&>(*this);
  }

  ResourceMonitor* resourceMonitor() const noexcept {
    return _resource_monitor;
  }

  template<typename A>
  bool operator==(const ResourceUsageAllocatorBase<A, ResourceMonitor>& other)
    const noexcept {
    return rawAllocator() == other.rawAllocator() &&
           _resource_monitor == other.resourceMonitor();
  }

 private:
  ResourceMonitor* _resource_monitor;
};

namespace detail {

template<typename T>
struct UsesAllocatorConstructionArgsT;

template<typename T>
inline constexpr auto kUsesAllocatorConstructionArgs =
  UsesAllocatorConstructionArgsT<T>{};

template<typename T>
struct UsesAllocatorConstructionArgsT {
  template<typename Alloc, typename... Args>
  auto operator()(const Alloc& alloc, Args&&... args) const {
    if constexpr (!std::uses_allocator<T, Alloc>::value) {
      return std::forward_as_tuple(std::forward<Args>(args)...);
    } else if constexpr (std::is_constructible_v<T, std::allocator_arg_t, Alloc,
                                                 Args...>) {
      return std::tuple<std::allocator_arg_t, const Alloc&, Args&&...>(
        std::allocator_arg, alloc, std::forward<Args>(args)...);
    } else {
      return std::tuple<Args&&..., const Alloc&>(std::forward<Args>(args)...,
                                                 alloc);
    }
  }
};

template<typename U, typename V>
struct UsesAllocatorConstructionArgsT<std::pair<U, V>> {
  using T = std::pair<U, V>;
  template<typename Alloc, typename Tuple1, typename Tuple2>
  auto operator()(const Alloc& alloc, std::piecewise_construct_t, Tuple1&& t1,
                  Tuple2&& t2) const {
    return std::make_tuple(
      std::piecewise_construct,
      std::apply(
        [&alloc](auto&&... args1) {
          return kUsesAllocatorConstructionArgs<U>(
            alloc, std::forward<decltype(args1)>(args1)...);
        },
        std::forward<Tuple1>(t1)),
      std::apply(
        [&alloc](auto&&... args2) {
          return kUsesAllocatorConstructionArgs<V>(
            alloc, std::forward<decltype(args2)>(args2)...);
        },
        std::forward<Tuple2>(t2)));
  }
  template<typename Alloc>
  auto operator()(const Alloc& alloc) const {
    return uses_allocator_construction_args<T>(alloc, std::piecewise_construct,
                                               std::tuple<>{}, std::tuple<>{});
  }

  template<typename Alloc, typename A, typename B>
  auto operator()(const Alloc& alloc, A&& a, B&& b) const {
    return kUsesAllocatorConstructionArgs<T>(
      alloc, std::piecewise_construct,
      std::forward_as_tuple(std::forward<A>(a)),
      std::forward_as_tuple(std::forward<B>(b)));
  }

  template<typename Alloc, typename A, typename B>
  auto operator()(const Alloc& alloc, std::pair<A, B>& pr) const {
    return kUsesAllocatorConstructionArgs<T>(alloc, std::piecewise_construct,
                                             std::forward_as_tuple(pr.first),
                                             std::forward_as_tuple(pr.second));
  }

  template<typename Alloc, typename A, typename B>
  auto operator()(const Alloc& alloc, const std::pair<A, B>& pr) const {
    return kUsesAllocatorConstructionArgs<T>(alloc, std::piecewise_construct,
                                             std::forward_as_tuple(pr.first),
                                             std::forward_as_tuple(pr.second));
  }

  template<typename Alloc, typename A, typename B>
  auto operator()(const Alloc& alloc, std::pair<A, B>&& pr) const {
    return kUsesAllocatorConstructionArgs<T>(
      alloc, std::piecewise_construct,
      std::forward_as_tuple(std::move(pr.first)),
      std::forward_as_tuple(std::move(pr.second)));
  }
};

}  // namespace detail

template<typename T, typename ResourceMonitor>
struct ResourceUsageAllocator
  : ResourceUsageAllocatorBase<std::allocator<T>, ResourceMonitor> {
  using ResourceUsageAllocatorBase<std::allocator<T>,
                                   ResourceMonitor>::ResourceUsageAllocatorBase;

  template<typename U>
  struct Rebind {
    using other = ResourceUsageAllocator<U, ResourceMonitor>;
  };

  template<typename X, typename... Args>
  void construct(X* ptr, Args&&... args) {
    // Sadly libc++ on mac does not support this function. Thus we do it by
    // hand std::uninitialized_construct_using_allocator(ptr, *this,
    //                                              std::forward<Args>(args)...)
    std::apply(
      [&](auto&&... xs) {
        return std::construct_at(ptr, std::forward<decltype(xs)>(xs)...);
      },
      detail::kUsesAllocatorConstructionArgs<X>(*this,
                                                std::forward<Args>(args)...));
  }
};

}  // namespace sdb
