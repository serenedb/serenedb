////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <memory>
#include <type_traits>

#include "iresearch/utils/misc.hpp"

namespace irs {

// It's possible to allow unsafe swap: swap without allocator propagation.
// But beware of UB in case of non equal allocators.
template<typename M, typename A, bool SafeSwap = true>
class ManagedAllocator {
  using Traits = std::allocator_traits<A>;

 public:
  using value_type = typename Traits::value_type;
  using size_type = typename Traits::size_type;
  using difference_type = typename Traits::difference_type;
  using propagate_on_container_move_assignment =
    typename Traits::propagate_on_container_move_assignment;
  using propagate_on_container_copy_assignment =
    typename Traits::propagate_on_container_copy_assignment;
  using propagate_on_container_swap = std::bool_constant<SafeSwap>;

  template<typename... Args>
  ManagedAllocator(M& manager, Args&&... args) noexcept(
    std::is_nothrow_constructible_v<A, Args&&...>)
    : _manager{&manager}, _allocator{std::forward<Args>(args)...} {}

  ManagedAllocator(ManagedAllocator&& other) noexcept(
    std::is_nothrow_move_constructible_v<A>)
    : _manager{other._manager}, _allocator{std::move(other._allocator)} {}

  ManagedAllocator(const ManagedAllocator& other) noexcept(
    std::is_nothrow_copy_constructible_v<A>)
    : _manager{other._manager}, _allocator{other._allocator} {}

  // TODO(mbkkt) Assign:
  //  Is it safe in case of other == &this? Looks like yes
  //  Maybe swap idiom?

  ManagedAllocator& operator=(ManagedAllocator&& other) noexcept(
    std::is_nothrow_move_assignable_v<A>) {
    _manager = other._manager;
    _allocator = std::move(other._allocator);
    return *this;
  }

  ManagedAllocator& operator=(const ManagedAllocator& other) noexcept(
    std::is_nothrow_copy_assignable_v<A>) {
    _manager = other._manager;
    _allocator = other._allocator;
    return *this;
  }

  template<typename T>
  ManagedAllocator(const ManagedAllocator<M, T, SafeSwap>& other) noexcept(
    std::is_nothrow_constructible_v<A, const T&>)
    : _manager{&other.Manager()}, _allocator{other.Allocator()} {}

  value_type* allocate(size_type n) {
    _manager->Increase(sizeof(value_type) * n);
    Finally cleanup = [&]() noexcept {
      _manager->DecreaseChecked(sizeof(value_type) * n);
    };
    auto* p = _allocator.allocate(n);
    n = 0;
    return p;
  }

  void deallocate(value_type* p, size_type n) noexcept {
    _allocator.deallocate(p, n);
    SDB_ASSERT(n != 0);
    _manager->Decrease(sizeof(value_type) * n);
  }

  M& Manager() const noexcept { return *_manager; }
  const A& Allocator() const noexcept { return _allocator; }

  template<typename T>
  bool operator==(
    const ManagedAllocator<M, T, SafeSwap>& other) const noexcept {
    return _manager == &other.Manager() && _allocator == other.Allocator();
  }

 private:
  M* _manager;
  [[no_unique_address]] A _allocator;
};

}  // namespace irs
