////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <memory>

#include "basics/assert.h"
#include "basics/log.h"
#include "basics/math_utils.hpp"
#include "basics/noncopyable.hpp"
#include "basics/resource_manager.hpp"
#include "basics/shared.hpp"
#include "basics/type_utils.hpp"

namespace irs::memory {

constexpr size_t AlignUp(size_t size, size_t alignment) noexcept {
  SDB_ASSERT(math::IsPower2(alignment));
  return (size + alignment - 1) & (0 - alignment);
}

template<typename Alloc>
class AllocatorArrayDeallocator {
  using traits_t = std::allocator_traits<Alloc>;

 public:
  using allocator_type = typename traits_t::allocator_type;
  using pointer = typename traits_t::pointer;

  static_assert(std::is_nothrow_move_constructible_v<allocator_type>);
  static_assert(std::is_nothrow_move_assignable_v<allocator_type>);

  AllocatorArrayDeallocator(const allocator_type& alloc, size_t size) noexcept
    : _alloc{alloc}, _size{size} {}
  AllocatorArrayDeallocator(allocator_type&& alloc, size_t size) noexcept
    : _alloc{std::move(alloc)}, _size{size} {}

  void operator()(pointer p) noexcept {
    traits_t::deallocate(_alloc, p, _size);
  }

  AllocatorArrayDeallocator(AllocatorArrayDeallocator&& other) noexcept
    : _alloc{std::move(other._alloc)}, _size{std::exchange(other._size, 0)} {}

  AllocatorArrayDeallocator& operator=(
    AllocatorArrayDeallocator&& other) noexcept {
    if (this != &other) {
      _alloc = std::move(other._alloc);
      _size = std::exchange(other._size, 0);
    }
    return *this;
  }

  allocator_type& alloc() noexcept { return _alloc; }
  size_t size() const noexcept { return _size; }

 private:
  [[no_unique_address]] allocator_type _alloc;
  size_t _size;
};

struct Managed {
 protected:
  virtual ~Managed() = default;

 private:
  friend struct ManagedDeleter;

  // Const because we can allocate and then delete const object
  virtual void Destroy() const noexcept {}
};

template<typename Base>
struct OnHeap final : Base {
  static_assert(std::is_base_of_v<Managed, Base>);

  template<typename... Args>
  OnHeap(Args&&... args) : Base{std::forward<Args>(args)...} {}

 private:
  void Destroy() const noexcept final { delete this; }
};

template<typename Base>
struct Tracked final : Base {
  static_assert(std::is_base_of_v<Managed, Base>);

  template<typename... Args>
  Tracked(IResourceManager& rm, Args&&... args)
    : Base{std::forward<Args>(args)...}, _rm{rm} {
    rm.Increase(sizeof(*this));
  }

 private:
  void Destroy() const noexcept final {
    auto& rm = _rm;
    delete this;
    rm.Decrease(sizeof(*this));
  }

  IResourceManager& _rm;
};

struct ManagedDeleter {
  void operator()(const Managed* p) noexcept {
    SDB_ASSERT(p != nullptr);  // std::unique_ptr doesn't call dtor on nullptr
    p->Destroy();
  }
};

// NOLINTBEGIN
template<typename T>
class [[clang::trivial_abi]] managed_ptr final
  : std::unique_ptr<T, ManagedDeleter> {
 private:
  using Ptr = std::unique_ptr<T, ManagedDeleter>;

  template<typename U>
  friend class managed_ptr;

  template<typename Base, typename Derived>
  friend constexpr managed_ptr<Base> to_managed(Derived& p) noexcept;

  template<typename Base, typename Derived, typename... Args>
  friend managed_ptr<Base> make_managed(Args&&... args);
  template<typename Base, typename Derived, typename... Args>
  friend managed_ptr<Base> make_tracked(IResourceManager&, Args&&... args);

  constexpr explicit managed_ptr(T* p) noexcept : Ptr{p} {}

 public:
  using typename Ptr::element_type;
  using typename Ptr::pointer;

  static_assert(!std::is_array_v<T>);

  constexpr managed_ptr() noexcept = default;
  constexpr managed_ptr(managed_ptr&& u) noexcept = default;
  constexpr managed_ptr(std::nullptr_t) noexcept : Ptr{nullptr} {}
  template<typename U>
    requires std::is_convertible_v<U*, T*>
  constexpr managed_ptr(managed_ptr<U>&& u) noexcept : Ptr{std::move(u)} {}

  constexpr managed_ptr& operator=(managed_ptr&& t) noexcept = default;
  constexpr managed_ptr& operator=(std::nullptr_t) noexcept {
    Ptr::operator=(nullptr);
    return *this;
  }
  template<typename U,
           typename = std::enable_if_t<std::is_convertible_v<U*, T*>>>
  constexpr managed_ptr& operator=(managed_ptr<U>&& u) noexcept {
    Ptr::operator=(std::move(u));
    return *this;
  }

  constexpr void reset(std::nullptr_t = nullptr) noexcept { Ptr::reset(); }
  constexpr void swap(managed_ptr& t) noexcept { Ptr::swap(t); }

  using Ptr::get;
  using Ptr::operator bool;
  using Ptr::operator*;
  using Ptr::operator->;
};

template<typename T, typename U>
constexpr bool operator==(const managed_ptr<T>& t, const managed_ptr<U>& u) {
  return t.get() == u.get();
}

template<typename T>
constexpr bool operator==(const managed_ptr<T>& t, std::nullptr_t) noexcept {
  return !t;
}

template<typename Base, typename Derived = Base>
constexpr managed_ptr<Base> to_managed(Derived& p) noexcept {
  return managed_ptr<Base>{&p};
}

template<typename Base, typename Derived = Base, typename... Args>
managed_ptr<Base> make_managed(Args&&... args) {
  return managed_ptr<Base>{new OnHeap<Derived>{std::forward<Args>(args)...}};
}

template<typename Base, typename Derived = Base, typename... Args>
managed_ptr<Base> make_tracked(IResourceManager& rm, Args&&... args) {
  return managed_ptr<Base>{
    new Tracked<Derived>{rm, std::forward<Args>(args)...}};
}
// NOLINTEND

// do not construct objects in a block
struct AllocateOnlyTag final {};
inline constexpr AllocateOnlyTag kAllocateOnly{};

template<
  typename T, typename Alloc,
  typename = std::enable_if_t<std::is_array_v<T> && std::extent_v<T> == 0>>
auto AllocateUnique(Alloc&& alloc, size_t size, AllocateOnlyTag /*tag*/) {
  using traits_t = std::allocator_traits<std::remove_cvref_t<Alloc>>;
  using pointer = typename traits_t::pointer;
  using allocator_type = typename traits_t::allocator_type;
  using deleter_t = AllocatorArrayDeallocator<allocator_type>;
  using unique_ptr_t = std::unique_ptr<T, deleter_t>;

  pointer p = nullptr;

  if (!size) {
    return unique_ptr_t{p, deleter_t{std::forward<Alloc>(alloc), size}};
  }

  p = alloc.allocate(size);  // allocate space for 'size' object

  return unique_ptr_t{p, deleter_t{std::forward<Alloc>(alloc), size}};
}

// Decline wrong syntax
template<typename T, typename Alloc, typename... Types>
std::enable_if_t<std::extent_v<T> != 0, void> AllocateUnique(
  Alloc&, Types&&...) = delete;

}  // namespace irs::memory
