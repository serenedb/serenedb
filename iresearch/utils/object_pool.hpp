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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <atomic>
#include <utility>
#include <vector>

#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/noncopyable.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_utils.hpp"

namespace irs {

// Lock-free stack.
// Move construction/assignment is not thread-safe.
template<typename T = utils::Empty>
class ConcurrentStack : private util::Noncopyable {
 public:
  using element_type = T;

  struct NodeType {
    [[no_unique_address]] element_type value{};
    // next needs to be atomic because
    // nodes are kept in a free-list and reused!
    std::atomic<NodeType*> next{};
  };

  explicit ConcurrentStack(NodeType* head = nullptr) noexcept
    : _head{ConcurrentNode{head}} {}

  ConcurrentStack(ConcurrentStack&& rhs) noexcept {
    move_unsynchronized(std::move(rhs));
  }

  ConcurrentStack& operator=(ConcurrentStack&& rhs) noexcept {
    if (this != &rhs) {
      move_unsynchronized(std::move(rhs));
    }
    return *this;
  }

  bool empty() const noexcept {
    return nullptr == _head.load(std::memory_order_acquire).node;
  }

  NodeType* pop() noexcept {
    ConcurrentNode head = _head.load(std::memory_order_acquire);
    ConcurrentNode new_head;

    do {
      if (!head.node) {
        return nullptr;
      }

      new_head.node = head.node->next.load(std::memory_order_acquire);
      new_head.version = head.version + 1;
    } while (!_head.compare_exchange_weak(
      head, new_head, std::memory_order_acquire, std::memory_order_relaxed));

    return head.node;
  }

  void push(NodeType& new_node) noexcept {
    ConcurrentNode head = _head.load(std::memory_order_relaxed);
    ConcurrentNode new_head{&new_node};

    do {
      new_node.next.store(head.node, std::memory_order_release);

      new_head.version = head.version + 1;
    } while (!_head.compare_exchange_weak(
      head, new_head, std::memory_order_release, std::memory_order_relaxed));
  }

 private:
  struct alignas(sizeof(uintptr_t) * 2) ConcurrentNode {
    explicit ConcurrentNode(NodeType* node = nullptr) noexcept : node{node} {}

    uintptr_t version{0};  // avoid aba problem
    NodeType* node;
  };

  void move_unsynchronized(ConcurrentStack&& rhs) noexcept {
    _head.store(rhs._head.load(std::memory_order_relaxed),
                std::memory_order_relaxed);
    rhs._head.store(ConcurrentNode{}, std::memory_order_relaxed);
  }

  std::atomic<ConcurrentNode> _head{};
  static_assert(sizeof(_head) == alignof(std::atomic<ConcurrentNode>));
};

// Represents a control object of unbounded_object_pool
template<typename T, typename D>
class PoolControlPtr final : public std::unique_ptr<T, D> {
 public:
  using std::unique_ptr<T, D>::unique_ptr;

  PoolControlPtr() = default;

  // Intentionally hides std::unique_ptr<...>::reset() as we
  // disallow changing the owned pointer.
  void reset() noexcept { std::unique_ptr<T, D>::reset(); }
};

// Base class for all unbounded object pool implementations
template<typename T,
         typename =
           std::enable_if_t<kIsUniquePtr<typename T::ptr> &&
                            std::is_empty_v<typename T::ptr::deleter_type>>>
class UnboundedObjectPoolBase : private util::Noncopyable {
 public:
  using deleter_type = typename T::ptr::deleter_type;
  using element_type = typename T::ptr::element_type;
  using pointer = typename T::ptr::pointer;

 private:
  struct Slot : util::Noncopyable {
    pointer value{};
  };

 public:
  size_t size() const noexcept { return _pool.size(); }

 protected:
  using stack = ConcurrentStack<Slot>;
  using node = typename stack::NodeType;

  explicit UnboundedObjectPoolBase(size_t size)
    : _pool(size), _free_slots{_pool.data()} {
    // build up linked list
    for (auto begin = _pool.begin(), end = _pool.end(),
              next = begin < end ? (begin + 1) : end;
         next < end; begin = next, ++next) {
      begin->next = &*next;
    }
  }

  template<typename... Args>
  pointer acquire(Args&&... args) {
    auto* head = _free_objects.pop();

    if (head) {
      auto value = std::exchange(head->value.value, nullptr);
      SDB_ASSERT(value);
      _free_slots.push(*head);
      return value;
    }

    auto ptr = T::make(std::forward<Args>(args)...);

    return ptr.release();
  }

  void release(pointer value) noexcept {
    if (!value) {
      // do not hold nullptr values in the pool since
      // emplace(...) uses nullptr to denote creation failure
      return;
    }

    auto* slot = _free_slots.pop();

    if (!slot) {
      // no free slots
      deleter_type{}(value);
      return;
    }

    [[maybe_unused]] const auto old_value =
      std::exchange(slot->value.value, value);
    SDB_ASSERT(!old_value);
    _free_objects.push(*slot);
  }

  UnboundedObjectPoolBase(UnboundedObjectPoolBase&& rhs) noexcept
    : _pool{std::move(rhs._pool)} {
    // need for volatile pool only
  }
  UnboundedObjectPoolBase& operator=(UnboundedObjectPoolBase&&) = delete;

  std::vector<node> _pool;
  stack _free_objects;  // list of created objects that are ready to be reused
  stack _free_slots;    // list of free slots to be reused
};

// A fixed size pool of objects
// if the pool is empty then a new object is created via make(...)
// if an object is available in a pool then in is returned and no
// longer tracked by the pool
// when the object is released it is placed back into the pool if
// space in the pool is available
// pool owns produced object so it's not allowed to destroy before
// all acquired objects will be destroyed.
// Object 'ptr' that evaluate to false when returned back into the pool
// will be discarded instead.
template<typename T>
class UnboundedObjectPool : public UnboundedObjectPoolBase<T> {
 private:
  using base_t = UnboundedObjectPoolBase<T>;
  using node = typename base_t::node;

 public:
  using element_type = typename base_t::element_type;
  using pointer = typename base_t::pointer;

 private:
  // Private because we want to disallow upcasts to std::unique_ptr<...>.
  class Releaser final {
   public:
    explicit Releaser(UnboundedObjectPool& owner) noexcept : _owner{&owner} {}

    Releaser() noexcept : _owner{nullptr} {}

    void operator()(pointer p) const noexcept {
      SDB_ASSERT(p);  // Ensured by std::unique_ptr<...>
      SDB_ASSERT(_owner);
      _owner->release(p);
    }

   private:
    UnboundedObjectPool* _owner;
  };

 public:
  // Represents a control object of unbounded_object_pool
  using ptr = PoolControlPtr<element_type, Releaser>;

  explicit UnboundedObjectPool(size_t size = 0) : base_t{size} {}

  ~UnboundedObjectPool() {
    for (auto& slot : this->_pool) {
      if (auto p = slot.value.value; p != nullptr) {
        typename base_t::deleter_type{}(p);
      }
    }
  }

  // Clears all cached objects
  void clear() {
    node* head = nullptr;

    // reset all cached instances
    while ((head = this->_free_objects.pop())) {
      auto p = std::exchange(head->value.value, nullptr);
      SDB_ASSERT(p);
      typename base_t::deleter_type{}(p);
      this->_free_slots.push(*head);
    }
  }

  template<typename... Args>
  ptr emplace(Args&&... args) {
    return {this->acquire(std::forward<Args>(args)...), Releaser{*this}};
  }

 private:
  // disallow move
  UnboundedObjectPool(UnboundedObjectPool&&) = delete;
  UnboundedObjectPool& operator=(UnboundedObjectPool&&) = delete;
};

}  // namespace irs
