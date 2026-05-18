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

// S3-FIFO cache (Yang et al., SOSP'23).
//
// An entry T must publicly derive from s3fifo::Node. T may optionally expose:
//   bool Evictable() noexcept;  // returns false to refuse eviction (e.g.
//                               // pinned). Defaults to true if absent.
//   size_t Weight() const noexcept;  // weight contributed to cache capacity.
//                                    // Defaults to 1 if absent.
// Insert and the public Evict take a callback `void on_evict(T&)` that is
// invoked once per entry that the cache removes during the call. The
// callback runs after the entry has been fully unlinked.

#include <cstdint>
#include <limits>
#include <type_traits>

#include "basics/assert.h"

namespace sdb::containers::s3fifo {

// Hit thresholds per the S3-FIFO paper. A node's `_hits` is capped at
// kMaxHits. In the small queue, a node is promoted to main when its
// hits reach kPromoteHits. In the main queue, a node with at least
// kRotateHits is moved to the back instead of being evicted.
inline constexpr uint8_t kMaxHits = 3;
inline constexpr uint8_t kPromoteHits = 2;
inline constexpr uint8_t kRotateHits = 1;
inline constexpr uint64_t kNeverGhosted = std::numeric_limits<uint64_t>::max();

class Node {
 public:
  Node() noexcept = default;
  // A copied entry starts unlinked from any cache.
  Node(const Node&) noexcept : Node{} {}

  Node& operator=(const Node&) = delete;
  Node(Node&&) noexcept = delete;
  Node& operator=(Node&&) noexcept = delete;
  ~Node() noexcept { SDB_ASSERT(Detached()); }

  void Hit() noexcept {
    if (_hits < kMaxHits) {
      ++_hits;
    }
  }

  bool Detached() const noexcept { return _next == this; }

 private:
  template<typename>
  friend class Cache;

  void Unlink() noexcept {
    _prev->_next = _next;
    _next->_prev = _prev;
    _prev = _next = this;
  }

  void LinkTo(Node& list) noexcept {
    SDB_ASSERT(Detached());
    _prev = list._prev;
    _next = &list;
    list._prev->_next = this;
    list._prev = this;
  }

  enum class Queue : uint8_t {
    Main = 0,
    Small,
  };

  Node* _prev = this;
  Node* _next = this;
  uint64_t _ghost_at = kNeverGhosted;
  uint8_t _hits = 0;
  Queue _queue = Queue::Main;
};

template<typename T>
class Cache {
  static_assert(std::is_base_of_v<Node, T>);

 public:
  struct Config {
    size_t capacity;
    size_t small_size;
  };

  explicit Cache(Config config) noexcept
    : _capacity{config.capacity},
      _small_capacity{config.small_size},
      _main_capacity{_capacity - _small_capacity} {
    SDB_ASSERT(_capacity > _small_capacity);
  }

  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;
  Cache(Cache&&) noexcept = delete;
  Cache& operator=(Cache&&) noexcept = delete;
  ~Cache() noexcept {
    SDB_ASSERT(_small_list.Detached());
    SDB_ASSERT(_main_list.Detached());
  }

  template<typename OnEvict>
  void Insert(T& entry, OnEvict&& on_evict) noexcept {
    static_assert(std::is_nothrow_invocable_r_v<void, OnEvict&, T&>);
    while ((_small_size + _main_size) > _capacity) {
      if (!Evict(on_evict)) {
        break;
      }
    }

    auto& node = static_cast<Node&>(entry);
    if (IsGhost(entry)) {
      node._ghost_at = kNeverGhosted;
      node.LinkTo(_main_list);
      node._queue = Node::Queue::Main;
      _main_size += Weight(entry);
    } else {
      node.LinkTo(_small_list);
      node._queue = Node::Queue::Small;
      _small_size += Weight(entry);
    }
  }

  void Remove(T& entry) noexcept {
    if (entry.Detached()) {
      return;
    }
    if (entry._queue == Node::Queue::Main) {
      _main_size -= Weight(entry);
    } else {
      SDB_ASSERT(entry._queue == Node::Queue::Small);
      _small_size -= Weight(entry);
    }
    entry.Unlink();
  }

  bool IsGhost(const T& entry) const noexcept {
    if (entry._ghost_at == kNeverGhosted) {
      return false;
    }
    const auto expires = entry._ghost_at + _main_capacity;
    return expires > _ghost_age;
  }

  template<typename OnEvict>
  bool Evict(OnEvict&& on_evict) noexcept {
    static_assert(std::is_nothrow_invocable_r_v<void, OnEvict&, T&>);
    if (_small_size > _small_capacity && EvictSmall(on_evict)) {
      return true;
    }
    return EvictMain(on_evict);
  }

  void Clear() noexcept {
    UnlinkAll(_small_list);
    UnlinkAll(_main_list);
    _small_size = 0;
    _main_size = 0;
    _ghost_age = 0;
  }

 private:
  static bool Evictable(T& entry) noexcept {
    if constexpr (requires {
                    { entry.Evictable() } -> std::same_as<bool>;
                  }) {
      static_assert(noexcept(entry.Evictable()),
                    "T::Evictable() must be noexcept");
      return entry.Evictable();
    } else {
      return true;
    }
  }

  static size_t Weight(const T& entry) noexcept {
    if constexpr (requires {
                    { entry.Weight() } -> std::same_as<size_t>;
                  }) {
      static_assert(noexcept(entry.Weight()), "T::Weight() must be noexcept");
      return entry.Weight();
    } else {
      return 1;
    }
  }

  static void UnlinkAll(Node& list) noexcept {
    while (!list.Detached()) {
      list._next->Unlink();
    }
  }

  template<typename OnEvict>
  bool EvictSmall(OnEvict& on_evict) noexcept {
    auto* node = _small_list._next;
    while (node != &_small_list) {
      auto& entry = static_cast<T&>(*node);
      auto* next = node->_next;
      SDB_ASSERT(node->_queue == Node::Queue::Small);
      if (node->_hits >= kPromoteHits) {
        node->_hits = 0;
        node->Unlink();
        const auto weight = Weight(entry);
        _small_size -= weight;
        node->LinkTo(_main_list);
        node->_queue = Node::Queue::Main;
        _main_size += weight;
        if (_main_size > _main_capacity && EvictMain(on_evict)) {
          return true;
        }
      } else if (Evictable(entry)) {
        const auto weight = Weight(entry);
        node->Unlink();
        _small_size -= weight;
        node->_ghost_at = _ghost_age;
        _ghost_age += weight;
        on_evict(entry);
        return true;
      }
      node = next;
    }
    return false;
  }

  template<typename OnEvict>
  bool EvictMain(OnEvict& on_evict) noexcept {
    auto* node = _main_list._next;
    while (node != &_main_list) {
      auto& entry = static_cast<T&>(*node);
      auto* next = node->_next;
      SDB_ASSERT(node->_queue == Node::Queue::Main);
      if (node->_hits >= kRotateHits) {
        --node->_hits;
        node->Unlink();
        node->LinkTo(_main_list);
      } else if (Evictable(entry)) {
        node->Unlink();
        _main_size -= Weight(entry);
        on_evict(entry);
        return true;
      }
      node = next;
    }
    return false;
  }

  size_t _capacity;
  size_t _small_capacity;
  size_t _main_capacity;

  Node _small_list;
  Node _main_list;

  size_t _small_size = 0;
  size_t _main_size = 0;
  size_t _ghost_age = 0;
};

}  // namespace sdb::containers::s3fifo
