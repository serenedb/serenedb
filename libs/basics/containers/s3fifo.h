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

#include <algorithm>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/parent_from_member.hpp>
#include <concepts>

#include "basics/assert.h"

namespace sdb::containers {

template<typename F, typename E>
concept S3FIFOCacheEvictor = requires(F func, E& entry) {
  requires noexcept(func(entry));
  { func(entry) } -> std::same_as<bool>;
};

template<typename F, typename E>
concept S3FIFOCacheCost = requires(F func, const E& entry) {
  requires noexcept(func(entry));
  { func(entry) } -> std::same_as<size_t>;
};

class S3FIFOCacheHook {
 public:
  S3FIFOCacheHook() noexcept = default;

  S3FIFOCacheHook(const S3FIFOCacheHook&) = default;

  S3FIFOCacheHook& operator=(const S3FIFOCacheHook&) = delete;
  S3FIFOCacheHook(S3FIFOCacheHook&&) noexcept = delete;
  S3FIFOCacheHook& operator=(S3FIFOCacheHook&&) noexcept = delete;
  ~S3FIFOCacheHook() noexcept = default;

  void Touch() noexcept { _freq = std::min(_freq + 1, 3); }

  bool Evicted() const noexcept { return !_hook.is_linked(); }

 private:
  using HookType = boost::intrusive::list_member_hook<
    boost::intrusive::link_mode<boost::intrusive::safe_link>>;

  enum class Location : uint8_t {
    Main,
    Small,
  };

  template<typename T, S3FIFOCacheEvictor<T> Evictor, S3FIFOCacheCost<T> Cost>
    requires std::derived_from<T, S3FIFOCacheHook>
  friend class S3FIFOCache;

  HookType _hook;
  uint64_t _ghost_insertion_time = 0;
  bool _has_ghost_insertion_time = false;
  uint8_t _freq = 0;
  Location _location = Location::Main;
};

struct S3FIFODefaultCacheEvictor {
  bool operator()(auto&& /*entry*/) noexcept { return true; }
};

struct S3FIFODefaultCacheCost {
  size_t operator()(const auto& /*entry*/) noexcept { return 1; }
};

template<typename T, S3FIFOCacheEvictor<T> Evictor = S3FIFODefaultCacheEvictor,
         S3FIFOCacheCost<T> Cost = S3FIFODefaultCacheCost>
  requires std::derived_from<T, S3FIFOCacheHook>
class S3FIFOCache {
 public:
  struct Config {
    size_t cache_size;
    size_t small_size;
  };

  explicit S3FIFOCache(Config config, const Evictor& evict = Evictor{},
                       const Cost& cost = Cost{}) noexcept
    : _max_cache_size(config.cache_size),
      _max_small_queue_size(config.small_size),
      _max_main_queue_size(_max_cache_size - _max_small_queue_size),
      _evict(evict),
      _cost(cost) {
    SDB_ASSERT(_max_cache_size > _max_small_queue_size);
  }

  S3FIFOCache(const S3FIFOCache& other) = delete;
  S3FIFOCache& operator=(const S3FIFOCache& other) = delete;
  S3FIFOCache(S3FIFOCache&& other) noexcept = delete;
  S3FIFOCache& operator=(S3FIFOCache&& other) noexcept = delete;
  ~S3FIFOCache() noexcept = default;

  void Insert(T& entry) noexcept {
    while ((_small_queue_size + _main_queue_size) > _max_cache_size) {
      if (!Evict()) {
        break;
      }
    }

    auto& hook = static_cast<S3FIFOCacheHook&>(entry);
    if (GhostQueueContains(entry)) {
      hook._has_ghost_insertion_time = false;

      _main_fifo.push_back(entry);
      hook._location = S3FIFOCacheHook::Location::Main;
      _main_queue_size += _cost(entry);
    } else {
      _small_fifo.push_back(entry);
      hook._location = S3FIFOCacheHook::Location::Small;
      _small_queue_size += _cost(entry);
    }
  }

  void Remove(const T& entry) noexcept {
    const auto& hook = static_cast<const S3FIFOCacheHook&>(entry);

    if (hook.Evicted()) {
      return;
    }

    switch (hook._location) {
      case S3FIFOCacheHook::Location::Main:
        _main_queue_size -= _cost(entry);
        _main_fifo.erase(_main_fifo.iterator_to(entry));
        break;
      case S3FIFOCacheHook::Location::Small:
        _small_queue_size -= _cost(entry);
        _small_fifo.erase(_small_fifo.iterator_to(entry));
        break;
    }
  }

  bool GhostQueueContains(const T& entry) const noexcept {
    const auto& hook = static_cast<const S3FIFOCacheHook&>(entry);
    if (hook._has_ghost_insertion_time) {
      auto expires = hook._ghost_insertion_time + _max_main_queue_size;
      return expires > _ghost_queue_age;
    }
    return false;
  }

  bool Evict() noexcept {
    if (_small_queue_size > _max_small_queue_size) {
      if (EvictSmall()) {
        return true;
      }
    }
    return EvictMain();
  }

  struct Stat {
    size_t small_queue_size;
    size_t main_queue_size;
  };

  Stat GetStat() const noexcept {
    return {
      .small_queue_size = _small_queue_size,
      .main_queue_size = _main_queue_size,
    };
  }

 private:
  struct HookConverter {
    using hook_type = S3FIFOCacheHook::HookType;
    using hook_ptr = hook_type*;
    using const_hook_ptr = const hook_type*;
    using value_type = T;
    using pointer = value_type*;
    using const_pointer = const value_type*;

    static hook_ptr to_hook_ptr(value_type& value) {
      return &static_cast<S3FIFOCacheHook&>(value)._hook;
    }

    static const_hook_ptr to_hook_ptr(const value_type& value) {
      return &static_cast<const S3FIFOCacheHook&>(value)._hook;
    }

    static pointer to_value_ptr(hook_ptr hook) {
      return static_cast<T*>(boost::intrusive::get_parent_from_member(
        hook, &S3FIFOCacheHook::_hook));
    }

    static const_pointer to_value_ptr(const_hook_ptr hook) {
      return static_cast<const T*>(boost::intrusive::get_parent_from_member(
        hook, &S3FIFOCacheHook::_hook));
    }
  };

  using ListType =
    boost::intrusive::list<T, boost::intrusive::function_hook<HookConverter>,
                           boost::intrusive::constant_time_size<false>>;

  bool EvictSmall() noexcept {
    for (auto it = _small_fifo.begin(); it != _small_fifo.end();) {
      auto& entry = *it;
      auto& hook = static_cast<S3FIFOCacheHook&>(entry);
      SDB_ASSERT(hook._location == S3FIFOCacheHook::Location::Small);
      if (hook._freq > 1) {
        hook._freq = 0;

        it = _small_fifo.erase(it);
        const auto cost = _cost(entry);
        _small_queue_size -= cost;

        _main_fifo.push_back(entry);
        hook._location = S3FIFOCacheHook::Location::Main;
        _main_queue_size += cost;

        if (_main_queue_size > _max_main_queue_size) {
          if (EvictMain()) {
            return true;
          }
        }
      } else if (_evict(entry)) {
        it = _small_fifo.erase(it);
        const auto cost = _cost(entry);
        _small_queue_size -= cost;

        hook._ghost_insertion_time = _ghost_queue_age;
        hook._has_ghost_insertion_time = true;
        _ghost_queue_age += cost;
        return true;

      } else {
        ++it;
      }
    }
    return false;
  }

  bool EvictMain() noexcept {
    for (auto it = _main_fifo.begin(); it != _main_fifo.end();) {
      auto& entry = *it;
      auto& hook = static_cast<S3FIFOCacheHook&>(entry);
      SDB_ASSERT(hook._location == S3FIFOCacheHook::Location::Main);
      if (hook._freq > 0) {
        --hook._freq;
        it = _main_fifo.erase(it);
        _main_fifo.push_back(entry);
      } else if (_evict(entry)) {
        it = _main_fifo.erase(it);
        _main_queue_size -= _cost(entry);
        return true;
      } else {
        ++it;
      }
    }
    return false;
  }

  size_t _max_cache_size;
  size_t _max_small_queue_size;
  size_t _max_main_queue_size;

  Evictor _evict;
  Cost _cost;

  size_t _small_queue_size = 0;
  size_t _main_queue_size = 0;
  size_t _ghost_queue_age = 0;

  ListType _small_fifo;
  ListType _main_fifo;
};

}  // namespace sdb::containers
