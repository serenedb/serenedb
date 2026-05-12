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

#include <algorithm>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/parent_from_member.hpp>
#include <concepts>

// S3-FIFO cache (Yang et al., SOSP'23). Three logical queues: small,
// main, ghost. Inserts land in small unless the entry has a recent
// ghost record, in which case they go straight to main. Evictions from
// small either promote frequent items to main or send infrequent items
// to ghost; evictions from main rotate frequent items to the back and
// drop infrequent items. The ghost queue is encoded inline in each
// hook's `_ghost_insertion_time`: an entry is "in ghost" while
// `insertion_time + max_main_size > current_age`. There is no separate
// ghost list -- the age counter advances by cost on each eviction from
// small, so ghost membership decays naturally.
//
// The container is intrusive and single-threaded by design; callers
// serialise their own access. `Evictor` is a noexcept predicate over
// `T&` returning whether an entry can actually be evicted right now
// (used by callers that pin entries during reads). `Cost` returns the
// per-entry size in whatever unit the caller chose for `cache_size`
// and `small_size`.

namespace sdb::containers::s3_fifo {

template<typename F, typename E>
concept CacheEvictor = requires(F func, E& entry) {
  requires noexcept(func(entry));
  { func(entry) } -> std::same_as<bool>;
};

template<typename F, typename E>
concept CacheCost = requires(F func, const E& entry) {
  requires noexcept(func(entry));
  { func(entry) } -> std::same_as<size_t>;
};

class CacheHook {
 public:
  CacheHook() noexcept = default;

  CacheHook(const CacheHook&) = default;

  CacheHook& operator=(const CacheHook&) = delete;
  CacheHook(CacheHook&&) noexcept = delete;
  CacheHook& operator=(CacheHook&&) noexcept = delete;
  ~CacheHook() noexcept = default;

  void touch() noexcept { _freq = std::min(_freq + 1, 3); }

  [[nodiscard]] bool evicted() const noexcept { return !_hook.is_linked(); }

 private:
  using HookType = boost::intrusive::list_member_hook<
    boost::intrusive::link_mode<boost::intrusive::safe_link>>;

  enum class Location : uint8_t {
    Main,
    Small,
  };

  template<typename T, CacheEvictor<T> Evictor, CacheCost<T> Cost>
    requires std::derived_from<T, CacheHook>
  friend class Cache;

  HookType _hook;
  uint64_t _ghost_insertion_time = 0;
  bool _has_ghost_insertion_time = false;
  uint8_t _freq = 0;
  Location _location = Location::Main;
};

struct DefaultCacheEvictor {
  bool operator()(auto&& /*entry*/) noexcept { return true; }
};

struct DefaultCacheCost {
  size_t operator()(const auto& /*entry*/) noexcept { return 1; }
};

template<typename T, CacheEvictor<T> Evictor = DefaultCacheEvictor,
         CacheCost<T> Cost = DefaultCacheCost>
  requires std::derived_from<T, CacheHook>
class Cache {
 public:
  struct Config {
    size_t cache_size;
    size_t small_size;
  };

  explicit Cache(Config config, const Evictor& evict = Evictor{},
                 const Cost& cost = Cost{}) noexcept;

  Cache(const Cache& other) = delete;
  Cache& operator=(const Cache& other) = delete;
  Cache(Cache&& other) noexcept = delete;
  Cache& operator=(Cache&& other) noexcept = delete;
  ~Cache() noexcept = default;

  void Insert(T& entry) noexcept;

  void Remove(const T& entry) noexcept;

  [[nodiscard]] bool GhostQueueContains(const T& entry) const noexcept;

  bool Evict() noexcept;

  void Clear() noexcept;

  struct Stat {
    size_t small_queue_size;
    size_t main_queue_size;
  };

  [[nodiscard]] Stat GetStat() const noexcept;

 private:
  struct HookConverter {
    using hook_type = CacheHook::HookType;
    using hook_ptr = hook_type*;
    using const_hook_ptr = const hook_type*;
    using value_type = T;
    using pointer = value_type*;
    using const_pointer = const value_type*;

    static hook_ptr to_hook_ptr(value_type& value) {
      return &static_cast<CacheHook&>(value)._hook;
    }

    static const_hook_ptr to_hook_ptr(const value_type& value) {
      return &static_cast<const CacheHook&>(value)._hook;
    }

    static pointer to_value_ptr(hook_ptr hook) {
      return static_cast<T*>(
        boost::intrusive::get_parent_from_member(hook, &CacheHook::_hook));
    }

    static const_pointer to_value_ptr(const_hook_ptr hook) {
      return static_cast<const T*>(
        boost::intrusive::get_parent_from_member(hook, &CacheHook::_hook));
    }
  };

  using ListType =
    boost::intrusive::list<T, boost::intrusive::function_hook<HookConverter>,
                           boost::intrusive::constant_time_size<false>>;

  bool EvictSmall() noexcept;
  bool EvictMain() noexcept;

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

template<typename T, CacheEvictor<T> Evictor, CacheCost<T> Cost>
  requires std::derived_from<T, CacheHook>
Cache<T, Evictor, Cost>::Cache(Config config, const Evictor& evict,
                               const Cost& cost) noexcept
  : _max_cache_size(config.cache_size),
    _max_small_queue_size(config.small_size),
    _max_main_queue_size(_max_cache_size - _max_small_queue_size),
    _evict(evict),
    _cost(cost) {
  assert(_max_cache_size > _max_small_queue_size);
}

template<typename T, CacheEvictor<T> Evictor, CacheCost<T> Cost>
  requires std::derived_from<T, CacheHook>
void Cache<T, Evictor, Cost>::Insert(T& entry) noexcept {
  while ((_small_queue_size + _main_queue_size) > _max_cache_size) {
    if (!Evict()) {
      break;
    }
  }

  auto& hook = static_cast<CacheHook&>(entry);
  if (GhostQueueContains(entry)) {
    hook._has_ghost_insertion_time = false;

    _main_fifo.push_back(entry);
    hook._location = CacheHook::Location::Main;
    _main_queue_size += _cost(entry);
  } else {
    _small_fifo.push_back(entry);
    hook._location = CacheHook::Location::Small;
    _small_queue_size += _cost(entry);
  }
}

template<typename T, CacheEvictor<T> Evictor, CacheCost<T> Cost>
  requires std::derived_from<T, CacheHook>
void Cache<T, Evictor, Cost>::Remove(const T& entry) noexcept {
  const auto& hook = static_cast<const CacheHook&>(entry);

  if (hook.evicted()) {
    return;
  }

  switch (hook._location) {
    case CacheHook::Location::Main:
      _main_queue_size -= _cost(entry);
      _main_fifo.erase(_main_fifo.iterator_to(entry));
      break;
    case CacheHook::Location::Small:
      _small_queue_size -= _cost(entry);
      _small_fifo.erase(_small_fifo.iterator_to(entry));
      break;
  }
}

template<typename T, CacheEvictor<T> Evictor, CacheCost<T> Cost>
  requires std::derived_from<T, CacheHook>
void Cache<T, Evictor, Cost>::Clear() noexcept {
  _small_fifo.clear();
  _main_fifo.clear();
  _small_queue_size = 0;
  _main_queue_size = 0;
  _ghost_queue_age = 0;
}

template<typename T, CacheEvictor<T> Evictor, CacheCost<T> Cost>
  requires std::derived_from<T, CacheHook>
bool Cache<T, Evictor, Cost>::Evict() noexcept {
  if (_small_queue_size > _max_small_queue_size) {
    if (EvictSmall()) {
      return true;
    }
  }
  return EvictMain();
}

template<typename T, CacheEvictor<T> Evictor, CacheCost<T> Cost>
  requires std::derived_from<T, CacheHook>
struct Cache<T, Evictor, Cost>::Stat Cache<T, Evictor, Cost>::GetStat()
  const noexcept {
  return {
    .small_queue_size = _small_queue_size,
    .main_queue_size = _main_queue_size,
  };
}

template<typename T, CacheEvictor<T> Evictor, CacheCost<T> Cost>
  requires std::derived_from<T, CacheHook>
bool Cache<T, Evictor, Cost>::GhostQueueContains(
  const T& entry) const noexcept {
  const auto& hook = static_cast<const CacheHook&>(entry);
  if (hook._has_ghost_insertion_time) {
    auto expires = hook._ghost_insertion_time + _max_main_queue_size;
    return expires > _ghost_queue_age;
  }
  return false;
}

template<typename T, CacheEvictor<T> Evictor, CacheCost<T> Cost>
  requires std::derived_from<T, CacheHook>
bool Cache<T, Evictor, Cost>::EvictSmall() noexcept {
  for (auto it = _small_fifo.begin(); it != _small_fifo.end();) {
    auto& entry = *it;
    auto& hook = static_cast<CacheHook&>(entry);
    assert(hook._location == CacheHook::Location::Small);
    if (hook._freq > 1) {
      hook._freq = 0;

      it = _small_fifo.erase(it);
      const auto cost = _cost(entry);
      _small_queue_size -= cost;

      _main_fifo.push_back(entry);
      hook._location = CacheHook::Location::Main;
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

template<typename T, CacheEvictor<T> Evictor, CacheCost<T> Cost>
  requires std::derived_from<T, CacheHook>
bool Cache<T, Evictor, Cost>::EvictMain() noexcept {
  for (auto it = _main_fifo.begin(); it != _main_fifo.end();) {
    auto& entry = *it;
    auto& hook = static_cast<CacheHook&>(entry);
    assert(hook._location == CacheHook::Location::Main);
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

}  // namespace sdb::containers::s3_fifo
