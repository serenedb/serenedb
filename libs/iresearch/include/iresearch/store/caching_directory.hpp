////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/container/flat_hash_map.h>

#include <mutex>  // unique_lock

#include "basics/shared.hpp"
#include "iresearch/store/directory.hpp"

namespace irs {

template<typename Value>
class CachingHelper {
 public:
  explicit CachingHelper(size_t max_count) noexcept : _max_count{max_count} {}

  template<typename Visitor>
  bool Visit(std::string_view key, Visitor&& visitor) const noexcept {
    const size_t key_hash = _cache.hash_ref()(key);

    {
      absl::ReaderMutexLock lock{&_mutex};
      if (auto it = _cache.find(key, key_hash); it != _cache.end()) {
        return visitor(it->second);
      }
    }

    return false;
  }

  template<typename Constructor>
  bool Put(std::string_view key, Constructor&& ctor) noexcept {
    bool is_new = false;

    if (std::unique_lock lock{_mutex}; _cache.size() < _max_count) {
      try {
        _cache.lazy_emplace(key, [&](const auto& map_ctor) {
          is_new = true;
          map_ctor(key, ctor());
        });
      } catch (...) {
      }
    }

    return is_new;
  }

  void Remove(std::string_view key) noexcept {
    const size_t key_hash = _cache.hash_ref()(key);

    std::lock_guard lock{_mutex};
    if (const auto it = _cache.find(key, key_hash); it != _cache.end()) {
      _cache.erase(it);
    }
  }

  void Rename(std::string_view src, std::string_view dst) noexcept {
    const auto src_hash = _cache.hash_ref()(src);

    std::lock_guard lock{_mutex};
    if (auto src_it = _cache.find(src, src_hash); src_it != _cache.end()) {
      auto tmp = std::move(src_it->second);
      _cache.erase(src_it);
      try {
        SDB_ASSERT(!_cache.contains(dst));
        _cache[dst] = std::move(tmp);
      } catch (...) {
      }
    }
  }

  void Clear() const {
    std::lock_guard lock{_mutex};
    _cache.clear();
  }

  size_t Count() const noexcept {
    std::lock_guard lock{_mutex};
    return _cache.size();
  }

  size_t MaxCount() const noexcept { return _max_count; }

 private:
  mutable absl::Mutex _mutex;
  mutable absl::flat_hash_map<std::string, Value> _cache;
  size_t _max_count;
};

template<typename Impl, typename Value>
class CachingDirectoryBase : public Impl {
 public:
  using ImplType = Impl;

  template<typename... Args>
  explicit CachingDirectoryBase(size_t max_count, Args&&... args)
    : Impl{std::forward<Args>(args)...}, _cache{max_count} {}

  bool remove(std::string_view name) noexcept final {
#ifdef _WIN32
    cache_.Remove(name);  // On Windows it's important to first close the handle
    return Impl::remove(name);
#else
    if (Impl::remove(name)) {
      _cache.Remove(name);
      return true;
    }
    return false;
#endif
  }

  bool rename(std::string_view src, std::string_view dst) noexcept final {
#ifdef _WIN32
    cache_.Remove(src);  // On Windows it's impossible to move opened file
    return Impl::rename(src, dst);
#else
    if (Impl::rename(src, dst)) {
      _cache.Rename(src, dst);
      return true;
    }
    return false;
#endif
  }

  bool exists(bool& result, std::string_view name) const noexcept final {
    if (_cache.Visit(name, [&](auto&) noexcept {
          result = true;
          return true;
        })) {
      return true;
    }

    return Impl::exists(result, name);
  }

  const auto& Cache() const noexcept { return _cache; }

 protected:
  mutable CachingHelper<Value> _cache;
};

}  // namespace irs
