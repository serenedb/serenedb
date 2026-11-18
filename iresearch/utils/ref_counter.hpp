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

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/functional/any_invocable.h>
#include <absl/synchronization/mutex.h>

#include <functional>
#include <memory>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/noncopyable.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {

template<typename Key,
         typename Hasher = typename absl::flat_hash_set<Key>::hasher,
         typename Equal = typename absl::flat_hash_set<Key, Hasher>::key_equal>
class RefCounter : public util::Noncopyable {
 public:
  using ref_t = std::shared_ptr<const Key>;
  using Released = absl::AnyInvocable<void(const Key&) const noexcept>;

  struct EqualTo : Equal {
    using is_transparent = void;

    using Equal::operator();

    template<typename T>
    bool operator()(const T& lhs, const ref_t& rhs) const noexcept {
      SDB_ASSERT(rhs);
      return Equal::operator()(lhs, *rhs);
    }

    template<typename T>
    bool operator()(const ref_t& lhs, const T& rhs) const noexcept {
      SDB_ASSERT(lhs);
      return Equal::operator()(*lhs, rhs);
    }

    bool operator()(const ref_t& lhs, const ref_t& rhs) const noexcept {
      SDB_ASSERT(lhs);
      SDB_ASSERT(rhs);
      return Equal::operator()(*lhs, *rhs);
    }
  };

  struct Hash : Hasher {
    using is_transparent = void;

    using Hasher::operator();

    size_t operator()(const ref_t& value) const noexcept {
      SDB_ASSERT(value);
      return Hasher::operator()(*value);
    }
  };

  template<typename T>
  ref_t add(T&& key) {
    absl::MutexLock lock{&_lock};
    const auto it = _refs.try_emplace(std::forward<T>(key)).first;
    if (auto ref = it->second.lock()) {
      return ref;
    }
    auto held = std::make_shared<const Held>(it->first, _hook);
    ref_t ref{held, &held->key};
    it->second = ref;
    return ref;
  }

  template<typename T>
  void Track(T&& key) {
    absl::MutexLock lock{&_lock};
    _refs.try_emplace(std::forward<T>(key));
  }

  template<typename T>
  bool remove(T&& key) {
    absl::MutexLock lock{&_lock};
    return _refs.erase(std::forward<T>(key)) > 0;
  }

  template<typename T>
  bool contains(T&& key) const noexcept {
    absl::MutexLock lock{&_lock};
    return _refs.contains(std::forward<T>(key));
  }

  template<typename T>
  size_t find(T&& key) const noexcept {
    absl::MutexLock lock{&_lock};
    const auto it = _refs.find(std::forward<T>(key));
    return it == _refs.end() ? 0 : it->second.use_count();
  }

  bool empty() const noexcept {
    absl::MutexLock lock{&_lock};
    return _refs.empty();
  }

  template<typename Visitor>
  bool visit(const Visitor& visitor) const {
    absl::MutexLock lock{&_lock};
    for (const auto& [key, held] : _refs) {
      if (!visitor(key, static_cast<size_t>(held.use_count()))) {
        return false;
      }
    }
    return true;
  }

  template<typename Forget>
  void ForgetUnused(const Forget& forget) {
    absl::MutexLock lock{&_lock};
    absl::erase_if(_refs, [&](const auto& entry) {
      return entry.second.expired() && forget(entry.first);
    });
  }

  void OnRelease(Released released) { _hook->Set(std::move(released)); }

 private:
  class Hook {
   public:
    void Set(Released released) {
      auto shared = std::make_shared<const Released>(std::move(released));
      absl::MutexLock lock{&_mutex};
      _released = std::move(shared);
    }

    void Notify(const Key& key) const noexcept {
      std::shared_ptr<const Released> released;
      {
        absl::MutexLock lock{&_mutex};
        released = _released;
      }
      if (released) {
        (*released)(key);
      }
    }

   private:
    mutable absl::Mutex _mutex;
    std::shared_ptr<const Released> _released ABSL_GUARDED_BY(_mutex);
  };

  struct Held {
    Held(const Key& key, std::shared_ptr<const Hook> hook)
      : key{key}, hook{std::move(hook)} {}
    Held(const Held&) = delete;
    Held& operator=(const Held&) = delete;
    ~Held() { hook->Notify(key); }

    Key key;
    std::shared_ptr<const Hook> hook;
  };

  mutable absl::Mutex _lock;
  absl::flat_hash_map<Key, std::weak_ptr<const Key>, Hasher, Equal> _refs
    ABSL_GUARDED_BY(_lock);
  std::shared_ptr<Hook> _hook = std::make_shared<Hook>();
};

}  // namespace irs
