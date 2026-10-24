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

#include <absl/base/thread_annotations.h>
#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_set.h>
#include <absl/synchronization/mutex.h>

#include <functional>
#include <memory>
#include <vector>

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
    const auto settled = [&]() ABSL_NO_THREAD_SAFETY_ANALYSIS {
      return !_removing.contains(key);
    };
    _lock.Await(absl::Condition(&settled));

    auto it = _refs.lazy_emplace(key, [&](const auto& ctor) {
      ctor(std::make_shared<const Key>(std::forward<T>(key)));
    });

    return *it;
  }

  template<typename T>
  bool remove(T&& key) {
    absl::MutexLock lock{&_lock};
    return _refs.erase(std::forward<T>(key)) > 0;
  }

  template<typename Remove>
  auto RemoveUnused(const Remove& remove) {
    std::vector<Key> claimed;
    {
      absl::MutexLock lock{&_lock};
      for (const auto& ref : _refs) {
        SDB_ASSERT(ref);
        if (ref.use_count() == 1 && !_removing.contains(*ref)) {
          claimed.push_back(*ref);
        }
      }
      _removing.insert(claimed.begin(), claimed.end());
    }
    auto removed = claimed;
    bool finished = false;
    absl::Cleanup release = [&] noexcept {
      absl::MutexLock lock{&_lock};
      if (finished) {
        for (const auto& key : removed) {
          _refs.erase(key);
        }
      }
      for (const auto& key : claimed) {
        _removing.erase(key);
      }
    };
    auto result = remove(removed);
    finished = true;
    return result;
  }

 private:
  absl::Mutex _lock;
  absl::flat_hash_set<ref_t, Hash, EqualTo> _refs ABSL_GUARDED_BY(_lock);
  absl::flat_hash_set<Key, Hasher, Equal> _removing ABSL_GUARDED_BY(_lock);
};

}  // namespace irs
