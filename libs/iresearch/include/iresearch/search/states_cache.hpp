////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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

#include "basics/noncopyable.hpp"
#include "basics/resource_manager.hpp"
#include "basics/shared.hpp"

namespace irs {

struct SubReader;

class StatesCache {
 public:
  virtual ~StatesCache() = default;
};

// Generic cache for cached query states
// TODO: consider changing an API so that a sub_reader is indexable by an
//       integer. we can use a vector for lookup.
template<typename State>
class StatesCacheImpl : public StatesCache, private util::Noncopyable {
 public:
  using state_type = State;

  explicit StatesCacheImpl(IResourceManager& memory, size_t size)
    : _states{Alloc{memory}} {
    _states.reserve(size);
  }

  StatesCacheImpl(StatesCacheImpl&&) = default;
  StatesCacheImpl& operator=(StatesCacheImpl&&) = default;

  state_type& insert(const SubReader& segment) {
    auto result = _states.emplace(&segment, _states.get_allocator().Manager());
    return result.first->second;
  }

  void Merge(StatesCacheImpl&& other) {
    [[maybe_unused]] const size_t expected =
      _states.size() + other._states.size();
    _states.merge(other._states);
    SDB_ASSERT(_states.size() == expected);
  }

  const state_type* find(const SubReader& segment) const noexcept {
    const auto it = _states.find(&segment);
    return it != _states.end() ? &it->second : nullptr;
  }

  template<typename Pred>
  void erase_if(Pred pred) {
    absl::erase_if(_states, [&pred](const auto& v) { return pred(v.second); });
  }

  bool empty() const noexcept { return _states.empty(); }

 private:
  using Alloc =
    ManagedTypedAllocator<std::pair<const SubReader* const, state_type>>;

  using StatesMap = absl::flat_hash_map<
    const SubReader*, state_type,
    absl::container_internal::hash_default_hash<const SubReader*>,
    absl::container_internal::hash_default_eq<const SubReader*>, Alloc>;

  // FIXME use vector instead?
  StatesMap _states;
};

}  // namespace irs
