////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#include <absl/algorithm/container.h>

#include <vector>

#include "basics/assert.h"
#include "basics/noncopyable.hpp"
#include "basics/shared.hpp"

namespace irs {

//////////////////////////////////////////////////////////////////////////////
/// @class fst_states_map
/// @brief helper class for deduplication of fst states while building
///        minimal acyclic subsequential transducer
//////////////////////////////////////////////////////////////////////////////
template<typename Fst, typename State, typename PushState, typename Hash,
         typename StateEq, typename Fst::StateId NoStateId>
class FstStatesMap : private util::Noncopyable {
 public:
  using fst_type = Fst;
  using state_type = State;
  using state_id = typename fst_type::StateId;
  using push_state = PushState;
  using hasher = Hash;
  using state_equal = StateEq;

  explicit FstStatesMap(size_t capacity = 16,
                        const push_state state_emplace = {},
                        const hasher& hash_function = {},
                        const state_equal& state_eq = {})
    : _hasher{hash_function},
      _state_eq{state_eq},
      _push_state{state_emplace},
      _states(capacity, NoStateId) {}

  state_id insert(const state_type& s, fst_type& fst) {
    const size_t mask = _states.size() - 1;
    size_t pos = _hasher(s, fst) % mask;
    for (;; ++pos, pos %= mask) {
      auto& bucket = _states[pos];

      if (NoStateId == bucket) {
        const state_id id = bucket = _push_state(s, fst);
        SDB_ASSERT(_hasher(s, fst) == _hasher(id, fst));
        ++_count;

        if (_count > 2 * _states.size() / 3) {
          rehash(fst);
        }

        return id;
      }

      if (_state_eq(s, bucket, fst)) {
        return bucket;
      }
    }
  }

  void reset() noexcept {
    _count = 0;
    absl::c_fill(_states, NoStateId);
  }

 private:
  void rehash(const fst_type& fst) {
    std::vector<state_id> states(_states.size() * 2, NoStateId);
    const size_t mask = states.size() - 1;
    for (const auto id : _states) {
      if (NoStateId == id) {
        continue;
      }

      size_t pos = _hasher(id, fst) % mask;
      for (;; ++pos, pos %= mask) {
        auto& bucket = states[pos];

        if (NoStateId == bucket) {
          bucket = id;
          break;
        }
      }
    }

    _states = std::move(states);
  }

  [[no_unique_address]] hasher _hasher;
  [[no_unique_address]] state_equal _state_eq;
  [[no_unique_address]] push_state _push_state;
  std::vector<state_id> _states;
  size_t _count{};
};

}  // namespace irs
