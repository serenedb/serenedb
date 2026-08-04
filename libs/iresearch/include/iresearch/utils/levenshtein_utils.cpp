////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include "levenshtein_utils.hpp"

#include <absl/container/flat_hash_map.h>
#include <absl/hash/hash.h>

#include "basics/bit_utils.hpp"
#include "basics/containers/bitset.hpp"
#include "basics/containers/small_vector.h"
#include "basics/shared.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs {
namespace {

constexpr uint32_t kInvalidState = 0;

// describes parametric transition related to a certain parametric state
struct Position {
  explicit Position(uint32_t offset = 0, uint8_t distance = 0,
                    bool transpose = false) noexcept
    : offset(offset), distance(distance), transpose(transpose) {}

  bool operator==(const Position& other) const noexcept = default;
  auto operator<=>(const Position& other) const noexcept = default;

  uint32_t offset = 0;     // parametric position offset
  uint8_t distance = 0;    // parametric position distance
  bool transpose = false;  // position is introduced by transposition
};

IRS_FORCE_INLINE constexpr uint32_t AbsDiff(uint32_t lhs,
                                            uint32_t rhs) noexcept {
  return lhs < rhs ? rhs - lhs : lhs - rhs;
}

// aka |rhs.offset-lhs.offset| < rhs.distance - lhs.distance
IRS_FORCE_INLINE constexpr bool Subsumes(const Position& lhs,
                                         const Position& rhs) noexcept {
  return (lhs.transpose | (!rhs.transpose))
           ? AbsDiff(lhs.offset, rhs.offset) + lhs.distance <= rhs.distance
           : AbsDiff(lhs.offset, rhs.offset) + lhs.distance < rhs.distance;
}

// describes parametric state of levenshtein automaton, basically a set of
// positions
class ParametricState {
 public:
  bool Emplace(uint32_t offset, uint8_t distance, bool transpose) {
    return Emplace(Position(offset, distance, transpose));
  }

  bool Emplace(const Position& new_pos) {
    if (absl::c_any_of(_positions,
                       [&](auto& pos) { return Subsumes(pos, new_pos); })) {
      // nothing to do
      return false;
    }

    if (!_positions.empty()) {
      for (auto begin = _positions.data(),
                end = _positions.data() + _positions.size();
           begin != end;) {
        if (Subsumes(new_pos, *begin)) {
          // removed positions subsumed by new_pos
          irstd::SwapRemove(_positions, begin);
          --end;
        } else {
          ++begin;
        }
      }
    }

    _positions.emplace_back(new_pos);
    return true;
  }

  auto begin() noexcept { return _positions.begin(); }

  auto end() noexcept { return _positions.end(); }

  auto begin() const noexcept { return _positions.begin(); }

  auto end() const noexcept { return _positions.end(); }

  bool Empty() const noexcept { return _positions.empty(); }

  void Clear() noexcept { return _positions.clear(); }

  bool operator==(const ParametricState& rhs) const = default;

  template<typename H>
  friend H AbslHashValue(H h, const ParametricState& state) {
    for (const auto& pos : state) {
      auto value = (static_cast<uint64_t>(pos.offset) << 32) |
                   (static_cast<uint64_t>(pos.distance) << 1) |
                   static_cast<uint64_t>(pos.transpose);
      h = H::combine(std::move(h), value);
    }
    return std::move(h);
  }

 private:
  std::vector<Position> _positions;
};

static_assert(std::is_nothrow_move_constructible_v<ParametricState>);
static_assert(std::is_nothrow_move_assignable_v<ParametricState>);

// container ensures uniquiness of 'parametric_state's
class ParametricStates {
 public:
  explicit ParametricStates(size_t capacity = 0) {
    if (capacity) {
      _states.reserve(capacity);
      _states_by_id.reserve(capacity);
    }
  }

  uint32_t Emplace(ParametricState&& state) {
    const auto res = _states.try_emplace(std::move(state), _states.size());

    if (res.second) {
      _states_by_id.emplace_back(&res.first->first);
    }

    SDB_ASSERT(_states.size() == _states_by_id.size());

    return res.first->second;
  }

  const ParametricState& operator[](size_t i) const noexcept {
    SDB_ASSERT(i < _states_by_id.size());
    return *_states_by_id[i];
  }

  size_t Size() const noexcept { return _states.size(); }

 private:
  absl::flat_hash_map<ParametricState, uint32_t> _states;
  std::vector<const ParametricState*> _states_by_id;
};

// adds elementary transition denoted by 'pos' to parametric state
// 'state' according to a specified characteristic vector 'chi'
void AddElementaryTransitions(ParametricState& state, const Position& pos,
                              const uint64_t chi, const uint8_t max_distance,
                              const bool with_transpositions) {
  if (CheckBit(chi, 0)) {
    // Situation 1: [i+1,e] subsumes { [i,e+1], [i+1,e+1], [i+1,e] }
    state.Emplace(pos.offset + 1, pos.distance, false);

    if (pos.transpose) {
      state.Emplace(pos.offset + 2, pos.distance, false);
    }
  }

  if (pos.distance < max_distance) {
    // Situation 2, 3 [i,e+1] - X is inserted before X[i+1]
    state.Emplace(pos.offset, pos.distance + 1, false);

    // Situation 2, 3 [i+1,e+1] - X[i+1] is substituted by X
    state.Emplace(pos.offset + 1, pos.distance + 1, false);

    // Situation 2, [i+j,e+j-1] - elements X[i+1:i+j-1] are deleted
    for (size_t j = 1, max = max_distance + 1 - pos.distance; j < max; ++j) {
      if (CheckBit(chi, j)) {
        state.Emplace(static_cast<uint32_t>(pos.offset + 1 + j),
                      pos.distance + j, false);
      }
    }

    if (with_transpositions && CheckBit(chi, 1)) {
      state.Emplace(pos.offset, pos.distance + 1, true);
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief adds elementary transitions for corresponding transition from
///        parametric state denoted by 'from' to parametric state 'to'
///        according to a specified characteristic vector 'cv'
////////////////////////////////////////////////////////////////////////////////
void AddTransition(ParametricState& to, const ParametricState& from,
                   const uint64_t cv, const uint8_t max_distance,
                   const bool with_transpositions) {
  to.Clear();
  for (const auto& pos : from) {
    SDB_ASSERT(pos.offset < BitsRequired<decltype(cv)>());
    const auto chi = cv >> pos.offset;
    AddElementaryTransitions(to, pos, chi, max_distance, with_transpositions);
  }

  absl::c_sort(to);
}

////////////////////////////////////////////////////////////////////////////////
/// @returns size of characteristic vector
////////////////////////////////////////////////////////////////////////////////
IRS_FORCE_INLINE constexpr uint32_t ChiSize(uint32_t max_distance) noexcept {
  return 2 * max_distance + 1;
}

////////////////////////////////////////////////////////////////////////////////
/// @returns max value of characteristic vector
////////////////////////////////////////////////////////////////////////////////
IRS_FORCE_INLINE constexpr uint64_t ChiMax(uint32_t chi_size) noexcept {
  return UINT64_C(1) << chi_size;
}

////////////////////////////////////////////////////////////////////////////////
/// @returns number of states in parametric description according to
///          specified options
////////////////////////////////////////////////////////////////////////////////
size_t PredictNumStates(uint8_t max_distance,
                        bool with_transpositions) noexcept {
  static constexpr size_t kNumStates[]{
    2,    2,     // distance 0
    6,    8,     // distance 1
    31,   68,    // distance 2
    197,  769,   // distance 3
    1354, 9628,  // distance 4
    9714, 0      // distance 5
  };

  const size_t idx = static_cast<size_t>(2) * max_distance +
                     static_cast<size_t>(with_transpositions);
  return idx < std::size(kNumStates) ? kNumStates[idx] : 0;
}

uint32_t Normalize(ParametricState& state) noexcept {
  const auto it = absl::c_min_element(
    state, [](const Position& lhs, const Position& rhs) noexcept {
      return lhs.offset < rhs.offset;
    });

  const auto min_offset = (it == state.end() ? 0 : it->offset);

  for (auto& pos : state) {
    pos.offset -= min_offset;
  }

  absl::c_sort(state);

  return min_offset;
}

uint32_t Distance(const ParametricState& state, const uint32_t max_distance,
                  const uint32_t offset) noexcept {
  SDB_ASSERT(max_distance < ParametricDescription::kMaxDistance);
  uint32_t min_dist = max_distance + 1;

  for (const auto& pos : state) {
    const uint32_t dist = pos.distance + AbsDiff(offset, pos.offset);

    if (dist < min_dist) {
      min_dist = dist;
    }
  }

  return min_dist;
}

// characteristic vector for a given character
template<typename Iterator>
uint64_t Chi(Iterator begin, Iterator end, uint32_t c) noexcept {
  uint64_t chi = 0;
  for (size_t i = 0; begin < end; ++begin, ++i) {
    chi |= uint64_t(c == *begin) << i;
  }
  return chi;
}

}  // namespace

ParametricDescription::ParametricDescription(
  std::vector<transition_t>&& transitions, std::vector<byte_type>&& distance,
  uint8_t max_distance) noexcept
  : _transitions(std::move(transitions)),
    _distance(std::move(distance)),
    _chi_size(ChiSize(max_distance)),
    _chi_max(ChiMax(_chi_size)),  // can't be 0
    _num_states(_transitions.size() / _chi_max),
    _max_distance(max_distance) {
  SDB_ASSERT(0 == (_transitions.size() % _chi_max));
  SDB_ASSERT(0 == (_distance.size() % _chi_size));
}

ParametricDescription MakeParametricDescription(uint8_t max_distance,
                                                bool with_transpositions) {
  if (max_distance > ParametricDescription::kMaxDistance) {
    // invalid parametric description
    return {};
  }

  // predict number of states for known cases
  const size_t num_states = PredictNumStates(max_distance, with_transpositions);

  // evaluate shape of characteristic vector
  const uint32_t chi_size = ChiSize(max_distance);
  const uint64_t chi_max = ChiMax(chi_size);

  ParametricStates states(num_states);
  std::vector<ParametricDescription::transition_t> transitions;
  if (num_states) {
    transitions.reserve(num_states * chi_max);
  }

  // empty state
  ParametricState to;
  size_t from_id = states.Emplace(std::move(to));
  SDB_ASSERT(to.Empty());  // TODO(mbkkt) wtf?

  // initial state
  to.Emplace(UINT32_C(0), UINT8_C(0), false);
  states.Emplace(std::move(to));
  SDB_ASSERT(to.Empty());  // TODO(mbkkt) wtf?

  for (; from_id != states.Size(); ++from_id) {
    for (uint64_t chi = 0; chi < chi_max; ++chi) {
      AddTransition(to, states[from_id], chi, max_distance,
                    with_transpositions);

      const auto min_offset = Normalize(to);
      const auto to_id = states.Emplace(std::move(to));

      transitions.emplace_back(to_id, min_offset);
    }

    // optimization for known cases
    if (num_states && transitions.size() == transitions.capacity()) {
      break;
    }
  }

  std::vector<byte_type> distance(states.Size() * chi_size);
  auto begin = distance.begin();
  for (size_t i = 0, size = states.Size(); i < size; ++i) {
    const auto& state = states[i];
    for (uint32_t offset = 0; offset < chi_size; ++offset, ++begin) {
      *begin = static_cast<byte_type>(Distance(state, max_distance, offset));
    }
  }

  return {std::move(transitions), std::move(distance), max_distance};
}

void ParametricDescription::Write(DataOutput& out) const {
  uint32_t last_state = 0;
  uint32_t last_offset = 0;

  out.WriteByte(this->max_distance());

  const auto transitions = this->transitions();
  out.WriteV64(transitions.size());
  for (auto& transition : transitions) {
    WriteZV32(out, transition.first - last_state);
    WriteZV32(out, transition.second - last_offset);
    last_state = transition.first;
    last_offset = transition.second;
  }

  const auto distances = this->distances();
  out.WriteV64(distances.size_bytes());
  out.WriteData(distances.data(), distances.size_bytes());
}

ParametricDescription ParametricDescription::Read(DataInput& in) {
  const uint8_t max_distance = in.ReadByte();

  const size_t tcount = in.ReadV64();
  std::vector<ParametricDescription::transition_t> transitions(tcount);

  uint32_t last_state = 0;
  uint32_t last_offset = 0;
  for (auto& transition : transitions) {
    transition.first = last_state + ReadZV32(in);
    transition.second = last_offset + ReadZV32(in);
    last_state = transition.first;
    last_offset = transition.second;
  }

  const size_t dcount = in.ReadV64();
  std::vector<byte_type> distances(dcount);
  in.ReadData(distances.data(), distances.size());

  return {std::move(transitions), std::move(distances), max_distance};
}

size_t EditDistance(const ParametricDescription& description,
                    const byte_type* lhs, size_t lhs_size, const byte_type* rhs,
                    size_t rhs_size) {
  SDB_ASSERT(description);

  sdb::containers::SmallVector<uint32_t, 16> lhs_chars;
  utf8_utils::ToUTF32<false>({lhs, lhs_size}, std::back_inserter(lhs_chars));

  size_t state = 1;   // current parametric state
  size_t offset = 0;  // current offset

  for (auto* rhs_end = rhs + rhs_size; rhs < rhs_end;) {
    const auto c = utf8_utils::ToChar32(rhs);

    const auto begin = lhs_chars.begin() + ptrdiff_t(offset);
    const auto end =
      lhs_chars.begin() + std::min(offset + description.chi_size(),
                                   static_cast<uint64_t>(lhs_chars.size()));
    const auto chi = Chi(begin, end, c);
    const auto& transition = description.transition(state, chi);

    if (kInvalidState == transition.first) {
      return description.max_distance() + 1;
    }

    state = transition.first;
    offset += transition.second;
  }

  return description.distance(state, lhs_chars.size() - offset);
}

bool EditDistance(size_t& distance, const ParametricDescription& description,
                  const byte_type* lhs, size_t lhs_size, const byte_type* rhs,
                  size_t rhs_size) {
  SDB_ASSERT(description);

  sdb::containers::SmallVector<uint32_t, 16> lhs_chars;
  if (!utf8_utils::ToUTF32<true>({lhs, lhs_size},
                                 std::back_inserter(lhs_chars))) {
    return false;
  }

  size_t state = 1;   // current parametric state
  size_t offset = 0;  // current offset

  for (auto* rhs_end = rhs + rhs_size; rhs < rhs_end;) {
    const auto c = utf8_utils::ToChar32(rhs, rhs_end);

    if (c == utf8_utils::kInvalidChar32) {
      return false;
    }

    const auto begin = lhs_chars.begin() + static_cast<ptrdiff_t>(offset);
    const auto end =
      lhs_chars.begin() + std::min(offset + description.chi_size(),
                                   static_cast<uint64_t>(lhs_chars.size()));
    const auto chi = Chi(begin, end, c);
    const auto& transition = description.transition(state, chi);

    if (kInvalidState == transition.first) {
      distance = description.max_distance() + 1;
      return true;
    }

    state = transition.first;
    offset += transition.second;
  }

  distance = description.distance(state, lhs_chars.size() - offset);
  return true;
}

}  // namespace irs
