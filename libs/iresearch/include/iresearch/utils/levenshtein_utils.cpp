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

#include "automaton_utils.hpp"
#include "basics/bit_utils.hpp"
#include "basics/containers/bitset.hpp"
#include "basics/misc.hpp"
#include "basics/shared.hpp"
#include "basics/utf8_utils.hpp"
#include "hash_utils.hpp"
#include "iresearch/store/store_utils.hpp"

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

IRS_FORCE_INLINE uint32_t AbsDiff(uint32_t lhs, uint32_t rhs) noexcept {
  return lhs < rhs ? rhs - lhs : lhs - rhs;
}

// aka |rhs.offset-lhs.offset| < rhs.distance - lhs.distance
IRS_FORCE_INLINE bool Subsumes(const Position& lhs,
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
  if (irs::CheckBit<0>(chi)) {
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
      if (irs::CheckBit(chi, j)) {
        state.Emplace(static_cast<uint32_t>(pos.offset + 1 + j),
                      pos.distance + j, false);
      }
    }

    if (with_transpositions && irs::CheckBit<1>(chi)) {
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
    SDB_ASSERT(pos.offset < irs::BitsRequired<decltype(cv)>());
    const auto chi = cv >> pos.offset;
    AddElementaryTransitions(to, pos, chi, max_distance, with_transpositions);
  }

  absl::c_sort(to);
}

////////////////////////////////////////////////////////////////////////////////
/// @returns size of characteristic vector
////////////////////////////////////////////////////////////////////////////////
IRS_FORCE_INLINE uint32_t ChiSize(uint32_t max_distance) noexcept {
  return 2 * max_distance + 1;
}

////////////////////////////////////////////////////////////////////////////////
/// @returns max value of characteristic vector
////////////////////////////////////////////////////////////////////////////////
IRS_FORCE_INLINE uint64_t ChiMax(uint32_t chi_size) noexcept {
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

struct Character {
  bitset chi;  // characteristic vector
  byte_type utf8[utf8_utils::kMaxCharSize]{};
  size_t size{};
  uint32_t cp{};  // utf8 code point
};

// characteristic vectors for a specified word
std::vector<Character> MakeAlphabet(bytes_view word, size_t& utf8_size) {
  sdb::containers::SmallVector<uint32_t, 16> chars;
  utf8_utils::ToUTF32<false>(word, std::back_inserter(chars));
  utf8_size = chars.size();

  absl::c_sort(chars);
  auto cbegin = chars.begin();
  auto cend = std::unique(cbegin, chars.end());  // no need to erase here

  std::vector<Character> alphabet(
    1 + static_cast<size_t>(std::distance(cbegin, cend)));  // +1 for rho
  auto begin = alphabet.begin();

  // ensure we have enough capacity
  const auto capacity = utf8_size + BitsRequired<bitset::word_t>();

  begin->cp = std::numeric_limits<uint32_t>::max();
  begin->chi.reset(capacity);
  ++begin;

  for (; cbegin != cend; ++cbegin, ++begin) {
    const auto c = *cbegin;

    // set code point
    begin->cp = c;

    // set utf8 representation
    begin->size = utf8_utils::FromChar32(c, begin->utf8);

    // evaluate characteristic vector
    auto& chi = begin->chi;
    chi.reset(capacity);
    auto utf8_begin = word.data();
    for (size_t i = 0; i < utf8_size; ++i) {
      chi.reset(i, c == utf8_utils::ToChar32(utf8_begin));
    }
    SDB_ASSERT(utf8_begin == word.data() + word.size());
  }

  return alphabet;
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

// characteristic vector by a given offset and mask
uint64_t Chi(const bitset& bs, size_t offset, uint64_t mask) noexcept {
  auto word = bitset::word(offset);

  auto align = offset - bitset::bit_offset(word);
  if (!align) {
    return bs[word] & mask;
  }

  const auto lhs = bs[word] >> align;
  const auto rhs = bs[word + 1] << (BitsRequired<bitset::word_t>() - align);
  return (lhs | rhs) & mask;
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
  out.WriteBytes(distances.data(), distances.size_bytes());
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
  in.ReadBytes(distances.data(), distances.size());

  return {std::move(transitions), std::move(distances), max_distance};
}

automaton MakeLevenshteinAutomaton(const ParametricDescription& description,
                                   bytes_view prefix, bytes_view target) {
  SDB_ASSERT(description);

  struct State {
    State(size_t offset, uint32_t state_id, automaton::StateId from) noexcept
      : offset(offset), state_id(state_id), from(from) {}

    size_t offset;            // state offset
    uint32_t state_id;        // corresponding parametric state
    automaton::StateId from;  // automaton state
  };

  size_t utf8_size;
  const auto alphabet = MakeAlphabet(target, utf8_size);
  const auto num_offsets = 1 + utf8_size;
  const uint64_t mask = (UINT64_C(1) << description.chi_size()) - 1;

  // transitions table of resulting automaton
  std::vector<automaton::StateId> transitions(description.size() * num_offsets,
                                              fst::kNoStateId);

  automaton a;
  a.ReserveStates(transitions.size());

  // terminal state without outbound transitions
  const auto invalid_state = a.AddState();
  SDB_ASSERT(kInvalidState == invalid_state);
  IRS_IGNORE(invalid_state);

  // initial state
  auto start_state = a.AddState();
  a.SetStart(start_state);

  auto begin = prefix.data();
  auto end = prefix.data() + prefix.size();
  decltype(start_state) to;
  for (; begin != end;) {
    const byte_type* next = utf8_utils::Next(begin, end);
    to = a.AddState();
    auto dist = std::distance(begin, next);
    irs::Utf8EmplaceArc(a, start_state, bytes_view(begin, dist), to);
    start_state = to;
    begin = next;
  }

  // check if start state is final
  if (const auto distance = description.distance(1, utf8_size);
      distance <= description.max_distance()) {
    a.SetFinal(start_state, {true, distance});
  }

  // state stack
  std::vector<State> stack;
  stack.emplace_back(
    0, 1,
    start_state);  // 0 offset, 1st parametric state, initial automaton state

  std::vector<std::pair<bytes_view, automaton::StateId>> arcs;
  arcs.resize(utf8_size);  // allocate space for max possible number of arcs

  for (Utf8TransitionsBuilder builder; !stack.empty();) {
    const auto state = stack.back();
    stack.pop_back();
    arcs.clear();

    automaton::StateId default_state =
      fst::kNoStateId;  // destination of rho transition if exist
    bool ascii = true;  // ascii only input

    for (auto& entry : alphabet) {
      const auto chi = Chi(entry.chi, state.offset, mask);
      auto& transition = description.transition(state.state_id, chi);

      const size_t offset =
        transition.first ? transition.second + state.offset : 0;
      SDB_ASSERT(transition.first * num_offsets + offset < transitions.size());
      auto& to = transitions[transition.first * num_offsets + offset];

      if (kInvalidState == transition.first) {
        to = kInvalidState;
      } else if (fst::kNoStateId == to) {
        to = a.AddState();

        if (const auto distance =
              description.distance(transition.first, utf8_size - offset);
            distance <= description.max_distance()) {
          a.SetFinal(to, {true, distance});
        }

        stack.emplace_back(offset, transition.first, to);
      }

      if (chi && to != default_state) {
        arcs.emplace_back(bytes_view(entry.utf8, entry.size), to);
        ascii &= (entry.size == 1);
      } else {
        SDB_ASSERT(fst::kNoStateId == default_state || to == default_state);
        default_state = to;
      }
    }

    if (kInvalidState == default_state && arcs.empty()) {
      // optimization for invalid terminal state
      a.EmplaceArc(state.from, RangeLabel::From(0, 255), kInvalidState);
    } else if (kInvalidState == default_state && ascii &&
               !a.Final(state.from)) {
      // optimization for ascii only input without default state and weight
      for (auto& arc : arcs) {
        SDB_ASSERT(1 == arc.first.size());
        a.EmplaceArc(state.from, RangeLabel::From(arc.first.front()),
                     arc.second);
      }
    } else {
      builder.Insert(a, state.from, default_state, arcs.begin(), arcs.end());
    }
  }

#ifdef SDB_DEV
  // ensure resulting automaton is sorted and deterministic
  static constexpr auto kExpectedProperties =
    fst::kIDeterministic | fst::kILabelSorted | fst::kOLabelSorted |
    fst::kAcceptor | fst::kUnweighted;
  SDB_ASSERT(kExpectedProperties == a.Properties(kExpectedProperties, true));

  // ensure invalid state has no outbound transitions
  SDB_ASSERT(0 == a.NumArcs(kInvalidState));
#endif

  return a;
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
