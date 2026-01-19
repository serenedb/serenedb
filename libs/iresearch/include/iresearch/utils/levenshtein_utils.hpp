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

#pragma once

#include <algorithm>
#include <numeric>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "iresearch/utils/automaton_decl.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class DataOutput;
struct DataInput;

template<typename T, size_t SubstCost = 1>
inline size_t EditDistance(const T* lhs, size_t lhs_size, const T* rhs,
                           size_t rhs_size) {
  SDB_ASSERT(lhs || !lhs_size);
  SDB_ASSERT(rhs || !rhs_size);

  if (lhs_size > rhs_size) {
    std::swap(lhs, rhs);
    std::swap(lhs_size, rhs_size);
  }

  std::vector<size_t> cost(2 * (lhs_size + 1));

  auto current = cost.begin();
  auto next = cost.begin() + cost.size() / 2;
  std::iota(current, next, 0);

  for (size_t j = 1; j <= rhs_size; ++j) {
    next[0] = j;
    for (size_t i = 1; i <= lhs_size; ++i) {
      next[i] = std::min({
        next[i - 1] + 1,  // deletion
        current[i] + 1,   // insertion
        current[i - 1] +
          (lhs[i - 1] == rhs[j - 1] ? 0 : SubstCost)  // substitution
      });
    }
    std::swap(next, current);
  }

  return current[lhs_size];
}

/// @brief evaluates edit distance between the specified words
/// @param lhs string to compare
/// @param rhs string to compare
/// @returns edit distance
template<typename Char>
inline size_t EditDistance(basic_string_view<Char> lhs,
                           basic_string_view<Char> rhs) {
  return EditDistance(lhs.data(), lhs.size(), rhs.data(), rhs.size());
}

// Implementation of the algorithm of building Levenshtein automaton
// by Klaus Schulz, Stoyan Mihov described in
//   http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.16.652

/// @class parametric_description
/// @brief Parametric description of automaton for a particular edit distance.
///        Once created the description can be used for generating DFAs
///        accepting strings at edit distance less or equal than distance
///        specified in description.
class ParametricDescription {
 public:
  /// @brief describes trasition among parametric states
  ///        first - desination parametric state id
  ///        second - offset
  typedef std::pair<uint32_t, uint32_t> transition_t;

  /// @brief theoretically max possible distance we can evaluate, not really
  ///        feasible due to exponential growth of parametric description size
  static constexpr uint8_t kMaxDistance = 31;

  /// @brief creates default "invalid" description
  ParametricDescription() = default;

  /// @brief creates description
  ParametricDescription(std::vector<transition_t>&& transitions,
                        std::vector<byte_type>&& distance,
                        uint8_t max_distance) noexcept;

  /// @return transition from 'from' state matching a provided
  ///         characteristic vector
  const transition_t& transition(size_t from, uint64_t chi) const noexcept {
    SDB_ASSERT(from * _chi_max + chi < _transitions.size());
    return _transitions[from * _chi_max + chi];
  }

  /// @return parametric transitions table
  std::span<const transition_t> transitions() const noexcept {
    return _transitions;
  }

  /// @return edit distance of parametric state at a specified offset
  byte_type distance(size_t state, size_t offset) const noexcept {
    if (offset >= _chi_size) {
      return _max_distance + 1;
    }

    SDB_ASSERT(state * _chi_size + offset < _distance.size());
    return _distance[state * _chi_size + offset];
  }

  /// @return range of edit distances for all parametric states
  std::span<const byte_type> distances() const noexcept { return _distance; }

  /// @return number of states in parametric description
  size_t size() const noexcept { return _num_states; }

  /// @return length of characteristic vector
  /// @note 2*max_distance_ + 1
  uint64_t chi_size() const noexcept { return _chi_size; }

  /// @return max value of characteristic vector
  /// @note 1 << chi_size_
  uint64_t chi_max() const noexcept { return _chi_max; }

  /// @return the edit distance for which this description was built
  uint8_t max_distance() const noexcept { return _max_distance; }

  /// @return true if description is valid, false otherwise
  explicit operator bool() const noexcept { return _chi_size > 0; }

  /// @return true if description is equal to a specified one
  bool operator==(const ParametricDescription& rhs) const noexcept {
    // all other members are derived
    return _transitions == rhs._transitions && _distance == rhs._distance &&
           _max_distance == rhs._max_distance;
  }

  /// @brief writes parametric description to a specified output stream
  /// @param description parametric description to write
  /// @param out output stream
  void Write(DataOutput& out) const;

  /// @brief read parametric description from a specified input stream
  /// @param in input stream
  /// @returns the read parametric description
  static ParametricDescription Read(DataInput& in);

 private:
  std::vector<transition_t> _transitions;  // transition table
  std::vector<byte_type> _distance;        // distances per state and offset
  uint32_t _chi_size{};                    // 2*max_distance_+1
  uint64_t _chi_max{};                     // 1 << chi_size
  size_t _num_states{};                    // number of parametric states
  uint8_t _max_distance{};                 // max allowed distance
};

/// @brief builds parametric description of Levenshtein automaton
/// @param max_distance maximum allowed distance
/// @param with_transposition count transpositions
/// @returns parametric description of Levenshtein automaton for supplied args
ParametricDescription MakeParametricDescription(uint8_t max_distance,
                                                bool with_transposition);

/// @brief instantiates DFA based on provided parametric description and target
/// @param description parametric description
/// @param target valid UTF-8 encoded string
/// @returns DFA
/// @note if 'target' isn't a valid UTF-8 sequence, behaviour is undefined
automaton MakeLevenshteinAutomaton(const ParametricDescription& description,
                                   bytes_view prefix, bytes_view target);

/// @brief evaluates edit distance between the specified words up to
///        specified in description.max_distance
/// @param description parametric description
/// @param lhs string to compare (utf8 encoded)
/// @param lhs_size size of the string to comprare
/// @param rhs string to compare (utf8 encoded)
/// @param rhs_size size of the string to comprare
/// @returns edit_distance up to specified in description.max_distance
/// @note accepts only valid descriptions, calling function with
///       invalid description is undefined behaviour
size_t EditDistance(const ParametricDescription& description,
                    const byte_type* lhs, size_t lhs_size, const byte_type* rhs,
                    size_t rhs_size);

/// @brief evaluates edit distance between the specified words up to
///        specified in description.max_distance
/// @param description parametric description
/// @param lhs string to compare (utf8 encoded)
/// @param rhs string to compare (utf8 encoded)
/// @returns edit_distance up to specified in description.max_distance
/// @note accepts only valid descriptions, calling function with
///       invalid description is undefined behaviour
inline size_t EditDistance(const ParametricDescription& description,
                           bytes_view lhs, bytes_view rhs) {
  return EditDistance(description, lhs.data(), lhs.size(), rhs.data(),
                      rhs.size());
}

/// @brief evaluates edit distance between the specified words up to
///        specified in description.max_distance.
/// @param evaluated edit distance
/// @param description parametric description
/// @param lhs string to compare (utf8 encoded)
/// @param lhs_size size of the string to comprare
/// @param rhs string to compare (utf8 encoded)
/// @param rhs_size size of the string to comprare
/// @returns true if both lhs_string and rhs_strign are valid UTF-8 sequences,
///          false - otherwise
/// @note accepts only valid descriptions, calling function with
///       invalid description is undefined behaviour
bool EditDistance(size_t& distance, const ParametricDescription& description,
                  const byte_type* lhs, size_t lhs_size, const byte_type* rhs,
                  size_t rhs_size);

/// @brief evaluates edit distance between the specified words up to
///        specified in description.max_distance
/// @param description parametric description
/// @param lhs string to compare (utf8 encoded)
/// @param rhs string to compare (utf8 encoded)
/// @returns true if both lhs_string and rhs_strign are valid UTF-8 sequences,
///          false - otherwise
/// @note accepts only valid descriptions, calling function with
///       invalid description is undefined behaviour
inline bool EditDistance(size_t& distance,
                         const ParametricDescription& description,
                         bytes_view lhs, bytes_view rhs) {
  return EditDistance(distance, description, lhs.data(), lhs.size(), rhs.data(),
                      rhs.size());
}

}  // namespace irs
