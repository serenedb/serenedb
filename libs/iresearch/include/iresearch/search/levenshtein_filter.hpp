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

#include <memory>

#include "filter.hpp"
#include "iresearch/utils/automaton.hpp"
#include "iresearch/utils/levenshtein_default_pdp.hpp"
#include "iresearch/utils/levenshtein_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByEditDistance;
class LevenshteinAutomatonFilter;
struct FilterVisitor;
struct CompiledAcceptor;
struct PayAttr;

struct ByEditDistanceAllOptions {
  //////////////////////////////////////////////////////////////////////////////
  /// @brief parametric description provider
  //////////////////////////////////////////////////////////////////////////////
  using pdp_f = const ParametricDescription& (*)(uint8_t, bool);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief target value
  //////////////////////////////////////////////////////////////////////////////
  bstring term;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief match this number of characters from the beginning of the
  ///        target regardless of edit distance
  //////////////////////////////////////////////////////////////////////////////
  bstring prefix;

  //////////////////////////////////////////////////////////////////////////////
  /// @returns current parametric description provider, nullptr - use default
  /// @note since creation of parametric description is expensive operation,
  ///       especially for distances > 4, expert users may want to set its own
  ///       providers
  //////////////////////////////////////////////////////////////////////////////
  pdp_f provider{};

  //////////////////////////////////////////////////////////////////////////////
  /// @returns maximum allowed edit distance
  //////////////////////////////////////////////////////////////////////////////
  uint8_t max_distance{0};

  //////////////////////////////////////////////////////////////////////////////
  /// @brief consider transpositions as an atomic change
  //////////////////////////////////////////////////////////////////////////////
  bool with_transpositions{false};
};

////////////////////////////////////////////////////////////////////////////////
/// @struct ByEditDistanceOptions
/// @brief options for levenshtein filter
////////////////////////////////////////////////////////////////////////////////
struct ByEditDistanceOptions : ByEditDistanceAllOptions {
  using FilterType = ByEditDistance;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief maximum number of the most relevant terms to consider for scoring
  //////////////////////////////////////////////////////////////////////////////
  size_t max_terms{};

  bool operator==(const ByEditDistanceOptions& rhs) const noexcept {
    return term == rhs.term && max_distance == rhs.max_distance &&
           with_transpositions == rhs.with_transpositions &&
           max_terms == rhs.max_terms;
  }
};

template<typename Invalid, typename Term, typename Levenshtein>
auto ExecuteLevenshtein(uint8_t max_distance,
                        ByEditDistanceAllOptions::pdp_f provider,
                        bool with_transpositions, const bytes_view prefix,
                        const bytes_view target, Invalid&& inv, Term&& t,
                        Levenshtein&& lev) {
  if (!provider) {
    provider = &DefaultPDP;
  }

  if (0 == max_distance) {
    return t();
  }

  SDB_ASSERT(provider);
  const auto& d = (*provider)(max_distance, with_transpositions);

  if (!d) {
    return inv();
  }

  return lev(d, prefix, target);
}

////////////////////////////////////////////////////////////////////////////////
/// @class by_edit_distance
/// @brief user-side levenstein filter
////////////////////////////////////////////////////////////////////////////////
class ByEditDistance final : public FilterWithField<ByEditDistanceOptions> {
 public:
  static field_visitor visitor(const ByEditDistanceAllOptions& options);

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;
};

struct LevenshteinAutomatonOptions {
  using FilterType = LevenshteinAutomatonFilter;

  bstring target;
  std::shared_ptr<const CompiledAcceptor> compiled;
  uint32_t utf8_target_size{1};
  byte_type no_distance{1};
  size_t max_terms{};

  LevenshteinAutomatonOptions() = default;
  LevenshteinAutomatonOptions(const ParametricDescription& d, bytes_view prefix,
                              bytes_view term, size_t max_terms);

  bool operator==(const LevenshteinAutomatonOptions& rhs) const noexcept {
    return target == rhs.target && utf8_target_size == rhs.utf8_target_size &&
           no_distance == rhs.no_distance && max_terms == rhs.max_terms;
  }
};

class LevenshteinIterator {
 public:
  LevenshteinIterator(const TermReader& reader,
                      const LevenshteinAutomatonOptions& options);

  LevenshteinIterator(SeekTermIterator::ptr&& impl, byte_type no_distance,
                      uint32_t target_size);

  SeekTermIterator& GetImpl() noexcept { return *_impl; }
  score_t Boost() const noexcept { return _boost; }
  bytes_view value() const noexcept { return _impl->value(); }
  bool next();
  void read() { _impl->read(); }

 private:
  SeekTermIterator::ptr _impl;
  const PayAttr* _payload;
  byte_type _no_distance;
  uint32_t _target_size;
  score_t _boost{};
};

class LevenshteinAutomatonFilter final
  : public FilterWithField<LevenshteinAutomatonOptions> {
 public:
  static QueryBuilder::ptr PrepareSegment(
    const SubReader& segment, const PrepareContext& ctx, irs::field_id id,
    const LevenshteinAutomatonOptions& options, score_t boost);

  static field_visitor visitor(const LevenshteinAutomatonOptions& options);

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;
};

Filter::ptr LowerLevenshtein(irs::field_id id,
                             const ByEditDistanceOptions& opts, score_t boost);

}  // namespace irs
