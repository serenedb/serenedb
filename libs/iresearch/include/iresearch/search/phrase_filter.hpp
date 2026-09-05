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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <map>
#include <variant>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/search/automaton_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/term_set.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/levenshtein_default_pdp.hpp"

namespace irs {

class ByPhrase;

class ByPhraseOptions {
 private:
  using phrase_part =
    std::variant<ByTermOptions, ByPrefixOptions, ByWildcardOptions,
                 ByEditDistanceOptions, TermSetOptions, ByRangeOptions,
                 ByRegexpOptions, AutomatonOptions,
                 LevenshteinAutomatonOptions>;

  struct PhrasePartInfo {
    phrase_part part;
    PosAttr::value_t offs_min{0};
    PosAttr::value_t offs_max{0};

    bool operator==(const PhrasePartInfo& other) const = default;
  };

  using phrase_type = std::deque<PhrasePartInfo>;

 public:
  using FilterType = ByPhrase;

  template<typename PhrasePart>
  PhrasePart& push_back(size_t offs = 0) {
    return insert(PhrasePart{}, offs + 1, offs + 1);
  }

  template<typename PhrasePart>
  PhrasePart& push_back(size_t offs_min, size_t offs_max) {
    return insert(PhrasePart{}, offs_min, offs_max);
  }

  template<typename PhrasePart>
  PhrasePart& push_back(PhrasePart&& t, size_t offs = 0) {
    return insert(std::forward<PhrasePart>(t), offs + 1, offs + 1);
  }

  bool operator==(const ByPhraseOptions& rhs) const noexcept {
    return _phrase == rhs._phrase && _slop == rhs._slop;
  }

  bool LowerParts();

  void clear() noexcept {
    _phrase.clear();
    _is_simple_term_only = true;
    _slop = 0;
  }

  bool simple() const noexcept { return _is_simple_term_only; }

  bool empty() const noexcept { return _phrase.empty(); }

  size_t size() const noexcept { return _phrase.size(); }

  phrase_type::const_iterator begin() const noexcept { return _phrase.begin(); }

  phrase_type::const_iterator end() const noexcept { return _phrase.end(); }
  PosAttr::value_t slop() const noexcept { return _slop; }
  void set_slop(PosAttr::value_t value) noexcept { _slop = value; }

 private:
  template<typename PhrasePart>
  PhrasePart& insert(PhrasePart&& t, size_t offs_min, size_t offs_max) {
    SDB_ASSERT(offs_max >= offs_min);
    if (_phrase.empty()) {
      offs_max = offs_min = 0;
    }
    _is_simple_term_only &= std::is_same_v<PhrasePart, ByTermOptions>;
    _phrase.push_back(PhrasePartInfo{.part = std::forward<PhrasePart>(t),
                                     .offs_min = offs_min,
                                     .offs_max = offs_max});
    return std::get<std::decay_t<PhrasePart>>(_phrase.back().part);
  }

  phrase_type _phrase;
  bool _is_simple_term_only{true};
  PosAttr::value_t _slop{0};
};

class ByPhrase : public FilterWithField<ByPhraseOptions> {
 public:
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollectorImpl(const Scorer* scorer,
                                          StatsArena& stats,
                                          uint32_t threads) const final;
};

}  // namespace irs
