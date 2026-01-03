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
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/levenshtein_default_pdp.hpp"

namespace irs {

class ByPhrase;

// Options for phrase filter
class ByPhraseOptions {
 private:
  using phrase_part =
    std::variant<ByTermOptions, ByPrefixOptions, ByWildcardOptions,
                 ByEditDistanceOptions, ByTermsOptions, ByRangeOptions>;

  struct PhrasePartInfo {
    phrase_part part;
    // TODO(Dronplane) what if negative offset limits?
    PosAttr::value_t offs_min{0};
    PosAttr::value_t offs_max{0};

    bool operator==(const PhrasePartInfo& other) const = default;
  };

  using phrase_type = std::deque<PhrasePartInfo>;

 public:
  using FilterType = ByPhrase;

  // Appends phrase part of type "PhrasePart" at a specified offset
  // "offs" from the end of the phrase.
  // Returns reference to the inserted phrase part.
  template<typename PhrasePart>
  PhrasePart& push_back(size_t offs = 0) {
    // TODO (Dronplane): do we really want to keep this behaviour (implicit +1
    // offset)?
    return insert(PhrasePart{}, offs + 1, offs + 1);
  }

  template<typename PhrasePart>
  PhrasePart& push_back(size_t offs_min, size_t offs_max) {
    // TODO (Dronplane): Here it does not match behaviour of single offset
    // push_back and has no implicit +1. Is it ok?
    return insert(PhrasePart{}, offs_min, offs_max);
  }

  // Appends phrase part of type "PhrasePart" at a specified offset
  // "offs" from the end of the phrase. Returns a reference to the
  // inserted phrase part
  template<typename PhrasePart>
  PhrasePart& push_back(PhrasePart&& t, size_t offs = 0) {
    return insert(std::forward<PhrasePart>(t), offs + 1, offs + 1);
  }

  // Returns true is options are equal, false - otherwise
  bool operator==(const ByPhraseOptions& rhs) const noexcept {
    return _phrase == rhs._phrase;
  }

  // Clear phrase contents
  void clear() noexcept {
    _phrase.clear();
    _is_simple_term_only = true;
  }

  // Returns true if phrase composed of simple terms only, false - otherwise
  bool simple() const noexcept { return _is_simple_term_only; }

  // Returns true if phrase is empty, false - otherwise
  bool empty() const noexcept { return _phrase.empty(); }

  // Returns size of the phrase
  size_t size() const noexcept { return _phrase.size(); }

  // Returns iterator referring to the first part of the phrase
  phrase_type::const_iterator begin() const noexcept { return _phrase.begin(); }

  // Returns iterator referring to past-the-end element of the phrase
  phrase_type::const_iterator end() const noexcept { return _phrase.end(); }

 private:
  template<typename PhrasePart>
  PhrasePart& insert(PhrasePart&& t, size_t offs_min, size_t offs_max) {
    SDB_ASSERT(offs_max >= offs_min);
    if (_phrase.empty()) {
      offs_max = offs_min = 0;
    }
    _is_simple_term_only &= std::is_same_v<PhrasePart, ByTermOptions>;
    _phrase.push_back(PhrasePartInfo{.part = std::forward<PhrasePart>(t),
                                     .offs_max = offs_max,
                                     .offs_min = offs_min});
    return std::get<std::decay_t<PhrasePart>>(_phrase.back().part);
  }

  phrase_type _phrase;
  bool _is_simple_term_only{true};
};

class ByPhrase : public FilterWithField<ByPhraseOptions> {
 public:
  static Query::ptr Prepare(const PrepareContext& ctx, std::string_view field,
                            const ByPhraseOptions& options);

  Query::ptr prepare(const PrepareContext& ctx) const final {
    return Prepare(ctx.Boost(Boost()), field(), options());
  }
};

}  // namespace irs
