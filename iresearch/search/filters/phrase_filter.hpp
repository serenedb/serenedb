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

#include <functional>
#include <memory>
#include <set>
#include <variant>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/search/filters/automaton_filter.hpp"
#include "iresearch/search/filters/levenshtein_filter.hpp"
#include "iresearch/search/filters/prefix_filter.hpp"
#include "iresearch/search/filters/range_filter.hpp"
#include "iresearch/search/filters/regexp_filter.hpp"
#include "iresearch/search/filters/term_filter.hpp"
#include "iresearch/search/filters/wildcard_filter.hpp"
#include "iresearch/utils/levenshtein_default_pdp.hpp"

namespace irs {

struct TermSetOptions {
  std::set<bstring, std::less<>> terms;

  bool operator==(const TermSetOptions& rhs) const = default;
};

struct ShingleOptions {
  bstring term;
  std::vector<bstring> words;

  bool operator==(const ShingleOptions& rhs) const = default;
};

class ByPhrase;

enum class SlotKind : uint8_t {
  Term,
  Set,
  Expansion,
};

class ByPhraseOptions {
 public:
  using PhrasePart =
    std::variant<ByTermOptions, ByPrefixOptions, ByWildcardOptions,
                 ByRegexpOptions, ByEditDistanceOptions, TermSetOptions,
                 ByRangeOptions, AutomatonOptions, LevenshteinAutomatonOptions,
                 ShingleOptions>;

  struct PhrasePartInfo {
    PhrasePart part;
    PosAttr::value_t offs_min{0};
    PosAttr::value_t offs_max{0};

    bool operator==(const PhrasePartInfo& other) const = default;
  };

  static SlotKind KindOf(const PhrasePart& part) noexcept {
    if (std::holds_alternative<ByTermOptions>(part) ||
        std::holds_alternative<ShingleOptions>(part)) {
      return SlotKind::Term;
    }
    if (std::holds_alternative<TermSetOptions>(part)) {
      return SlotKind::Set;
    }
    return SlotKind::Expansion;
  }

  static bytes_view TermOf(const PhrasePart& part) noexcept {
    if (const auto* shingle = std::get_if<ShingleOptions>(&part)) {
      return shingle->term;
    }
    return std::get_if<ByTermOptions>(&part)->term;
  }

  using FilterType = ByPhrase;
  using TokenizerFactory =
    std::function<std::shared_ptr<analysis::Tokenizer>()>;

  template<typename PhrasePart>
  PhrasePart& push_back(PosAttr::value_t offs = 0) {
    return insert(PhrasePart{}, offs + 1, offs + 1);
  }

  template<typename PhrasePart>
  PhrasePart& push_back(PosAttr::value_t offs_min, PosAttr::value_t offs_max) {
    return insert(PhrasePart{}, offs_min, offs_max);
  }

  template<typename PhrasePart>
  PhrasePart& push_back(PhrasePart&& t, PosAttr::value_t offs = 0) {
    return insert(std::forward<PhrasePart>(t), offs + 1, offs + 1);
  }

  bool operator==(const ByPhraseOptions& rhs) const noexcept {
    return _phrase == rhs._phrase && _slop == rhs._slop &&
           _word_separator == rhs._word_separator && _text == rhs._text;
  }

  bool LowerParts();

  void clear() noexcept {
    _phrase.clear();
    _slop = 0;
    _word_separator.clear();
    _text = field_limits::invalid();
    _tokenizer = nullptr;
    _explicit_pos = false;
  }

  bool simple() const noexcept {
    for (const auto& info : _phrase) {
      if (KindOf(info.part) != SlotKind::Term) {
        return false;
      }
    }
    return true;
  }

  bool empty() const noexcept { return _phrase.empty(); }

  size_t size() const noexcept { return _phrase.size(); }

  auto begin() const noexcept { return _phrase.begin(); }

  auto end() const noexcept { return _phrase.end(); }

  PosAttr::value_t slop() const noexcept { return _slop; }
  void set_slop(PosAttr::value_t value) noexcept { _slop = value; }

  bytes_view word_separator() const noexcept { return _word_separator; }
  void set_word_separator(bytes_view value) { _word_separator = value; }

  field_id text() const noexcept { return _text; }
  const TokenizerFactory& tokenizer() const noexcept { return _tokenizer; }
  bool explicit_pos() const noexcept { return _explicit_pos; }
  void set_text(field_id text, TokenizerFactory tokenizer) {
    _text = text;
    _tokenizer = std::move(tokenizer);
    _explicit_pos = _tokenizer && _tokenizer()->Traits().explicit_pos;
  }

 private:
  template<typename PhrasePart>
  PhrasePart& insert(PhrasePart&& t, PosAttr::value_t offs_min,
                     PosAttr::value_t offs_max) {
    SDB_ASSERT(offs_max >= offs_min);
    if (_phrase.empty()) {
      offs_max = offs_min = 0;
      _phrase.reserve(kReservedParts);
    }
    return std::get<std::decay_t<PhrasePart>>(
      _phrase.emplace_back(std::forward<PhrasePart>(t), offs_min, offs_max)
        .part);
  }

  static constexpr size_t kReservedParts = 4;

  std::vector<PhrasePartInfo> _phrase;
  PosAttr::value_t _slop{0};
  bstring _word_separator;
  field_id _text{field_limits::invalid()};
  TokenizerFactory _tokenizer;
  bool _explicit_pos{false};
};

class ByPhrase : public FilterWithField<ByPhraseOptions> {
 public:
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollectorImpl(const Scorer* scorer,
                                          StatsArena& stats,
                                          uint32_t threads) const final;
};

Filter::ptr PartFilter(field_id field, ByPhraseOptions::PhrasePart part,
                       bool constant);

}  // namespace irs
