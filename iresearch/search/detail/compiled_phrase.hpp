////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/container/inlined_vector.h>

#include <duckdb/common/types/string_type.hpp>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "iresearch/analysis/text/dict/string_table.hpp"
#include "iresearch/analysis/text/term_view.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/phrase_slop_matcher.hpp"
#include "iresearch/search/detail/term_predicate.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct PhraseWordStat {
  uint64_t docs = 0;
  uint64_t freq = 0;
};

bool IsAnchored(const ByPhraseOptions& phrase) noexcept;
bool IsStandalone(const ByPhraseOptions& phrase) noexcept;
bool HasPatternParts(const ByPhraseOptions& phrase) noexcept;
std::vector<PhraseWordStat> PhraseWordStats(const ByPhraseOptions& phrase,
                                            const TermReader& reader);
std::vector<PhraseWordStat> PhraseWordStats(const ByPhraseOptions& phrase,
                                            const IndexReader& index,
                                            field_id field);

struct CompiledPhrase {
  static constexpr uint32_t kMaxBits = 64;

  struct Slot {
    PosAttr::value_t offs_min = 0;
    PosAttr::value_t offs_max = 0;
    std::optional<duckdb::string_t> word;
    TermPredicate::ptr pattern;
    uint64_t bit = 0;
  };

  struct Accept {
    absl::InlinedVector<uint32_t, 4> slots;
    uint64_t mask = 0;
  };

  struct Slop {
    PosAttr::value_t max = 0;
    std::vector<int64_t> offsets;
    std::vector<detail::slop::GroupPair> pairs;
  };

  struct Anchor {
    uint32_t slot = 0;
    uint64_t left = 0;
    uint64_t right = 0;
  };

  struct Automaton {
    struct Extra {
      uint64_t range = 0;
      uint64_t bit = 0;
    };

    std::vector<Extra> extras;
    uint64_t wild = 0;
    uint64_t last = 0;
  };

  CompiledPhrase(const ByPhraseOptions& phrase,
                 std::span<const std::vector<bstring>> expanded,
                 std::span<const PhraseWordStat> words);

  CompiledPhrase(CompiledPhrase&&) = delete;
  CompiledPhrase& operator=(CompiledPhrase&&) = delete;

  bool Accepts(const Slot& slot, const duckdb::string_t& term) const {
    return slot.word ? *slot.word == term : AcceptsTerm(slot, term);
  }
  bool AcceptsTerm(const Slot& slot, const duckdb::string_t& term) const;
  uint64_t MaskOf(const duckdb::string_t& term) const;

  template<typename Visitor>
  void ForEachSlot(const duckdb::string_t& term, Visitor&& visit) const {
    if (const auto* found = accept.Find(term)) {
      for (const auto slot : found->slots) {
        visit(slot);
      }
    }
    ForEachPattern(term, visit);
  }

  template<typename Visitor>
  void ForEachPattern(const duckdb::string_t& term, Visitor&& visit) const {
    if (pattern_slots.empty()) {
      return;
    }
    const auto view = AsBytesView(term);
    for (const auto slot : pattern_slots) {
      if (slots[slot].pattern->Accepts(view)) {
        visit(slot);
      }
    }
  }

  std::vector<Slot> slots;
  std::vector<bstring> terms;
  Slop slop;
  analysis::dict::StringMap<std::string_view, Accept> accept;
  std::vector<uint32_t> pattern_slots;
  std::optional<Anchor> anchor;
  std::optional<Automaton> automaton;
};

}  // namespace irs
