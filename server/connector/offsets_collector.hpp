////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/ngram_similarity_query.hpp>
#include <iresearch/search/phrase_query.hpp>
#include <iresearch/search/prepared_state_visitor.hpp>
#include <iresearch/search/states/multiterm_state.hpp>
#include <iresearch/search/states/ngram_state.hpp>
#include <iresearch/search/states/phrase_state.hpp>
#include <iresearch/search/states/term_state.hpp>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace sdb::connector {

// A single sub-filter that contributes offsets for a field.
// For term/multiterm: holds the SeekCookie pointer (reader->Iterator is used).
// For phrase/ngram: holds the query pointer (ExecuteWithOffsets is used).
struct FilterEntry {
  std::variant<const irs::SeekCookie*, const irs::FixedPhraseQuery*,
               const irs::VariadicPhraseQuery*,
               const irs::NGramSimilarityQuery*>
    filter;
  // Lazy: null until first use in a segment, then reused for all docs.
  irs::DocIterator::ptr docs;
  irs::PosAttr* pos = nullptr;
  const irs::OffsAttr* offs = nullptr;
};

// Per-field state, rebuilt on each segment transition.
struct PerFieldState {
  const irs::TermReader* reader = nullptr;
  std::vector<FilterEntry> entries;
  containers::FlatHashSet<const irs::SeekCookie*> seen_cookies;

  void Clear() noexcept {
    reader = nullptr;
    entries.clear();
    seen_cookies.clear();
  }
};

// Visits a prepared filter tree and collects per-field offset state.
// field_names[i] must be the 8-byte big-endian encoding of the catalog column
// ID for state[i].
class OffsetsCollector final : public irs::PreparedStateVisitor {
 public:
  OffsetsCollector(std::span<const std::string> field_names,
                   std::vector<PerFieldState>& state) noexcept
    : _field_names{field_names}, _state{state} {}

  bool Visit(const irs::BooleanQuery&, irs::score_t) final { return true; }
  bool Visit(const irs::ByNestedQuery&, irs::score_t) final { return false; }

  bool Visit(const irs::TermQuery&, const irs::TermState& state,
             irs::score_t) final {
    const auto fi = FindField(state.reader);
    if (fi < 0) {
      return true;
    }
    auto& fs = _state[fi];
    if (fs.seen_cookies.insert(state.cookie.get()).second) {
      fs.reader = state.reader;
      fs.entries.push_back({state.cookie.get()});
    }
    return true;
  }

  bool Visit(const irs::MultiTermQuery&, const irs::MultiTermState& state,
             irs::score_t) final {
    const auto fi = FindField(state.reader);
    if (fi < 0) {
      return true;
    }
    auto& fs = _state[fi];
    fs.reader = state.reader;
    for (const auto& ts : state.scored_states) {
      if (fs.seen_cookies.insert(ts.cookie.get()).second) {
        fs.entries.push_back({ts.cookie.get()});
      }
    }
    for (const auto& ts : state.unscored_states) {
      if (fs.seen_cookies.insert(ts.get()).second) {
        fs.entries.push_back({ts.get()});
      }
    }
    return true;
  }

  bool Visit(const irs::FixedPhraseQuery& query,
             const irs::FixedPhraseState& state, irs::score_t) final {
    const auto fi = FindField(state.reader);
    if (fi < 0) {
      return true;
    }
    _state[fi].reader = state.reader;
    _state[fi].entries.push_back({&query});
    return true;
  }

  bool Visit(const irs::VariadicPhraseQuery& query,
             const irs::VariadicPhraseState& state, irs::score_t) final {
    const auto fi = FindField(state.reader);
    if (fi < 0) {
      return true;
    }
    _state[fi].reader = state.reader;
    _state[fi].entries.push_back({&query});
    return true;
  }

  bool Visit(const irs::NGramSimilarityQuery& query,
             const irs::NGramState& state, irs::score_t) final {
    const auto fi = FindField(state.reader);
    if (fi < 0) {
      return true;
    }
    _state[fi].reader = state.reader;
    _state[fi].entries.push_back({&query});
    return true;
  }

 private:
  int FindField(const irs::TermReader* reader) const noexcept {
    if (!reader) {
      return -1;
    }
    const auto& name = reader->meta().name;
    for (size_t i = 0; i < _field_names.size(); ++i) {
      if (name == _field_names[i]) {
        return static_cast<int>(i);
      }
    }
    return -1;
  }

  std::span<const std::string> _field_names;
  std::vector<PerFieldState>& _state;
};

}  // namespace sdb::connector
