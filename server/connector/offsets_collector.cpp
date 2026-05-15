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

#include "connector/offsets_collector.hpp"

#include <absl/algorithm/container.h>
#include <absl/functional/overload.h>

#include <algorithm>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/ngram_similarity_query.hpp>
#include <iresearch/search/phrase_query.hpp>
#include <iresearch/search/states/multiterm_state.hpp>
#include <iresearch/search/states/ngram_state.hpp>
#include <iresearch/search/states/phrase_state.hpp>
#include <iresearch/search/states/term_state.hpp>

namespace sdb::connector {

void PrepareFilterEntry(FilterEntry& entry, const irs::TermReader* reader,
                        const irs::SubReader& segment) {
  if (entry.docs) {
    return;
  }
  auto docs = std::visit(
    absl::Overload{
      [&](const irs::SeekCookie* cookie) {
        static constexpr auto kFeatures = irs::IndexFeatures::Freq |
                                          irs::IndexFeatures::Pos |
                                          irs::IndexFeatures::Offs;
        return reader->Iterator(kFeatures,
                                irs::PostingCookie{.cookie = cookie});
      },
      [&](const auto* query) { return query->ExecuteWithOffsets(segment); }},
    entry.filter);

  if (!docs || irs::doc_limits::eof(docs->value())) {
    return;
  }
  entry.pos = irs::GetMutable<irs::PosAttr>(docs.get());
  if (!entry.pos) {
    return;
  }
  entry.offs = irs::get<irs::OffsAttr>(*entry.pos);
  if (!entry.offs) {
    return;
  }

  entry.docs = std::move(docs);
}

void FillRowOffsets(FieldState& state, const irs::SubReader& segment,
                    irs::doc_id_t doc_id, size_t max_pairs,
                    std::vector<highlight::HitRange>& hits) {
  hits.clear();
  for (auto& entry : state.entries) {
    PrepareFilterEntry(entry, state.reader, segment);
    if (!entry.docs) {
      continue;
    }
    if (entry.docs->seek(doc_id) != doc_id) {
      continue;
    }
    const size_t entry_start = hits.size();
    while (entry.pos->next()) {
      if (hits.size() - entry_start >= max_pairs) {
        break;
      }
      hits.emplace_back(entry.offs->start, entry.offs->end);
    }
  }
  if (state.entries.size() > 1) {
    absl::c_sort(hits);
    hits.erase(std::unique(hits.begin(), hits.end()), hits.end());
  }
  if (hits.size() > max_pairs) {
    hits.resize(max_pairs);
  }
}

FieldState* OffsetsCollector::FindFieldState(
  const irs::TermReader* reader) noexcept {
  if (!reader) {
    return nullptr;
  }
  const auto& name = reader->meta().name;
  auto it = absl::c_find_if(
    _entries, [&](const FieldEntry& entry) { return entry.name == name; });
  return it == _entries.end() ? nullptr : &it->state;
}

namespace {

void RecordCookie(FieldState& field, const irs::TermReader* reader,
                  const irs::SeekCookie* cookie) {
  if (field.seen_cookies.insert(cookie).second) {
    field.reader = reader;
    field.entries.emplace_back(cookie);
  }
}

}  // namespace

bool OffsetsCollector::Visit(const irs::TermQuery&, const irs::TermState& state,
                             irs::score_t) {
  if (auto* field = FindFieldState(state.reader)) {
    RecordCookie(*field, state.reader, state.cookie.get());
  }
  return true;
}

bool OffsetsCollector::Visit(const irs::MultiTermQuery&,
                             const irs::MultiTermState& state, irs::score_t) {
  auto* field = FindFieldState(state.reader);
  if (!field) {
    return true;
  }
  for (const auto& term_state : state.scored_states) {
    RecordCookie(*field, state.reader, term_state.cookie.get());
  }
  for (const auto& term_state : state.unscored_states) {
    RecordCookie(*field, state.reader, term_state.get());
  }
  return true;
}

// Phrase/ngram queries don't expose per-position cookies, so record the
// query pointer itself as the FilterEntry; ExecuteWithOffsets builds the
// per-segment iterator. No dedup -- the prepared filter tree visits each
// node once.
template<typename Q>
void OffsetsCollector::RecordQuery(const Q& query,
                                   const irs::TermReader* reader) {
  if (auto* field = FindFieldState(reader)) {
    field->reader = reader;
    field->entries.emplace_back(&query);
  }
}

bool OffsetsCollector::Visit(const irs::FixedPhraseQuery& query,
                             const irs::FixedPhraseState& state, irs::score_t) {
  RecordQuery(query, state.reader);
  return true;
}

bool OffsetsCollector::Visit(const irs::VariadicPhraseQuery& query,
                             const irs::VariadicPhraseState& state,
                             irs::score_t) {
  RecordQuery(query, state.reader);
  return true;
}

bool OffsetsCollector::Visit(const irs::NGramSimilarityQuery& query,
                             const irs::NGramState& state, irs::score_t) {
  RecordQuery(query, state.reader);
  return true;
}

}  // namespace sdb::connector
