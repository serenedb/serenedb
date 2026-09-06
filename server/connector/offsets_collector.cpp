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

#include "connector/offsets_collector.hpp"

#include <absl/algorithm/container.h>
#include <absl/functional/overload.h>

#include <algorithm>
#include <iresearch/search/ngram_similarity_query.hpp>
#include <iresearch/search/phrase_query.hpp>
#include <iresearch/search/states/multiterm_state.hpp>
#include <iresearch/search/states/ngram_state.hpp>
#include <iresearch/search/states/phrase_state.hpp>
#include <iresearch/search/states/term_state.hpp>
#include <span>

namespace sdb::connector {

void PrepareFilterEntry(FilterEntry& entry, const irs::TermReader* reader) {
  if (entry.root || entry.absent) {
    return;
  }
  // The three streams are the field's, so they are resolved here rather than
  // by each shape: a field that stores no offsets has none for any of its
  // terms, whichever shape asks.
  irs::offsets::Handles handles;
  if (!irs::offsets::Resolve(reader, handles)) {
    entry.absent = true;
    return;
  }
  entry.root =
    std::visit(absl::Overload{
                 [&](const irs::PostingMeta* cookie) {
                   return irs::offsets::MakePosting(*cookie, handles);
                 },
                 [](const auto* query) { return irs::offsets::Make(*query); }},
               entry.filter);
  entry.absent = entry.root == nullptr;
}

void FillRowOffsets(FieldState& state, irs::doc_id_t doc_id, size_t max_pairs,
                    std::vector<highlight::HitRange>& hits) {
  hits.clear();
  if (max_pairs == 0) {
    return;
  }
  // Asked for in chunks rather than in one span the size of the cap: the cap
  // is what the row is allowed to show and an absent one arrives as SIZE_MAX,
  // which is not a buffer size. A full chunk is what says to ask again.
  static constexpr size_t kChunk = 256;
  state.scratch.resize(std::min(kChunk, max_pairs));
  for (auto& entry : state.entries) {
    PrepareFilterEntry(entry, state.reader);
    if (!entry.root) {
      continue;
    }
    // Each leaf may go up to the whole row's cap on its own, because it may
    // be the only one that matched. Asking again with the same document is
    // what continues it, so the loop needs no state of its own.
    for (size_t taken = 0; taken < max_pairs;) {
      const std::span chunk{state.scratch.data(),
                            std::min(state.scratch.size(), max_pairs - taken)};
      const auto count = entry.root->Run(doc_id, chunk);
      for (uint32_t i = 0; i != count; ++i) {
        hits.emplace_back(chunk[i].start, chunk[i].end);
      }
      taken += count;
      if (count != chunk.size()) {
        break;
      }
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
  const auto id = reader->meta().id;
  auto it = absl::c_find_if(
    _entries, [&](const FieldEntry& entry) { return entry.id == id; });
  return it == _entries.end() ? nullptr : &it->state;
}

namespace {

void RecordCookie(FieldState& field, const irs::TermReader* reader,
                  const irs::PostingMeta& cookie) {
  if (cookie.docs_count == 0) {
    return;
  }
  if (field.seen_cookies.insert(&cookie).second) {
    field.reader = reader;
    field.entries.emplace_back(&cookie);
  }
}

}  // namespace

bool OffsetsCollector::Visit(const irs::TermState& state, irs::score_t) {
  if (auto* field = FindFieldState(state.reader)) {
    RecordCookie(*field, state.reader, state.cookie);
  }
  return true;
}

bool OffsetsCollector::Visit(const irs::MultiTermQuery&,
                             const irs::MultiTermState& state, irs::score_t) {
  auto* field = FindFieldState(state.Reader());
  if (!field) {
    return true;
  }
  // A term set is not a disjunction here: offsets have no algebra, so each
  // of its terms is a leaf of its own and the dedup is what keeps a term
  // reached twice from reporting twice.
  for (const auto& entry : state.Terms()) {
    RecordCookie(*field, state.Reader(), entry.cookie);
  }
  return true;
}

// Phrase/ngram queries don't expose per-position cookies, so the query itself
// is the leaf: it owns the positional match that has to be replayed. No dedup
// -- the prepared filter tree visits each node once.
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
