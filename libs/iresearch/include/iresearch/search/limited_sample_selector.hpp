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

#include <absl/container/flat_hash_map.h>

#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/states/multiterm_state.hpp"
#include "iresearch/search/top_k_heap.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct SubReader;
struct IndexReader;

// Object to collect and track a limited number of scorers,
// terms with longer postings are treated as more important.
// Each candidate references an entry in a per-segment MultiTermState by offset;
// the cookie itself lives in that state, not here.
template<typename Key, typename Comparer = std::less<Key>>
class LimitedSampleSelector : private util::Noncopyable {
 public:
  using key_type = Key;
  using comparer_type = Comparer;

  // A candidate term still in the running for the global top-K.
  struct Candidate {
    MultiTermState* state;  // state owning the referenced term
    uint32_t offset;        // index into state->terms
    bstring term;           // term value, kept for dedup at score()
    Key key;                // sampling key
  };

  explicit LimitedSampleSelector(size_t scored_terms_limit,
                                 const comparer_type& comparer = {})
    : _comparer{comparer}, _heap{scored_terms_limit, CandidateLess{comparer}} {}

  // Collect the term just appended to state->terms at the given offset.
  // terms - segment term-iterator positioned at that term.
  void collect(MultiTermState& state, uint32_t offset,
               const SeekTermIterator& terms, const Key& key) {
    if (!_heap.Capacity()) {
      return;  // nothing scored; the entry stays unscored (optimization)
    }
    if (_heap.Full() && !_comparer(_heap.Min().key, key)) {
      return;  // can't beat the current minimum; skip building the term
    }
    _heap.Push(Candidate{&state, offset, bstring{terms.value()}, key});
  }

  // Fold another collector's candidates into this one.
  void Merge(LimitedSampleSelector&& other) {
    SDB_ASSERT(_heap.Capacity() == other._heap.Capacity());
    _heap.Merge(std::move(other._heap));
  }

  // Finish collecting: assign stat offsets to the surviving (scored) terms and
  // evaluate their shared stats. Term/field statistics are accumulated into the
  // caller-owned collectors.
  void score(const Scorer* scorer, const FieldCollector& field,
             TermCollectorsFlat& terms, ManagedVector<bstring>& stats) {
    if (!_heap.Capacity() || _heap.Empty()) {
      return;
    }
    SDB_ASSERT(scorer);
    // distinct term value -> stat offset shared across segments
    absl::flat_hash_map<hashed_bytes_view, uint32_t, HashedStrHash> offsets;

    for (auto& candidate : _heap.Finalize()) {
      auto& terms_states = candidate.state->Terms();
      auto& entry = terms_states[candidate.offset];
      auto [it, inserted] =
        offsets.try_emplace(hashed_bytes_view{candidate.term}, uint32_t{0});
      if (inserted) {
        it->second = static_cast<uint32_t>(terms.PushBack());
      }
      const uint32_t idx = it->second;
      terms.Collect(idx, *entry.cookie);
      entry.stat_offset = idx;
    }

    stats.resize(terms.Size());
    for (const auto& [term, idx] : offsets) {
      terms.Finish(stats[idx], idx, &field);
    }
  }

 private:
  struct CandidateLess {
    [[no_unique_address]] comparer_type comparer;
    bool operator()(const Candidate& lhs, const Candidate& rhs) const {
      return comparer(lhs.key, rhs.key);
    }
  };

  [[no_unique_address]] comparer_type _comparer;
  TopKHeap<Candidate, CandidateLess> _heap;
};

struct TermFrequency {
  uint32_t offset;
  uint32_t frequency;
  score_t boost;

  static TermFrequency Make(uint32_t offset, uint32_t docs_count,
                            score_t boost) noexcept {
    return {.offset = offset, .frequency = docs_count, .boost = boost};
  }

  explicit operator score_t() const noexcept { return boost; }

  bool operator<(const TermFrequency& rhs) const noexcept {
    return frequency < rhs.frequency ||
           (frequency == rhs.frequency && offset < rhs.offset);
  }
};

// Filter visitor for multiterm queries sampled into a LimitedSampleSelector.
// The Key type (TermFrequency, ...) selects the sampling order.
template<typename Key>
class SampledMultiTermVisitor {
 public:
  SampledMultiTermVisitor(LimitedSampleSelector<Key>& collector,
                          MultiTermState& state)
    : _collector{collector}, _state{state} {}

  void Prepare(const SubReader& /*segment*/, const TermReader& reader,
               const SeekTermIterator& terms) {
    // get term metadata
    auto* meta = irs::get<TermMeta>(terms);

    // NOTE: we can't use reference to 'docs_count' here, like
    // 'const auto& docs_count = meta ? meta->docs_count : NO_DOCS;'
    // since not gcc4.9 nor msvc2015-2019 can handle this correctly
    // probably due to broken optimization
    _docs_count = meta ? &meta->docs_count : &_no_docs;

    _state.Prepare(&reader);

    _terms = &terms;
    _offset = 0;
  }

  // FIXME can incorporate boost into collecting logic
  void Visit(score_t boost) {
    SDB_ASSERT(_docs_count && _terms);
    const uint32_t docs_count = *_docs_count;
    _state.Push(MultiTermState::Entry{
      .cookie = _terms->cookie(),
      .docs_count = docs_count,
      .boost = boost,
    });

    _collector.collect(_state, _state.TermsSize() - 1, *_terms,
                       Key::Make(_offset, docs_count, boost));
    ++_offset;
  }

 private:
  const decltype(TermMeta::docs_count) _no_docs = 0;
  LimitedSampleSelector<Key>& _collector;
  MultiTermState& _state;
  const SeekTermIterator* _terms = nullptr;
  const decltype(TermMeta::docs_count)* _docs_count = nullptr;
  uint32_t _offset = 0;
};

// Per-segment collector for limited-sample (top-K) filters. Owns the field
// collector and the limited sample collector; the per-term collector is built
// in place at Finish, since the number of distinct winners is only known after
// all segments are merged.
class LimitedTermsCollector final : public PrepareCollector {
 public:
  LimitedTermsCollector(const Scorer* scorer, size_t scored_terms_limit)
    : _scorer{scorer}, _limited{scorer ? scored_terms_limit : 0} {}

  FieldCollector& Field() noexcept { return _field; }
  LimitedSampleSelector<TermFrequency>& Limited() noexcept { return _limited; }

  const Scorer* GetScorer() const noexcept final { return _scorer; }

  void Merge(PrepareCollector&& other) final {
    auto& rhs = sdb::basics::downCast<LimitedTermsCollector>(other);
    const FieldCollector fields[]{_field, rhs._field};
    _field = MergeFieldCollectors(fields);
    _limited.Merge(std::move(rhs._limited));
  }

  StatsBuffer Finish(IResourceManager& memory) final {
    StatsBuffer::Storage stats{{memory}};
    TermCollectorsFlat terms{_scorer, 0};
    _limited.score(_scorer, _field, terms, stats);
    return StatsBuffer{std::move(stats), _scorer};
  }

 private:
  const Scorer* _scorer;
  FieldCollector _field;
  LimitedSampleSelector<TermFrequency> _limited;
};

}  // namespace irs
