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
class LimitedSampleCollector : private util::Noncopyable {
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

  explicit LimitedSampleCollector(size_t scored_terms_limit,
                                  const comparer_type& comparer = {})
    : _comparer{comparer}, _scored_terms_limit{scored_terms_limit} {
    _scored.reserve(scored_terms_limit);
    _heap.reserve(scored_terms_limit);
  }

  // Collect the term just appended to state->terms at the given offset.
  // terms - segment term-iterator positioned at that term.
  void collect(MultiTermState& state, uint32_t offset,
               const SeekTermIterator& terms, const Key& key) {
    if (!_scored_terms_limit) {
      return;  // nothing scored; the entry stays unscored (optimization)
    }
    if (_scored.size() >= _scored_terms_limit) {
      const size_t min_idx = _heap.front();
      if (!_comparer(_scored[min_idx].key, key)) {
        return;  // can't beat the current minimum; skip building the term
      }
    }
    Push(Candidate{&state, offset, bstring{terms.value()}, key});
  }

  // Fold another collector's candidates into this one.
  void Merge(LimitedSampleCollector&& other) {
    SDB_ASSERT(_scored_terms_limit == other._scored_terms_limit);
    for (auto& candidate : other._scored) {
      Push(std::move(candidate));
    }
    other._scored.clear();
    other._heap.clear();
  }

  // Finish collecting: assign stat offsets to the surviving (scored) terms and
  // evaluate their shared stats. Term/field statistics are accumulated into the
  // caller-owned collectors.
  void score(const Scorer* scorer, const FieldCollector& field,
             TermCollectorsFlat& terms, ManagedVector<bstring>& stats) {
    if (!_scored_terms_limit || _scored.empty()) {
      return;
    }
    SDB_ASSERT(scorer);
    // distinct term value -> stat offset shared across segments
    absl::flat_hash_map<hashed_bytes_view, uint32_t, HashedStrHash> offsets;

    for (auto& candidate : _scored) {
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
  void Push(Candidate&& candidate) {
    if (_scored.size() < _scored_terms_limit) {
      _heap.emplace_back(_scored.size());
      _scored.emplace_back(std::move(candidate));

      if (_scored.size() == _scored_terms_limit) [[unlikely]] {
        absl::c_make_heap(
          _heap, [&](const size_t lhs, const size_t rhs) noexcept {
            return _comparer(_scored[rhs].key, _scored[lhs].key);
          });
      }
      return;
    }

    const size_t min_idx = _heap.front();
    if (_comparer(_scored[min_idx].key, candidate.key)) {
      PopHeap();
      _scored[min_idx] = std::move(candidate);
      PushHeap();
    }
    // else: drop the candidate; its entry stays unscored
  }

  void PushHeap() noexcept {
    absl::c_push_heap(_heap, [&](const size_t lhs, const size_t rhs) noexcept {
      return _comparer(_scored[rhs].key, _scored[lhs].key);
    });
  }

  void PopHeap() noexcept {
    absl::c_pop_heap(_heap, [&](const size_t lhs, const size_t rhs) noexcept {
      return _comparer(_scored[rhs].key, _scored[lhs].key);
    });
  }

  [[no_unique_address]] comparer_type _comparer;
  std::vector<Candidate> _scored;
  // use external heap as candidates are big
  std::vector<size_t> _heap;
  size_t _scored_terms_limit;
};

struct TermFrequency {
  uint32_t offset;
  uint32_t frequency;
  score_t boost;

  explicit operator score_t() const noexcept { return boost; }

  bool operator<(const TermFrequency& rhs) const noexcept {
    return frequency < rhs.frequency ||
           (frequency == rhs.frequency && offset < rhs.offset);
  }
};

// Filter visitor for multiterm queries
class MultiTermVisitor {
 public:
  MultiTermVisitor(LimitedSampleCollector<TermFrequency>& collector,
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

    const TermFrequency key{
      .offset = _offset,
      .frequency = docs_count,
      .boost = boost,
    };
    _collector.collect(_state, _state.TermsSize() - 1, *_terms, key);
    ++_offset;
  }

 private:
  const decltype(TermMeta::docs_count) _no_docs = 0;
  LimitedSampleCollector<TermFrequency>& _collector;
  MultiTermState& _state;
  const SeekTermIterator* _terms = nullptr;
  const decltype(TermMeta::docs_count)* _docs_count = nullptr;
  uint32_t _offset = 0;
};

struct TermScore {
  uint32_t offset;
  score_t boost;

  explicit operator score_t() const noexcept { return boost; }

  bool operator<(const TermScore& rhs) const noexcept {
    return boost < rhs.boost || (boost == rhs.boost && offset > rhs.offset);
  }
};

// Filter visitor for multiterm queries sampled by score
class ScoredMultiTermVisitor {
 public:
  ScoredMultiTermVisitor(LimitedSampleCollector<TermScore>& collector,
                         MultiTermState& state)
    : _collector{collector}, _state{state} {}

  void Prepare(const SubReader& /*segment*/, const TermReader& reader,
               const SeekTermIterator& terms) {
    auto* meta = irs::get<TermMeta>(terms);
    _docs_count = meta ? &meta->docs_count : &_no_docs;

    _state.Prepare(&reader);

    _terms = &terms;
    _offset = 0;
  }

  void Visit(score_t boost) {
    SDB_ASSERT(_docs_count && _terms);
    const uint32_t docs_count = *_docs_count;
    _state.Push(MultiTermState::Entry{
      .cookie = _terms->cookie(),
      .docs_count = docs_count,
      .boost = boost,
    });

    const TermScore key{
      .offset = _offset,
      .boost = boost,
    };
    _collector.collect(_state, _state.TermsSize() - 1, *_terms, key);
    ++_offset;
  }

 private:
  const decltype(TermMeta::docs_count) _no_docs = 0;
  LimitedSampleCollector<TermScore>& _collector;
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
  LimitedSampleCollector<TermFrequency>& Limited() noexcept { return _limited; }

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
  LimitedSampleCollector<TermFrequency> _limited;
};

class ScoredTermsCollector final : public PrepareCollector {
 public:
  ScoredTermsCollector(const Scorer* scorer, size_t scored_terms_limit)
    : _scorer{scorer}, _limited{scorer ? scored_terms_limit : 0} {}

  FieldCollector& Field() noexcept { return _field; }
  LimitedSampleCollector<TermScore>& Limited() noexcept { return _limited; }

  void Merge(PrepareCollector&& other) final {
    auto& rhs = sdb::basics::downCast<ScoredTermsCollector>(other);
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
  LimitedSampleCollector<TermScore> _limited;
};

}  // namespace irs
