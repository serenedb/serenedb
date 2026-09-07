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

#include <memory>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/shared.hpp"
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

template<typename Key, typename Comparer = std::less<Key>>
class LimitedSampleSelector : private util::Noncopyable {
 public:
  using key_type = Key;
  using comparer_type = Comparer;

  struct Candidate {
    MultiTermState* state;
    uint32_t offset;
    bstring term;
    Key key;
  };

  explicit LimitedSampleSelector(size_t scored_terms_limit,
                                 const comparer_type& comparer = {})
    : _comparer{comparer}, _heap{scored_terms_limit, CandidateLess{comparer}} {}

  bool Samples() const noexcept { return _heap.Capacity() != 0; }

  void collect(MultiTermState& state, uint32_t offset,
               const TermIterator& terms, const Key& key) {
    if (!_heap.Capacity()) {
      return;
    }
    if (_heap.Full() && !_comparer(_heap.Min().key, key)) {
      return;
    }
    _heap.Push(Candidate{&state, offset, bstring{terms.value()}, key});
  }

  void MergeUnsafe(LimitedSampleSelector&& other) {
    SDB_ASSERT(_heap.Capacity() == other._heap.Capacity());
    _heap.PushUnsafe(std::move(other._heap));
  }

  void score(const Scorer* scorer, const FieldCollector& field,
             StatsArena& arena) {
    if (!_heap.Capacity() || _heap.Empty()) {
      return;
    }
    SDB_ASSERT(scorer);
    struct Slot {
      TermCollector counter;
      byte_type* stats = nullptr;
    };
    sdb::containers::FlatHashMap<hashed_bytes_view, Slot, HashedStrHash> terms;

    for (auto& candidate : _heap.Finalize()) {
      auto& entry = candidate.state->Terms()[candidate.offset];
      auto [it, inserted] =
        terms.try_emplace(hashed_bytes_view{candidate.term});
      if (inserted) {
        it->second.stats = arena.Allocate(StatsSlot(scorer));
      }
      it->second.counter.Collect(entry.cookie);
      entry.stats = it->second.stats;
    }

    for (auto& [term, slot] : terms) {
      scorer->collect(slot.stats, &field, &slot.counter);
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

template<typename Key>
class SampledMultiTermVisitor {
 public:
  SampledMultiTermVisitor(LimitedSampleSelector<Key>* collector,
                          MultiTermState& state)
    : _collector{collector}, _state{state} {}

  void Prepare(const SubReader&, const TermReader& reader,
               TermIterator& terms) {
    _state.Prepare(&reader);

    _terms = &terms;
    _offset = 0;
  }

  bool Visit(score_t boost) {
    SDB_ASSERT(_terms);
    const auto& meta = _terms->cookie();
    const uint32_t docs_count = meta.docs_count;
    _state.Push(meta, boost);

    if (_collector) {
      _collector->collect(_state, _state.TermsSize() - 1, *_terms,
                          Key::Make(_offset, docs_count, boost));
    }
    ++_offset;
    return true;
  }

 private:
  LimitedSampleSelector<Key>* _collector;
  MultiTermState& _state;
  TermIterator* _terms = nullptr;
  uint32_t _offset = 0;
};

class LimitedTermsCollector final : public FieldPrepareCollector {
 public:
  LimitedTermsCollector(const Scorer* scorer, size_t scored_terms_limit,
                        StatsArena& stats, uint32_t threads)
    : FieldPrepareCollector{scorer, stats, threads, 0, false},
      _limit{scorer ? scored_terms_limit : 0},
      _limited(threads) {}

  LimitedSampleSelector<TermFrequency>& Limited(uint32_t thread) {
    SDB_ASSERT(thread < _limited.size());
    auto& sampler = _limited[thread];
    if (!sampler) {
      sampler = std::make_unique<LimitedSampleSelector<TermFrequency>>(_limit);
    }
    return *sampler;
  }

  void Finish(StatsArena& stats) final {
    LimitedSampleSelector<TermFrequency>* head = nullptr;
    for (auto& sampler : _limited) {
      if (!sampler) {
        continue;
      }
      if (head == nullptr) {
        head = sampler.get();
        continue;
      }
      head->MergeUnsafe(std::move(*sampler));
    }
    if (head == nullptr) {
      return;
    }
    const auto field = _counters.TotalField();
    head->score(_scorer, field, stats);
  }

 private:
  size_t _limit;
  std::vector<std::unique_ptr<LimitedSampleSelector<TermFrequency>>> _limited;
};

}  // namespace irs
