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
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct SubReader;
struct IndexReader;

// Object to collect and track a limited number of scorers,
// terms with longer postings are treated as more important
template<typename Key, typename Comparer = std::less<Key>>
class LimitedSampleCollector : private util::Noncopyable {
 public:
  using key_type = Key;
  using comparer_type = Comparer;

  explicit LimitedSampleCollector(size_t scored_terms_limit,
                                  const comparer_type& comparer = {})
    : _comparer{comparer}, _scored_terms_limit{scored_terms_limit} {
    _scored_states.reserve(scored_terms_limit);
    _scored_states_heap.reserve(scored_terms_limit);
  }

  // Prepare scorer for terms collecting
  // segment - segment reader for the current term
  // state - state containing this scored term
  // terms - segment term-iterator positioned at the current term
  void Prepare(const SubReader& segment, const SeekTermIterator& terms,
               MultiTermState& scored_state) noexcept {
    _state.state = &scored_state;
    _state.segment = &segment;
    _state.terms = &terms;

    // get term metadata
    auto* meta = irs::get<TermMeta>(terms);
    _state.docs_count = meta ? &meta->docs_count : &_no_docs;
  }

  // Collect current term
  void collect(const Key& key) {
    SDB_ASSERT(_state.segment && _state.terms && _state.state);

    if (!_scored_terms_limit) {
      // state will not be scored

      _state.state->unscored_terms.emplace_back(_state.terms->cookie());
      _state.state->unscored_states_estimation += *_state.docs_count;
      return;  // nothing to collect (optimization)
    }

    if (_scored_states.size() < _scored_terms_limit) {
      // have not reached the scored state limit yet
      _scored_states_heap.emplace_back(_scored_states.size());
      _scored_states.emplace_back(key, _state);

      push();
      return;
    }

    const size_t min_state_idx = _scored_states_heap.front();

    if (_scored_states[min_state_idx].key < key) {
      pop();

      auto& min_state = _scored_states[min_state_idx];

      SDB_ASSERT(min_state.cookie);
      // state will not be scored
      min_state.state->unscored_terms.emplace_back(std::move(min_state.cookie));
      min_state.state->unscored_states_estimation += min_state.docs_count;

      // update min state
      min_state.docs_count = *_state.docs_count;
      min_state.state = _state.state;
      min_state.cookie = _state.terms->cookie();
      min_state.term = _state.terms->value();
      min_state.segment = _state.segment;
      min_state.key = key;

      push();
    } else {
      // state will not be scored
      _state.state->unscored_terms.emplace_back(_state.terms->cookie());
      _state.state->unscored_states_estimation += *_state.docs_count;
    }
  }

  // Finish collecting and evaluate stats
  void score(const IndexReader& index, const Scorers& order,
             MultiTermQuery::Stats& stats) {
    if (!_scored_terms_limit) {
      return;  // nothing to score (optimization)
    }

    // stats for a specific term
    absl::flat_hash_map<hashed_bytes_view, StatsState, HashedStrHash>
      term_stats;

    // iterate over all the states from which statistcis should be collected
    uint32_t stats_offset = 0;
    for (auto& scored_state : _scored_states) {
      SDB_ASSERT(scored_state.cookie);
      auto& field = *scored_state.state->reader;

      // find the stats for the current term
      const auto res =
        term_stats.try_emplace(hashed_bytes_view{scored_state.term}, index,
                               field, order, stats_offset);

      auto& stats_entry = res.first->second;

      // collect statistics, 0 because only 1 term
      stats_entry.term_stats.collect(*scored_state.segment, field, 0,
                                     *scored_state.cookie);

      scored_state.state->scored_states.emplace_back(
        std::move(scored_state.cookie), stats_entry.stats_offset,
        static_cast<score_t>(scored_state.key));

      // update estimation for scored state
      scored_state.state->scored_states_estimation += scored_state.docs_count;
    }

    // iterate over all stats and apply/store order stats
    stats.resize(stats_offset);
    for (auto& entry : term_stats) {
      auto& stats_entry = stats[entry.second.stats_offset];
      stats_entry.resize(order.stats_size());
      auto* stats_buf = const_cast<byte_type*>(stats_entry.data());

      entry.second.term_stats.finish(stats_buf, 0, entry.second.field_stats,
                                     index);
    }
  }

 private:
  struct StatsState {
    explicit StatsState(const IndexReader& index, const TermReader& field,
                        const Scorers& order, uint32_t& state_offset)
      : field_stats(order),
        term_stats(order, 1) {  // 1 term per bstring because a range is
                                // treated as a disjunction

      // once per every 'state' collect field statistics over the entire index
      for (auto& segment : index) {
        // FIXME
        field_stats.collect(
          segment, field);  // collect field statistics once per segment
      }

      stats_offset = state_offset++;
    }

    FieldCollectors field_stats;
    TermCollectors term_stats;
    uint32_t stats_offset;
  };

  // A representation of state of the collector
  struct CollectorState {
    const SubReader* segment{};
    const SeekTermIterator* terms{};
    MultiTermState* state{};
    const uint32_t* docs_count{};
  };

  // A representation of a term cookie with its associated range_state
  struct ScoredTermState {
    ScoredTermState(const Key& key, const CollectorState& state)
      : key(key),
        cookie(state.terms->cookie()),
        state(state.state),
        segment(state.segment),
        term(state.terms->value()),
        docs_count(*state.docs_count) {
      SDB_ASSERT(this->cookie);
    }

    ScoredTermState(ScoredTermState&&) = default;
    ScoredTermState& operator=(ScoredTermState&&) = default;

    Key key;
    SeekCookie::ptr cookie;    // term offset cache
    MultiTermState* state;     // state containing this scored term
    const SubReader* segment;  // segment reader for the current term
    bstring term;              // actual term value this state is for
    uint32_t docs_count;
  };

  void push() noexcept {
    absl::c_push_heap(
      _scored_states_heap, [&](const size_t lhs, const size_t rhs) noexcept {
        return _comparer(_scored_states[rhs].key, _scored_states[lhs].key);
      });
  }

  void pop() noexcept {
    absl::c_pop_heap(
      _scored_states_heap, [&](const size_t lhs, const size_t rhs) noexcept {
        return _comparer(_scored_states[rhs].key, _scored_states[lhs].key);
      });
  }

  [[no_unique_address]] comparer_type _comparer;
  const decltype(TermMeta::docs_count) _no_docs = 0;
  CollectorState _state;
  std::vector<ScoredTermState> _scored_states;
  // use external heap as states are big
  std::vector<size_t> _scored_states_heap;
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
template<typename States>
class MultiTermVisitor {
 public:
  MultiTermVisitor(LimitedSampleCollector<TermFrequency>& collector,
                   States& states)
    : _collector(collector), _states(states) {}

  void Prepare(const SubReader& segment, const TermReader& reader,
               const SeekTermIterator& terms) {
    // get term metadata
    auto* meta = irs::get<TermMeta>(terms);

    // NOTE: we can't use reference to 'docs_count' here, like
    // 'const auto& docs_count = meta ? meta->docs_count : NO_DOCS;'
    // since not gcc4.9 nor msvc2015-2019 can handle this correctly
    // probably due to broken optimization
    _docs_count = meta ? &meta->docs_count : &_no_docs;

    // get state for current segment
    auto& state = _states.insert(segment);
    state.reader = &reader;

    _collector.Prepare(segment, terms, state);
    _key.offset = 0;
  }

  // FIXME can incorporate boost into collecting logic
  void Visit(score_t boost) {
    // fill scoring candidates
    SDB_ASSERT(_docs_count);
    _key.frequency = *_docs_count;
    _key.boost = boost;
    _collector.collect(_key);
    ++_key.offset;
  }

 private:
  const decltype(TermMeta::docs_count) _no_docs = 0;
  LimitedSampleCollector<TermFrequency>& _collector;
  States& _states;
  TermFrequency _key;
  const decltype(TermMeta::docs_count)* _docs_count = nullptr;
};

}  // namespace irs
