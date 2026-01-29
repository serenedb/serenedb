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

#include "multiterm_query.hpp"

#include "basics/containers/bitset.hpp"
#include "basics/shared.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/bitset_doc_iterator.hpp"
#include "iresearch/search/disjunction.hpp"
#include "iresearch/search/min_match_disjunction.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"

namespace {

using namespace irs;

class LazyBitsetIterator : public BitsetDocIterator {
 public:
  LazyBitsetIterator(const SubReader& segment, const TermReader& field,
                     std::span<const MultiTermState::UnscoredTermState> states,
                     CostAttr::Type estimation) noexcept
    : BitsetDocIterator(estimation),
      _field(&field),
      _segment(&segment),
      _states(states) {
    SDB_ASSERT(!_states.empty());
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return irs::Type<ScoreAttr>::id() == id ? &_score
                                            : BitsetDocIterator::GetMutable(id);
  }

 protected:
  bool refill(const word_t** begin, const word_t** end) final;

 private:
  ScoreAttr _score;
  std::unique_ptr<word_t[]> _set;
  const TermReader* _field;
  const SubReader* _segment;
  std::span<const MultiTermState::UnscoredTermState> _states;
};

bool LazyBitsetIterator::refill(const word_t** begin, const word_t** end) {
  if (!_field) {
    return false;
  }

  const size_t bits = _segment->docs_count() + irs::doc_limits::min();
  const size_t words = bitset::bits_to_words(bits);
  _set = std::make_unique<word_t[]>(words);
  std::memset(_set.get(), 0, sizeof(word_t) * words);

  auto provider = [begin = _states.begin(),
                   end =
                     _states.end()]() mutable noexcept -> const SeekCookie* {
    if (begin != end) {
      auto* cookie = begin->get();
      ++begin;
      return cookie;
    }
    return nullptr;
  };

  const size_t count = _field->BitUnion(provider, _set.get());
  _field = nullptr;

  if (count) {
    // we don't want to emit doc_limits::invalid()
    // ensure first bit isn't set,
    SDB_ASSERT(!irs::CheckBit(_set[0], 0));

    *begin = _set.get();
    *end = _set.get() + words;
    return true;
  }

  return false;
}

}  // namespace

namespace irs {

void MultiTermQuery::visit(const SubReader& segment,
                           PreparedStateVisitor& visitor, score_t boost) const {
  if (auto state = _states.find(segment)) {
    visitor.Visit(*this, *state, boost * _boost);
  }
}

DocIterator::ptr MultiTermQuery::execute(const ExecutionContext& ctx) const {
  auto& segment = ctx.segment;
  auto& ord = ctx.scorers;

  // get term state for the specified reader
  auto state = _states.find(segment);

  if (!state ||
      (state->scored_states.empty() && state->unscored_states.empty())) {
    // invalid state
    return DocIterator::empty();
  }

  auto* reader = state->reader;
  SDB_ASSERT(reader);

  // Get required features
  const IndexFeatures features = ord.features();
  const std::span stats{_stats};

  // add an iterator for each of the scored states
  const bool no_score = ord.empty();
  const bool has_unscored_terms = !state->unscored_states.empty();

  IteratorOptions options{ctx.wand};
  uint32_t current_cookie_idx = 0;

  auto make_score = [&](uint32_t cookie_idx,
                        const AttributeProvider& cookie_attrs) {
    SDB_ASSERT(cookie_idx < state->scored_states.size());
    cookie_idx = current_cookie_idx + cookie_idx;
    auto& entry = state->scored_states[cookie_idx];
    SDB_ASSERT(entry.stat_offset < stats.size());
    auto* stat = stats[entry.stat_offset].c_str();
    const auto boost = entry.boost * _boost;
    return ord.buckets().front().bucket->PrepareScorer(
      segment, state->reader->meta(), stat, cookie_attrs, boost);
  };
  if (!no_score && options.Enabled() && ord.buckets().front().bucket) {
    options.make_score = make_score;
  }

  auto compile_score = [&](uint32_t cookie_idx,
                           AttributeProvider& cookie_attrs) {
    auto* score = irs::GetMutable<ScoreAttr>(&cookie_attrs);
    SDB_ASSERT(score);
    cookie_idx = current_cookie_idx + cookie_idx;
    SDB_ASSERT(cookie_idx < state->scored_states.size());
    auto& entry = state->scored_states[cookie_idx];
    SDB_ASSERT(entry.stat_offset < stats.size());
    auto* stat = stats[entry.stat_offset].c_str();
    const auto boost = entry.boost * _boost;
    CompileScore(*score, ord.buckets(), segment, *state->reader, stat,
                 cookie_attrs, boost);
  };
  if (!no_score) {
    options.compile_score = compile_score;
  }

  if (!has_unscored_terms) {
    std::vector<const SeekCookie*> cookies;
    cookies.reserve(state->scored_states.size());
    for (auto& entry : state->scored_states) {
      SDB_ASSERT(entry.cookie);
      cookies.push_back(entry.cookie.get());
    }

    auto docs = reader->Iterator(features, cookies, options, _min_match,
                                 _merge_type, ord.buckets().size());
    return docs ? std::move(docs) : DocIterator::empty();
  }

  ScoreAdapters itrs(state->scored_states.size() + size_t(has_unscored_terms));
  auto it = std::begin(itrs);

  for (auto& entry : state->scored_states) {
    SDB_ASSERT(entry.cookie);
    auto docs = reader->Iterator(features, *entry.cookie, options);
    ++current_cookie_idx;
    if (!docs) [[unlikely]] {
      continue;
    }

    SDB_ASSERT(it != std::end(itrs));
    *it = std::move(docs);
    ++it;
  }

  if (has_unscored_terms) {
    DocIterator::ptr docs = memory::make_managed<LazyBitsetIterator>(
      segment, *state->reader, state->unscored_states,
      state->unscored_states_estimation);

    SDB_ASSERT(it != std::end(itrs));
    *it = std::move(docs);
    ++it;
  }

  itrs.erase(it, std::end(itrs));

  return ResolveMergeType(
    _merge_type, ord.buckets().size(), [&]<typename A>(A&& aggregator) {
      using Disjunction = MinMatchIterator<ScoreAdapter, A>;
      return MakeWeakDisjunction<Disjunction>(ctx.wand, std::move(itrs),
                                              _min_match, std::move(aggregator),
                                              state->estimation());
    });
}

}  // namespace irs
