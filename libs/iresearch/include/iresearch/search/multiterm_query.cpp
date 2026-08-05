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
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/bitset_doc_iterator.hpp"
#include "iresearch/search/make_disjunction.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {
namespace {

class LazyBitsetIterator : public BitsetDocIterator {
 public:
  LazyBitsetIterator(const SubReader& segment, const TermReader& field,
                     std::span<const MultiTermState::Entry> terms,
                     CostAttr::Type estimation) noexcept
    : BitsetDocIterator(estimation),
      _field(&field),
      _segment(&segment),
      _terms(terms) {
    SDB_ASSERT(!_terms.empty());
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return BitsetDocIterator::GetMutable(id);
  }

 protected:
  bool refill(const word_t** begin, const word_t** end) final;

 private:
  std::unique_ptr<word_t[]> _set;
  const TermReader* _field;
  const SubReader* _segment;
  std::span<const MultiTermState::Entry> _terms;
};

bool LazyBitsetIterator::refill(const word_t** begin, const word_t** end) {
  if (!_field) {
    return false;
  }

  const size_t bits = _segment->docs_count() + irs::doc_limits::min();
  const size_t words = bitset::bits_to_words(bits);
  _set = std::make_unique<word_t[]>(words);
  std::memset(_set.get(), 0, sizeof(word_t) * words);

  auto provider = [begin = _terms.begin(),
                   end = _terms.end()] mutable noexcept -> const PostingMeta* {
    while (begin != end) {
      const auto& entry = *begin++;
      if (entry.stat_offset == MultiTermState::kUnscored) {
        return &entry.cookie;
      }
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

void MultiTermQuery::Visit(PreparedStateVisitor& visitor, score_t boost) const {
  visitor.Visit(*this, _state, boost * _boost);
}

DocIterator::ptr MultiTermQuery::Execute(const ExecutionContext& ctx,
                                         const StatsBuffer& stats) const {
  if (_state.Empty()) {
    // invalid state
    return DocIterator::empty();
  }

  // TODO(mbkkt) fold the mask into the pruning iterator during the deletes
  // rework and drop this.
  const bool score_prune = ctx.score_prune && _segment.docs_mask() == nullptr;

  auto* reader = _state.Reader();
  SDB_ASSERT(reader);

  // Get required features
  const auto* scorer = stats.GetScorer();
  const IndexFeatures features = GetFeatures(scorer);
  const std::span all_stats{stats.GetAllStats()};

  const auto& terms = _state.Terms();
  if (terms.size() < _min_match) {
    // fewer matched terms than required to satisfy min_match
    return DocIterator::empty();
  }

  // partition the collected terms into scored / unscored
  CostAttr::Type unscored_estimation = 0;
  CostAttr::Type total_estimation = 0;
  size_t scored_count = 0;
  for (const auto& entry : terms) {
    total_estimation += entry.cookie.docs_count;
    if (entry.stat_offset != MultiTermState::kUnscored) {
      ++scored_count;
    } else {
      unscored_estimation += entry.cookie.docs_count;
    }
  }

  const bool has_unscored_terms = scored_count != terms.size();

  if (!has_unscored_terms) {
    std::vector<PostingCookie> cookies;
    cookies.reserve(scored_count);
    for (const auto& entry : terms) {
      cookies.emplace_back(
        &entry.cookie, scorer ? all_stats[entry.stat_offset].c_str() : nullptr,
        entry.boost * _boost, reader->meta());
    }

    auto docs = reader->Iterator(features, cookies, score_prune, _min_match,
                                 scorer ? _merge_type : ScoreMergeType::Noop);
    return docs ? std::move(docs) : DocIterator::empty();
  }

  ScoreAdapters itrs(scored_count + size_t{1});
  auto it = std::begin(itrs);

  for (const auto& entry : terms) {
    if (entry.stat_offset == MultiTermState::kUnscored) {
      continue;
    }
    auto docs = reader->Iterator(
      features,
      {
        .cookie = &entry.cookie,
        .stats = scorer ? all_stats[entry.stat_offset].c_str() : nullptr,
        .boost = entry.boost * _boost,
        .field = reader->meta(),
      },
      score_prune);
    if (!docs) [[unlikely]] {
      continue;
    }

    SDB_ASSERT(it != std::end(itrs));
    *it = std::move(docs);
    ++it;
  }

  {
    DocIterator::ptr docs = memory::make_managed<LazyBitsetIterator>(
      _segment, *reader, terms, unscored_estimation);

    SDB_ASSERT(it != std::end(itrs));
    *it = std::move(docs);
    ++it;
  }

  itrs.erase(it, std::end(itrs));

  return ResolveMergeType(
    scorer ? _merge_type : ScoreMergeType::Noop,
    [&]<ScoreMergeType MergeType>() {
      using Disjunction = MinMatchIterator<ScoreAdapter, MergeType>;
      return MakeWeakDisjunction<Disjunction>(
        score_prune, static_cast<doc_id_t>(_segment.docs_count()),
        std::move(itrs), _min_match, total_estimation);
    });
}

}  // namespace irs
