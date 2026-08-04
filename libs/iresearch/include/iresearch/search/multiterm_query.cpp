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

// The unscored terms of a multi-term query, unioned into one bitset instead of
// one iterator each. The union is per row group, in that row group's local id
// space -- the currency every other leaf of the query tree runs in.
class LazyBitsetIterator : public BitsetDocIterator {
 public:
  LazyBitsetIterator(const SubReader& segment, const TermReader& field,
                     std::vector<const TermCookie*>&& cookies,
                     CostAttr::Type estimation, uint32_t rg) noexcept
    : BitsetDocIterator(estimation),
      _field(&field),
      _segment(&segment),
      _cookies(std::move(cookies)),
      _rg(rg) {
    SDB_ASSERT(!_cookies.empty());
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
  std::vector<const TermCookie*> _cookies;
  uint32_t _rg;
};

bool LazyBitsetIterator::refill(const word_t** begin, const word_t** end) {
  if (!_field) {
    return false;
  }

  const size_t bits = _segment->RowGroups().Rows(_rg) + irs::doc_limits::min();
  const size_t words = bitset::bits_to_words(bits);
  _set = std::make_unique<word_t[]>(words);
  std::memset(_set.get(), 0, sizeof(word_t) * words);

  auto provider = [begin = _cookies.begin(),
                   end =
                     _cookies.end()]() mutable noexcept -> const TermCookie* {
    if (begin != end) {
      auto* cookie = *begin;
      ++begin;
      return cookie;
    }
    return nullptr;
  };

  const size_t count = _field->RowGroupBitUnion(provider, _rg, _set.get());
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
  std::vector<const TermCookie*> unscored;
  CostAttr::Type unscored_estimation = 0;
  CostAttr::Type total_estimation = 0;
  size_t scored_count = 0;
  for (const auto& entry : terms) {
    total_estimation += entry.docs_count;
    if (entry.stat_offset != MultiTermState::kUnscored) {
      ++scored_count;
    } else {
      SDB_ASSERT(!entry.cookie.rgs.empty());
      unscored.emplace_back(&entry.cookie);
      unscored_estimation += entry.docs_count;
    }
  }

  const bool has_unscored_terms = !unscored.empty();

  if (!has_unscored_terms) {
    std::vector<TermLeaf> leaves;
    leaves.reserve(scored_count);
    for (const auto& entry : terms) {
      SDB_ASSERT(!entry.cookie.rgs.empty());
      leaves.emplace_back(
        &entry.cookie, scorer ? all_stats[entry.stat_offset].c_str() : nullptr,
        entry.boost * _boost, reader->meta());
    }

    auto docs =
      reader->RowGroupIterator(features, leaves, ctx.rg, ctx.wand, _min_match,
                               scorer ? _merge_type : ScoreMergeType::Noop);
    return docs ? std::move(docs) : DocIterator::empty();
  }

  ScoreAdapters itrs(scored_count + size_t{1});
  auto it = std::begin(itrs);

  for (const auto& entry : terms) {
    if (entry.stat_offset == MultiTermState::kUnscored) {
      continue;
    }
    SDB_ASSERT(!entry.cookie.rgs.empty());
    auto docs = reader->RowGroupIterator(
      features,
      {
        .cookie = &entry.cookie,
        .stats = scorer ? all_stats[entry.stat_offset].c_str() : nullptr,
        .boost = entry.boost * _boost,
        .field = reader->meta(),
      },
      ctx.rg, ctx.wand);
    if (!docs) [[unlikely]] {
      continue;
    }

    SDB_ASSERT(it != std::end(itrs));
    *it = std::move(docs);
    ++it;
  }

  {
    DocIterator::ptr docs = memory::make_managed<LazyBitsetIterator>(
      _segment, *reader, std::move(unscored), unscored_estimation, ctx.rg);

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
        ctx.wand, static_cast<doc_id_t>(_segment.docs_count()), std::move(itrs),
        _min_match, total_estimation);
    });
}

}  // namespace irs
