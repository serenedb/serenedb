////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "same_position_filter.hpp"

#include "basics/down_cast.h"
#include "basics/misc.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/conjunction.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {
namespace {

template<typename Conjunction>
class SamePositionIterator : public DocIterator {
 public:
  using Positions = std::vector<PosAttr*>;

  template<typename... Args>
  SamePositionIterator(ScoreMergeType merge_type, doc_id_t docs_count,
                       Positions&& pos, Args&&... args)
    : _approx{merge_type, docs_count, std::forward<Args>(args)...},
      _pos(std::move(pos)) {
    SDB_ASSERT(!_pos.empty());
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _approx.GetMutable(type);
  }

  doc_id_t advance() final {
    while (true) {
      const auto doc = _approx.advance();
      if (doc_limits::eof(doc) || FindSamePosition()) {
        return _doc = doc;
      }
    }
  }

  doc_id_t seek(doc_id_t target) final {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    const auto doc = _approx.seek(target);
    if (doc_limits::eof(doc) || FindSamePosition()) {
      return _doc = doc;
    }
    return advance();
  }

  doc_id_t LazySeek(doc_id_t target) final {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    const auto doc = _approx.LazySeek(target);
    if (target != doc) {
      return doc;
    }
    if (doc_limits::eof(doc) || FindSamePosition()) {
      return _doc = doc;
    }
    return doc + 1;
  }

  uint32_t count() final { return CountImpl(*this); }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    CollectImpl(*this, scorer, fetcher, collector);
  }

  std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                      uint64_t* mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final {
    return FillBlockImpl(*this, min, max, mask, score, match);
  }

 private:
  bool FindSamePosition() {
    auto target = pos_limits::min();

    for (auto begin = _pos.begin(), end = _pos.end(); begin != end;) {
      auto& pos = **begin;

      if (target != pos.seek(target)) {
        target = pos.value();
        if (pos_limits::eof(target)) {
          return false;
        }
        begin = _pos.begin();
      } else {
        ++begin;
      }
    }

    return true;
  }

  Conjunction _approx;
  Positions _pos;
};

class SamePositionQuery : public Filter::Query {
 public:
  using StatesT = BySamePosition::StatesT;
  using StatsT = ManagedVector<bstring>;

  explicit SamePositionQuery(StatesT&& states, StatsT&& stats, score_t boost)
    : _states{std::move(states)}, _stats{std::move(stats)}, _boost{boost} {}

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {
    // FIXME(gnusi): implement
  }

  DocIterator::ptr execute(const ExecutionContext& ctx) const final {
    auto& segment = ctx.segment;
    auto query_state = _states.find(segment);
    if (!query_state) {
      return DocIterator::empty();
    }

    const IndexFeatures features =
      GetFeatures(ctx.scorer) | BySamePosition::kRequiredFeatures;
    ScoreAdapters itrs;
    itrs.reserve(query_state->size());

    std::vector<PosAttr*> positions;
    positions.reserve(itrs.size());

    auto term_stats = _stats.begin();
    for (auto& term_state : *query_state) {
      auto* reader = term_state.reader;
      SDB_ASSERT(reader);

      auto docs =
        reader->Iterator(features, {.cookie = term_state.cookie.get()});
      if (!docs) {
        return DocIterator::empty();
      }

      auto* pos = irs::GetMutable<PosAttr>(docs.get());
      if (!pos) {
        return DocIterator::empty();
      }

      positions.emplace_back(pos);

      itrs.emplace_back(std::move(docs));

      ++term_stats;
    }

    // TODO(mbkkt) Implement wand?
    return MakeConjunction<SamePositionIterator>(
      ScoreMergeType::Noop, {}, static_cast<doc_id_t>(ctx.segment.docs_count()),
      std::move(itrs), std::move(positions));
  }

  score_t Boost() const noexcept final { return _boost; }

 private:
  StatesT _states;
  StatsT _stats;
  score_t _boost;
};

}  // namespace

void BySamePosition::Buffer::PrepareSegment(const SubReader& segment) {
  TermsStatesT term_states{TermsStatesT::allocator_type{*_memory}};
  term_states.reserve(_terms->size());

  size_t term_idx = 0;
  for (const auto& branch : *_terms) {
    Finally next_stats = [&term_idx]() noexcept { ++term_idx; };

    const TermReader* field = segment.field(branch.first);
    if (!field) {
      continue;
    }

    if (kRequiredFeatures !=
        (field->meta().index_features & kRequiredFeatures)) {
      continue;
    }

    _field_stats[term_idx].collect(segment, *field);

    SeekTermIterator::ptr term = field->iterator(SeekMode::NORMAL);

    if (!term->seek(branch.second)) {
      if (!_has_scorer) {
        return;
      }
      // continue here because we should collect stats for other terms
      continue;
    }

    term->read();
    _term_stats.collect(segment, *field, term_idx, *term);
    term_states.emplace_back(*_memory);
    auto& state = term_states.back();
    state.cookie = term->cookie();
    state.reader = field;
  }

  if (term_states.size() != _terms->size()) {
    return;
  }

  auto& state = _states.insert(segment);
  state = std::move(term_states);
}

void BySamePosition::Buffer::Merge(PrepareBuffer&& other) {
  auto& rhs = sdb::basics::downCast<Buffer>(other);
  SDB_ASSERT(_field_stats.size() == rhs._field_stats.size());
  for (size_t i = 0, n = _field_stats.size(); i < n; ++i) {
    _field_stats[i].collect(std::move(rhs._field_stats[i]));
  }
  _term_stats.collect(std::move(rhs._term_stats));
  _states.Merge(std::move(rhs._states));
}

Filter::Query::ptr BySamePosition::Buffer::Compile(
  const PrepareContext& ctx) && {
  const auto size = _terms->size();
  SamePositionQuery::StatsT stats(
    size, SamePositionQuery::StatsT::allocator_type{ctx.memory});
  size_t term_idx = 0;
  for (auto& stat : stats) {
    stat.resize(GetStatsSize(ctx.scorer));
    _term_stats.finish(stat.data(), term_idx, _field_stats[term_idx],
                       ctx.index);
    ++term_idx;
  }

  return memory::make_tracked<SamePositionQuery>(ctx.memory, std::move(_states),
                                                 std::move(stats), _boost);
}

}  // namespace irs
