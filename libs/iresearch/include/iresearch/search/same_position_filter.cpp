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

#include "basics/misc.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/conjunction.hpp"
#include "iresearch/search/states/term_state.hpp"
#include "iresearch/search/states_cache.hpp"

namespace irs {
namespace {

template<typename Conjunction>
class SamePositionIterator : public Conjunction {
 public:
  typedef std::vector<PosAttr::ref> PositionsT;

  template<typename... Args>
  SamePositionIterator(PositionsT&& pos, Args&&... args)
    : Conjunction{std::forward<Args>(args)...}, _pos(std::move(pos)) {
    SDB_ASSERT(!_pos.empty());
  }

  doc_id_t advance() final {
    while (true) {
      const auto doc = Conjunction::advance();
      if (doc_limits::eof(doc) || FindSamePosition()) {
        return doc;
      }
    }
  }

  doc_id_t seek(doc_id_t target) final {
    if (const auto doc = this->value(); target <= doc) [[unlikely]] {
      return doc;
    }
    const auto doc = Conjunction::seek(target);
    if (doc_limits::eof(doc) || FindSamePosition()) {
      return doc;
    }
    return advance();
  }

  uint32_t count() final { return DocIterator::Count(*this); }

 private:
  bool FindSamePosition() {
    auto target = pos_limits::min();

    for (auto begin = _pos.begin(), end = _pos.end(); begin != end;) {
      PosAttr& pos = *begin;

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

  PositionsT _pos;
};

class SamePositionQuery : public Filter::Query {
 public:
  using TermsStatesT = ManagedVector<TermState>;
  using StatesT = StatesCache<TermsStatesT>;
  using StatsT = ManagedVector<bstring>;

  explicit SamePositionQuery(StatesT&& states, StatsT&& stats, score_t boost)
    : _states{std::move(states)}, _stats{std::move(stats)}, _boost{boost} {}

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {
    // FIXME(gnusi): implement
  }

  DocIterator::ptr execute(const ExecutionContext& ctx) const final {
    auto& segment = ctx.segment;
    auto& ord = ctx.scorers;

    // get query state for the specified reader
    auto query_state = _states.find(segment);
    if (!query_state) {
      // invalid state
      return DocIterator::empty();
    }

    // get features required for query & order
    const IndexFeatures features =
      ord.features() | BySamePosition::kRequiredFeatures;

    ScoreAdapters itrs;
    itrs.reserve(query_state->size());

    std::vector<PosAttr::ref> positions;
    positions.reserve(itrs.size());

    const bool no_score = ord.empty();
    auto term_stats = _stats.begin();
    for (auto& term_state : *query_state) {
      auto* reader = term_state.reader;
      SDB_ASSERT(reader);

      // get postings
      auto docs = reader->postings(*term_state.cookie, features);
      SDB_ASSERT(docs);

      // get needed postings attributes
      auto* pos = irs::GetMutable<PosAttr>(docs.get());

      if (!pos) {
        // positions not found
        return DocIterator::empty();
      }

      positions.emplace_back(std::ref(*pos));

      if (!no_score) {
        auto* score = irs::GetMutable<irs::ScoreAttr>(docs.get());
        SDB_ASSERT(score);

        CompileScore(*score, ord.buckets(), segment, *term_state.reader,
                     term_stats->c_str(), *docs, _boost);
      }

      // add iterator
      itrs.emplace_back(std::move(docs));

      ++term_stats;
    }

    return irs::ResolveMergeType(
      irs::ScoreMergeType::Sum, ord.buckets().size(),
      [&]<typename A>(A&& aggregator) -> DocIterator::ptr {
        return MakeConjunction<SamePositionIterator>(
          // TODO(mbkkt) Implement wand?
          {}, std::move(aggregator), std::move(itrs), std::move(positions));
      });
  }

  score_t Boost() const noexcept final { return _boost; }

 private:
  StatesT _states;
  StatsT _stats;
  score_t _boost;
};

}  // namespace

Filter::Query::ptr BySamePosition::prepare(const PrepareContext& ctx) const {
  auto& terms = options().terms;
  const auto size = terms.size();

  if (0 == size) {
    // empty field or phrase
    return Filter::Query::empty();
  }

  // per segment query state
  SamePositionQuery::StatesT query_states{ctx.memory, ctx.index.size()};

  // per segment terms states
  SamePositionQuery::StatesT::state_type term_states{
    SamePositionQuery::StatesT::state_type::allocator_type{ctx.memory}};
  term_states.reserve(size);

  // !!! FIXME !!!
  // that's completely wrong, we have to collect stats for each field
  // instead of aggregating them using a single collector
  FieldCollectors field_stats(ctx.scorers);

  // prepare phrase stats (collector for each term)
  TermCollectors term_stats(ctx.scorers, size);

  for (const auto& segment : ctx.index) {
    size_t term_idx = 0;

    for (const auto& branch : terms) {
      Finally next_stats = [&term_idx]() noexcept { ++term_idx; };

      // get term dictionary for field
      const TermReader* field = segment.field(branch.first);
      if (!field) {
        continue;
      }

      // check required features
      if (kRequiredFeatures !=
          (field->meta().index_features & kRequiredFeatures)) {
        continue;
      }

      // collect field statistics once per segment
      field_stats.collect(segment, *field);

      // find terms
      SeekTermIterator::ptr term = field->iterator(SeekMode::NORMAL);

      if (!term->seek(branch.second)) {
        if (ctx.scorers.empty()) {
          break;
        } else {
          // continue here because we should collect
          // stats for other terms in phrase
          continue;
        }
      }

      term->read();  // read term attributes
      term_stats.collect(segment, *field, term_idx, *term);
      term_states.emplace_back(ctx.memory);

      auto& state = term_states.back();

      state.cookie = term->cookie();
      state.reader = field;
    }

    if (term_states.size() != terms.size()) {
      // we have not found all needed terms
      term_states.clear();
      continue;
    }

    auto& state = query_states.insert(segment);
    state = std::move(term_states);

    term_states.clear();
    term_states.reserve(terms.size());
  }

  // finish stats
  size_t term_idx = 0;
  SamePositionQuery::StatsT stats(
    size, SamePositionQuery::StatsT::allocator_type{ctx.memory});
  for (auto& stat : stats) {
    stat.resize(ctx.scorers.stats_size());
    auto* stats_buf = stat.data();
    term_stats.finish(stats_buf, term_idx++, field_stats, ctx.index);
  }

  return memory::make_tracked<SamePositionQuery>(
    ctx.memory, std::move(query_states), std::move(stats), ctx.boost * Boost());
}

}  // namespace irs
