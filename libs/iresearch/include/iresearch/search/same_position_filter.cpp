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
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/conjunction.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/states/term_state.hpp"

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

class SamePositionQuery : public QueryBuilder {
 public:
  using TermsStatesT = ManagedVector<TermState>;

  explicit SamePositionQuery(const SubReader& segment, TermsStatesT&& states,
                             score_t boost)
    : QueryBuilder{segment}, _states{std::move(states)}, _boost{boost} {}

  void Visit(PreparedStateVisitor&, score_t) const final {}

  DocIterator::ptr Execute(const ExecutionContext&,
                           const StatsBuffer& stats) const final {
    if (_states.empty()) {
      return DocIterator::empty();
    }

    const IndexFeatures features =
      GetFeatures(stats.GetScorer()) | BySamePosition::kRequiredFeatures;
    ScoreAdapters itrs;
    itrs.reserve(_states.size());

    std::vector<PosAttr*> positions;
    positions.reserve(_states.size());

    for (auto& term_state : _states) {
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
    }

    // TODO(mbkkt) Implement wand?
    return MakeConjunction<SamePositionIterator>(
      ScoreMergeType::Noop, {}, static_cast<doc_id_t>(_segment.docs_count()),
      std::move(itrs), std::move(positions));
  }

  score_t Boost() const noexcept final { return _boost; }

 private:
  TermsStatesT _states;
  score_t _boost;
};

}  // namespace

QueryBuilder::ptr BySamePosition::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  auto& terms = options().terms;
  const auto size = terms.size();

  if (0 == size) {
    // empty field or phrase
    return QueryBuilder::Empty();
  }

  auto* collector = ctx.collector
                      ? &sdb::basics::downCast<ByTermsCollector>(*ctx.collector)
                      : nullptr;

  SamePositionQuery::TermsStatesT term_states{
    SamePositionQuery::TermsStatesT::allocator_type{ctx.memory}};
  term_states.reserve(size);

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
    if (collector) {
      collector->Field().Collect(*field);
    }

    // find terms
    SeekTermIterator::ptr term = field->iterator(SeekMode::NORMAL);

    if (!term->seek(branch.second)) {
      continue;
    }

    term->read();  // read term attributes
    if (collector) {
      collector->Terms()[term_idx].Collect(*term);
    }
    term_states.emplace_back(field, term->cookie());
  }

  if (term_states.size() != terms.size()) {
    // we have not found all needed terms
    return QueryBuilder::Empty();
  }

  return memory::make_tracked<SamePositionQuery>(
    ctx.memory, segment, std::move(term_states), ctx.boost * Boost());
}

PrepareCollector::ptr BySamePosition::MakeCollector(
  const Scorer* scorer) const {
  return std::make_unique<ByTermsCollector>(scorer, options().terms.size());
}

}  // namespace irs
