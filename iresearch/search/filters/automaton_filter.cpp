////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "automaton_filter.hpp"

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/multiterm_collector.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/filters/filter_visitor.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"

namespace irs {

AutomatonOptions::AutomatonOptions(bytes_view pattern, PatternKind kind)
  : pattern{pattern}, source{MakePatternSource(pattern, kind)}, kind{kind} {}

AutomatonOptions::AutomatonOptions(bytes_view pattern,
                                   TermAcceptorSource::ptr source)
  : pattern{pattern}, source{std::move(source)}, kind{PatternKind::Fused} {}

field_visitor AutomatonFilter::visitor(TermAcceptorSource::ptr source) {
  return [source = std::move(source)](const SubReader& segment,
                                      const TermReader& field,
                                      FilterVisitor& visitor) {
    auto terms = source->Iterator(field);
    SDB_ASSERT(terms);
    if (!terms->next()) {
      return;
    }
    visitor.Prepare(segment, field, *terms);
    VisitTerms(*terms, visitor);
  };
}

QueryBuilder::ptr AutomatonFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  SDB_ASSERT(options().source);
  const auto* reader = segment.field(field_id());
  if (!reader) {
    return QueryBuilder::Empty();
  }

  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost * GetBoost(),
    ScoreMergeType::Sum);
  MultiTermVisitor mtv{ctx, query->State(), *reader};
  auto terms = options().source->Iterator(*reader);
  SDB_ASSERT(terms);
  if (terms->next()) {
    mtv.Prepare(segment, *reader, *terms);
    VisitTerms(*terms, mtv);
  }
  return MultiTermQuery::Finish(std::move(query), ctx);
}

PrepareCollector::ptr AutomatonFilter::MakeCollectorImpl(
  const Scorer* scorer, StatsArena& stats, uint32_t threads) const {
  if (!ScoresPerDoc(scorer)) {
    return std::make_unique<AllCollector>(scorer, stats);
  }
  return std::make_unique<MultiTermCollector>(scorer, stats, threads);
}

TermPredicate::ptr AutomatonFilter::CompileTermPredicate() const {
  if (!options().source) {
    return nullptr;
  }
  return options().source->Predicate();
}

TermIterator::ptr AutomatonFilter::CompileTermIterator(
  const TermReader& reader) const {
  if (!options().source) {
    return nullptr;
  }
  auto it = options().source->Iterator(reader);
  SDB_ASSERT(it);
  return it;
}

}  // namespace irs
