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
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/limited_sample_selector.hpp"
#include "iresearch/search/multiterm_query.hpp"

namespace irs {

AutomatonOptions::AutomatonOptions(bytes_view pattern, PatternKind kind,
                                   RegexpSyntax syntax,
                                   size_t scored_terms_limit)
  : pattern{pattern},
    source{MakePatternSource(bstring{pattern}, kind, syntax)},
    kind{kind},
    scored_terms_limit{scored_terms_limit} {}

AutomatonOptions::AutomatonOptions(bytes_view pattern,
                                   TermAcceptorSource::ptr source,
                                   size_t scored_terms_limit)
  : pattern{pattern},
    source{std::move(source)},
    kind{PatternKind::Fused},
    scored_terms_limit{scored_terms_limit} {}

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

QueryBuilder::ptr PrepareAcceptorSegment(const SubReader& segment,
                                         const PrepareContext& ctx,
                                         irs::field_id field,
                                         const TermAcceptorSource& source,
                                         score_t boost) {
  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost * boost, ScoreMergeType::Sum,
    size_t{1});

  const auto* reader = segment.field(field);
  if (!reader) {
    return query;
  }

  auto* collector =
    ctx.collector
      ? &sdb::basics::downCast<LimitedTermsCollector>(*ctx.collector)
      : nullptr;
  if (collector) {
    collector->Field().Collect(*reader);
  }
  SampledMultiTermVisitor mtv{collector ? &collector->Limited() : nullptr,
                              query->State()};
  auto terms = source.Iterator(*reader);
  SDB_ASSERT(terms);
  if (!terms->next()) {
    return query;
  }
  mtv.Prepare(segment, *reader, *terms);
  VisitTerms(*terms, mtv);
  return query;
}

QueryBuilder::ptr AutomatonFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  SDB_ASSERT(options().source);
  return PrepareAcceptorSegment(segment, ctx, field_id(), *options().source,
                                Boost());
}

PrepareCollector::ptr AutomatonFilter::MakeCollector(
  const Scorer* scorer) const {
  return std::make_unique<LimitedTermsCollector>(scorer,
                                                 options().scored_terms_limit);
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
  return options().source->Iterator(reader);
}

}  // namespace irs
