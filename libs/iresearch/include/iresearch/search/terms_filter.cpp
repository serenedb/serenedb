////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#include "terms_filter.hpp"

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/all_terms_visitor.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/term_filter.hpp"

namespace irs {
namespace {

template<typename Visitor>
void VisitImpl(ByTermsIterator& terms, Visitor& visitor) {
  for (;;) {
    if constexpr (requires { visitor.SetIndex(terms.Index()); }) {
      visitor.SetIndex(terms.Index());
    }
    if (!visitor.Visit(terms.Boost())) {
      return;
    }
    if (!terms.next()) {
      return;
    }
  }
}

}  // namespace

ByTermsIterator::ByTermsIterator(const TermReader& reader,
                                 const ByTermsOptions::search_terms& terms)
  : _impl{reader.iterator(SeekMode::NORMAL)},
    _cursor{terms.begin()},
    _end{terms.end()} {
  if (!_impl || !AdvanceToMatch()) {
    _impl = SeekTermIterator::empty();
  }
}

ByTermsIterator::ByTermsIterator(const TermReader& reader,
                                 const ByTermsOptions& options)
  : ByTermsIterator{reader, options.terms} {}

void ByTerms::visit(const SubReader& segment, const TermReader& field,
                    const ByTermsOptions& options, FilterVisitor& visitor) {
  ByTermsIterator terms(field, options.terms);
  if (IsNull(terms.value())) {
    return;
  }
  visitor.Prepare(segment, field, terms.GetImpl());
  VisitImpl(terms, visitor);
}

QueryBuilder::ptr ByTerms::PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          irs::field_id field,
                                          const ByTermsOptions& options,
                                          score_t boost) {
  const auto& [terms, min_match, merge_type] = options;
  const size_t size = terms.size();
  SDB_ASSERT(size);
  SDB_ASSERT(min_match <= size);
  SDB_ASSERT(size > 1);
  SDB_ASSERT(min_match != 0);

  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost * boost, merge_type, min_match);

  const auto* reader = segment.field(field);
  if (!reader) {
    return query;
  }

  auto* collector = ctx.collector
                      ? &sdb::basics::downCast<ByTermsCollector>(*ctx.collector)
                      : nullptr;
  AllTermsVisitor mtv{query->State(), collector ? &collector->Field() : nullptr,
                      collector ? &collector->Terms() : nullptr};
  ByTermsIterator it(*reader, options.terms);
  if (!IsNull(it.value())) {
    mtv.Prepare(segment, *reader, it.GetImpl());
    VisitImpl(it, mtv);
  }
  return query;
}

QueryBuilder::ptr ByTerms::PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx) const {
  return PrepareSegment(segment, ctx, field_id(), options(), Boost());
}

PrepareCollector::ptr ByTerms::MakeCollector(const Scorer* scorer) const {
  return std::make_unique<ByTermsCollector>(scorer, options().terms.size());
}

}  // namespace irs
