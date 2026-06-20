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
void VisitImpl(const SubReader& segment, const TermReader& field,
               const ByTermsOptions::search_terms& search_terms,
               Visitor& visitor) {
  auto terms = field.iterator(SeekMode::NORMAL);

  if (!terms) [[unlikely]] {
    return;
  }

  visitor.Prepare(segment, field, *terms);

  [[maybe_unused]] uint32_t idx = 0;
  for (auto& term : search_terms) {
    if constexpr (requires { visitor.SetIndex(idx); }) {
      visitor.SetIndex(idx++);
    }

    if (!terms->seek(term.term)) {
      continue;
    }

    terms->read();

    visitor.Visit(term.boost);
  }
}

}  // namespace

void ByTerms::visit(const SubReader& segment, const TermReader& field,
                    const ByTermsOptions::search_terms& terms,
                    FilterVisitor& visitor) {
  VisitImpl(segment, field, terms, visitor);
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
  VisitImpl(segment, *reader, terms, mtv);
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
