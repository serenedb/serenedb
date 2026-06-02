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
#include "iresearch/search/boolean_filter.hpp"
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
                                          std::string_view field,
                                          const ByTermsOptions& options) {
  const auto& [terms, min_match, merge_type] = options;
  const size_t size = terms.size();

  if (0 == size || min_match > size) {
    // Empty or unreachable search criteria
    return QueryBuilder::Empty();
  }
  SDB_ASSERT(min_match != 0);

  if (1 == size) {
    const auto term = std::begin(terms);
    return ByTerm::PrepareSegment(segment, ctx.Boost(term->boost), field,
                                  term->term);
  }

  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost, merge_type, min_match);

  const auto* reader = segment.field(field);
  if (!reader) {
    return query;
  }

  auto& collector = sdb::basics::downCast<TermsCollector>(*ctx.collector);
  AllTermsVisitor mtv{query->State(), collector.Field(), collector.Terms()};
  VisitImpl(segment, *reader, terms, mtv);
  return query;
}

namespace {

// min_match == 0 means "match all documents". When scored, each matched term
// contributes to the score, so we model it as `All OR ByTerms(min_match=1)`.
// When unscored, the boolean optimizer collapses this back to a plain All.
std::unique_ptr<Or> MakeMatchAllDisjunction(const ByTerms& self) {
  auto disjunction = std::make_unique<Or>();
  disjunction->add(self.MakeAllDocsFilter(0.F));
  auto& terms = disjunction->add<ByTerms>();
  terms.boost(self.Boost());
  *terms.mutable_field() = self.field();
  *terms.mutable_options() = self.options();
  terms.mutable_options()->min_match = 1;
  return disjunction;
}

}  // namespace

QueryBuilder::ptr ByTerms::PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx) const {
  if (!options().terms.empty() && options().min_match == 0) {
    return MakeMatchAllDisjunction(*this)->PrepareSegment(segment, ctx);
  }
  return PrepareSegment(segment, ctx.Boost(Boost()), field(), options());
}

PrepareCollector::ptr ByTerms::MakeCollector(const Scorer* scorer) const {
  if (!options().terms.empty() && options().min_match == 0) {
    return MakeMatchAllDisjunction(*this)->MakeCollector(scorer);
  }
  return std::make_unique<TermsCollector>(scorer, options().terms.size());
}

}  // namespace irs
