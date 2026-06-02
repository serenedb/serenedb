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

// Per-segment additive visitor: every matched term gets its own stat slot.
class MultiTermsVisitor {
 public:
  MultiTermsVisitor(TermsCollector& collector, MultiTermState& state) noexcept
    : _collector(collector), _state(state) {}

  void SetIndex(uint32_t term_idx) noexcept { _stat_index = term_idx; }

  void Prepare(const SubReader& /*segment*/, const TermReader& field,
               const SeekTermIterator& terms) {
    _collector.Field().Collect(field);
    _state.Prepare(&field);
    _terms = &terms;
  }

  void Visit(score_t boost) {
    SDB_ASSERT(_terms);
    _collector.Terms().Collect(_stat_index, *_terms);

    auto* meta = irs::get<TermMeta>(*_terms);
    _state.Push(MultiTermState::Entry{
      .cookie = _terms->cookie(),
      .docs_count = meta ? meta->docs_count : 0,
      .boost = boost,
      .stat_offset = _stat_index,
    });
  }

 private:
  TermsCollector& _collector;
  MultiTermState& _state;
  const SeekTermIterator* _terms = nullptr;
  uint32_t _stat_index = 0;
};

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
  MultiTermsVisitor mtv{collector, query->State()};
  VisitImpl(segment, *reader, terms, mtv);
  return query;
}

QueryBuilder::ptr ByTerms::PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx) const {
  if (!options().terms.empty() && options().min_match == 0) {
    // "match all documents"; the scored variant (All OR terms) needs the
    // boolean Or filter, which is not yet migrated to PrepareSegment.
    return MakeAllDocsFilter(kNoBoost)->PrepareSegment(segment, ctx);
  }
  return PrepareSegment(segment, ctx.Boost(Boost()), field(), options());
}

PrepareCollector::ptr ByTerms::MakeCollector(const Scorer* scorer) const {
  if (!options().terms.empty() && options().min_match == 0) {
    return MakeAllDocsFilter(kNoBoost)->MakeCollector(scorer);
  }
  return std::make_unique<TermsCollector>(scorer, options().terms.size());
}

}  // namespace irs
