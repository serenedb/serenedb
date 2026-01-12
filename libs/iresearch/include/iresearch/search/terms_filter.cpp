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
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/all_terms_collector.hpp"
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

  for (auto& term : search_terms) {
    if (!terms->seek(term.term)) {
      continue;
    }

    terms->read();

    visitor.Visit(term.boost);
  }
}

template<typename Collector>
class TermsVisitor {
 public:
  explicit TermsVisitor(Collector& collector) noexcept
    : _collector(collector) {}

  void Prepare(const SubReader& segment, const TermReader& field,
               const SeekTermIterator& terms) {
    _collector.Prepare(segment, field, terms);
    _collector.stat_index(0);
  }

  void Visit(score_t boost) {
    auto stat_index = _collector.stat_index();
    _collector.Visit(boost);
    _collector.stat_index(++stat_index);
  }

 private:
  Collector& _collector;
};

template<typename Collector>
void CollectTerms(const IndexReader& index, std::string_view field,
                  const ByTermsOptions::search_terms& terms,
                  Collector& collector) {
  TermsVisitor<Collector> visitor(collector);

  for (auto& segment : index) {
    auto* reader = segment.field(field);

    if (!reader) {
      continue;
    }

    VisitImpl(segment, *reader, terms, visitor);
  }
}

}  // namespace

void ByTerms::visit(const SubReader& segment, const TermReader& field,
                    const ByTermsOptions::search_terms& terms,
                    FilterVisitor& visitor) {
  VisitImpl(segment, field, terms, visitor);
}

Filter::Query::ptr ByTerms::Prepare(const PrepareContext& ctx,
                                    std::string_view field,
                                    const ByTermsOptions& options) {
  const auto& [terms, min_match, merge_type] = options;
  const size_t size = terms.size();

  if (0 == size || min_match > size) {
    // Empty or unreachable search criteria
    return Query::empty();
  }
  SDB_ASSERT(min_match != 0);

  if (1 == size) {
    const auto term = std::begin(terms);
    auto sub_ctx = ctx;
    sub_ctx.boost = ctx.boost * term->boost;
    return ByTerm::prepare(sub_ctx, field, term->term);
  }

  FieldCollectors field_stats{ctx.scorers};
  TermCollectors term_stats{ctx.scorers, size};
  MultiTermQuery::States states{ctx.memory, ctx.index.size()};
  AllTermsCollector collector{states, field_stats, term_stats};
  CollectTerms(ctx.index, field, terms, collector);

  // FIXME(gnusi): Filter out unmatched states during collection
  if (min_match > 1) {
    states.erase_if([min = min_match](const auto& state) noexcept {
      return state.scored_states.size() < min;
    });
  }

  if (states.empty()) {
    return Query::empty();
  }

  MultiTermQuery::Stats stats{{ctx.memory}};
  stats.resize(size);
  for (size_t term_idx = 0; auto& stat : stats) {
    stat.resize(ctx.scorers.stats_size(), 0);
    auto* stats_buf = stat.data();
    term_stats.finish(stats_buf, term_idx++, field_stats, ctx.index);
  }

  return memory::make_tracked<MultiTermQuery>(ctx.memory, std::move(states),
                                              std::move(stats), ctx.boost,
                                              merge_type, min_match);
}

Filter::Query::ptr ByTerms::prepare(const PrepareContext& ctx) const {
  if (options().terms.empty() || options().min_match != 0) {
    return Prepare(ctx.Boost(Boost()), field(), options());
  }
  if (ctx.scorers.empty()) {
    return MakeAllDocsFilter(kNoBoost)->prepare({
      .index = ctx.index,
      .memory = ctx.memory,
    });
  }
  Or disj;
  // Don't contribute to the score
  disj.add(MakeAllDocsFilter(0.F));
  // Reset min_match to 1
  auto& terms = disj.add<ByTerms>();
  terms.boost(Boost());
  *terms.mutable_field() = field();
  *terms.mutable_options() = options();
  terms.mutable_options()->min_match = 1;
  return disj.prepare({
    .index = ctx.index,
    .memory = ctx.memory,
    .scorers = ctx.scorers,
    .ctx = ctx.ctx,
  });
}

}  // namespace irs
