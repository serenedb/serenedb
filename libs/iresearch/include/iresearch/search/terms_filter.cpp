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
#include "iresearch/search/all_terms_collector.hpp"
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

template<typename Collector>
class TermsVisitor {
 public:
  explicit TermsVisitor(Collector& collector) noexcept
    : _collector(collector) {}

  void SetIndex(uint32_t term_idx) noexcept { _collector.stat_index(term_idx); }

  void Prepare(const SubReader& segment, const TermReader& field,
               const SeekTermIterator& terms) {
    _collector.Prepare(segment, field, terms);
  }

  void Visit(score_t boost) { _collector.Visit(boost); }

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

  FieldCollector field_stats;
  TermCollectorsFlat term_stats{ctx.scorer, size};
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
    stat.resize(GetStatsSize(ctx.scorer), 0);
    auto* stats_buf = stat.data();
    term_stats.Finish(stats_buf, term_idx++, &field_stats);
  }

  return memory::make_tracked<MultiTermQuery>(ctx.memory, std::move(states),
                                              std::move(stats), ctx.boost,
                                              merge_type, min_match);
}

Filter::Query::ptr ByTerms::prepare(const PrepareContext& ctx) const {
  SDB_ASSERT(options().min_match != 0 || options().terms.empty());
  return Prepare(ctx.Boost(Boost()), field(), options());
}

}  // namespace irs
