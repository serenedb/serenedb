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

class Buffer final : public MultiTermQuery::BufferBase {
 public:
  Buffer(const PrepareContext& ctx, std::string_view field,
         const ByTermsOptions& options)
    : BufferBase{ctx, options.terms.size(), options.merge_type,
                 options.min_match},
      _field{field},
      _terms{&options.terms},
      _collector{_states, _field_stats, _term_stats},
      _visitor{_collector} {}

  void PrepareSegment(const SubReader& segment) final {
    auto* reader = segment.field(_field);
    if (!reader) {
      return;
    }
    VisitImpl(segment, *reader, *_terms, _visitor);
  }

 private:
  std::string_view _field;
  const ByTermsOptions::search_terms* _terms;
  AllTermsCollector<MultiTermQuery::States> _collector;
  TermsVisitor<AllTermsCollector<MultiTermQuery::States>> _visitor;
};

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

  Buffer buf{ctx, field, options};
  for (auto& segment : ctx.index) {
    buf.PrepareSegment(segment);
  }
  return std::move(buf).Compile(ctx);
}

std::unique_ptr<Filter::PrepareBuffer> ByTerms::CreateBuffer(
  const PrepareContext& ctx) const {
  return std::make_unique<Buffer>(ctx, field(), options());
}

Filter::Query::ptr ByTerms::prepare(const PrepareContext& ctx) const {
  if (options().terms.empty() || options().min_match != 0) {
    return Prepare(ctx.Boost(Boost()), field(), options());
  }
  if (!ctx.scorer) {
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
    .scorer = ctx.scorer,
    .ctx = ctx.ctx,
  });
}

}  // namespace irs
