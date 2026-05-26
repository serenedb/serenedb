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
         const ByTermsOptions& options, score_t boost = kNoBoost)
    : BufferBase{ctx, options.terms.size(), options.merge_type,
                 options.min_match, boost},
      _field{field},
      _options{&options},
      _collector{_states, _field_stats, _term_stats},
      _visitor{_collector} {}

  void PrepareSegment(const SubReader& segment) final {
    auto* reader = segment.field(_field);
    if (!reader) {
      return;
    }
    VisitImpl(segment, *reader, _options->terms, _visitor);
  }

 private:
  std::string_view _field;
  const ByTermsOptions* _options;
  AllTermsCollector<MultiTermQuery::States> _collector;
  TermsVisitor<AllTermsCollector<MultiTermQuery::States>> _visitor;
};

class OwningBuffer final : public Filter::PrepareBuffer {
 public:
  OwningBuffer(const PrepareContext& ctx, std::string_view field,
               ByTermsOptions&& options, score_t boost = kNoBoost)
    : _options{std::move(options)}, _inner{ctx, field, _options, boost} {}

  void PrepareSegment(const SubReader& segment) final {
    _inner.PrepareSegment(segment);
  }

  void Merge(PrepareBuffer&& other) final {
    _inner.Merge(std::move(sdb::basics::downCast<OwningBuffer>(other)._inner));
  }

  bool Empty() const noexcept final { return _inner.Empty(); }

  Filter::Query::ptr Compile(const PrepareContext& ctx) && final {
    return std::move(_inner).Compile(ctx);
  }

 private:
  ByTermsOptions _options;  // declared before _inner so it outlives the borrow
  Buffer _inner;
};

class AllScoringBuffer final : public Filter::PrepareBuffer {
 public:
  AllScoringBuffer(const PrepareContext& ctx, std::string_view field,
                   const ByTermsOptions& options, score_t boost,
                   AllDocsProvider::Ptr all_docs) {
    _disj.add(std::move(all_docs));
    auto& terms = _disj.add<ByTerms>();
    terms.boost(boost);
    *terms.mutable_field() = field;
    *terms.mutable_options() = options;
    terms.mutable_options()->min_match = 1;
    _inner = _disj.CreateBuffer(ctx);
  }

  void PrepareSegment(const SubReader& segment) final {
    _inner->PrepareSegment(segment);
  }

  void Merge(PrepareBuffer&& other) final {
    _inner->Merge(
      std::move(*sdb::basics::downCast<AllScoringBuffer>(other)._inner));
  }

  bool Empty() const noexcept final { return _inner->Empty(); }

  Filter::Query::ptr Compile(const PrepareContext& ctx) && final {
    return std::move(*_inner).Compile(ctx);
  }

 private:
  Or _disj;  // declared before _inner so it outlives the borrow
  std::unique_ptr<PrepareBuffer> _inner;
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
  return Filter::PrepareWithBuffer<Buffer>(buf, ctx);
}

std::unique_ptr<Filter::PrepareBuffer> ByTerms::CreateBuffer(
  const PrepareContext& ctx, std::string_view field, ByTermsOptions options) {
  const auto& [terms, min_match, merge_type] = options;
  const size_t size = terms.size();
  if (0 == size || min_match > size) {
    return std::make_unique<Filter::EmptyBuffer>();
  }
  SDB_ASSERT(min_match != 0);
  if (1 == size) {
    const auto term = std::begin(terms);
    return std::make_unique<ByTerm::Buffer>(ctx, field, term->term,
                                            term->boost);
  }
  return std::make_unique<OwningBuffer>(ctx, field, std::move(options));
}

std::unique_ptr<Filter::PrepareBuffer> ByTerms::CreateBuffer(
  const PrepareContext& ctx) const {
  if (!options().terms.empty() && options().min_match == 0) {
    if (!ctx.scorer) {
      return MakeAllDocsFilter(kNoBoost)->CreateBuffer(ctx);
    }
    return std::make_unique<AllScoringBuffer>(ctx, field(), options(), Boost(),
                                              MakeAllDocsFilter(0.F));
  }
  return std::make_unique<Buffer>(ctx, field(), options(), Boost());
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
