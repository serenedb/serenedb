////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "granular_range_filter.hpp"

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/multiterm_collector.hpp"
#include "iresearch/search/detail/term_iterator.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"

namespace irs {
namespace {

uint64_t VisitInto(MultiTermQuery& query, const PrepareContext& ctx,
                   const TermReader& reader, bytes_view from, bytes_view to) {
  MultiTermVisitor mtv{ctx, query.State(), reader};
  uint64_t docs = 0;
  bool pieces = false;
  reader.VisitRange(from, to,
                    [&](bytes_view term, const PostingMeta& meta, bool piece) {
                      pieces |= piece;
                      docs += meta.docs_count;
                      mtv.Visit(term, meta, kNoBoost);
                    });
  if (pieces) {
    query.State().MarkPieces();
  }
  return docs;
}

QueryBuilder::ptr Complement(const SubReader& segment,
                             const PrepareContext& ctx,
                             const TermReader& reader, bytes_view from,
                             bytes_view to, uint64_t inside) {
  const auto& meta = reader.meta();
  if (ctx.KeepsTerms() || !reader.single_valued ||
      meta.dict.layout != DictLayout::Numeric) {
    return nullptr;
  }
  const auto* nulls = segment.field(
    meta.id, {.layout = meta.dict.layout, .member = DictMember::Nulls});
  const uint64_t null_docs = nulls != nullptr ? nulls->docs_count() : 0;
  const uint64_t values = reader.docs_count();
  if (values + null_docs != segment.docs_count()) {
    return nullptr;
  }
  const uint64_t outside = values - inside + null_docs;
  if (2 * inside <= 3 * outside) {
    return nullptr;
  }
  if (inside == segment.docs_count()) {
    return memory::make_tracked<AllQuery>(ctx.memory, segment, ctx.boost);
  }
  auto tails = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, kNoBoost, ScoreMergeType::Sum);
  if (!from.empty()) {
    VisitInto(*tails, ctx, reader, {}, from);
  }
  if (!to.empty()) {
    VisitInto(*tails, ctx, reader, to, {});
  }
  BooleanBuilder builder{segment, ctx.memory, 0, ctx.boost,
                         ScoreMergeType::Sum};
  builder.Add(memory::make_tracked<AllQuery>(ctx.memory, segment, kNoBoost),
              Occur::Must);
  builder.Add(MultiTermQuery::Finish(std::move(tails), ctx), Occur::MustNot);
  if (null_docs != 0) {
    builder.AddTerm(nulls, nulls->Lookup(kSlotTerm), kNoBoost, Occur::MustNot,
                    {});
  }
  return builder.Finish();
}

}  // namespace

QueryBuilder::ptr ByGranularRange::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  auto sub_ctx = ctx;
  sub_ctx.boost *= GetBoost();
  return PrepareSegment(segment, sub_ctx, segment.field(field_id(), dict()),
                        options());
}

QueryBuilder::ptr ByGranularRange::PrepareSegment(const SubReader& segment,
                                                  const PrepareContext& ctx,
                                                  const TermReader* reader,
                                                  const options_type& options) {
  const auto& rng = options.range;
  if (rng.max_type == BoundType::Exclusive && rng.max.empty()) {
    return QueryBuilder::Empty();
  }
  if (!reader) {
    return QueryBuilder::Empty();
  }
  const bool has_min = rng.min_type != BoundType::Unbounded && !rng.min.empty();
  const bool has_max = rng.max_type != BoundType::Unbounded && !rng.max.empty();
  const bstring from = has_min ? RangeLower(rng.min, rng.min_type) : bstring{};
  const bstring to = has_max ? RangeUpper(rng.max, rng.max_type) : bstring{};
  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost, ScoreMergeType::Sum);
  const auto inside = VisitInto(*query, ctx, *reader, from, to);
  if (auto complement = Complement(segment, ctx, *reader, from, to, inside)) {
    return complement;
  }
  return MultiTermQuery::Finish(std::move(query), ctx);
}

PrepareCollector::ptr ByGranularRange::MakeCollectorImpl(
  const Scorer* scorer, StatsArena& stats, uint32_t threads) const {
  if (!ScoresPerDoc(scorer)) {
    return std::make_unique<AllCollector>(scorer, stats);
  }
  return std::make_unique<MultiTermCollector>(scorer, stats, threads);
}

}  // namespace irs
