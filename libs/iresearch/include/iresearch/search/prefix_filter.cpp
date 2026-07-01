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
////////////////////////////////////////////////////////////////////////////////

#include "prefix_filter.hpp"

#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/limited_sample_selector.hpp"
#include "iresearch/search/multiterm_query.hpp"

namespace irs {
namespace {

template<typename Visitor>
void VisitImpl(ByPrefixIterator& terms, Visitor& visitor) {
  for (;;) {
    if (!visitor.Visit(kNoBoost)) {
      return;
    }
    if (!terms.next()) {
      return;
    }
  }
}

}  // namespace

ByPrefixIterator::ByPrefixIterator(const TermReader& reader,
                                   const ByPrefixOptions& options)
  : ByPrefixIterator{reader.iterator(SeekMode::NORMAL), options.term} {}

QueryBuilder::ptr ByPrefix::PrepareSegment(const SubReader& segment,
                                           const PrepareContext& ctx) const {
  auto sub_ctx = ctx;
  sub_ctx.boost *= Boost();
  return PrepareSegment(segment, sub_ctx, field_id(), options().term);
}

QueryBuilder::ptr ByPrefix::PrepareSegment(const SubReader& segment,
                                           const PrepareContext& ctx,
                                           const irs::field_id field,
                                           const bytes_view term) {
  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost, ScoreMergeType::Sum, size_t{1});

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
  if (auto impl = reader->iterator(SeekMode::NORMAL)) {
    ByPrefixIterator terms(std::move(impl), term);
    if (terms.value().data() != nullptr) {
      mtv.Prepare(segment, *reader, terms.GetImpl());
      VisitImpl(terms, mtv);
    }
  }
  return query;
}

PrepareCollector::ptr ByPrefix::MakeCollector(const Scorer* scorer) const {
  return std::make_unique<LimitedTermsCollector>(scorer,
                                                 options().scored_terms_limit);
}

void ByPrefix::visit(const SubReader& segment, const TermReader& reader,
                     const ByPrefixOptions& options, FilterVisitor& visitor) {
  auto impl = reader.iterator(SeekMode::NORMAL);
  if (!impl) {
    return;
  }
  ByPrefixIterator terms(std::move(impl), options.term);
  if (terms.value().data() == nullptr) {
    return;
  }
  visitor.Prepare(segment, reader, terms.GetImpl());
  VisitImpl(terms, visitor);
}

}  // namespace irs
