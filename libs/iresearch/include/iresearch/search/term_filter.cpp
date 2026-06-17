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

#include "term_filter.hpp"

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/term_query.hpp"

namespace irs {
namespace {

SeekTermIterator::ptr GetTermsIterator(const TermReader& field,
                                       const bytes_view term) {
  auto terms = field.iterator(SeekMode::RandomOnly);
  if (!terms) [[unlikely]] {
    return nullptr;
  }
  if (!terms->seek(term)) {
    return nullptr;
  }
  terms->read();
  return terms;
}

}  // namespace

void ByTerm::Visit(const SubReader& segment, const TermReader& field,
                   const bytes_view term, FilterVisitor& visitor) {
  auto terms = GetTermsIterator(field, term);
  if (!terms) {
    return;
  }
  visitor.Prepare(segment, field, *terms);
  visitor.Visit(kNoBoost);
}

QueryBuilder::ptr ByTerm::PrepareSegment(const SubReader& segment,
                                         const PrepareContext& ctx,
                                         const irs::field_id field,
                                         const bytes_view term) {
  const auto* reader = segment.field(field);
  if (!reader) {
    // field absent in this segment: a boost-carrying empty query so the boost
    // is still observable and consistent with the multi-term path
    return memory::make_tracked<TermQuery>(
      ctx.memory, segment, TermState{nullptr, nullptr}, ctx.boost);
  }
  auto& collector = sdb::basics::downCast<TermsCollector>(*ctx.collector);
  collector.Field().Collect(*reader);
  auto terms = GetTermsIterator(*reader, term);
  if (!terms) {
    return memory::make_tracked<TermQuery>(
      ctx.memory, segment, TermState{nullptr, nullptr}, ctx.boost);
  }
  collector.Terms().Collect(0, *terms);
  TermState state{reader, terms->cookie()};
  return memory::make_tracked<TermQuery>(ctx.memory, segment, std::move(state),
                                         ctx.boost);
}

PrepareCollector::ptr ByTerm::MakeCollector(const Scorer* scorer) const {
  return std::make_unique<TermsCollector>(scorer, 1);
}

}  // namespace irs
