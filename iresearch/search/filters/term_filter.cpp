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

#include <tuple>
#include <utility>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/term_iterator.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/filters/filter_visitor.hpp"
#include "iresearch/search/queries/term_query.hpp"

namespace irs {

QueryBuilder::ptr ByTerm::PrepareSegment(const SubReader& segment,
                                         const PrepareContext& ctx,
                                         const irs::field_id field,
                                         const bytes_view term) {
  const auto* reader = segment.field(field);
  if (!reader) {
    return QueryBuilder::Empty();
  }
  auto* const collector =
    ctx.collector ? &irs::utils::downCast<ByTermsCollector>(*ctx.collector)
                  : nullptr;
  if (collector) {
    SDB_ASSERT(collector->Size() == 1);
    collector->Field(ctx.thread).Collect(*reader);
  }
  const auto meta = reader->Lookup(term);
  if (meta.docs_count == 0) {
    return QueryBuilder::Empty();
  }
  if (collector) {
    collector->Term(ctx.thread, 0).Collect(meta);
    return MakeTermQuery(ctx.memory, segment, reader, meta, ctx.boost,
                         collector->Record(0));
  }
  if (meta.docs_count == segment.docs_count() && !ctx.needs_terms) {
    return MakeAllQuery(segment, ctx, kNoBoost);
  }
  return MakeTermQuery(ctx.memory, segment, reader, meta, ctx.boost);
}

PrepareCollector::ptr ByTerm::MakeCollectorImpl(const Scorer* scorer,
                                                StatsArena& stats,
                                                uint32_t threads) const {
  return std::make_unique<ByTermsCollector>(scorer, 1, stats, threads);
}

TermPredicate::ptr ByTerm::CompileTermPredicate() const {
  return MakeTermPredicate(TermAcceptor{options().term});
}

TermIterator::ptr ByTerm::CompileTermIterator(const TermReader& reader) const {
  return memory::make_managed<ByTermIterator>(reader, options().term);
}

}  // namespace irs
