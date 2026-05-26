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

#include "basics/down_cast.h"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/term_query.hpp"

namespace irs {

void ByTerm::visit(const SubReader& segment, const TermReader& field,
                   bytes_view term, FilterVisitor& visitor) {
  auto terms = field.iterator(SeekMode::RandomOnly);
  if (!terms) [[unlikely]] {
    return;
  }
  if (!terms->seek(term)) {
    return;
  }
  visitor.Prepare(segment, field, *terms);
  terms->read();
  visitor.Visit(kNoBoost);
}

void ByTerm::Buffer::PrepareSegment(const SubReader& segment) {
  const auto* reader = segment.field(_field);
  if (!reader) {
    return;
  }
  _field_stats.collect(segment, *reader);

  auto terms = reader->iterator(SeekMode::RandomOnly);
  if (!terms) [[unlikely]] {
    return;
  }
  if (!terms->seek(_term)) {
    return;
  }
  terms->read();
  _term_stats.collect(segment, *reader, 0, *terms);
  auto& state = _states.insert(segment);
  state.reader = reader;
  state.cookie = terms->cookie();
}

void ByTerm::Buffer::Merge(PrepareBuffer&& other) {
  auto& rhs = sdb::basics::downCast<Buffer>(other);
  _field_stats.collect(std::move(rhs._field_stats));
  _term_stats.collect(std::move(rhs._term_stats));
  _states.Merge(std::move(rhs._states));
}

Filter::Query::ptr ByTerm::Buffer::Compile(const PrepareContext& ctx) && {
  bstring stats(GetStatsSize(ctx.scorer), 0);
  _term_stats.finish(stats.data(), 0, _field_stats, ctx.index);
  return memory::make_tracked<TermQuery>(ctx.memory, std::move(_states),
                                         std::move(stats), _boost);
}

Filter::Query::ptr ByTerm::prepare(const PrepareContext& ctx,
                                   std::string_view field, bytes_view term) {
  Buffer buf{ctx, field, term};
  for (const auto& segment : ctx.index) {
    buf.PrepareSegment(segment);
  }

#ifndef SDB_GTEST  // TODO(mbkkt) adjust tests
  if (buf.Empty()) {
    return Query::empty();
  }
#endif

  return std::move(buf).Compile(ctx);
}

}  // namespace irs
