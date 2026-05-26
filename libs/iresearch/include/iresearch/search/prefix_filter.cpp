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
#include "iresearch/search/limited_sample_collector.hpp"
#include "iresearch/search/states_cache.hpp"

namespace irs {
namespace {

template<typename Visitor>
void VisitImpl(const SubReader& segment, const TermReader& reader,
               bytes_view prefix, Visitor& visitor) {
  auto terms = reader.iterator(SeekMode::NORMAL);

  // seek to prefix
  if (!terms) [[unlikely]] {
    return;
  }
  if (SeekResult::End == terms->seek_ge(prefix)) {
    return;
  }

  auto* term = irs::get<TermAttr>(*terms);

  if (!term) [[unlikely]] {
    return;
  }

  if (term->value.starts_with(prefix)) {
    terms->read();

    visitor.Prepare(segment, reader, *terms);

    do {
      visitor.Visit(kNoBoost);

      if (!terms->next()) {
        break;
      }

      terms->read();
    } while (term->value.starts_with(prefix));
  }
}

}  // namespace

void ByPrefix::Buffer::PrepareSegment(const SubReader& segment) {
  if (const auto* reader = segment.field(_field); reader) {
    VisitImpl(segment, *reader, _prefix, _visitor);
  }
}

Filter::Query::ptr ByPrefix::Prepare(const PrepareContext& ctx,
                                     std::string_view field, bytes_view prefix,
                                     size_t scored_terms_limit) {
  Buffer buf{ctx, field, prefix, scored_terms_limit};
  return Filter::PrepareWithBuffer<Buffer>(buf, ctx);
}

std::unique_ptr<Filter::PrepareBuffer> ByPrefix::CreateBuffer(
  const PrepareContext& ctx) const {
  return std::make_unique<Buffer>(ctx, field(), options().term,
                                  options().scored_terms_limit, Boost());
}

void ByPrefix::visit(const SubReader& segment, const TermReader& reader,
                     bytes_view prefix, FilterVisitor& visitor) {
  VisitImpl(segment, reader, prefix, visitor);
}

}  // namespace irs
