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
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/term_query.hpp"

namespace irs {
namespace {

class ByTermIterator : public TermIterator {
 public:
  ByTermIterator(const TermReader& reader, bytes_view term)
    : _reader{&reader}, _meta{reader.Lookup(term)} {
    _term.value = term;
  }

  bytes_view value() const noexcept final { return _term.value; }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    return id == irs::Type<TermAttr>::id() ? &_term : nullptr;
  }

  const PostingMeta& cookie() const final { return _meta; }

  DocIterator::ptr postings(IndexFeatures features) const final {
    return _reader->Iterator(features, PostingCookie{.cookie = &_meta});
  }

  bool next() final { return std::exchange(_found, false); }

 private:
  const TermReader* _reader;
  const PostingMeta _meta;
  TermAttr _term;
  bool _found{_meta.docs_count != 0};
};

}  // namespace

void ByTerm::Visit(const SubReader& segment, const TermReader& field,
                   const ByTermOptions& options, FilterVisitor& visitor) {
  ByTermIterator term{field, options.term};
  if (!term.next()) {
    return;
  }
  visitor.Prepare(segment, field, term);
  std::ignore = visitor.Visit(kNoBoost);
}

QueryBuilder::ptr ByTerm::PrepareSegment(const SubReader& segment,
                                         const PrepareContext& ctx,
                                         const irs::field_id field,
                                         const bytes_view term) {
  const auto* reader = segment.field(field);
  if (!reader) {
    // field absent in this segment: a boost-carrying empty query so the boost
    // is still observable and consistent with the multi-term path
    return memory::make_tracked<TermQuery>(ctx.memory, segment, nullptr,
                                           kNoPosting, ctx.boost);
  }
  const auto meta = reader->Lookup(term);
  if (ctx.collector) {
    auto& collector = sdb::basics::downCast<ByTermsCollector>(*ctx.collector);
    SDB_ASSERT(collector.Terms().size() == 1);
    collector.Field().Collect(*reader);
    if (meta.docs_count != 0) {
      collector.Terms()[0].Collect(meta);
    }
  }
  return memory::make_tracked<TermQuery>(ctx.memory, segment, reader, meta,
                                         ctx.boost);
}

PrepareCollector::ptr ByTerm::MakeCollector(const Scorer* scorer) const {
  return std::make_unique<ByTermsCollector>(scorer, 1);
}

TermPredicate::ptr ByTerm::CompileTermPredicate() const {
  return MakeTermPredicate(TermAcceptor{options().term});
}

TermIterator::ptr ByTerm::CompileTermIterator(const TermReader& reader) const {
  return memory::make_managed<ByTermIterator>(reader, options().term);
}

}  // namespace irs
