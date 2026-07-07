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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/term_query.hpp"

namespace irs {
namespace {

class ByTermIterator : public WrappedTermIterator {
 public:
  ByTermIterator(const TermReader& reader, bytes_view term)
    : WrappedTermIterator{reader.iterator(SeekMode::RandomOnly)},
      _found{_impl->seek(term)} {}

  bool next() final { return std::exchange(_found, false); }

 private:
  bool _found;
};

}  // namespace

void ByTerm::Visit(const SubReader& segment, const TermReader& field,
                   const ByTermOptions& options, FilterVisitor& visitor) {
  auto terms = field.iterator(SeekMode::RandomOnly);
  if (!terms || !terms->seek(options.term)) {
    return;
  }
  visitor.Prepare(segment, field, *terms);
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
    return memory::make_tracked<TermQuery>(
      ctx.memory, segment, TermState{nullptr, nullptr}, ctx.boost);
  }
  auto terms = reader->iterator(SeekMode::RandomOnly);
  if (terms && !terms->seek(term)) {
    terms = nullptr;
  }
  if (ctx.collector) {
    auto& collector = sdb::basics::downCast<ByTermsCollector>(*ctx.collector);
    SDB_ASSERT(collector.Terms().size() == 1);
    collector.Field().Collect(*reader);
    if (terms) {
      collector.Terms()[0].Collect(*terms);
    }
  }
  TermState state{reader, terms ? terms->cookie() : nullptr};
  return memory::make_tracked<TermQuery>(ctx.memory, segment, std::move(state),
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
