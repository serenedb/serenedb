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

// Filter visitor for term queries
class TermVisitor : private util::Noncopyable {
 public:
  TermVisitor(const TermCollectors& term_stats, TermQuery::States& states)
    : _term_stats(term_stats), _states(states) {}

  void Prepare(const SubReader& segment, const TermReader& field,
               const SeekTermIterator& terms) noexcept {
    _segment = &segment;
    _reader = &field;
    _terms = &terms;
  }

  void Visit(score_t /*boost*/) {
    // collect statistics
    SDB_ASSERT(_segment && _reader && _terms);
    _term_stats.collect(*_segment, *_reader, 0, *_terms);

    // Cache term state in prepared query attributes.
    // Later, using cached state we could easily "jump" to
    // postings without relatively expensive FST traversal
    auto& state = _states.insert(*_segment);
    state.reader = _reader;
    state.cookie = _terms->cookie();
  }

 private:
  const TermCollectors& _term_stats;
  TermQuery::States& _states;
  const SubReader* _segment{};
  const TermReader* _reader{};
  const SeekTermIterator* _terms{};
};

template<typename Visitor>
void VisitImpl(const SubReader& segment, const TermReader& field,
               bytes_view term, Visitor& visitor) {
  // find term
  auto terms = field.iterator(SeekMode::RandomOnly);

  if (!terms) [[unlikely]] {
    return;
  }
  if (!terms->seek(term)) {
    return;
  }

  visitor.Prepare(segment, field, *terms);

  // read term attributes
  terms->read();

  visitor.Visit(kNoBoost);
}

}  // namespace

void ByTerm::visit(const SubReader& segment, const TermReader& field,
                   bytes_view term, FilterVisitor& visitor) {
  VisitImpl(segment, field, term, visitor);
}

Filter::Query::ptr ByTerm::prepare(const PrepareContext& ctx,
                                   std::string_view field, bytes_view term) {
  TermQuery::States states{ctx.memory, ctx.index.size()};
  FieldCollectors field_stats{ctx.scorers};
  TermCollectors term_stats{ctx.scorers, 1};

  TermVisitor visitor(term_stats, states);

  // iterate over the segments
  for (const auto& segment : ctx.index) {
    // get field
    const auto* reader = segment.field(field);

    if (!reader) {
      continue;
    }

    field_stats.collect(segment, *reader);
    // collect field statistics once per segment

    VisitImpl(segment, *reader, term, visitor);
  }

#ifndef SDB_GTEST  // TODO(mbkkt) adjust tests
  if (states.empty()) {
    return Query::empty();
  }
#endif

  bstring stats(ctx.scorers.stats_size(), 0);
  auto* stats_buf = stats.data();

  term_stats.finish(stats_buf, 0, field_stats, ctx.index);

  return memory::make_tracked<TermQuery>(ctx.memory, std::move(states),
                                         std::move(stats), ctx.boost);
}

}  // namespace irs
