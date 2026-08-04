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

#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/all_terms_visitor.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/term_iterator.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {
namespace {

std::vector<bytes_view> Probes(const ByTermsOptions::search_terms& terms) {
  std::vector<bytes_view> probes;
  probes.reserve(terms.size());
  for (const auto& term : terms) {
    probes.emplace_back(term.term);
  }
  return probes;
}

class ByTermsIterator : public WrappedTermIterator {
 public:
  ByTermsIterator(const TermReader& reader,
                  const ByTermsOptions::search_terms& terms)
    : ByTermsIterator{reader, terms, Probes(terms)} {}

  score_t Boost() const noexcept { return _boost; }
  uint32_t Index() const noexcept { return _index; }

  bool next() final { return _batch ? NextReported() : NextProbed(); }

 private:
  // What the probe set is resolved through: the dictionary's own batch pass, or
  // a plain walk this class seeks itself when the dictionary has none.
  struct Walk {
    SeekTermIterator::ptr it;
    bool batch;
  };

  static Walk MakeWalk(const TermReader& reader,
                       std::span<const bytes_view> probes) {
    auto batch = reader.BatchIterator(probes);
    if (batch) {
      return {std::move(batch), true};
    }
    return {reader.iterator(SeekMode::NORMAL), false};
  }

  // `probes` is taken by reference so that it is still filled when the walk is
  // built from it: the batch pass borrows the span, and only a moved vector
  // keeps the buffer that span points into.
  ByTermsIterator(const TermReader& reader,
                  const ByTermsOptions::search_terms& terms,
                  std::vector<bytes_view>&& probes)
    : ByTermsIterator{terms, probes, MakeWalk(reader, probes)} {}

  ByTermsIterator(const ByTermsOptions::search_terms& terms,
                  std::vector<bytes_view>& probes, Walk&& walk)
    : WrappedTermIterator{std::move(walk.it)},
      _probes{std::move(probes)},
      _cursor{terms.begin()},
      _end{terms.end()},
      _batch{walk.batch} {}

  // The batch pass reports the probes that hit, in probe order, so the term
  // set is walked alongside it and everything skipped over is a miss.
  bool NextReported() {
    if (!_impl->next()) {
      return false;
    }
    const auto term = _impl->value();
    while (_cursor != _end && _cursor->term != term) {
      ++_cursor;
      ++_index;
    }
    SDB_ENSURE(_cursor != _end,
               "terms filter: batch iterator reported a term that is not in "
               "the probe set, or reported out of probe order");
    _boost = _cursor->boost;
    return true;
  }

  bool NextProbed() {
    if (_started) {
      ++_cursor;
      ++_index;
    }
    _started = true;
    for (; _cursor != _end; ++_cursor, ++_index) {
      if (_impl->seek(_cursor->term)) {
        _boost = _cursor->boost;
        return true;
      }
    }
    return false;
  }

  std::vector<bytes_view> _probes;
  ByTermsOptions::search_terms::const_iterator _cursor;
  ByTermsOptions::search_terms::const_iterator _end;
  score_t _boost = kNoBoost;
  uint32_t _index = 0;
  bool _batch;
  bool _started{false};
};

}  // namespace

void ByTerms::visit(const SubReader& segment, const TermReader& field,
                    const ByTermsOptions& options, FilterVisitor& visitor) {
  ByTermsIterator terms(field, options.terms);
  visitor.Prepare(segment, field, terms.GetImpl());
  if (!terms.next()) {
    return;
  }
  VisitTerms(terms, visitor);
}

QueryBuilder::ptr ByTerms::PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          irs::field_id field,
                                          const ByTermsOptions& options,
                                          score_t boost) {
  const auto& [terms, min_match, merge_type] = options;
  const size_t size = terms.size();
  SDB_ASSERT(size);
  SDB_ASSERT(min_match <= size);
  SDB_ASSERT(size > 1);
  SDB_ASSERT(min_match != 0);

  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost * boost, merge_type, min_match);

  const auto* reader = segment.field(field);
  if (!reader) {
    return query;
  }

  auto* collector = ctx.collector
                      ? &sdb::basics::downCast<ByTermsCollector>(*ctx.collector)
                      : nullptr;
  AllTermsVisitor mtv{query->State(), collector ? &collector->Field() : nullptr,
                      collector ? &collector->Terms() : nullptr};
  ByTermsIterator it(*reader, options.terms);
  if (it.next()) {
    mtv.Prepare(segment, *reader, it.GetImpl());
    VisitTerms(it, mtv);
  }
  return query;
}

QueryBuilder::ptr ByTerms::PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx) const {
  return PrepareSegment(segment, ctx, field_id(), options(), Boost());
}

PrepareCollector::ptr ByTerms::MakeCollector(const Scorer* scorer) const {
  return std::make_unique<ByTermsCollector>(scorer, options().terms.size());
}

TermPredicate::ptr ByTerms::CompileTermPredicate() const {
  if (options().min_match != 1) {
    return nullptr;
  }
  return MakeTermPredicate(TermSetAcceptor{&options().terms});
}

TermIterator::ptr ByTerms::CompileTermIterator(const TermReader& reader) const {
  if (options().min_match != 1) {
    return nullptr;
  }
  return memory::make_managed<ByTermsIterator>(reader, options().terms);
}

}  // namespace irs
