////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include "levenshtein_filter.hpp"

#include "basics/noncopyable.hpp"
#include "basics/shared.hpp"
#include "basics/std.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/all_terms_visitor.hpp"
#include "iresearch/search/automaton_filter.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/term_iterator.hpp"
#include "iresearch/search/top_terms_selector.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/levenshtein_default_pdp.hpp"
#include "iresearch/utils/levenshtein_utils.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {
namespace {

////////////////////////////////////////////////////////////////////////////////
/// @returns levenshtein similarity
////////////////////////////////////////////////////////////////////////////////
IRS_FORCE_INLINE score_t Similarity(uint32_t distance, uint32_t size) noexcept {
  SDB_ASSERT(size);

  static_assert(sizeof(score_t) == sizeof(uint32_t));

  return 1.f - static_cast<score_t>(distance) / static_cast<score_t>(size);
}

struct AggregatedStatsVisitor : util::Noncopyable {
  AggregatedStatsVisitor(MultiTermState& state, FieldCollector* field_stat,
                         TermCollector* term_stat) noexcept
    : state{state}, field_stat{field_stat}, term_stat{term_stat} {}

  void operator()(const SubReader&, const TermReader& field, uint32_t) const {
    if (field_stat && !field_collected) {
      field_stat->Collect(field);
      field_collected = true;
    }
    state.Prepare(&field);
  }

  void operator()(TermCookie& cookie) const {
    if (term_stat) {
      term_stat->Collect(cookie.stats);
    }
    const uint32_t docs_count = cookie.stats.docs_count;
    state.Push(MultiTermState::Entry{
      .cookie = std::move(cookie),
      .docs_count = docs_count,
      .boost = boost,
      .stat_offset = 0,
    });
  }

  MultiTermState& state;
  FieldCollector* field_stat;
  TermCollector* term_stat;
  score_t boost{kNoBoost};
  mutable bool field_collected{false};
};

// The parametric tables are stepped directly by the dictionary that has a
// backend for them, and scanned against otherwise. Nothing is materialized
// either way: building an automaton per query cost 32-660x what the tables do.
SeekTermIterator::ptr MakeLevenshteinTermIterator(
  const TermReader& reader, const LevenshteinAutomatonOptions& options) {
  SDB_ENSURE(options.parametric, "filter has no acceptor");
  auto it = reader.iterator(*options.parametric);
  if (it) {
    return it;
  }
  // A reader with no direct-stepping backend reports no distance, exactly as it
  // did when the walk went through a compiled automaton.
  return memory::make_managed<FilteredSeekTermIterator>(
    reader.iterator(SeekMode::NORMAL),
    MakeTermPredicate([acceptor = options.parametric](bytes_view term) {
      return acceptor->Matches(term);
    }));
}

class LevenshteinIterator : public WrappedTermIterator {
 public:
  LevenshteinIterator(const TermReader& reader,
                      const LevenshteinAutomatonOptions& options)
    : WrappedTermIterator{MakeLevenshteinTermIterator(reader, options)},
      _payload{irs::get<PayAttr>(*_impl)},
      _no_distance{options.no_distance},
      _target_size{options.utf8_target_size} {}

  score_t Boost() const noexcept { return _boost.value; }

  bool next() final {
    if (!_impl->next()) {
      return false;
    }
    const byte_type distance = !_payload || _payload->value.empty()
                                 ? _no_distance
                                 : _payload->value.front();
    const auto utf8_value_size =
      static_cast<uint32_t>(utf8_utils::Length(_impl->value()));
    _boost.value =
      Similarity(distance, std::min(utf8_value_size, _target_size));
    return true;
  }

  Attribute* GetMutable(TypeInfo::type_id id) noexcept final {
    if (irs::Type<TermBoost>::id() == id) {
      return &_boost;
    }
    return _impl->GetMutable(id);
  }

 private:
  const PayAttr* _payload;
  byte_type _no_distance;
  uint32_t _target_size;
  TermBoost _boost;
};

template<typename Visitor>
void VisitImpl(const SubReader& segment, const TermReader& reader,
               const LevenshteinAutomatonOptions& options, Visitor&& visitor) {
  SDB_ASSERT(options.parametric);
  LevenshteinIterator it(reader, options);
  if (!it.next()) {
    return;
  }
  visitor.Prepare(segment, reader, it.GetImpl());
  VisitTerms(it, visitor);
}

uint32_t Utf8TargetSize(bytes_view prefix, bytes_view term) {
  return std::max(1U, static_cast<uint32_t>(utf8_utils::Length(prefix) +
                                            utf8_utils::Length(term)));
}

QueryBuilder::ptr PrepareLevenshteinSegment(
  const SubReader& segment, const PrepareContext& ctx, irs::field_id field,
  const LevenshteinAutomatonOptions& options, size_t terms_limit,
  score_t boost) {
  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, ctx.boost * boost, ScoreMergeType::Max,
    size_t{1});

  const auto* reader = segment.field(field);
  if (!reader) {
    return query;
  }

  auto* collector = ctx.collector
                      ? &sdb::basics::downCast<ByTermsCollector>(*ctx.collector)
                      : nullptr;

  if (!terms_limit) {
    AllTermsVisitor term_collector{query->State(),
                                   collector ? &collector->Field() : nullptr,
                                   collector ? &collector->Terms() : nullptr};
    VisitImpl(segment, *reader, options, term_collector);
  } else {
    TopTermsSelector<TopTermState<score_t>> selector{terms_limit};
    VisitImpl(segment, *reader, options, selector);

    AggregatedStatsVisitor aggregate_stats{
      query->State(), collector ? &collector->Field() : nullptr,
      collector ? &collector->Terms()[0] : nullptr};
    selector.Visit([&aggregate_stats](TopTermState<score_t>& s) {
      aggregate_stats.boost = std::max(0.f, s.key);
      s.Visit(aggregate_stats);
    });
  }

  return query;
}

}  // namespace

QueryBuilder::ptr ByEditDistance::PrepareSegment(const SubReader&,
                                                 const PrepareContext&) const {
  THROW_SQL_ERROR(
    ERR_MSG("ByEditDistance must be lowered by the optimizer before prepare"));
}

QueryBuilder::ptr LevenshteinAutomatonFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx, irs::field_id id,
  const LevenshteinAutomatonOptions& options, score_t boost) {
  SDB_ASSERT(options.parametric);
  return PrepareLevenshteinSegment(segment, ctx, id, options, options.max_terms,
                                   boost);
}

field_visitor LevenshteinAutomatonFilter::visitor(
  const LevenshteinAutomatonOptions& options) {
  if (!options.parametric) {
    return [](const SubReader&, const TermReader&, FilterVisitor&) {};
  }

  return [options](const SubReader& segment, const TermReader& field,
                   FilterVisitor& visitor) {
    return VisitImpl(segment, field, options, visitor);
  };
}

QueryBuilder::ptr LevenshteinAutomatonFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  return PrepareSegment(segment, ctx, field_id(), options(), Boost());
}

PrepareCollector::ptr LevenshteinAutomatonFilter::MakeCollector(
  const Scorer* scorer) const {
  return std::make_unique<ByTermsCollector>(scorer, 1);
}

LevenshteinAutomatonOptions::LevenshteinAutomatonOptions(
  const ParametricDescription& d, bytes_view prefix, bytes_view term,
  size_t max_terms)
  : parametric{std::make_shared<const LevenshteinAcceptor>(d, prefix, term)},
    utf8_target_size{Utf8TargetSize(prefix, term)},
    no_distance{static_cast<byte_type>(d.max_distance() + 1)},
    max_terms{max_terms} {
  target.reserve(prefix.size() + term.size());
  target += prefix;
  target += term;
}

Filter::ptr LowerLevenshtein(irs::field_id id,
                             const ByEditDistanceOptions& opts, score_t boost) {
  return ExecuteLevenshtein(
    opts.max_distance, opts.provider, opts.with_transpositions, opts.prefix,
    opts.term, [] -> Filter::ptr { return std::make_unique<Empty>(); },
    [&] -> Filter::ptr {
      auto filter = std::make_unique<ByTerm>();
      *filter->mutable_field_id() = id;
      auto& target = filter->mutable_options()->term;
      target.reserve(opts.prefix.size() + opts.term.size());
      target += opts.prefix;
      target += opts.term;
      filter->boost(boost);
      return filter;
    },
    [&](const ParametricDescription& d, const bytes_view prefix,
        const bytes_view term) -> Filter::ptr {
      LevenshteinAutomatonOptions lowered{d, prefix, term, opts.max_terms};
      auto filter = std::make_unique<LevenshteinAutomatonFilter>();
      *filter->mutable_field_id() = id;
      *filter->mutable_options() = std::move(lowered);
      filter->boost(boost);
      return filter;
    });
}

TermPredicate::ptr LevenshteinAutomatonFilter::CompileTermPredicate() const {
  if (!options().parametric) {
    return nullptr;
  }
  return MakeTermPredicate(
    [acceptor = options().parametric](bytes_view term) noexcept {
      return acceptor->Matches(term);
    });
}

TermPredicate::ptr ByEditDistance::CompileTermPredicate() const {
  auto lowered = LowerLevenshtein(field_id(), options(), kNoBoost);
  if (!lowered) {
    return nullptr;
  }
  auto predicate = lowered->CompileTermPredicate();
  if (!predicate) {
    return nullptr;
  }
  return MakeTermPredicate([lowered = std::move(lowered),
                            predicate = std::move(predicate)](bytes_view term) {
    return predicate->Accepts(term);
  });
}

TermIterator::ptr LevenshteinAutomatonFilter::CompileTermIterator(
  const TermReader& reader) const {
  if (!options().parametric) {
    return nullptr;
  }
  return memory::make_managed<LevenshteinIterator>(reader, options());
}

}  // namespace irs
