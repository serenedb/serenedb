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

#include "phrase_filter.hpp"

#include <absl/container/flat_hash_map.h>

#include <span>

#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/phrase_matcher.hpp"
#include "iresearch/search/detail/term_iterator.hpp"
#include "iresearch/search/detail/top_terms_selector.hpp"
#include "iresearch/search/filters/filter_visitor.hpp"
#include "iresearch/search/filters/levenshtein_filter.hpp"
#include "iresearch/search/filters/prefix_filter.hpp"
#include "iresearch/search/filters/range_filter.hpp"
#include "iresearch/search/filters/term_filter.hpp"
#include "iresearch/search/filters/wildcard_filter.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/phrase_state.hpp"
#include "iresearch/search/queries/prepared_state_visitor.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/system_compiler.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace irs {
namespace {

struct TopTerms {
  static bytes_view Term(const TopTerm<score_t>& term) noexcept {
    return term.term;
  }
  static score_t Boost(const TopTerm<score_t>& term) noexcept {
    return term.key;
  }
};

void VisitTopTerms(const SubReader& segment, const TermReader& field,
                   std::span<const TopTerm<score_t>> terms,
                   FilterVisitor& visitor) {
  SeekTermsIterator<const TopTerm<score_t>*, TopTerms> itr{
    field, terms.data(), terms.data() + terms.size()};
  visitor.Prepare(segment, field, itr.GetImpl());
  if (!itr.next()) {
    return;
  }
  VisitTerms(itr, visitor);
}

struct TopTermsVisitor final : FilterVisitor {
  explicit TopTermsVisitor(size_t size) : _impl{size} { SDB_ASSERT(size); }

  void Prepare(const SubReader& segment, const TermReader& field,
               TermIterator& terms) final {
    _impl.Prepare(segment, field, terms);
  }

  bool Visit(score_t boost) final {
    _impl.Visit(boost);
    return true;
  }

  field_visitor ToVisitor() {
    std::vector<TopTerm<score_t>> terms;
    _impl.Visit(
      [&](TopTerm<score_t>& term) { terms.push_back(std::move(term)); });
    absl::c_sort(terms,
                 [](const TopTerm<score_t>& lhs, const TopTerm<score_t>& rhs) {
                   return lhs.term < rhs.term;
                 });
    return [terms = std::move(terms)](const SubReader& segment,
                                      const TermReader& field,
                                      FilterVisitor& visitor) {
      return VisitTopTerms(segment, field, terms, visitor);
    };
  }

 private:
  TopTermsSelector<TopTerm<score_t>> _impl;
};

struct GetVisitor {
  field_visitor operator()(const ByPrefixOptions& options) const {
    return [&](const SubReader& segment, const TermReader& field,
               FilterVisitor& visitor) {
      return ByPrefix::visit(segment, field, options, visitor);
    };
  }
  field_visitor operator()(const auto&) const { SDB_UNREACHABLE(); }

  field_visitor operator()(const AutomatonOptions& options) const {
    SDB_ASSERT(options.compiled);
    return AutomatonFilter::visitor(options.compiled->acceptor);
  }

  field_visitor operator()(const LevenshteinAutomatonOptions& options) const {
    if (options.max_terms != 0) {
      return {};
    }
    return LevenshteinAutomatonFilter::visitor(options);
  }

  field_visitor operator()(const ByRangeOptions& options) const {
    return [&](const SubReader& segment, const TermReader& field,
               FilterVisitor& visitor) {
      return ByRange::visit(segment, field, options, visitor);
    };
  }
};

class PhraseTermVisitor final : public FilterVisitor,
                                private util::Noncopyable {
 public:
  PhraseTermVisitor(PhraseState& state, bool boosted) noexcept
    : _state(state), _boosted{boosted} {}

  void Prepare(const SubReader&, const TermReader&,
               TermIterator& terms) noexcept final {
    _terms = &terms;
  }

  bool Visit(score_t boost) final {
    SDB_ASSERT(_terms);

    boost = std::max(0.f, boost);

    const auto& meta = _terms->cookie();
    const auto term = _terms->value();
    if (_expanded) {
      _expanded->try_emplace(term).first->second.Collect(meta);
    }
    _has_boosts |= (boost != kNoBoost);

    if (_visited_terms) {
      _visited_terms->emplace_back(term.data(), term.size());
    }
    _state.metas.emplace_back(meta);
    if (_boosted) {
      _state.boosts.emplace_back(boost);
    }
    return true;
  }

  void Reset(ExpandedSlotsCollector::Terms* expanded,
             std::vector<bstring>* visited_terms) noexcept {
    _terms = nullptr;
    _expanded = expanded;
    _visited_terms = visited_terms;
  }

  bool HasBoosts() const noexcept { return _has_boosts; }

 private:
  PhraseState& _state;
  ExpandedSlotsCollector::Terms* _expanded = nullptr;
  std::vector<bstring>* _visited_terms = nullptr;
  TermIterator* _terms = nullptr;
  bool _boosted;
  bool _has_boosts = false;
};

bool HasIntervalOffsets(const ByPhraseOptions& options) noexcept {
  for (const auto& info : options) {
    if (info.offs_min != info.offs_max) {
      return true;
    }
  }
  return false;
}

bool IsEmpty(irs::field_id field, const ByPhraseOptions& options) noexcept {
  return !irs::field_limits::valid(field) || options.empty();
}

struct SlotCounts {
  size_t terms = 0;
  size_t expanded = 0;
  bool boosted = false;
};

SlotCounts CountSlots(const ByPhraseOptions& options) {
  SlotCounts counts;
  for (const auto& word : options) {
    switch (ByPhraseOptions::KindOf(word.part)) {
      case SlotKind::Term:
        ++counts.terms;
        break;
      case SlotKind::Set:
        counts.terms += std::get<TermSetOptions>(word.part).terms.size();
        break;
      case SlotKind::Expansion:
        ++counts.expanded;
        counts.boosted |=
          std::holds_alternative<LevenshteinAutomatonOptions>(word.part);
        break;
    }
  }
  return counts;
}

PhraseQuery::Positions MakePositions(const ByPhraseOptions& options) {
  PhraseQuery::Positions positions(options.size());
  auto position = positions.begin();
  PosAttr::value_t look_back = 0;
  for (const auto& word : options) {
    position->offs_max = word.offs_max;
    position->offs_min = word.offs_min;
    position->lead_offset = look_back += word.offs_max;
    ++position;
  }
  return positions;
}

void ApplyTermGroups(const ByPhraseOptions& options,
                     std::span<const std::vector<bstring>> part_terms,
                     std::span<TermInterval> positions) {
  const auto n = positions.size();
  for (size_t i = 0; i != n; ++i) {
    positions[i].term_group = static_cast<uint32_t>(i);
  }
  const auto find = [&](uint32_t x) {
    while (positions[x].term_group != x) {
      positions[x].term_group = positions[positions[x].term_group].term_group;
      x = positions[x].term_group;
    }
    return x;
  };

  absl::flat_hash_map<bytes_view, uint32_t> first_owner;
  const auto add_term = [&](bytes_view term, uint32_t slot) {
    const auto [it, inserted] = first_owner.emplace(term, slot);
    if (!inserted) {
      const auto a = find(it->second);
      const auto b = find(slot);
      if (a != b) {
        positions[b].term_group = a;
      }
    }
  };

  uint32_t slot = 0;
  for (const auto& word : options) {
    switch (ByPhraseOptions::KindOf(word.part)) {
      case SlotKind::Term:
        add_term(std::get<ByTermOptions>(word.part).term, slot);
        break;
      case SlotKind::Set:
        for (const auto& term : std::get<TermSetOptions>(word.part).terms) {
          add_term(term, slot);
        }
        break;
      case SlotKind::Expansion:
        for (const auto& term : part_terms[slot]) {
          add_term(term, slot);
        }
        break;
    }
    ++slot;
  }

  for (size_t i = 0; i != n; ++i) {
    positions[i].term_group = find(static_cast<uint32_t>(i));
  }
}

QueryBuilder::ptr PhrasePrepareSegment(const SubReader& segment,
                                       const PrepareContext& ctx,
                                       irs::field_id field,
                                       const ByPhraseOptions& options) {
  const auto phrase_size = options.size();
  auto* collector = ctx.collector
                      ? &irs::utils::downCast<SlotsCollector>(*ctx.collector)
                      : nullptr;
  const auto is_ord_empty = !collector || !collector->GetScorer();

  PhraseState state{ctx.memory};
  const auto* reader = segment.field(field);
  state.reader = reader;
  if (!detail::ResolvePhrase(reader, state.handles)) {
    return QueryBuilder::Empty();
  }
  if (collector) {
    collector->Field(ctx.thread).Collect(*reader);
  }

  const auto counts = CountSlots(options);

  std::vector<field_visitor> expand_visitors;
  std::vector<field_visitor*> all_terms_visitors;
  std::vector<TopTermsVisitor> top_terms_visitors;
  std::vector<std::vector<bstring>> part_terms;
  if (counts.expanded != 0) {
    expand_visitors.reserve(counts.expanded);
    for (const auto& word : options) {
      if (ByPhraseOptions::KindOf(word.part) != SlotKind::Expansion) {
        continue;
      }
      auto& visitor =
        expand_visitors.emplace_back(std::visit(GetVisitor{}, word.part));
      if (!visitor) {
        auto& opts = std::get<LevenshteinAutomatonOptions>(word.part);
        visitor = LevenshteinAutomatonFilter::visitor(opts);
        all_terms_visitors.push_back(&visitor);
        top_terms_visitors.emplace_back(opts.max_terms);
      }
    }

    if (!all_terms_visitors.empty()) {
      auto it = top_terms_visitors.begin();
      for (auto* visitor : all_terms_visitors) {
        (*visitor)(segment, *reader, *it++);
      }
      it = top_terms_visitors.begin();
      for (auto* visitor : all_terms_visitors) {
        *visitor = it++->ToVisitor();
      }
    }

    if (options.slop() != 0) {
      part_terms.resize(phrase_size);
    }
  }

  state.metas.reserve(counts.terms);
  const bool boosted = counts.boosted && !is_ord_empty;
  if (boosted) {
    state.boosts.reserve(counts.terms);
  }
  state.offsets.reserve(phrase_size + 1);
  state.offsets.emplace_back(0);

  PhraseTermVisitor ptv{state, boosted};
  auto terms = reader->iterator();

  bool any_empty = false;
  size_t counter_idx = 0;
  size_t expanded_idx = 0;
  size_t slot = 0;
  for (const auto& word : options) {
    switch (ByPhraseOptions::KindOf(word.part)) {
      case SlotKind::Term: {
        if (terms->seek(std::get<ByTermOptions>(word.part).term)) {
          const auto& meta = terms->cookie();
          if (collector) {
            collector->Term(ctx.thread, counter_idx).Collect(meta);
          }
          state.metas.emplace_back(meta);
          if (boosted) {
            state.boosts.emplace_back(kNoBoost);
          }
        }
        ++counter_idx;
      } break;
      case SlotKind::Set: {
        const auto& set = std::get<TermSetOptions>(word.part).terms;
        size_t i = 0;
        for (const auto& term : set) {
          if (terms->seek(term)) {
            const auto& meta = terms->cookie();
            if (collector) {
              collector->Term(ctx.thread, counter_idx + i).Collect(meta);
            }
            state.metas.emplace_back(meta);
            if (boosted) {
              state.boosts.emplace_back(kNoBoost);
            }
          }
          ++i;
        }
        counter_idx += set.size();
      } break;
      case SlotKind::Expansion: {
        ptv.Reset(collector
                    ? &irs::utils::downCast<ExpandedSlotsCollector>(*collector)
                         .Expanded(ctx.thread, expanded_idx)
                    : nullptr,
                  part_terms.empty() ? nullptr : &part_terms[slot]);
        expand_visitors[expanded_idx](segment, *reader, ptv);
        ++expanded_idx;
      } break;
    }
    state.offsets.emplace_back(static_cast<uint32_t>(state.metas.size()));
    any_empty |= state.offsets[slot] == state.offsets[slot + 1];
    if (any_empty && is_ord_empty) {
      return QueryBuilder::Empty();
    }
    ++slot;
  }
  if (any_empty) {
    return QueryBuilder::Empty();
  }

  if (!ptv.HasBoosts()) {
    state.boosts.clear();
  }

  if (phrase_size == 1 && state.metas.size() == 1) {
    return MakeTermQuery(ctx.memory, segment, state.reader, state.metas.front(),
                         ctx.boost, ctx.Record());
  }

  auto positions = MakePositions(options);
  if (options.slop() != 0) {
    ApplyTermGroups(options, part_terms, positions);
  }

  const auto boost = ctx.boost;
  if (state.Fixed()) {
    auto query = memory::make_tracked<FixedPhraseQuery>(
      ctx.memory, segment, std::move(state), std::move(positions), boost,
      options.slop());
    query->SetStats(ctx.Record());
    return query;
  }

  auto query = memory::make_tracked<VariadicPhraseQuery>(
    ctx.memory, segment, std::move(state), std::move(positions), boost,
    options.slop());
  query->SetStats(ctx.Record());
  return query;
}

}  // namespace

QueryBuilder::ptr ByPhrase::PrepareSegment(const SubReader& segment,
                                           const PrepareContext& ctx) const {
  auto sub_ctx = ctx;
  sub_ctx.Boost(GetBoost());
  if (IsEmpty(field_id(), options())) {
    return QueryBuilder::Empty();
  }
  SDB_ENSURE(options().slop() == 0 || !HasIntervalOffsets(options()),
             "slop and intervals are mutually exclusive");
  return PhrasePrepareSegment(segment, sub_ctx, field_id(), options());
}

PrepareCollector::ptr ByPhrase::MakeCollectorImpl(const Scorer* scorer,
                                                  StatsArena& stats,
                                                  uint32_t threads) const {
  if (IsEmpty(field_id(), options())) {
    return nullptr;
  }
  const auto counts = CountSlots(options());
  if (counts.expanded == 0) {
    return std::make_unique<SlotsCollector>(scorer, counts.terms, stats,
                                            threads);
  }
  return std::make_unique<ExpandedSlotsCollector>(
    scorer, counts.terms, counts.expanded, stats, threads);
}

bool ByPhraseOptions::LowerParts() {
  bool changed = false;
  for (auto& info : _phrase) {
    if (const auto* t = std::get_if<TermSetOptions>(&info.part);
        t != nullptr && t->terms.size() == 1) {
      ByTermOptions opts;
      opts.term = *t->terms.begin();
      info.part = std::move(opts);
      changed = true;
    } else if (const auto* w = std::get_if<ByWildcardOptions>(&info.part); w) {
      bstring buf;
      info.part = ExecuteWildcard(
        buf, bytes_view{w->term},
        [](bytes_view term) -> PhrasePart {
          ByTermOptions opts;
          opts.term = term;
          return opts;
        },
        [](bytes_view prefix) -> PhrasePart {
          ByPrefixOptions opts;
          opts.term = prefix;
          return opts;
        },
        [](bytes_view term) -> PhrasePart {
          return AutomatonOptions{FromWildcard(term), term};
        });
      changed = true;
    } else if (const auto* e = std::get_if<ByEditDistanceOptions>(&info.part);
               e) {
      const auto max_terms = e->max_terms;
      info.part = ExecuteLevenshtein(
        e->max_distance, e->provider, e->with_transpositions, e->prefix,
        e->term, [] -> PhrasePart { return TermSetOptions{}; },
        [&] -> PhrasePart {
          ByTermOptions opts;
          opts.term.reserve(e->prefix.size() + e->term.size());
          opts.term += e->prefix;
          opts.term += e->term;
          return opts;
        },
        [max_terms](const ParametricDescription& d, bytes_view prefix,
                    bytes_view term) -> PhrasePart {
          return LevenshteinAutomatonOptions{d, prefix, term, max_terms};
        });
      changed = true;
    }
  }
  return changed;
}

}  // namespace irs
