////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "boolean_filter.hpp"

#include <algorithm>
#include <ranges>
#include <utility>

#include "basics/down_cast.h"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/term_filter.hpp"

namespace irs {
namespace {

template<typename OnTerms, typename OnFilter>
void WalkClauses(const BooleanFilter& node, OnTerms on_terms,
                 OnFilter on_filter) {
  for (const auto occur : kAllOccur) {
    const auto terms = node.Terms(occur);
    for (size_t begin = 0; begin != terms.size();) {
      size_t end = begin + 1;
      while (end != terms.size() && terms[end].field == terms[begin].field &&
             terms[end].scorer == terms[begin].scorer) {
        ++end;
      }
      on_terms(terms.subspan(begin, end - begin), occur);
      begin = end;
    }
    for (const auto& filter : node.Filters(occur)) {
      on_filter(*filter, occur);
    }
  }
}

const Scorer* ClauseScorer(const Scorer* scorer, const Scorer* own) {
  const auto* winner = ResolveScorer(own, scorer);
  return IsUnscored(*winner) ? nullptr : winner;
}

class AllOfPredicate final : public TermPredicate {
 public:
  explicit AllOfPredicate(std::vector<TermPredicate::ptr>&& preds) noexcept
    : _preds{std::move(preds)} {}

  bool Accepts(bytes_view term) const final {
    return absl::c_all_of(_preds,
                          [&](const auto& p) { return p->Accepts(term); });
  }

 private:
  std::vector<TermPredicate::ptr> _preds;
};

class MinMatchPredicate final : public TermPredicate {
 public:
  MinMatchPredicate(std::vector<TermPredicate::ptr>&& preds,
                    size_t min_match) noexcept
    : _preds{std::move(preds)}, _min_match{min_match} {}

  bool Accepts(bytes_view term) const final {
    size_t matched = 0;
    for (const auto& p : _preds) {
      if (p->Accepts(term) && ++matched == _min_match) {
        return true;
      }
    }
    return false;
  }

 private:
  std::vector<TermPredicate::ptr> _preds;
  size_t _min_match;
};

class NegatedPredicate final : public TermPredicate {
 public:
  explicit NegatedPredicate(TermPredicate::ptr pred) noexcept
    : _pred{std::move(pred)} {}

  bool Accepts(bytes_view term) const final { return !_pred->Accepts(term); }

 private:
  TermPredicate::ptr _pred;
};

class TermsIterator : public WrappedTermIterator {
 public:
  TermsIterator(const TermReader& reader, std::span<const TermClause> terms)
    : WrappedTermIterator{reader.iterator()}, _terms{terms} {}

  bool next() final {
    for (; _next != _terms.size(); ++_next) {
      if (_impl->seek(_terms[_next].term)) {
        ++_next;
        return true;
      }
    }
    return false;
  }

 private:
  std::span<const TermClause> _terms;
  size_t _next = 0;
};

bool SingleField(std::span<const TermClause> terms) noexcept {
  return absl::c_all_of(
    terms, [field = terms.front().field](const TermClause& clause) noexcept {
      return clause.field == field;
    });
}

TermPredicate::ptr CombinePredicates(std::vector<TermPredicate::ptr>&& preds) {
  if (preds.empty()) {
    return nullptr;
  }
  if (preds.size() == 1) {
    return std::move(preds.front());
  }
  return std::make_unique<AllOfPredicate>(std::move(preds));
}

class TermSetPredicate final : public TermPredicate {
 public:
  explicit TermSetPredicate(std::span<const TermClause> terms) noexcept
    : _terms{terms} {}

  bool Accepts(bytes_view term) const final {
    const auto it =
      std::lower_bound(_terms.begin(), _terms.end(), term,
                       [](const TermClause& lhs, bytes_view rhs) noexcept {
                         return bytes_view{lhs.term} < rhs;
                       });
    return it != _terms.end() && bytes_view{it->term} == term;
  }

 private:
  std::span<const TermClause> _terms;
};

bool CompileBucketInto(const Clauses& clauses,
                       std::vector<TermPredicate::ptr>& preds,
                       bool fuse_terms) {
  if (fuse_terms && clauses.terms.size() > 1 && SingleField(clauses.terms)) {
    preds.push_back(std::make_unique<TermSetPredicate>(clauses.terms));
  } else {
    for (const auto& term : clauses.terms) {
      preds.push_back(MakeTermPredicate(TermAcceptor{term.term}));
    }
  }
  return absl::c_all_of(clauses.filters, [&](const auto& filter) {
    auto pred = filter->CompileTermPredicate();
    if (!pred) {
      return false;
    }
    preds.push_back(std::move(pred));
    return true;
  });
}

}  // namespace

void BooleanFilter::Add(TermClause clause, Occur occur) {
  auto& terms = Bucket(occur).terms;
  constexpr TermClauseLess kLess{};
  if (occur == Occur::MustNot) {
    clause.scorer = nullptr;
    clause.boost = kNoBoost;
    const auto it = std::ranges::lower_bound(terms, clause, kLess);
    if (it != terms.end() && !kLess(clause, *it)) {
      return;
    }
    terms.insert(it, std::move(clause));
    return;
  }
  terms.insert(std::ranges::upper_bound(terms, clause, kLess),
               std::move(clause));
}

void BooleanFilter::Add(Filter::ptr filter, Occur occur) {
  SDB_ASSERT(filter);
  if (filter->type() == irs::Type<ByTerm>::id()) {
    auto& term = sdb::basics::downCast<ByTerm>(*filter);
    Add(TermClause{.field = term.field_id(),
                   .scorer = term.GetScorer(),
                   .term = std::move(term.mutable_options()->term),
                   .boost = term.GetBoost()},
        occur);
    return;
  }
  if (occur == Occur::MustNot) {
    filter->SetScorer(nullptr);
    filter->SetBoost(kNoBoost);
  }
  Bucket(occur).filters.emplace_back(std::move(filter));
}

void BooleanFilter::SetMinShouldMatch(uint32_t value) noexcept {
  SDB_ASSERT(value <= _should.size());
  SDB_ASSERT(value != 0 || !_must.empty() || _should.empty());
  _min_should_match = value;
}

TermPredicate::ptr BooleanFilter::CompileTermPredicate() const {
  if (_must.empty() && _should.empty() && _must_not.empty()) {
    return nullptr;
  }

  std::vector<TermPredicate::ptr> preds;
  preds.reserve(_must.size() + _must_not.size() + 1);

  if (!CompileBucketInto(_must, preds, false)) {
    return nullptr;
  }

  std::vector<TermPredicate::ptr> excluded;
  excluded.reserve(_must_not.size());
  if (!CompileBucketInto(_must_not, excluded, true)) {
    return nullptr;
  }
  for (auto& pred : excluded) {
    preds.push_back(std::make_unique<NegatedPredicate>(std::move(pred)));
  }

  if (!_should.empty()) {
    if (_min_should_match == 0 || _min_should_match > _should.size()) {
      return nullptr;
    }
    std::vector<TermPredicate::ptr> optional;
    optional.reserve(_should.size());
    if (!CompileBucketInto(_should, optional, _min_should_match == 1)) {
      return nullptr;
    }
    if (optional.size() == 1) {
      preds.push_back(std::move(optional.front()));
    } else {
      preds.push_back(std::make_unique<MinMatchPredicate>(std::move(optional),
                                                          _min_should_match));
    }
  }

  return CombinePredicates(std::move(preds));
}

TermIterator::ptr BooleanFilter::CompileTermIterator(
  const TermReader& reader) const {
  if (_must.empty() && _must_not.empty() && _min_should_match == 1 &&
      _should.filters.empty() && !_should.terms.empty() &&
      SingleField(_should.terms)) {
    return memory::make_managed<TermsIterator>(reader, _should.terms);
  }
  if (_must.empty() || !_should.empty()) {
    return Filter::CompileTermIterator(reader);
  }
  std::vector<TermPredicate::ptr> preds;
  preds.reserve(_must.size() + _must_not.size());
  TermIterator::ptr lead;
  std::span<const Filter::ptr> rest{_must.filters};
  if (!_must.terms.empty()) {
    const std::span terms{_must.terms};
    lead = memory::make_managed<TermsIterator>(reader, terms.first(1));
    for (const auto& term : terms.subspan(1)) {
      preds.push_back(MakeTermPredicate(TermAcceptor{term.term}));
    }
  } else {
    lead = _must.filters.front()->CompileTermIterator(reader);
    rest = rest.subspan(1);
  }
  if (!lead) {
    return Filter::CompileTermIterator(reader);
  }
  for (const auto& child : rest) {
    auto pred = child->CompileTermPredicate();
    if (!pred) {
      return Filter::CompileTermIterator(reader);
    }
    preds.push_back(std::move(pred));
  }
  for (const auto& term : _must_not.terms) {
    preds.push_back(std::make_unique<NegatedPredicate>(
      MakeTermPredicate(TermAcceptor{term.term})));
  }
  for (const auto& child : _must_not.filters) {
    auto pred = child->CompileTermPredicate();
    if (!pred) {
      return Filter::CompileTermIterator(reader);
    }
    preds.push_back(std::make_unique<NegatedPredicate>(std::move(pred)));
  }
  auto predicate = CombinePredicates(std::move(preds));
  if (!predicate) {
    return lead;
  }
  return memory::make_managed<FilteredTermIterator>(std::move(lead),
                                                    std::move(predicate));
}

bool BooleanFilter::Valid() const noexcept {
  if (_must.empty() && _should.empty() && _must_not.empty()) {
    return false;
  }
  if (_min_should_match > _should.size()) {
    return false;
  }
  return !_must.empty() || _should.empty() || _min_should_match >= 1;
}

bool BooleanFilter::equals(const Filter& rhs) const noexcept {
  if (!Filter::equals(rhs)) {
    return false;
  }
  const auto& typed_rhs = sdb::basics::downCast<BooleanFilter>(rhs);
  if (_min_should_match != typed_rhs._min_should_match ||
      _merge_type != typed_rhs._merge_type) {
    return false;
  }
  const auto same = [](const Filter::ptr& lhs, const Filter::ptr& rhs) {
    return (lhs == nullptr) == (rhs == nullptr) && (!lhs || *lhs == *rhs);
  };
  return std::ranges::all_of(kAllOccur, [&](Occur occur) {
    const auto& lhs = Bucket(occur);
    const auto& other = typed_rhs.Bucket(occur);
    return lhs.terms == other.terms &&
           std::ranges::equal(lhs.filters, other.filters, same);
  });
}

PrepareCollector::ptr BooleanFilter::MakeCollectorImpl(const Scorer* scorer,
                                                       StatsArena& stats,
                                                       uint32_t threads) const {
  auto compound = std::make_unique<CompoundCollector>(scorer);
  WalkClauses(
    *this,
    [&](std::span<const TermClause> run, Occur occur) {
      const auto* const own = ClauseScorer(scorer, run.front().scorer);
      if (occur == Occur::MustNot || own == nullptr) {
        compound->Add(nullptr);
        return;
      }
      compound->Add(
        std::make_unique<ByTermsCollector>(own, run.size(), stats, threads));
    },
    [&](const Filter& filter, Occur occur) {
      compound->Add(occur == Occur::MustNot
                      ? nullptr
                      : filter.MakeCollector(*scorer, stats, threads));
    });
  return compound;
}

QueryBuilder::ptr BooleanFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  SDB_ASSERT(Valid());
  auto* compound = ctx.collector
                     ? &sdb::basics::downCast<CompoundCollector>(*ctx.collector)
                     : nullptr;
  const auto composite_boost = ctx.boost * GetBoost();

  BooleanBuilder builder{segment,         ctx.memory,  _min_should_match,
                         composite_boost, _merge_type, compound,
                         ctx.needs_terms};

  PrepareContext child = ctx;
  uint32_t idx = 0;
  const auto advance = [&] {
    child.collector = compound ? compound->Child(idx++) : nullptr;
  };
  field_id opened = field_limits::invalid();
  const TermReader* reader = nullptr;
  WalkClauses(
    *this,
    [&](std::span<const TermClause> run, Occur occur) {
      advance();
      if (opened != run.front().field) {
        opened = run.front().field;
        reader = segment.field(opened);
      }
      if (reader == nullptr) {
        for (size_t i = 0; i != run.size(); ++i) {
          builder.AddTerm(nullptr, kNoPosting, kNoBoost, occur, {});
        }
        return;
      }
      auto* const collector =
        child.collector
          ? &sdb::basics::downCast<ByTermsCollector>(*child.collector)
          : nullptr;
      if (collector) {
        SDB_ASSERT(collector->Size() == run.size());
        collector->Field(ctx.thread).Collect(*reader);
      }
      for (size_t i = 0; i != run.size(); ++i) {
        const auto meta = reader->Lookup(run[i].term);
        if (collector && meta.docs_count != 0) {
          collector->Term(ctx.thread, i).Collect(meta);
        }
        builder.AddTerm(
          reader, meta,
          occur == Occur::MustNot ? kNoBoost : composite_boost * run[i].boost,
          occur,
          collector != nullptr ? collector->Record(i) : search::StatsRecord{});
      }
    },
    [&](const Filter& filter, Occur occur) {
      advance();
      child.boost = composite_boost;
      builder.Add(filter.PrepareSegment(segment, child), occur);
    });

  return builder.Finish();
}

}  // namespace irs
