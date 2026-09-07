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

#include "iresearch/search/optimizer/boolean_rules.hpp"

#include <absl/algorithm/container.h>
#include <absl/container/inlined_vector.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <re2/re2.h>

#include <algorithm>
#include <array>
#include <iterator>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/down_cast.h"
#include "basics/system-compiler.h"
#include "iresearch/search/automaton_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/constant_score.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/granular_range_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/optimizer/common.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/regexp_utils.hpp"

namespace irs::optimizer {
namespace {

struct BooleanNormalizeTermsRule {
  static constexpr std::string_view kName = "boolean_normalize_terms";
  static constexpr std::array kTargets{Type<BooleanFilter>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct BooleanMinShouldMatchRule {
  static constexpr std::string_view kName = "boolean_min_should_match";
  static constexpr std::array kTargets{Type<BooleanFilter>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct BooleanAbsorbRule {
  static constexpr std::string_view kName = "boolean_absorb";
  static constexpr std::array kTargets{Type<BooleanFilter>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct BooleanDedupRule {
  static constexpr std::string_view kName = "boolean_dedup";
  static constexpr std::array kTargets{Type<BooleanFilter>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct BooleanNullMarkerRule {
  static constexpr std::string_view kName = "boolean_null_marker";
  static constexpr std::array kTargets{Type<BooleanFilter>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct BooleanFlattenRule {
  static constexpr std::string_view kName = "boolean_flatten";
  static constexpr std::array kTargets{Type<BooleanFilter>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct BooleanSingleClauseRule {
  static constexpr std::string_view kName = "boolean_single_clause";
  static constexpr std::array kTargets{Type<BooleanFilter>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

bool IsEmptyFilter(const Filter& filter) noexcept {
  return filter.type() == Type<Empty>::id();
}

bool IsDisjunction(const BooleanFilter& node) noexcept {
  return node.MinShouldMatch() == 1 && node.Size(Occur::Should) != 0 &&
         node.Size(Occur::Must) == 0 && node.Size(Occur::MustNot) == 0;
}

bool IsConjunction(const BooleanFilter& node) noexcept {
  return node.MinShouldMatch() == 0 && node.Size(Occur::Should) == 0 &&
         node.Size(Occur::Must) + node.Size(Occur::MustNot) != 0;
}

bool IsNegation(const BooleanFilter& node) noexcept {
  return IsConjunction(node) && node.Size(Occur::Must) == 0;
}

bool ClauseScores(const Filter& child, Occur occur,
                  const OptimizeContext& ctx) noexcept {
  if (!ctx.scored || occur == Occur::MustNot) {
    return false;
  }
  const auto* const scorer = child.GetScorer();
  return scorer == nullptr || !IsUnscored(*scorer);
}

BooleanFilter* AsNested(Filter& child, const BooleanFilter& parent, Occur occur,
                        const OptimizeContext& ctx) noexcept {
  if (child.type() != Type<BooleanFilter>::id()) {
    return nullptr;
  }
  auto& node = sdb::basics::downCast<BooleanFilter>(child);
  if (!ClauseScores(child, occur, ctx)) {
    return &node;
  }
  return node.MergeType() == parent.MergeType() ? &node : nullptr;
}

void PushDown(BooleanFilter& node) {
  const auto boost = node.GetBoost();
  const auto* const scorer = node.GetScorer();
  if (boost == kNoBoost && scorer == nullptr) {
    return;
  }
  for (const auto occur : kAllOccur) {
    auto& bucket = node.Bucket(occur);
    for (auto& term : bucket.terms) {
      term.boost *= boost;
      term.scorer = ResolveScorer(term.scorer, scorer);
    }
    for (auto& child : bucket.filters) {
      child->SetBoost(child->GetBoost() * boost);
      child->SetScorer(ResolveScorer(child->GetScorer(), scorer));
    }
  }
  node.SetBoost(kNoBoost);
  node.SetScorer(nullptr);
}

void MoveClauses(BooleanFilter& to, Occur occur, Clauses& from) {
  for (auto& term : from.terms) {
    to.Add(std::move(term), occur);
  }
  from.terms.clear();
  for (auto& filter : from.filters) {
    to.Add(std::move(filter), occur);
  }
  from.filters.clear();
}

size_t EraseFilters(std::vector<Filter::ptr>& filters, auto predicate) {
  const auto before = filters.size();
  std::erase_if(filters,
                [&](const Filter::ptr& child) { return predicate(*child); });
  return before - filters.size();
}

bool AnyFilter(std::span<const Filter::ptr> filters, auto predicate) {
  return absl::c_any_of(
    filters, [&](const Filter::ptr& child) { return predicate(*child); });
}

bool Overlaps(const Clauses& lhs, const Clauses& rhs) {
  constexpr TermPostingLess kLess{};
  auto left = lhs.terms.begin();
  auto right = rhs.terms.begin();
  while (left != lhs.terms.end() && right != rhs.terms.end()) {
    if (kLess(*left, *right)) {
      ++left;
    } else if (kLess(*right, *left)) {
      ++right;
    } else {
      return true;
    }
  }
  return AnyFilter(lhs.filters, [&](const Filter& child) {
    return AnyFilter(rhs.filters,
                     [&](const Filter& other) { return child == other; });
  });
}

size_t DedupTerms(std::vector<TermClause>& terms, ScoreMergeType merge) {
  if (terms.size() < 2) {
    return 0;
  }
  constexpr TermClauseLess kLess{};
  size_t kept = 0;
  for (size_t i = 1; i != terms.size(); ++i) {
    if (!kLess(terms[kept], terms[i])) {
      MergeBoost(terms[kept].boost, terms[i].boost, merge);
      continue;
    }
    if (++kept != i) {
      terms[kept] = std::move(terms[i]);
    }
  }
  const auto dropped = terms.size() - (kept + 1);
  terms.resize(kept + 1);
  return dropped;
}

bool Unreachable(const BooleanFilter& node) noexcept {
  return node.MinShouldMatch() > node.Size(Occur::Should);
}

size_t DedupFilters(std::vector<Filter::ptr>& filters) {
  const auto before = filters.size();
  for (size_t i = 0; i < filters.size(); ++i) {
    const auto& keep = *filters[i];
    filters.erase(
      std::remove_if(filters.begin() + i + 1, filters.end(),
                     [&](const Filter::ptr& child) { return *child == keep; }),
      filters.end());
  }
  return before - filters.size();
}

}  // namespace

bool NormalizeTerms(BooleanFilter& node) {
  bool changed = false;
  const auto merge = node.MergeType();
  for (const auto occur : kAllOccur) {
    auto& filters = node.Bucket(occur).filters;
    const auto tail = std::partition(
      filters.begin(), filters.end(), [](const Filter::ptr& child) {
        return child->type() != Type<ByTerm>::id();
      });
    if (tail == filters.end()) {
      continue;
    }
    std::vector<Filter::ptr> terms{std::make_move_iterator(tail),
                                   std::make_move_iterator(filters.end())};
    filters.erase(tail, filters.end());
    for (auto& term : terms) {
      node.Add(std::move(term), occur);
    }
    if (occur != Occur::Should || node.MinShouldMatch() <= 1) {
      DedupTerms(node.Bucket(occur).terms,
                 occur == Occur::MustNot ? ScoreMergeType::Noop : merge);
    }
    changed = true;
  }
  return changed;
}

bool BooleanNormalizeTermsRule::Apply(Filter::ptr& slot,
                                      const OptimizeContext&) {
  return NormalizeTerms(sdb::basics::downCast<BooleanFilter>(*slot));
}

bool BooleanMinShouldMatchRule::Apply(Filter::ptr& slot,
                                      const OptimizeContext&) {
  auto& node = sdb::basics::downCast<BooleanFilter>(*slot);
  const auto min_should_match = node.MinShouldMatch();
  const auto size = node.Size(Occur::Should);
  if (min_should_match > size) {
    slot = std::make_unique<Empty>();
    return true;
  }
  if (min_should_match == 0 || min_should_match != size) {
    return false;
  }
  auto should = std::exchange(node.Bucket(Occur::Should), Clauses{});
  node.SetMinShouldMatch(0);
  MoveClauses(node, Occur::Must, should);
  return true;
}

bool BooleanAbsorbRule::Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<BooleanFilter>(*slot);
  if (AnyFilter(node.Filters(Occur::Must), IsEmptyFilter) ||
      AnyFilter(node.Filters(Occur::MustNot), IsAllDocs)) {
    slot = std::make_unique<Empty>();
    return true;
  }
  if (node.Size(Occur::MustNot) != 0 && node.Size(Occur::Must) == 0 &&
      (node.MinShouldMatch() == 0 || node.Size(Occur::Should) == 0)) {
    slot = std::make_unique<Empty>();
    return true;
  }
  bool changed =
    EraseFilters(node.Bucket(Occur::MustNot).filters, IsEmptyFilter) != 0;
  changed |=
    EraseFilters(node.Bucket(Occur::Should).filters, IsEmptyFilter) != 0;
  if (Unreachable(node)) {
    slot = std::make_unique<Empty>();
    return true;
  }

  if (!ctx.scored) {
    auto& required = node.Bucket(Occur::Must);
    const auto includes_only_all =
      node.Size(Occur::MustNot) != 0 && required.terms.empty() &&
      (node.MinShouldMatch() == 0 || node.Size(Occur::Should) == 0) &&
      absl::c_all_of(required.filters, [](const Filter::ptr& child) {
        return IsAllDocs(*child);
      });
    if (!includes_only_all) {
      changed |= EraseFilters(required.filters, IsAllDocs) != 0;
    }
    const auto dropped =
      EraseFilters(node.Bucket(Occur::Should).filters, IsAllDocs);
    auto min_should_match = node.MinShouldMatch();
    min_should_match -=
      std::min<uint32_t>(min_should_match, static_cast<uint32_t>(dropped));
    if (dropped != 0 && min_should_match == 0 &&
        node.Size(Occur::MustNot) != 0 && node.Size(Occur::Must) == 0) {
      node.Add(std::make_unique<All>(), Occur::Must);
      changed = true;
    }
    if (min_should_match == 0 && node.Size(Occur::Should) != 0) {
      node.Bucket(Occur::Should) = {};
      changed = true;
    }
    if (min_should_match != node.MinShouldMatch()) {
      node.SetMinShouldMatch(min_should_match);
      changed = true;
    }
  }

  if (changed && node.MinShouldMatch() == 0 && node.Size(Occur::Must) == 0 &&
      node.Size(Occur::Should) == 0 && node.Size(Occur::MustNot) == 0) {
    auto all = std::make_unique<All>();
    all->SetBoost(node.GetBoost());
    all->SetScorer(node.GetScorer());
    slot = std::move(all);
    return true;
  }
  return changed;
}

bool BooleanDedupRule::Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<BooleanFilter>(*slot);
  if (Overlaps(node.Bucket(Occur::Must), node.Bucket(Occur::MustNot))) {
    slot = std::make_unique<Empty>();
    return true;
  }

  bool changed = false;
  auto& should = node.Bucket(Occur::Should);
  for (const auto occur : {Occur::Must, Occur::MustNot}) {
    const bool required = occur == Occur::Must;
    const bool by_posting = !required || !ctx.scored;
    const auto less = [by_posting](const TermClause& lhs,
                                   const TermClause& rhs) noexcept {
      return by_posting ? TermPostingLess{}(lhs, rhs)
                        : TermClauseLess{}(lhs, rhs);
    };
    auto& other = node.Bucket(occur);
    size_t dropped = 0;

    auto out = should.terms.begin();
    auto match = other.terms.begin();
    for (auto it = should.terms.begin(); it != should.terms.end(); ++it) {
      while (match != other.terms.end() && less(*match, *it)) {
        ++match;
      }
      if (match != other.terms.end() && !less(*it, *match)) {
        if (required) {
          MergeBoost(match->boost, it->boost, node.MergeType());
        }
        ++dropped;
        continue;
      }
      if (out != it) {
        *out = std::move(*it);
      }
      ++out;
    }
    should.terms.erase(out, should.terms.end());

    const bool by_scorer = required && ctx.scored;
    dropped += EraseFilters(should.filters, [&](const Filter& child) {
      const auto survivor =
        absl::c_find_if(other.filters, [&](const Filter::ptr& candidate) {
          return *candidate == child &&
                 (!by_scorer || candidate->GetScorer() == child.GetScorer());
        });
      if (survivor == other.filters.end()) {
        return false;
      }
      if (required && ctx.scored) {
        auto boost = (**survivor).GetBoost();
        MergeBoost(boost, child.GetBoost(), node.MergeType());
        (**survivor).SetBoost(boost);
      }
      return true;
    });

    if (dropped == 0) {
      continue;
    }
    changed = true;
    if (required) {
      const auto min_should_match = node.MinShouldMatch();
      node.SetMinShouldMatch(
        min_should_match -
        std::min<uint32_t>(min_should_match, static_cast<uint32_t>(dropped)));
    }
  }

  if (Unreachable(node)) {
    slot = std::make_unique<Empty>();
    return true;
  }

  const auto merge = node.MergeType();
  changed |= DedupTerms(node.Bucket(Occur::Must).terms, merge) != 0;
  if (node.MinShouldMatch() <= 1) {
    const auto dropped = DedupTerms(should.terms, merge);
    changed |= dropped != 0;
    if (Unreachable(node)) {
      slot = std::make_unique<Empty>();
      return true;
    }
  }

  changed |= DedupFilters(node.Bucket(Occur::MustNot).filters) != 0;
  if (!ctx.scored) {
    changed |= DedupFilters(node.Bucket(Occur::Must).filters) != 0;
    if (node.MinShouldMatch() <= 1) {
      changed |= DedupFilters(should.filters) != 0;
      if (Unreachable(node)) {
        slot = std::make_unique<Empty>();
        return true;
      }
    }
  }
  return changed;
}

namespace {

template<typename... Ts>
field_id AnyFieldOf(const Filter& node) {
  field_id field = field_limits::invalid();
  (void)((node.type() == Type<Ts>::id()
            ? (field = sdb::basics::downCast<Ts>(node).field_id(), true)
            : false) ||
         ...);
  return field;
}

field_id RequiringLeafFieldOf(const Filter& node) {
  return AnyFieldOf<ByTerm, ByPrefix, ByRange, ByGranularRange, ByPhrase,
                    AutomatonFilter, LevenshteinAutomatonFilter>(node);
}

void CollectAnchors(const Filter& node,
                    sdb::containers::FlatHashSet<field_id>& out);

void CollectBucketAnchors(const BooleanFilter& node,
                          sdb::containers::FlatHashSet<field_id>& out) {
  const auto& must = node.Bucket(Occur::Must);
  for (const auto& term : must.terms) {
    out.insert(term.field);
  }
  for (const auto& child : must.filters) {
    CollectAnchors(*child, out);
  }

  const auto& should = node.Bucket(Occur::Should);
  if (node.MinShouldMatch() < 1 || should.empty()) {
    return;
  }
  sdb::containers::FlatHashSet<field_id> common;
  sdb::containers::FlatHashSet<field_id> branch;
  bool first = true;
  const auto fold = [&](auto&& collect) {
    if (!first && common.empty()) {
      return;
    }
    branch.clear();
    collect(branch);
    if (first) {
      first = false;
      common.swap(branch);
      return;
    }
    absl::erase_if(
      common, [&](const field_id field) { return !branch.contains(field); });
  };
  for (const auto& term : should.terms) {
    fold([&](auto& b) { b.insert(term.field); });
  }
  for (const auto& child : should.filters) {
    fold([&](auto& b) { CollectAnchors(*child, b); });
  }
  out.insert(common.begin(), common.end());
}

void CollectAnchors(const Filter& node,
                    sdb::containers::FlatHashSet<field_id>& out) {
  if (node.type() == Type<BooleanFilter>::id()) {
    CollectBucketAnchors(sdb::basics::downCast<BooleanFilter>(node), out);
    return;
  }
  if (const auto field = RequiringLeafFieldOf(node);
      field_limits::valid(field)) {
    out.insert(field);
  }
}

}  // namespace

bool BooleanNullMarkerRule::Apply(Filter::ptr& slot,
                                  const OptimizeContext& ctx) {
  if (ctx.null_markers == nullptr || ctx.null_markers->empty()) {
    return false;
  }
  auto& node = sdb::basics::downCast<BooleanFilter>(*slot);
  auto& excluded = node.Bucket(Occur::MustNot);
  if (excluded.empty()) {
    return false;
  }
  sdb::containers::FlatHashSet<field_id> anchors;
  bool ready = false;
  const auto vacuous = [&](field_id marker) {
    const auto it = ctx.null_markers->find(marker);
    if (it == ctx.null_markers->end()) {
      return false;
    }
    if (!ready) {
      ready = true;
      CollectBucketAnchors(node, anchors);
    }
    return anchors.contains(it->second);
  };

  const auto before = excluded.size();
  std::erase_if(excluded.terms, [&](const TermClause& clause) {
    return vacuous(clause.field);
  });
  std::erase_if(excluded.filters, [&](const Filter::ptr& child) {
    const auto field = RequiringLeafFieldOf(*child);
    return field_limits::valid(field) && vacuous(field);
  });
  return excluded.size() != before;
}

bool BooleanFlattenRule::Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<BooleanFilter>(*slot);

  const auto dissolves = [&](Occur occur, const BooleanFilter& nested) {
    switch (occur) {
      case Occur::Must:
        return IsConjunction(nested) ||
               (node.MinShouldMatch() == 0 && nested.MinShouldMatch() == 0 &&
                nested.Size(Occur::Must) != 0);
      case Occur::Should:
        return node.MinShouldMatch() == 1 && IsDisjunction(nested);
      case Occur::MustNot:
        return IsDisjunction(nested) || IsNegation(nested);
    }
    SDB_UNREACHABLE();
  };

  absl::InlinedVector<std::pair<Occur, Filter::ptr>, 4> nested;
  for (const auto occur : kAllOccur) {
    for (auto& child : node.Bucket(occur).filters) {
      const auto* inner = AsNested(*child, node, occur, ctx);
      if (inner != nullptr && dissolves(occur, *inner)) {
        nested.emplace_back(occur, std::move(child));
      }
    }
  }
  if (nested.empty()) {
    return false;
  }
  for (const auto occur : kAllOccur) {
    std::erase_if(node.Bucket(occur).filters,
                  [](const Filter::ptr& child) { return child == nullptr; });
  }

  for (auto& [occur, child] : nested) {
    auto& inner = sdb::basics::downCast<BooleanFilter>(*child);
    PushDown(inner);
    if (occur == Occur::Should) {
      MoveClauses(node, Occur::Should, inner.Bucket(Occur::Should));
      continue;
    }
    if (occur == Occur::Must) {
      MoveClauses(node, Occur::Must, inner.Bucket(Occur::Must));
      MoveClauses(node, Occur::MustNot, inner.Bucket(Occur::MustNot));
      MoveClauses(node, Occur::Should, inner.Bucket(Occur::Should));
      continue;
    }
    if (IsDisjunction(inner)) {
      MoveClauses(node, Occur::MustNot, inner.Bucket(Occur::Should));
      continue;
    }
    auto& excluded = inner.Bucket(Occur::MustNot);
    if (excluded.size() == 1) {
      MoveClauses(node, Occur::Must, excluded);
      continue;
    }
    inner.Bucket(Occur::Should) = std::exchange(excluded, Clauses{});
    inner.SetMinShouldMatch(1);
    node.Add(std::move(child), Occur::Must);
  }
  return true;
}

bool BooleanSingleClauseRule::Apply(Filter::ptr& slot,
                                    const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<BooleanFilter>(*slot);
  if (node.Size(Occur::MustNot) != 0) {
    return false;
  }
  const auto must = node.Size(Occur::Must);
  const auto should = node.Size(Occur::Should);
  Occur occur;
  if (must == 1 && should == 0) {
    occur = Occur::Must;
  } else if (must == 0 && should == 1 && node.MinShouldMatch() == 1) {
    occur = Occur::Should;
  } else {
    return false;
  }

  auto& bucket = node.Bucket(occur);
  if (!bucket.terms.empty()) {
    auto& clause = bucket.terms.front();
    auto by_term = std::make_unique<ByTerm>();
    *by_term->mutable_field_id() = clause.field;
    by_term->mutable_options()->term = std::move(clause.term);
    by_term->SetBoost(node.GetBoost() * clause.boost);
    by_term->SetScorer(ResolveScorer(clause.scorer, node.GetScorer()));
    slot = std::move(by_term);
    return true;
  }
  auto& child = *bucket.filters.front();
  FoldBoost(child, node.GetBoost(), ctx.scored);
  child.SetScorer(ResolveScorer(child.GetScorer(), node.GetScorer()));
  slot = std::move(bucket.filters.front());
  return true;
}

namespace {

struct OrAcceptorFusionRule {
  static constexpr std::string_view kName = "or_acceptor_fusion";
  static constexpr std::array kTargets{Type<BooleanFilter>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);

 private:
  struct AcceptorInfo {
    field_id field;
    score_t boost;
    size_t scored_terms_limit;
  };

  static std::optional<AcceptorInfo> InfoOf(const Filter& child);
  static void RenderQuoted(std::string& out, bytes_view bytes);
  static bool RenderWildcard(std::string& out, bytes_view pattern);
  static bool Render(std::string& out, const Filter& child);
};

struct AndAcceptorFusionRule {
  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);

 private:
  struct Operand {
    field_id field;
    automaton acceptor;
    bstring pattern;
  };

  static std::optional<Operand> OperandOf(const Filter& child);
};

inline constexpr size_t kTermRank = 0;

bool IsAlternation(const BooleanFilter& node) noexcept {
  return node.MinShouldMatch() == 1 && node.Size(Occur::Must) == 0 &&
         node.Size(Occur::MustNot) == 0;
}

bool IsAllRequired(const BooleanFilter& node) noexcept {
  return node.MinShouldMatch() == 0 && node.Size(Occur::Should) == 0;
}

}  // namespace

std::optional<OrAcceptorFusionRule::AcceptorInfo> OrAcceptorFusionRule::InfoOf(
  const Filter& child) {
  const auto info = [](const auto& filter, size_t scored_terms_limit) {
    return AcceptorInfo{filter.field_id(), filter.GetBoost(),
                        scored_terms_limit};
  };
  const auto type = child.type();
  if (type == Type<ByTerm>::id()) {
    return info(sdb::basics::downCast<ByTerm>(child), 0);
  }
  if (type == Type<ByPrefix>::id()) {
    const auto& filter = sdb::basics::downCast<ByPrefix>(child);
    return info(filter, filter.options().scored_terms_limit);
  }
  if (type == Type<ByWildcard>::id()) {
    const auto& filter = sdb::basics::downCast<ByWildcard>(child);
    return info(filter, filter.options().scored_terms_limit);
  }
  if (type == Type<ByRegexp>::id()) {
    const auto& filter = sdb::basics::downCast<ByRegexp>(child);
    return info(filter, filter.options().scored_terms_limit);
  }
  if (type == Type<AutomatonFilter>::id()) {
    const auto& filter = sdb::basics::downCast<AutomatonFilter>(child);
    return info(filter, filter.options().scored_terms_limit);
  }
  return std::nullopt;
}

void OrAcceptorFusionRule::RenderQuoted(std::string& out, bytes_view bytes) {
  const auto chars = ViewCast<char>(bytes);
  absl::StrAppend(&out, RE2::QuoteMeta({chars.data(), chars.size()}));
}

bool OrAcceptorFusionRule::RenderWildcard(std::string& out,
                                          bytes_view pattern) {
  bstring chunk;
  const auto flush = [&] {
    RenderQuoted(out, chunk);
    chunk.clear();
  };
  for (size_t i = 0; i < pattern.size(); ++i) {
    switch (pattern[i]) {
      case '%':
        flush();
        absl::StrAppend(&out, "(?s:.)*");
        break;
      case '_':
        flush();
        absl::StrAppend(&out, "(?s:.)");
        break;
      case '\\':
        if (++i == pattern.size()) {
          return false;
        }
        chunk += pattern[i];
        break;
      default:
        chunk += pattern[i];
        break;
    }
  }
  flush();
  return true;
}

bool OrAcceptorFusionRule::Render(std::string& out, const Filter& child) {
  const auto type = child.type();
  if (type == Type<ByTerm>::id()) {
    RenderQuoted(out, sdb::basics::downCast<ByTerm>(child).options().term);
    return true;
  }
  if (type == Type<ByPrefix>::id()) {
    RenderQuoted(out, sdb::basics::downCast<ByPrefix>(child).options().term);
    absl::StrAppend(&out, "(?s:.)*");
    return true;
  }
  if (type == Type<ByWildcard>::id()) {
    return RenderWildcard(
      out, sdb::basics::downCast<ByWildcard>(child).options().term);
  }
  if (type == Type<AutomatonFilter>::id()) {
    const auto& options =
      sdb::basics::downCast<AutomatonFilter>(child).options();
    if (options.pattern.empty()) {
      return false;
    }
    const auto chars = ViewCast<char>(bytes_view{options.pattern});
    absl::StrAppend(&out, "(?:");
    out.append(chars.data(), chars.size());
    absl::StrAppend(&out, ")");
    return true;
  }
  SDB_ASSERT(type == Type<ByRegexp>::id());
  const auto& options = sdb::basics::downCast<ByRegexp>(child).options();
  if (options.syntax != RegexpSyntax::Perl) {
    return false;
  }
  const auto chars = ViewCast<char>(bytes_view{options.pattern});
  out.append(chars.data(), chars.size());
  return true;
}

bool OrAcceptorFusionRule::Apply(Filter::ptr& slot,
                                 const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<BooleanFilter>(*slot);
  const auto count = node.Size(Occur::Should);
  if (count < 2 || !IsAlternation(node)) {
    return false;
  }
  const auto terms = node.Terms(Occur::Should);
  const auto filters = node.Filters(Occur::Should);

  field_id field;
  score_t boost;
  const Scorer* scorer;
  if (!terms.empty()) {
    field = terms.front().field;
    boost = terms.front().boost;
    scorer = terms.front().scorer;
  } else if (const auto head = InfoOf(*filters.front()); head) {
    field = head->field;
    boost = head->boost;
    scorer = filters.front()->GetScorer();
  } else {
    return false;
  }

  size_t seekable = terms.size();
  size_t scored_terms_limit = 0;
  for (const auto& term : terms) {
    if (term.field != field || term.scorer != scorer) {
      return false;
    }
    if (ctx.scored && term.boost != boost) {
      return false;
    }
  }
  for (const auto& child : filters) {
    const auto info = InfoOf(*child);
    if (!info || info->field != field || child->GetScorer() != scorer) {
      return false;
    }
    if (ctx.scored && info->boost != boost) {
      return false;
    }
    seekable += child->type() == Type<ByPrefix>::id();
    scored_terms_limit += info->scored_terms_limit;
  }
  if (filters.empty()) {
    return false;
  }
  if (seekable == count && !ctx.fuse_seekable_acceptors) {
    return false;
  }

  std::vector<std::string> fragments;
  fragments.reserve(count);
  for (const auto& term : terms) {
    auto& fragment = fragments.emplace_back("(?:");
    RenderQuoted(fragment, term.term);
    absl::StrAppend(&fragment, ")");
  }
  for (const auto& child : filters) {
    auto& fragment = fragments.emplace_back("(?:");
    if (!Render(fragment, *child)) {
      return false;
    }
    absl::StrAppend(&fragment, ")");
  }
  const auto rendered = absl::StrJoin(fragments, "|");
  const auto pattern = ViewCast<byte_type>(std::string_view{rendered});
  auto dfa = FromRegexp(pattern, kDefaultMaxDfaStates, RegexpSyntax::Perl);
  if (dfa.NumStates() == 0 || !Validate(dfa)) {
    return false;
  }
  auto fused = std::make_unique<AutomatonFilter>();
  *fused->mutable_field_id() = field;
  *fused->mutable_options() =
    AutomatonOptions{std::move(dfa), pattern, scored_terms_limit};
  fused->SetBoost(ctx.scored ? node.GetBoost() * boost : node.GetBoost());
  fused->SetScorer(scorer);
  slot = std::move(fused);
  return true;
}

std::optional<AndAcceptorFusionRule::Operand> AndAcceptorFusionRule::OperandOf(
  const Filter& child) {
  const auto type = child.type();
  if (type == Type<ByTerm>::id()) {
    const auto& filter = sdb::basics::downCast<ByTerm>(child);
    return Operand{filter.field_id(), MakeTermAcceptor(filter.options().term),
                   filter.options().term};
  }
  if (type == Type<ByPrefix>::id()) {
    const auto& filter = sdb::basics::downCast<ByPrefix>(child);
    auto pattern = filter.options().term;
    pattern += static_cast<byte_type>('%');
    return Operand{filter.field_id(), MakePrefixAcceptor(filter.options().term),
                   std::move(pattern)};
  }
  if (type == Type<ByRange>::id()) {
    const auto& filter = sdb::basics::downCast<ByRange>(child);
    const auto& range = filter.options().range;
    const auto bound = [](const bstring& value, BoundType type) {
      return type == BoundType::Unbounded ? bytes_view{} : bytes_view{value};
    };
    bstring pattern;
    pattern += static_cast<byte_type>(
      range.min_type == BoundType::Exclusive ? '(' : '[');
    pattern += range.min;
    pattern += static_cast<byte_type>('.');
    pattern += static_cast<byte_type>('.');
    pattern += range.max;
    pattern += static_cast<byte_type>(
      range.max_type == BoundType::Exclusive ? ')' : ']');
    return Operand{filter.field_id(),
                   MakeRangeAcceptor(bound(range.min, range.min_type),
                                     bound(range.max, range.max_type),
                                     range.min_type == BoundType::Inclusive,
                                     range.max_type == BoundType::Inclusive),
                   std::move(pattern)};
  }
  if (type == Type<AutomatonFilter>::id()) {
    const auto& filter = sdb::basics::downCast<AutomatonFilter>(child);
    const auto& options = filter.options();
    if (!options.compiled) {
      return std::nullopt;
    }
    return Operand{filter.field_id(), options.compiled->acceptor,
                   options.pattern};
  }
  if (type == Type<LevenshteinAutomatonFilter>::id()) {
    const auto& filter =
      sdb::basics::downCast<LevenshteinAutomatonFilter>(child);
    const auto& options = filter.options();
    if (!options.compiled) {
      return std::nullopt;
    }
    auto pattern = options.target;
    pattern += static_cast<byte_type>('~');
    return Operand{filter.field_id(), options.compiled->acceptor,
                   std::move(pattern)};
  }
  return std::nullopt;
}

bool AndAcceptorFusionRule::Apply(Filter::ptr& slot,
                                  const OptimizeContext& ctx) {
  if (!ctx.fuse_acceptor_intersections || ctx.scored) {
    return false;
  }
  auto& node = sdb::basics::downCast<BooleanFilter>(*slot);
  if (node.Size(Occur::Must) < 2 || !IsAllRequired(node)) {
    return false;
  }
  auto& must = node.Bucket(Occur::Must);

  const auto term_count = must.terms.size();
  const auto is_term = [&](size_t index) { return index < term_count; };
  const auto rank = [&](size_t index) {
    return is_term(index) ? kTermRank
                          : AcceptorRank(*must.filters[index - term_count]);
  };
  const auto operand_of = [&](size_t index) -> std::optional<Operand> {
    if (!is_term(index)) {
      return OperandOf(*must.filters[index - term_count]);
    }
    const auto& term = must.terms[index];
    return Operand{term.field, MakeTermAcceptor(term.term), term.term};
  };

  absl::InlinedVector<size_t, 8> order;
  order.reserve(must.size());
  for (size_t i = 0; i < must.size(); ++i) {
    order.emplace_back(i);
  }
  absl::c_stable_sort(
    order, [&](size_t lhs, size_t rhs) { return rank(lhs) < rank(rhs); });
  if (!is_term(order.front()) &&
      must.filters[order.front() - term_count]->type() ==
        Type<LevenshteinAutomatonFilter>::id()) {
    return false;
  }
  auto driver = operand_of(order.front());
  if (!driver) {
    return false;
  }

  auto fused = std::move(driver->acceptor);
  auto pattern = std::move(driver->pattern);
  absl::InlinedVector<size_t, 8> consumed;
  for (const auto index : std::span{order}.subspan(1)) {
    auto operand = operand_of(index);
    if (!operand || operand->field != driver->field) {
      continue;
    }
    auto product =
      IntersectAcceptors(fused, operand->acceptor, kDefaultMaxDfaStates);
    if (!product || !Validate(*product)) {
      continue;
    }
    fused = std::move(*product);
    pattern += static_cast<byte_type>('&');
    pattern += operand->pattern;
    consumed.emplace_back(index);
  }
  if (consumed.empty()) {
    return false;
  }

  auto fused_filter = std::make_unique<AutomatonFilter>();
  *fused_filter->mutable_field_id() = driver->field;
  *fused_filter->mutable_options() =
    AutomatonOptions{std::move(fused), pattern, 0};

  consumed.emplace_back(order.front());
  std::vector<bool> dead(must.size(), false);
  for (const auto index : consumed) {
    dead[index] = true;
  }
  auto terms_out = must.terms.begin();
  for (size_t i = 0; i != term_count; ++i) {
    if (!dead[i]) {
      if (terms_out != must.terms.begin() + static_cast<ptrdiff_t>(i)) {
        *terms_out = std::move(must.terms[i]);
      }
      ++terms_out;
    }
  }
  must.terms.erase(terms_out, must.terms.end());
  auto filters_out = must.filters.begin();
  for (size_t i = 0; i != must.filters.size(); ++i) {
    if (!dead[term_count + i]) {
      if (filters_out != must.filters.begin() + static_cast<ptrdiff_t>(i)) {
        *filters_out = std::move(must.filters[i]);
      }
      ++filters_out;
    }
  }
  must.filters.erase(filters_out, must.filters.end());

  if (must.empty() && node.Size(Occur::MustNot) == 0) {
    slot = std::move(fused_filter);
    return true;
  }
  node.Add(Filter::ptr{std::move(fused_filter)}, Occur::Must);
  return true;
}

size_t AcceptorRank(const Filter& filter) noexcept {
  const auto type = filter.type();
  if (type == Type<ByTerm>::id()) {
    return kTermRank;
  }
  if (type == Type<ByPrefix>::id() || type == Type<ByRange>::id()) {
    return 2;
  }
  if (type == Type<AutomatonFilter>::id() ||
      type == Type<LevenshteinAutomatonFilter>::id()) {
    return 3;
  }
  return 4;
}

void FuseConjunctions(Filter::ptr& root, const OptimizeContext& ctx) {
  if (!root || !ctx.fuse_acceptor_intersections) {
    return;
  }
  TraverseFilter(root, [&](Filter::ptr& slot) {
    if (slot->type() == Type<BooleanFilter>::id()) {
      AndAcceptorFusionRule::Apply(slot, ctx);
    }
  });
}

void InitBooleanNormalizeTerms() { RegisterRule<BooleanNormalizeTermsRule>(); }

void InitBooleanMinShouldMatch() { RegisterRule<BooleanMinShouldMatchRule>(); }

void InitBooleanAbsorb() { RegisterRule<BooleanAbsorbRule>(); }

void InitBooleanDedup() { RegisterRule<BooleanDedupRule>(); }

void InitBooleanNullMarker() { RegisterRule<BooleanNullMarkerRule>(); }

void InitBooleanFlatten() { RegisterRule<BooleanFlattenRule>(); }

void InitBooleanSingleClause() { RegisterRule<BooleanSingleClauseRule>(); }

void InitOrAcceptorFusion() { RegisterRule<OrAcceptorFusionRule>(); }

}  // namespace irs::optimizer
