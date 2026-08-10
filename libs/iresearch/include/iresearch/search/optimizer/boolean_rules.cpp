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
#include <absl/hash/hash.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <re2/re2.h>

#include <algorithm>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/system-compiler.h"
#include "iresearch/search/automaton_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/granular_range_filter.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/optimizer/common.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::optimizer {
namespace {

struct FlattenAnd {
  static constexpr std::string_view kName = "flatten_and";
  static constexpr std::array kTargets{Type<And>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct FlattenOr {
  static constexpr std::string_view kName = "flatten_or";
  static constexpr std::array kTargets{Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct AndExclusionCoalesceRule {
  static constexpr std::string_view kName = "and_exclusion_coalesce";
  static constexpr std::array kTargets{Type<And>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct AndEmptyRule {
  static constexpr std::string_view kName = "and_empty";
  static constexpr std::array kTargets{Type<And>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct OrEmptyRule {
  static constexpr std::string_view kName = "or_empty";
  static constexpr std::array kTargets{Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct AndAllFoldRule {
  static constexpr std::string_view kName = "and_all_fold";
  static constexpr std::array kTargets{Type<And>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct OrAllFoldRule {
  static constexpr std::string_view kName = "or_all_fold";
  static constexpr std::array kTargets{Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct SingleChildRule {
  static constexpr std::string_view kName = "single_child";
  static constexpr std::array kTargets{Type<And>::id(), Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

// Exclusion normal form. Three algebraic steps over the excludes list:
// an excluded any-of disjunction (min_match 1) splits into flat siblings
// (A \ (a|b) == A \ a \ b), excluding the same filter twice is excluding
// it once, and a null-marker exclude is dropped when the include side anchors
// one of its column's value fields -- a postings-only positive on F already
// rejects every row F's marker matches (OptimizeContext.null_markers is the
// embedder's contract for that disjointness; without the map the marker step is
// inert). Only the flat Exclusion shape needs handling: coalescing normalizes
// every And/Not combination to it within the same slot visit.
struct ExclusionNormalizeRule {
  static constexpr std::string_view kName = "exclusion_normalize";
  static constexpr std::array kTargets{Type<Exclusion>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct OrAcceptorFusionRule {
  static constexpr std::string_view kName = "or_acceptor_fusion";
  static constexpr std::array kTargets{Type<Or>::id()};
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
    bstring pattern;
    TermBounds bounds;
    TermAcceptorSource::ptr exact;
  };

  static std::optional<Operand> OperandOf(const Filter& child);
};

struct ByTermsRule {
  static constexpr std::string_view kName = "by_terms";
  static constexpr std::array kTargets{Type<And>::id(), Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct OrUnsatRule {
  static constexpr std::string_view kName = "or_unsat";
  static constexpr std::array kTargets{Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct OrAllRequiredRule {
  static constexpr std::string_view kName = "or_all_required";
  static constexpr std::array kTargets{Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct MixedDegenerateRule {
  static constexpr std::string_view kName = "mixed_degenerate";
  static constexpr std::array kTargets{Type<MixedBooleanFilter>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

template<typename T>
bool CanSplice(const T& parent, const Filter& child) noexcept {
  if (child.type() != Type<T>::id()) {
    return false;
  }
  const auto& inner = sdb::basics::downCast<T>(child);
  if (inner.empty() || inner.Boost() != kNoBoost ||
      inner.merge_type() != parent.merge_type()) {
    return false;
  }
  if constexpr (std::is_same_v<T, Or>) {
    return inner.min_match_count() == 1 && parent.min_match_count() == 1;
  } else {
    return true;
  }
}

template<typename T>
bool Flatten(T& node) {
  auto& children = node.mutable_filters();

  std::vector<bool> splice(children.size(), false);
  size_t spliced = 0;
  size_t spliced_count = 0;
  for (size_t i = 0; i < children.size(); ++i) {
    if (CanSplice(node, *children[i])) {
      splice[i] = true;
      spliced_count++;
      spliced += sdb::basics::downCast<T>(*children[i]).size();
    }
  }
  if (spliced == 0) {
    return false;
  }

  std::vector<Filter::ptr> flat;
  flat.reserve(children.size() - spliced_count + spliced);
  for (size_t i = 0; i < children.size(); ++i) {
    if (splice[i]) {
      for (auto& grandchild :
           sdb::basics::downCast<T>(*children[i]).mutable_filters()) {
        flat.push_back(std::move(grandchild));
      }
    } else {
      flat.push_back(std::move(children[i]));
    }
  }
  children = std::move(flat);
  return true;
}

void MergeBoost(score_t& boost, score_t other, ScoreMergeType type) {
  switch (type) {
    case irs::ScoreMergeType::Sum:
      boost += other;
      break;
    case irs::ScoreMergeType::Max:
      boost = std::max(boost, other);
      break;
    case irs::ScoreMergeType::Noop:
      break;
    default:
      SDB_UNREACHABLE();
  }
}

template<typename T>
std::pair<size_t, score_t> CountAllDocs(const T& node) {
  size_t count = 0;
  score_t boost = 0.F;
  for (const auto& child : node) {
    if (IsAllDocs(*child)) {
      ++count;
      MergeBoost(boost, child->BoostImpl(), node.merge_type());
    }
  }
  return {count, boost};
}

template<typename T>
void EraseAllDocs(T& node) {
  auto& children = node.mutable_filters();
  const auto it =
    std::remove_if(children.begin(), children.end(),
                   [](const auto& child) { return IsAllDocs(*child); });
  children.erase(it, children.end());
}

}  // namespace

bool FlattenAnd::Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
  return Flatten(sdb::basics::downCast<And>(*slot));
}

bool FlattenOr::Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
  return Flatten(sdb::basics::downCast<Or>(*slot));
}

bool AndExclusionCoalesceRule::Apply(Filter::ptr& slot,
                                     const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<And>(*slot);
  auto& children = node.mutable_filters();

  const auto is_not = [](const Filter::ptr& child) {
    return child->type() == Type<Not>::id() &&
           sdb::basics::downCast<Not>(*child).filter() != nullptr;
  };
  const auto is_coalescable_exclusion = [&](const Filter::ptr& child) {
    return child->type() == Type<Exclusion>::id();
  };

  const size_t coalescable = absl::c_count_if(children, [&](const auto& child) {
    return is_not(child) || is_coalescable_exclusion(child);
  });
  if (coalescable == 0 || children.size() == 1) {
    return false;
  }
  const bool produces_exclude =
    absl::c_any_of(children, [&](const Filter::ptr& child) {
      if (is_not(child)) {
        return true;
      }
      return is_coalescable_exclusion(child) &&
             !sdb::basics::downCast<Exclusion>(*child).GetExcludes().empty();
    });
  if (!produces_exclude) {
    return false;
  }

  std::vector<Filter::ptr> includes;
  std::vector<Filter::ptr> excludes;
  includes.reserve(children.size());
  excludes.reserve(children.size());
  for (auto& child : children) {
    if (is_not(child)) {
      excludes.emplace_back(
        std::move(sdb::basics::downCast<Not>(*child).mutable_filter()));
    } else if (is_coalescable_exclusion(child)) {
      auto& ex = sdb::basics::downCast<Exclusion>(*child);
      if (auto& incl = ex.mutable_include(); incl) {
        includes.emplace_back(std::move(incl));
      }
      for (auto& excl : ex.mutable_excludes()) {
        excludes.emplace_back(std::move(excl));
      }
    } else {
      includes.emplace_back(std::move(child));
    }
  }

  auto exclusion = std::make_unique<Exclusion>();
  if (!includes.empty()) {
    auto include = includes.size() == 1
                     ? std::move(includes.front())
                     : MakeBoolean<And>(std::move(includes), node.merge_type());
    exclusion->include(std::move(include));
  }
  for (auto& exclude : excludes) {
    exclusion->exclude(std::move(exclude));
  }
  exclusion->boost(node.Boost());
  slot = std::move(exclusion);
  return true;
}

bool AndEmptyRule::Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
  const auto& node = sdb::basics::downCast<And>(*slot);
  const bool has_empty = absl::c_any_of(
    node, [](const auto& child) { return child->type() == Type<Empty>::id(); });
  if (node.size() != 0 && !has_empty) {
    return false;
  }
  slot = std::make_unique<Empty>();
  return true;
}

bool OrEmptyRule::Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<Or>(*slot);
  SDB_ASSERT(node.min_match_count());
  auto& children = node.mutable_filters();
  const auto it = std::remove_if(
    children.begin(), children.end(),
    [](const auto& child) { return child->type() == Type<Empty>::id(); });
  if (it == children.end()) {
    return false;
  }
  children.erase(it, children.end());
  if (children.empty()) {
    slot = std::make_unique<Empty>();
  }
  return true;
}

bool AndAllFoldRule::Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<And>(*slot);
  const auto [all_count, all_boost] = CountAllDocs(node);
  if (all_count == 0) {
    return false;
  }
  if (all_count == node.size()) {
    slot = node.MakeAllDocsFilter(node.Boost() * all_boost);
    return true;
  }
  if (!ctx.scored && node.size() - all_count == 1) {
    auto& children = node.mutable_filters();
    const auto it = absl::c_find_if(
      children, [](const auto& child) { return !IsAllDocs(*child); });
    SDB_ASSERT(it != children.end());
    if (auto* boostable = dynamic_cast<FilterWithBoost*>(it->get())) {
      boostable->boost(boostable->Boost() + all_boost);
      EraseAllDocs(node);
      return true;
    }
  }
  if (all_count < 2) {
    return false;
  }
  EraseAllDocs(node);
  node.mutable_filters().emplace_back(node.MakeAllDocsFilter(all_boost));
  return true;
}

bool OrAllFoldRule::Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<Or>(*slot);
  const auto min_match = node.min_match_count();
  SDB_ASSERT(min_match);
  const auto [all_count, all_boost] = CountAllDocs(node);
  if (all_count == 0) {
    return false;
  }
  if (min_match <= all_count) {
    if (!ctx.scored || all_count == node.size()) {
      slot = node.MakeAllDocsFilter(node.Boost() * all_boost);
      return true;
    }
  }
  if (all_count < 2) {
    return false;
  }
  EraseAllDocs(node);
  auto& children = node.mutable_filters();
  children.emplace_back(node.MakeAllDocsFilter(all_boost));
  const size_t new_min_match = std::max(min_match - (all_count - 1), 1UL);
  auto replacement = MakeBoolean<Or>(std::move(children), node.merge_type());
  replacement->min_match_count(new_min_match);
  replacement->boost(node.Boost());
  slot = std::move(replacement);
  return true;
}

bool SingleChildRule::Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<BooleanFilter>(*slot);
  if (node.size() != 1) {
    return false;
  }
  if (slot->type() == Type<Or>::id() &&
      sdb::basics::downCast<Or>(node).min_match_count() != 1) {
    return false;
  }
  auto& front = node.mutable_filters().front();
  if (!TryFoldBoost(*front, node.Boost(), ctx.scored)) {
    return false;
  }
  auto child = std::move(front);
  slot = std::move(child);
  return true;
}

bool ByTermsRule::Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<BooleanFilter>(*slot);
  if (node.size() < 2) {
    return false;
  }
  const bool is_and = slot->type() == Type<And>::id();
  const size_t min_match =
    is_and ? 0 : sdb::basics::downCast<Or>(node).min_match_count();
  if (!is_and && min_match == 0) {
    return false;
  }
  if (node[0].type() != Type<ByTerm>::id()) {
    return false;
  }
  const auto field = sdb::basics::downCast<ByTerm>(node[0]).field_id();
  const bool same_field = absl::c_all_of(node, [&](const auto& child) {
    return child->type() == Type<ByTerm>::id() &&
           sdb::basics::downCast<ByTerm>(*child).field_id() == field;
  });
  if (!same_field) {
    return false;
  }
  ByTermsOptions options;
  options.merge_type = node.merge_type();
  bool has_duplicates = false;
  for (const auto& child : node) {
    auto& term_filter = sdb::basics::downCast<ByTerm>(*child);
    auto it =
      options.terms.emplace(term_filter.options().term, term_filter.Boost());
    if (!it.second) {
      MergeBoost(const_cast<score_t&>(it.first->boost), term_filter.Boost(),
                 node.merge_type());
      has_duplicates = true;
    }
  }
  if (has_duplicates && !is_and && min_match != 1) {
    return false;
  }
  options.min_match = is_and ? options.terms.size() : min_match;
  auto by_terms = std::make_unique<ByTerms>();
  *by_terms->mutable_field_id() = field;
  *by_terms->mutable_options() = std::move(options);
  by_terms->boost(node.Boost());
  slot = std::move(by_terms);
  return true;
}

std::optional<OrAcceptorFusionRule::AcceptorInfo> OrAcceptorFusionRule::InfoOf(
  const Filter& child) {
  const auto info = [](const auto& filter, size_t scored_terms_limit) {
    return AcceptorInfo{filter.field_id(), filter.Boost(), scored_terms_limit};
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
      // `.` is `[^\n]` unless `(?s)` says otherwise, and the wildcard dialect
      // puts no such hole in what `%` and `_` stand for.
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
    // The rendering is a Perl regexp, so only a pattern that is one can be
    // spliced into it: a wildcard reads `%` and `_` as literals here, and a
    // fused pattern parses as neither dialect.
    if (options.kind != PatternKind::RegexpPerl || options.pattern.empty()) {
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
  auto& node = sdb::basics::downCast<Or>(*slot);
  if (node.size() < 2 || node.min_match_count() != 1) {
    return false;
  }
  const auto head = InfoOf(node[0]);
  if (!head) {
    return false;
  }
  size_t seekable = 0;
  size_t scored_terms_limit = 0;
  for (const auto& child : node) {
    const auto info = InfoOf(*child);
    if (!info || info->field != head->field) {
      return false;
    }
    if (ctx.scored && info->boost != head->boost) {
      return false;
    }
    seekable += child->type() == Type<ByTerm>::id() ||
                child->type() == Type<ByPrefix>::id();
    scored_terms_limit += info->scored_terms_limit;
  }
  if (seekable == node.size() && !ctx.fuse_seekable_acceptors) {
    return false;
  }
  std::vector<std::string> fragments;
  fragments.reserve(node.size());
  for (const auto& child : node) {
    auto& fragment = fragments.emplace_back("(?:");
    if (!Render(fragment, *child)) {
      return false;
    }
    absl::StrAppend(&fragment, ")");
  }
  const auto rendered = absl::StrJoin(fragments, "|");
  const auto pattern = ViewCast<byte_type>(std::string_view{rendered});
  AutomatonOptions options{pattern, PatternKind::RegexpPerl,
                           scored_terms_limit};
  if (!options.source->ok()) {
    return false;
  }
  auto fused = std::make_unique<AutomatonFilter>();
  *fused->mutable_field_id() = head->field;
  *fused->mutable_options() = std::move(options);
  fused->boost(ctx.scored ? node.Boost() * head->boost : node.Boost());
  slot = std::move(fused);
  return true;
}

// What makes an operand a good driver, which is not what makes it a cheap
// predicate: a driver is the one walk the conjunction runs, so an operand that
// can prune inside a dictionary walk beats one that can only bound it. `ByTerm`
// stays first because its bounds are a single key -- there is nothing left to
// prune.
size_t DriverRank(const Filter& filter) noexcept {
  const auto type = filter.type();
  if (type == Type<ByTerm>::id()) {
    return 0;
  }
  if (type == Type<AutomatonFilter>::id()) {
    return 1;
  }
  if (type == Type<ByTerms>::id()) {
    return 2;
  }
  if (type == Type<ByPrefix>::id() || type == Type<ByRange>::id()) {
    return 3;
  }
  return 4;
}

std::optional<AndAcceptorFusionRule::Operand> AndAcceptorFusionRule::OperandOf(
  const Filter& child) {
  const auto type = child.type();
  if (type == Type<ByTerm>::id()) {
    const auto& filter = sdb::basics::downCast<ByTerm>(child);
    const auto& term = filter.options().term;
    auto upper = term;
    upper += byte_type{0};
    return Operand{filter.field_id(), term, TermBounds{term, std::move(upper)},
                   nullptr};
  }
  if (type == Type<ByPrefix>::id()) {
    const auto& filter = sdb::basics::downCast<ByPrefix>(child);
    const auto& prefix = filter.options().term;
    auto pattern = prefix;
    pattern += static_cast<byte_type>('%');
    return Operand{filter.field_id(), std::move(pattern),
                   TermBounds{prefix, UpperBoundOf(prefix)}, nullptr};
  }
  if (type == Type<ByRange>::id()) {
    const auto& filter = sdb::basics::downCast<ByRange>(child);
    const auto& range = filter.options().range;
    bstring pattern;
    pattern += static_cast<byte_type>(
      range.min_type == BoundType::Exclusive ? '(' : '[');
    pattern += range.min;
    pattern += static_cast<byte_type>('.');
    pattern += static_cast<byte_type>('.');
    pattern += range.max;
    pattern += static_cast<byte_type>(
      range.max_type == BoundType::Exclusive ? ')' : ']');
    // Both bounds only narrow the walk -- the operand's own predicate decides
    // the open ends -- so an exclusive minimum needs no adjustment here and an
    // inclusive maximum needs the key just above it.
    TermBounds bounds;
    if (range.min_type != BoundType::Unbounded) {
      bounds.lower = range.min;
    }
    if (range.max_type != BoundType::Unbounded) {
      bounds.upper = range.max;
      if (range.max_type == BoundType::Inclusive) {
        bounds.upper += byte_type{0};
      }
    }
    return Operand{filter.field_id(), std::move(pattern), std::move(bounds),
                   nullptr};
  }
  if (type == Type<AutomatonFilter>::id()) {
    const auto& filter = sdb::basics::downCast<AutomatonFilter>(child);
    const auto& options = filter.options();
    if (!options.source) {
      return std::nullopt;
    }
    return Operand{filter.field_id(), options.pattern, TermBounds{},
                   options.source};
  }
  if (type == Type<LevenshteinAutomatonFilter>::id()) {
    const auto& filter =
      sdb::basics::downCast<LevenshteinAutomatonFilter>(child);
    const auto& options = filter.options();
    if (!options.parametric) {
      return std::nullopt;
    }
    auto pattern = options.target;
    pattern += static_cast<byte_type>('~');
    const auto prefix = options.parametric->LowerBound();
    return Operand{filter.field_id(), std::move(pattern),
                   TermBounds{bstring{prefix}, UpperBoundOf(prefix)}, nullptr};
  }
  return std::nullopt;
}

bool AndAcceptorFusionRule::Apply(Filter::ptr& slot,
                                  const OptimizeContext& ctx) {
  if (!ctx.fuse_acceptor_intersections || ctx.scored) {
    return false;
  }
  auto& node = sdb::basics::downCast<And>(*slot);
  if (node.size() < 2) {
    return false;
  }
  auto& children = node.mutable_filters();
  absl::InlinedVector<size_t, 8> order(children.size());
  absl::c_iota(order, size_t{0});
  absl::c_stable_sort(order, [&](size_t lhs, size_t rhs) {
    return DriverRank(*children[lhs]) < DriverRank(*children[rhs]);
  });
  if (children[order.front()]->type() ==
      Type<LevenshteinAutomatonFilter>::id()) {
    return false;
  }
  auto driver = OperandOf(*children[order.front()]);
  if (!driver) {
    return false;
  }
  auto pattern = std::move(driver->pattern);
  auto bounds = std::move(driver->bounds);
  // Every operand's predicate is applied to what the driver walk reports; the
  // driver's own is skipped only when its source walks its language exactly.
  auto residual = std::make_unique<And>();
  auto& operands = residual->mutable_filters();
  for (const auto index : std::span{order}.subspan(1)) {
    auto& child = children[index];
    auto operand = OperandOf(*child);
    if (!operand || operand->field != driver->field) {
      continue;
    }
    pattern += static_cast<byte_type>('&');
    pattern += operand->pattern;
    // A folded operand still bounds the walk even though its language is only
    // re-tested: the conjunction lies inside every operand's range, and the
    // tightest of them is free to apply.
    if (bounds.lower < operand->bounds.lower) {
      bounds.lower = std::move(operand->bounds.lower);
    }
    if (!operand->bounds.upper.empty() &&
        (bounds.upper.empty() || operand->bounds.upper < bounds.upper)) {
      bounds.upper = std::move(operand->bounds.upper);
    }
    operands.emplace_back(std::move(child));
  }
  if (operands.empty()) {
    return false;
  }
  auto& slot_child = children[order.front()];
  if (!driver->exact) {
    operands.emplace(operands.begin(), std::move(slot_child));
  }
  auto fused_filter = std::make_unique<AutomatonFilter>();
  *fused_filter->mutable_field_id() = driver->field;
  *fused_filter->mutable_options() = AutomatonOptions{
    pattern,
    MakeConjunctionSource(std::move(driver->exact), std::move(bounds),
                          std::move(residual)),
    0};
  slot_child = std::move(fused_filter);
  std::erase_if(children, [](const auto& child) { return !child; });
  if (children.size() == 1) {
    slot = std::move(children.front());
  }
  return true;
}

bool OrUnsatRule::Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
  const auto& node = sdb::basics::downCast<Or>(*slot);
  const auto min_match = node.min_match_count();
  if (min_match == 0 || min_match <= node.size()) {
    return false;
  }
  slot = std::make_unique<Empty>();
  return true;
}

bool OrAllRequiredRule::Apply(Filter::ptr& slot,
                              const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<Or>(*slot);
  if (node.size() < 2 || node.min_match_count() != node.size()) {
    return false;
  }
  auto& children = node.mutable_filters();
  auto replacement = MakeBoolean<And>(std::move(children), node.merge_type());
  replacement->boost(node.Boost());
  slot = std::move(replacement);
  return true;
}

bool MixedDegenerateRule::Apply(Filter::ptr& slot,
                                const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<MixedBooleanFilter>(*slot);
  const auto no_clauses = [](const Filter::ptr& side) {
    const auto tid = side->type();
    if (tid == Type<Empty>::id()) {
      return true;
    }
    if (tid == Type<And>::id() || tid == Type<Or>::id()) {
      return sdb::basics::downCast<BooleanFilter>(*side).empty();
    }
    return false;
  };
  if (no_clauses(node.RequiredSlot())) {
    auto side = std::move(node.OptionalSlot());
    slot = std::move(side);
    return true;
  }
  if (no_clauses(node.OptionalSlot())) {
    auto side = std::move(node.RequiredSlot());
    slot = std::move(side);
    return true;
  }
  return false;
}

void InitBooleanRules() {
  RegisterRule<FlattenAnd>();
  RegisterRule<FlattenOr>();
  RegisterRule<AndExclusionCoalesceRule>();
  RegisterRule<AndEmptyRule>();
  RegisterRule<OrEmptyRule>();
  RegisterRule<AndAllFoldRule>();
  RegisterRule<OrAllFoldRule>();
  RegisterRule<SingleChildRule>();
  RegisterRule<ByTermsRule>();
  RegisterRule<ExclusionNormalizeRule>();
  RegisterRule<OrAcceptorFusionRule>();
  RegisterRule<OrUnsatRule>();
  RegisterRule<OrAllRequiredRule>();
  RegisterRule<MixedDegenerateRule>();
}

namespace {

// The field a postings-requiring leaf constrains: every match holds at
// least one posting of that field. Shared by anchor collection (such a
// leaf proves the field non-NULL for every match) and marker pruning (on
// a one-term marker dictionary any such leaf is the marker itself).
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
  if (node.type() == Type<ByTerms>::id()) {
    const auto& terms = sdb::basics::downCast<ByTerms>(node);
    if (terms.options().min_match >= 1) {
      return terms.field_id();
    }
    return field_limits::invalid();
  }
  return AnyFieldOf<ByTerm, ByPrefix, ByRange, ByGranularRange, ByPhrase,
                    AutomatonFilter, LevenshteinAutomatonFilter>(node);
}

// Dedup identity for excludes: deep operator== plus a consistent hash
// over (leaf field, ByTerm term bytes) -- the shape negation claims
// mass-produce -- so duplicates resolve in one probe; unequal filters
// may hash-collide and the set's equality disambiguates.
struct ExcludeHasher {
  size_t operator()(const Filter* filter) const {
    bytes_view term;
    if (filter->type() == Type<ByTerm>::id()) {
      term = sdb::basics::downCast<ByTerm>(*filter).options().term;
    }
    return absl::HashOf(
      AnyFieldOf<ByTerm, ByTerms, ByPrefix, ByRange, ByGranularRange, ByPhrase,
                 AutomatonFilter, LevenshteinAutomatonFilter>(*filter),
      term);
  }
};

struct ExcludeEqual {
  bool operator()(const Filter* lhs, const Filter* rhs) const noexcept {
    return *lhs == *rhs;
  }
};

void CollectAnchorFields(Filter& node,
                         sdb::containers::FlatHashSet<field_id>& out) {
  if (node.type() == Type<And>::id()) {
    for (auto& child : node.GetChildren()) {
      if (child) {
        CollectAnchorFields(*child, out);
      }
    }
    return;
  }
  if (node.type() == Type<Exclusion>::id()) {
    auto& exclusion = sdb::basics::downCast<Exclusion>(node);
    if (exclusion.GetInclude()) {
      CollectAnchorFields(*exclusion.mutable_include(), out);
    }
    return;
  }
  if (node.type() == Type<Or>::id()) {
    auto& disjunction = sdb::basics::downCast<Or>(node);
    const auto children = disjunction.GetChildren();
    if (disjunction.min_match_count() < 1 || children.empty() ||
        !children.front()) {
      return;
    }
    sdb::containers::FlatHashSet<field_id> common;
    CollectAnchorFields(*children.front(), common);
    sdb::containers::FlatHashSet<field_id> branch;
    for (auto& child : children.subspan(1)) {
      if (!child || common.empty()) {
        return;
      }
      branch.clear();
      CollectAnchorFields(*child, branch);
      if (branch.size() < common.size()) {
        std::swap(common, branch);
      }
      absl::erase_if(
        common, [&](const field_id field) { return !branch.contains(field); });
    }
    out.insert(common.begin(), common.end());
    return;
  }
  if (const auto field = RequiringLeafFieldOf(node);
      field_limits::valid(field)) {
    out.insert(field);
  }
}

}  // namespace

bool ExclusionNormalizeRule::Apply(Filter::ptr& slot,
                                   const OptimizeContext& ctx) {
  auto& exclusion = sdb::basics::downCast<Exclusion>(*slot);
  bool changed = false;
  std::vector<Filter::ptr> split;
  for (auto& excluded : exclusion.mutable_excludes()) {
    if (!excluded || excluded->type() != Type<Or>::id()) {
      continue;
    }
    auto& disjunction = sdb::basics::downCast<Or>(*excluded);
    if (disjunction.min_match_count() != 1) {
      continue;
    }
    auto& branches = disjunction.mutable_filters();
    if (branches.empty()) {
      continue;
    }
    for (size_t i = 1; i < branches.size(); ++i) {
      split.push_back(std::move(branches[i]));
    }
    auto first = std::move(branches.front());
    excluded = std::move(first);
    changed = true;
  }
  for (auto& extra : split) {
    exclusion.exclude(std::move(extra));
  }

  const bool can_prune =
    ctx.null_markers && !ctx.null_markers->empty() && exclusion.GetInclude();
  // `null_markers` is keyed by the value field, and a column may have several
  // of them, so the prunable set is the anchors' image over the map. Walking
  // the include tree to build it is only worth doing for an exclude that names
  // a marker at all, which the map's own values answer in one lookup.
  sdb::containers::FlatHashSet<field_id> markers;
  if (can_prune) {
    markers.reserve(ctx.null_markers->size());
    for (const auto& [value_field, marker] : *ctx.null_markers) {
      markers.insert(marker);
    }
  }
  sdb::containers::FlatHashSet<field_id> anchored_markers;
  bool anchors_ready = false;
  sdb::containers::FlatHashSet<const Filter*, ExcludeHasher, ExcludeEqual> kept;
  kept.reserve(exclusion.GetExcludes().size());
  changed |= exclusion.EraseExcludesIf([&](const Filter::ptr& f) {
    if (!f) {
      return false;
    }
    if (can_prune) {
      if (const auto field = RequiringLeafFieldOf(*f);
          field_limits::valid(field) && markers.contains(field)) {
        if (!anchors_ready) {
          anchors_ready = true;
          sdb::containers::FlatHashSet<field_id> anchors;
          CollectAnchorFields(*exclusion.mutable_include(), anchors);
          for (const auto anchor : anchors) {
            if (const auto it = ctx.null_markers->find(anchor);
                it != ctx.null_markers->end()) {
              anchored_markers.insert(it->second);
            }
          }
        }
        if (anchored_markers.contains(field)) {
          return true;
        }
      }
    }
    return !kept.insert(f.get()).second;
  });
  return changed;
}

size_t AcceptorRank(const Filter& filter) noexcept {
  const auto type = filter.type();
  if (type == Type<ByTerm>::id()) {
    return 0;
  }
  if (type == Type<ByTerms>::id()) {
    return 1;
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

void FuseIntersections(Filter::ptr& root, const OptimizeContext& ctx) {
  if (!root || !ctx.fuse_acceptor_intersections) {
    return;
  }
  TraverseFilter(root, [&](Filter::ptr& slot) {
    if (slot->type() == Type<And>::id()) {
      AndAcceptorFusionRule::Apply(slot, ctx);
    }
  });
}

}  // namespace irs::optimizer
