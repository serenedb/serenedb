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

#include "filter_optimizer.hpp"

#include <absl/algorithm/container.h>
#include <absl/container/inlined_vector.h>

#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"

namespace irs {
namespace {

template<typename T>
std::unique_ptr<T> MakeBoolean(std::vector<Filter::ptr> children,
                               ScoreMergeType merge_type) {
  auto node = std::make_unique<T>();
  node->merge_type(merge_type);
  for (auto& child : children) {
    node->add(std::move(child));
  }
  return node;
}

template<typename Visitor>
void EnumerateChildSlots(Filter& node, Visitor&& visit) {
  const auto tid = node.type();

  if (tid == Type<And>::id() || tid == Type<Or>::id()) {
    for (auto& slot :
         sdb::basics::downCast<BooleanFilter>(node).mutable_filters()) {
      visit(slot);
    }
  } else if (tid == Type<Exclusion>::id()) {
    auto& node_ex = sdb::basics::downCast<Exclusion>(node);
    if (auto& inc = node_ex.mutable_include(); inc) {
      visit(inc);
    }
    if (auto& exc = node_ex.mutable_exclude(); exc) {
      visit(exc);
    }
  } else if (tid == Type<Not>::id()) {
    if (auto& inner = sdb::basics::downCast<Not>(node).mutable_filter();
        inner) {
      visit(inner);
    }
  } else if (tid == Type<MixedBooleanFilter>::id()) {
    auto& mixed = sdb::basics::downCast<MixedBooleanFilter>(node);
    visit(mixed.RequiredSlot());
    visit(mixed.OptionalSlot());
  }
}

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

  size_t spliced = 0;
  for (const auto& child : children) {
    if (CanSplice(node, *child)) {
      spliced += sdb::basics::downCast<T>(*child).size();
    }
  }
  if (spliced == 0) {
    return false;
  }

  std::vector<Filter::ptr> flat;
  flat.reserve(children.size() + spliced);
  for (auto& child : children) {
    if (CanSplice(node, *child)) {
      for (auto& grandchild :
           sdb::basics::downCast<T>(*child).mutable_filters()) {
        flat.push_back(std::move(grandchild));
      }
    } else {
      flat.push_back(std::move(child));
    }
  }
  children = std::move(flat);
  return true;
}

struct NotSimplifyRule {
  static constexpr std::string_view kName = "not_simplify";
  static constexpr std::array kTargets{Type<Not>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    size_t negations = 0;
    Filter::ptr* inner = &slot;
    while ((*inner)->type() == Type<Not>::id()) {
      auto& node = sdb::basics::downCast<Not>(**inner);
      if (!node.mutable_filter()) {
        return false;
      }
      ++negations;
      inner = &node.mutable_filter();
    }

    auto& base = *inner;
    const bool odd = (negations % 2) != 0;

    if (base->type() == Type<Empty>::id()) {
      if (odd) {
        slot = AllDocsProvider::Default(kNoBoost);
      } else {
        slot = std::move(base);
      }
      return true;
    }
    if (*AllDocsProvider::Default(kNoBoost) == *base) {
      if (odd) {
        slot = std::make_unique<Empty>();
      } else {
        slot = std::move(base);
      }
      return true;
    }
    if (negations < 2) {
      return false;
    }
    if (odd) {
      slot = std::make_unique<Not>(std::move(base));
    } else {
      slot = std::move(base);
    }
    return true;
  }
};

struct NotLowerRule {
  static constexpr std::string_view kName = "not_lower";
  static constexpr std::array kTargets{Type<Not>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    auto& node = sdb::basics::downCast<Not>(*slot);
    auto exclusion = std::make_unique<Exclusion>();
    if (auto& inner = node.mutable_filter(); inner) {
      exclusion->exclude(std::move(inner));
    }
    exclusion->boost(node.Boost());
    slot = std::move(exclusion);
    return true;
  }
};

bool TryFoldBoost(Filter& survivor, score_t boost, const Scorer* scorer) {
  if (boost == kNoBoost || scorer == nullptr) {
    return true;
  }
  auto* boostable = dynamic_cast<FilterWithBoost*>(&survivor);
  if (boostable == nullptr) {
    return false;
  }
  boostable->boost(boostable->Boost() * boost);
  return true;
}

struct ExclusionRule {
  static constexpr std::string_view kName = "exclusion_simplify";
  static constexpr std::array kTargets{Type<Exclusion>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
    auto& node = sdb::basics::downCast<Exclusion>(*slot);
    auto& incl = node.mutable_include();
    auto& excl = node.mutable_exclude();
    if (excl) {
      return false;
    }
    if (!incl) {
      slot = AllDocsProvider::Default(node.Boost());
      return true;
    }
    if (!TryFoldBoost(*incl, node.Boost(), ctx.scorer)) {
      return false;
    }
    slot = std::move(incl);
    return true;
  }
};

struct FlattenAnd {
  static constexpr std::string_view kName = "flatten_and";
  static constexpr std::array kTargets{Type<And>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    return Flatten(sdb::basics::downCast<And>(*slot));
  }
};

struct FlattenOr {
  static constexpr std::string_view kName = "flatten_or";
  static constexpr std::array kTargets{Type<Or>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    return Flatten(sdb::basics::downCast<Or>(*slot));
  }
};

struct AndExclusionCoalesceRule {
  static constexpr std::string_view kName = "and_exclusion_coalesce";
  static constexpr std::array kTargets{Type<And>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
    auto& node = sdb::basics::downCast<And>(*slot);
    auto& children = node.mutable_filters();

    const auto is_not = [](const Filter::ptr& child) {
      return child->type() == Type<Not>::id() &&
             sdb::basics::downCast<Not>(*child).filter() != nullptr;
    };
    const auto is_coalescable_exclusion = [&](const Filter::ptr& child) {
      return child->type() == Type<Exclusion>::id() &&
             !(child->BoostImpl() != kNoBoost && ctx.scorer != nullptr);
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
               sdb::basics::downCast<Exclusion>(*child).exclude() != nullptr;
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
        if (auto& excl = ex.mutable_exclude(); excl) {
          excludes.emplace_back(std::move(excl));
        }
      } else {
        includes.emplace_back(std::move(child));
      }
    }

    auto exclude = excludes.size() == 1 ? std::move(excludes.front())
                                        : MakeBoolean<Or>(std::move(excludes),
                                                          ScoreMergeType::Sum);

    if (includes.empty()) {
      auto not_node = std::make_unique<Not>(std::move(exclude));
      not_node->boost(node.Boost());
      slot = std::move(not_node);
      return true;
    }

    auto include = includes.size() == 1
                     ? std::move(includes.front())
                     : MakeBoolean<And>(std::move(includes), node.merge_type());

    auto exclusion = std::make_unique<Exclusion>();
    exclusion->include(std::move(include));
    exclusion->exclude(std::move(exclude));
    exclusion->boost(node.Boost());
    slot = std::move(exclusion);
    return true;
  }
};

struct AndEmptyRule {
  static constexpr std::string_view kName = "and_empty";
  static constexpr std::array kTargets{Type<And>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    const auto& node = sdb::basics::downCast<And>(*slot);
    const bool has_empty = absl::c_any_of(node, [](const auto& child) {
      return child->type() == Type<Empty>::id();
    });
    if (!has_empty) {
      return false;
    }
    slot = std::make_unique<Empty>();
    return true;
  }
};

struct OrEmptyRule {
  static constexpr std::string_view kName = "or_empty";
  static constexpr std::array kTargets{Type<Or>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    auto& node = sdb::basics::downCast<Or>(*slot);
    if (node.min_match_count() == 0) {
      return false;
    }
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
};

template<typename T>
std::pair<size_t, score_t> CountAllDocs(const T& node, const Filter& all) {
  size_t count = 0;
  score_t boost = 0.F;
  for (const auto& child : node) {
    if (all == *child) {
      ++count;
      boost += child->BoostImpl();
    }
  }
  return {count, boost};
}

template<typename T>
void EraseAllDocs(T& node, const Filter& all) {
  auto& children = node.mutable_filters();
  const auto it =
    std::remove_if(children.begin(), children.end(),
                   [&](const auto& child) { return all == *child; });
  children.erase(it, children.end());
}

struct AndAllFoldRule {
  static constexpr std::string_view kName = "and_all_fold";
  static constexpr std::array kTargets{Type<And>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
    auto& node = sdb::basics::downCast<And>(*slot);
    const auto all = node.MakeAllDocsFilter(kNoBoost);
    const auto [all_count, all_boost] = CountAllDocs(node, *all);
    if (all_count == 0) {
      return false;
    }
    if (all_count == node.size()) {
      slot = node.MakeAllDocsFilter(node.Boost() * all_boost);
      return true;
    }
    if (ctx.scorer != nullptr && all_count < 2) {
      return false;
    }
    EraseAllDocs(node, *all);
    if (ctx.scorer != nullptr) {
      node.mutable_filters().emplace_back(node.MakeAllDocsFilter(all_boost));
    }
    return true;
  }
};

struct OrAllFoldRule {
  static constexpr std::string_view kName = "or_all_fold";
  static constexpr std::array kTargets{Type<Or>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
    auto& node = sdb::basics::downCast<Or>(*slot);
    const auto min_match = node.min_match_count();
    if (min_match == 0) {
      return false;
    }
    const auto all = node.MakeAllDocsFilter(kNoBoost);
    const auto [all_count, all_boost] = CountAllDocs(node, *all);
    if (all_count == 0) {
      return false;
    }
    if (ctx.scorer == nullptr && min_match <= all_count) {
      slot = node.MakeAllDocsFilter(kNoBoost);
      return true;
    }
    if (all_count < 2) {
      return false;
    }
    EraseAllDocs(node, *all);
    auto& children = node.mutable_filters();
    children.emplace_back(node.MakeAllDocsFilter(all_boost));
    const size_t new_min_match =
      min_match > all_count - 1 ? min_match - (all_count - 1) : 1;
    auto replacement = MakeBoolean<Or>(std::move(children), node.merge_type());
    replacement->min_match_count(new_min_match);
    replacement->boost(node.Boost());
    slot = std::move(replacement);
    return true;
  }
};

struct SingleChildRule {
  static constexpr std::string_view kName = "single_child";
  static constexpr std::array kTargets{Type<And>::id(), Type<Or>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
    auto& node = sdb::basics::downCast<BooleanFilter>(*slot);
    if (node.size() != 1) {
      return false;
    }
    if (slot->type() == Type<Or>::id() &&
        sdb::basics::downCast<Or>(node).min_match_count() != 1) {
      return false;
    }
    auto& front = node.mutable_filters().front();
    if (!TryFoldBoost(*front, node.Boost(), ctx.scorer)) {
      return false;
    }
    auto child = std::move(front);
    slot = std::move(child);
    return true;
  }
};

struct ByTermsRule {
  static constexpr std::string_view kName = "by_terms";
  static constexpr std::array kTargets{Type<And>::id(), Type<Or>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
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
        const_cast<score_t&>(it.first->boost) += term_filter.Boost();
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
};

struct OrMinMatchZeroRule {
  static constexpr std::string_view kName = "or_min_match_zero";
  static constexpr std::array kTargets{Type<Or>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    auto& node = sdb::basics::downCast<Or>(*slot);
    if (node.min_match_count() != 0) {
      return false;
    }
    slot = node.MakeAllDocsFilter(node.Boost());
    return true;
  }
};

struct OrUnsatRule {
  static constexpr std::string_view kName = "or_unsat";
  static constexpr std::array kTargets{Type<Or>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    const auto& node = sdb::basics::downCast<Or>(*slot);
    const auto min_match = node.min_match_count();
    if (min_match == 0 || min_match <= node.size()) {
      return false;
    }
    slot = std::make_unique<Empty>();
    return true;
  }
};

struct OrAllRequiredRule {
  static constexpr std::string_view kName = "or_all_required";
  static constexpr std::array kTargets{Type<Or>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
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
};

struct ByTermsMinMatchZeroRule {
  static constexpr std::string_view kName = "by_terms_min_match_zero";
  static constexpr std::array kTargets{Type<ByTerms>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
    auto& node = sdb::basics::downCast<ByTerms>(*slot);
    if (node.options().terms.empty() || node.options().min_match != 0) {
      return false;
    }
    if (ctx.scorer == nullptr) {
      slot = node.MakeAllDocsFilter(kNoBoost);
      return true;
    }
    auto terms = std::make_unique<ByTerms>();
    terms->boost(node.Boost());
    *terms->mutable_field_id() = node.field_id();
    *terms->mutable_options() = node.options();
    terms->mutable_options()->min_match = 1;

    std::vector<Filter::ptr> children;
    children.emplace_back(node.MakeAllDocsFilter(0.F));
    children.emplace_back(std::move(terms));
    slot = MakeBoolean<Or>(std::move(children), ScoreMergeType::Sum);
    return true;
  }
};

struct MixedDegenerateRule {
  static constexpr std::string_view kName = "mixed_degenerate";
  static constexpr std::array kTargets{Type<MixedBooleanFilter>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
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
};

struct WildcardLowerRule {
  static constexpr std::string_view kName = "wildcard_lower";
  static constexpr std::array kTargets{Type<ByWildcard>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    auto& node = sdb::basics::downCast<ByWildcard>(*slot);
    slot = LowerWildcard(node.field_id(), node.options().term,
                         node.options().scored_terms_limit, node.Boost());
    return true;
  }
};

struct RegexpLowerRule {
  static constexpr std::string_view kName = "regexp_lower";
  static constexpr std::array kTargets{Type<ByRegexp>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    auto& node = sdb::basics::downCast<ByRegexp>(*slot);
    slot = LowerRegexp(node.field_id(), node.options().pattern,
                       node.options().syntax, node.options().scored_terms_limit,
                       node.Boost());
    return true;
  }
};

struct PhraseLowerRule {
  static constexpr std::string_view kName = "phrase_lower";
  static constexpr std::array kTargets{Type<ByPhrase>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    return sdb::basics::downCast<ByPhrase>(*slot)
      .mutable_options()
      ->LowerWildcardParts();
  }
};

constexpr auto kDefaultRulesStorage = std::to_array({
  MakeRule<ExclusionRule>(),
  MakeRule<NotSimplifyRule>(),
  MakeRule<AndEmptyRule>(),
  MakeRule<OrEmptyRule>(),
  MakeRule<OrMinMatchZeroRule>(),
  MakeRule<OrUnsatRule>(),
  MakeRule<AndAllFoldRule>(),
  MakeRule<OrAllFoldRule>(),
  MakeRule<FlattenAnd>(),
  MakeRule<FlattenOr>(),
  MakeRule<AndExclusionCoalesceRule>(),
  MakeRule<OrAllRequiredRule>(),
  MakeRule<ByTermsRule>(),
  MakeRule<ByTermsMinMatchZeroRule>(),
  MakeRule<SingleChildRule>(),
  MakeRule<MixedDegenerateRule>(),
  MakeRule<WildcardLowerRule>(),
  MakeRule<RegexpLowerRule>(),
  MakeRule<PhraseLowerRule>(),
});

constexpr auto kLoweringRulesStorage = std::to_array({
  MakeRule<NotLowerRule>(),
});

void RunRules(Filter::ptr& slot, const OptimizeContext& ctx,
              std::span<const RuleDesc> rules) {
  bool changed = true;
  while (changed) {
    changed = false;
    const auto tid = slot->type();
    for (const auto& rule : rules) {
      if (absl::c_find(rule.targets, tid) == rule.targets.end()) {
        continue;
      }
      if (rule.apply(slot, ctx)) {
        SDB_ASSERT(slot != nullptr);
        changed = true;
        break;
      }
    }
  }
}

}  // namespace

constinit const std::span<const RuleDesc> kDefaultRules{kDefaultRulesStorage};

namespace {

std::vector<RuleDesc>& RuleRegistry() {
  static std::vector<RuleDesc> gRules(kDefaultRulesStorage.begin(),
                                      kDefaultRulesStorage.end());
  return gRules;
}

}  // namespace

void RegisterRule(const RuleDesc& rule) { RuleRegistry().push_back(rule); }

std::span<const RuleDesc> ActiveRules() { return RuleRegistry(); }

namespace {

void RunPass(Filter::ptr& root, const OptimizeContext& ctx,
             std::span<const RuleDesc> rules) {
  struct Frame {
    Filter::ptr* slot;
    bool children_visited;
  };

  absl::InlinedVector<Frame, 16> stack;
  stack.push_back({&root, false});
  while (!stack.empty()) {
    auto& frame = stack.back();
    if (frame.children_visited) {
      RunRules(*frame.slot, ctx, rules);
      stack.pop_back();
      continue;
    }
    frame.children_visited = true;
    EnumerateChildSlots(**frame.slot, [&](Filter::ptr& child) {
      stack.push_back({&child, false});
    });
  }
}

}  // namespace

void Optimize(Filter::ptr& root, const OptimizeContext& ctx,
              std::span<const RuleDesc> rules) {
  if (!root) {
    return;
  }

  RunPass(root, ctx, rules);
  RunPass(root, ctx, kLoweringRulesStorage);
}

}  // namespace irs
