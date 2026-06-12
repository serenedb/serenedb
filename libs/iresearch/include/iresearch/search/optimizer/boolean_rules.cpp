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

#include <algorithm>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/optimizer/common.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"

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

struct ByTermsRule {
  static constexpr std::string_view kName = "by_terms";
  static constexpr std::array kTargets{Type<And>::id(), Type<Or>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct OrMinMatchZeroRule {
  static constexpr std::string_view kName = "or_min_match_zero";
  static constexpr std::array kTargets{Type<Or>::id()};
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

struct EmptyAndRule {
  static constexpr std::string_view kName = "empty_and";
  static constexpr std::array kTargets{Type<And>::id()};
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

template<typename T>
std::pair<size_t, score_t> CountAllDocs(const T& node) {
  size_t count = 0;
  score_t boost = 0.F;
  for (const auto& child : node) {
    if (IsAllDocs(*child)) {
      ++count;
      boost += child->BoostImpl();
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

  auto exclude = excludes.size() == 1
                   ? std::move(excludes.front())
                   : MakeBoolean<Or>(std::move(excludes), ScoreMergeType::Sum);

  if (includes.empty()) {
    auto exclusion = std::make_unique<Exclusion>();
    exclusion->exclude(std::move(exclude));
    exclusion->boost(node.Boost());
    slot = std::move(exclusion);
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

bool AndEmptyRule::Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
  const auto& node = sdb::basics::downCast<And>(*slot);
  const bool has_empty = absl::c_any_of(
    node, [](const auto& child) { return child->type() == Type<Empty>::id(); });
  if (!has_empty) {
    return false;
  }
  slot = std::make_unique<Empty>();
  return true;
}

bool OrEmptyRule::Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
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
  if (ctx.scorer != nullptr && node.size() - all_count == 1) {
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
  if (ctx.scorer != nullptr && all_count < 2) {
    return false;
  }
  EraseAllDocs(node);
  if (ctx.scorer != nullptr) {
    node.mutable_filters().emplace_back(node.MakeAllDocsFilter(all_boost));
  }
  return true;
}

bool OrAllFoldRule::Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<Or>(*slot);
  const auto min_match = node.min_match_count();
  if (min_match == 0) {
    return false;
  }
  const auto [all_count, all_boost] = CountAllDocs(node);
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
  if (!TryFoldBoost(*front, node.Boost(), ctx.scorer)) {
    return false;
  }
  auto child = std::move(front);
  slot = std::move(child);
  return true;
}

bool ByTermsRule::Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
  if (ctx.scorer != nullptr) {
    return false;
  }
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

bool OrMinMatchZeroRule::Apply(Filter::ptr& slot,
                               const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<Or>(*slot);
  if (node.min_match_count() != 0) {
    return false;
  }
  slot = node.MakeAllDocsFilter(node.Boost());
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

bool EmptyAndRule::Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
  if (!sdb::basics::downCast<And>(*slot).empty()) {
    return false;
  }
  slot = std::make_unique<Empty>();
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
  RegisterRule<OrMinMatchZeroRule>();
  RegisterRule<OrUnsatRule>();
  RegisterRule<OrAllRequiredRule>();
  RegisterRule<EmptyAndRule>();
  RegisterRule<MixedDegenerateRule>();
}

}  // namespace irs::optimizer
