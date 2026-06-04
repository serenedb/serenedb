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
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"

namespace irs {

class FilterMutator {
 public:
  static MutableFilter Of(BooleanFilter& node) noexcept {
    return node.GetMutable();
  }
  static Filter::ptr& Child(Not& node) noexcept { return node.ChildSlot(); }
};

namespace {

template<typename Visitor>
void EnumerateChildSlots(Filter& node, Visitor&& visit) {
  const auto tid = node.type();

  if (tid == Type<And>::id() || tid == Type<Or>::id()) {
    for (auto& slot :
         FilterMutator::Of(sdb::basics::downCast<BooleanFilter>(node))
           .ChildSlots()) {
      visit(slot);
    }
  } else if (tid == Type<Not>::id()) {
    if (auto& slot = FilterMutator::Child(sdb::basics::downCast<Not>(node));
        slot) {
      visit(slot);
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
      inner.MergeType() != parent.MergeType()) {
    return false;
  }
  if constexpr (std::is_same_v<T, Or>) {
    return inner.MinMatchCount() == 1 && parent.MinMatchCount() == 1;
  } else {
    return true;
  }
}

template<typename T>
bool Flatten(T& node) {
  auto& children = FilterMutator::Of(node).Children();

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
           FilterMutator::Of(sdb::basics::downCast<T>(*child)).Children()) {
        flat.push_back(std::move(grandchild));
      }
    } else {
      flat.push_back(std::move(child));
    }
  }
  children = std::move(flat);
  return true;
}

struct NotRule {
  static constexpr std::string_view kName = "not_to_exclude";
  static constexpr std::array kTargets{Type<Not>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    auto& node = sdb::basics::downCast<Not>(*slot);
    auto& child = FilterMutator::Child(node);
    if (!child) {
      slot = std::make_unique<Empty>();
      return true;
    }
    if (child->type() == Type<Not>::id()) {
      auto inner =
        std::move(FilterMutator::Child(sdb::basics::downCast<Not>(*child)));
      slot = inner ? std::move(inner) : Filter::ptr{std::make_unique<Empty>()};
      return true;
    }
    if (const auto all = node.MakeAllDocsFilter(kNoBoost); *all == *child) {
      slot = std::make_unique<Empty>();
      return true;
    }
    return false;
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

struct AndExcludeOnlyRule {
  static constexpr std::string_view kName = "and_exclude_only";
  static constexpr std::array kTargets{Type<And>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    auto& node = sdb::basics::downCast<And>(*slot);
    if (node.size() != 0 || node.ExcludesEmpty()) {
      return false;
    }
    FilterMutator::Of(node).Children().emplace_back(
      node.MakeAllDocsFilter(kNoBoost));
    return true;
  }
};

struct OrEmptyRule {
  static constexpr std::string_view kName = "or_empty";
  static constexpr std::array kTargets{Type<Or>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    auto& node = sdb::basics::downCast<Or>(*slot);
    auto& children = FilterMutator::Of(node).Children();
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
  auto& children = FilterMutator::Of(node).Children();
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
      if (!node.ExcludesEmpty()) {
        return false;
      }
      slot = node.MakeAllDocsFilter(node.Boost() * all_boost);
      return true;
    }
    if (ctx.scorer != nullptr && all_count < 2) {
      return false;
    }
    EraseAllDocs(node, *all);
    if (ctx.scorer != nullptr) {
      FilterMutator::Of(node).Children().emplace_back(
        node.MakeAllDocsFilter(all_boost));
    }
    return true;
  }
};

struct OrAllFoldRule {
  static constexpr std::string_view kName = "or_all_fold";
  static constexpr std::array kTargets{Type<Or>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
    auto& node = sdb::basics::downCast<Or>(*slot);
    const auto min_match = node.MinMatchCount();
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
    auto& children = FilterMutator::Of(node).Children();
    children.emplace_back(node.MakeAllDocsFilter(all_boost));
    const size_t new_min_match =
      min_match > all_count - 1 ? min_match - (all_count - 1) : 1;
    auto replacement = std::make_unique<Or>(std::move(children),
                                            node.MergeType(), new_min_match);
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
    if (node.size() != 1 || !node.ExcludesEmpty()) {
      return false;
    }
    if (slot->type() == Type<Or>::id() &&
        sdb::basics::downCast<Or>(node).MinMatchCount() != 1) {
      return false;
    }
    if (node.Boost() != kNoBoost && ctx.scorer != nullptr) {
      return false;
    }
    auto child = std::move(FilterMutator::Of(node).Children().front());
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
      is_and ? 0 : sdb::basics::downCast<Or>(node).MinMatchCount();
    if (!is_and && min_match == 0) {
      return false;
    }
    if (node[0].type() != Type<ByTerm>::id()) {
      return false;
    }
    const auto field = sdb::basics::downCast<ByTerm>(node[0]).field();
    const bool same_field = absl::c_all_of(node, [&](const auto& child) {
      return child->type() == Type<ByTerm>::id() &&
             sdb::basics::downCast<ByTerm>(*child).field() == field;
    });
    if (!same_field) {
      return false;
    }
    ByTermsOptions options;
    options.merge_type = node.MergeType();
    bool has_duplicates = false;
    for (const auto& child : node) {
      auto& term_filter = sdb::basics::downCast<ByTerm>(*child);
      auto it =
        options.terms.emplace(term_filter.options().term, term_filter.Boost());
      if (!it.second) {
        const_cast<score_t&>(it.first->boost) *= term_filter.Boost();
        has_duplicates = true;
      }
    }
    if (has_duplicates && !is_and && min_match != 1) {
      return false;
    }
    options.min_match = is_and ? options.terms.size() : min_match;
    auto by_terms = std::make_unique<ByTerms>();
    *by_terms->mutable_field() = std::string{field};
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
    if (node.MinMatchCount() != 0) {
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
    const auto min_match = node.MinMatchCount();
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
    if (node.size() < 2 || node.MinMatchCount() != node.size()) {
      return false;
    }
    auto& children = FilterMutator::Of(node).Children();
    auto replacement =
      std::make_unique<And>(std::move(children), node.MergeType());
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
    *terms->mutable_field() = std::string{node.field()};
    *terms->mutable_options() = node.options();
    terms->mutable_options()->min_match = 1;

    std::vector<Filter::ptr> children;
    children.emplace_back(node.MakeAllDocsFilter(0.F));
    children.emplace_back(std::move(terms));
    slot = std::make_unique<Or>(std::move(children));
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

constexpr std::array<RuleDesc, 15> kDefaultRulesStorage{{
  MakeRule<NotRule>(),
  MakeRule<AndEmptyRule>(),
  MakeRule<AndExcludeOnlyRule>(),
  MakeRule<OrEmptyRule>(),
  MakeRule<OrMinMatchZeroRule>(),
  MakeRule<OrUnsatRule>(),
  MakeRule<AndAllFoldRule>(),
  MakeRule<OrAllFoldRule>(),
  MakeRule<FlattenAnd>(),
  MakeRule<FlattenOr>(),
  MakeRule<OrAllRequiredRule>(),
  MakeRule<ByTermsRule>(),
  MakeRule<ByTermsMinMatchZeroRule>(),
  MakeRule<SingleChildRule>(),
  MakeRule<MixedDegenerateRule>(),
}};

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

void Optimize(Filter::ptr& root, const OptimizeContext& ctx,
              std::span<const RuleDesc> rules) {
  if (!root) {
    return;
  }

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

}  // namespace irs
