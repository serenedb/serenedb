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
#include "iresearch/search/exclude_filter.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/terms_filter.hpp"

namespace irs {
namespace {

template<typename Visitor>
void EnumerateChildSlots(Filter& node, Visitor&& visit) {
  const auto tid = node.type();

  if (tid == Type<And>::id() || tid == Type<Or>::id()) {
    for (auto& slot : sdb::basics::downCast<BooleanFilter>(node).ChildSlots()) {
      visit(slot);
    }
  } else if (tid == Type<Not>::id()) {
    if (auto& slot = sdb::basics::downCast<Not>(node).ChildSlot(); slot) {
      visit(slot);
    }
  } else if (tid == Type<Exclude>::id()) {
    if (auto& slot = sdb::basics::downCast<Exclude>(node).ChildSlot(); slot) {
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
  auto& children = node.MutableChildren();

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
           sdb::basics::downCast<T>(*child).MutableChildren()) {
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
  static constexpr std::array kTargets{Type<Not>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    auto& node = sdb::basics::downCast<Not>(*slot);
    auto& child = node.ChildSlot();
    if (!child) {
      slot = std::make_unique<Empty>();
      return true;
    }
    if (child->type() == Type<Exclude>::id()) {
      auto inner =
        std::move(sdb::basics::downCast<Exclude>(*child).ChildSlot());
      slot = std::move(inner);
      return true;
    }
    if (const auto all = node.MakeAllDocsFilter(kNoBoost); *all == *child) {
      slot = std::make_unique<Empty>();
      return true;
    }
    auto exclude = std::make_unique<Exclude>();
    exclude->boost(node.Boost());
    exclude->ChildSlot() = std::move(child);
    slot = std::move(exclude);
    return true;
  }
};

struct FlattenAnd {
  static constexpr std::array kTargets{Type<And>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    return Flatten(sdb::basics::downCast<And>(*slot));
  }
};

struct FlattenOr {
  static constexpr std::array kTargets{Type<Or>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    return Flatten(sdb::basics::downCast<Or>(*slot));
  }
};

struct AndEmptyRule {
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
  static constexpr std::array kTargets{Type<Or>::id()};

  static bool Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
    auto& node = sdb::basics::downCast<Or>(*slot);
    auto& children = node.MutableChildren();
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
  auto& children = node.MutableChildren();
  const auto it =
    std::remove_if(children.begin(), children.end(),
                   [&](const auto& child) { return all == *child; });
  children.erase(it, children.end());
}

struct AndAllFoldRule {
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
      node.add(node.MakeAllDocsFilter(all_boost));
    }
    return true;
  }
};

struct OrAllFoldRule {
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
    node.add(node.MakeAllDocsFilter(all_boost));
    node.min_match_count(min_match > all_count - 1 ? min_match - (all_count - 1)
                                                   : 1);
    return true;
  }
};

struct SingleChildRule {
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
    if (node.Boost() != kNoBoost && ctx.scorer != nullptr) {
      return false;
    }
    auto child = std::move(node.MutableChildren().front());
    slot = std::move(child);
    return true;
  }
};

struct ByTermsRule {
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
    const auto field = sdb::basics::downCast<ByTerm>(node[0]).field();
    const bool same_field = absl::c_all_of(node, [&](const auto& child) {
      return child->type() == Type<ByTerm>::id() &&
             sdb::basics::downCast<ByTerm>(*child).field() == field;
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

constexpr std::array<RuleDesc, 9> kDefaultRulesStorage{{
  {"not_to_exclude", NotRule::kTargets, &NotRule::Apply},
  {"and_empty", AndEmptyRule::kTargets, &AndEmptyRule::Apply},
  {"or_empty", OrEmptyRule::kTargets, &OrEmptyRule::Apply},
  {"and_all_fold", AndAllFoldRule::kTargets, &AndAllFoldRule::Apply},
  {"or_all_fold", OrAllFoldRule::kTargets, &OrAllFoldRule::Apply},
  {"flatten_and", FlattenAnd::kTargets, &FlattenAnd::Apply},
  {"flatten_or", FlattenOr::kTargets, &FlattenOr::Apply},
  {"by_terms", ByTermsRule::kTargets, &ByTermsRule::Apply},
  {"single_child", SingleChildRule::kTargets, &SingleChildRule::Apply},
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

[[maybe_unused]] bool HasNotTarget(std::span<const RuleDesc> rules) noexcept {
  return absl::c_any_of(rules, [](const auto& rule) {
    return absl::c_find(rule.targets, Type<Not>::id()) != rule.targets.end();
  });
}

[[maybe_unused]] bool NoNotRemains(Filter& root) noexcept {
  absl::InlinedVector<Filter*, 16> stack{&root};
  while (!stack.empty()) {
    auto* node = stack.back();
    stack.pop_back();
    if (node->type() == Type<Not>::id()) {
      return false;
    }
    EnumerateChildSlots(
      *node, [&](Filter::ptr& child) { stack.push_back(child.get()); });
  }
  return true;
}

}  // namespace

constinit const std::span<const RuleDesc> kDefaultRules{kDefaultRulesStorage};

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

  SDB_ASSERT(!HasNotTarget(rules) || NoNotRemains(*root));
}

}  // namespace irs
