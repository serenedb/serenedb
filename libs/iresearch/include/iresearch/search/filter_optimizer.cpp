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

constexpr std::array<RuleDesc, 3> kDefaultRulesStorage{{
  {"not_to_exclude", NotRule::kTargets, &NotRule::Apply},
  {"flatten_and", FlattenAnd::kTargets, &FlattenAnd::Apply},
  {"flatten_or", FlattenOr::kTargets, &FlattenOr::Apply},
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
