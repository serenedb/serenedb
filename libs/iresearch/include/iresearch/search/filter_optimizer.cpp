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

#include <absl/container/inlined_vector.h>

#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/optimizer/boolean_rules.hpp"
#include "iresearch/search/optimizer/lowering_rules.hpp"
#include "iresearch/search/optimizer/negation_rules.hpp"
#include "iresearch/search/optimizer/range_rules.hpp"
#include "iresearch/search/optimizer/terms_rules.hpp"

namespace irs {
namespace {

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

auto& RuleRegistry() {
  static sdb::containers::FlatHashMap<TypeInfo::type_id, std::vector<RuleDesc>>
    gRules;
  return gRules;
}

void RunRules(Filter::ptr& slot, const OptimizeContext& ctx) {
  bool changed = true;
  const auto& rules = RuleRegistry();
  while (changed) {
    changed = false;
    const auto it = rules.find(slot->type());
    if (it == rules.end()) {
      break;
    }
    for (const auto& rule : it->second) {
      if (rule.apply(slot, ctx)) {
        SDB_ASSERT(slot != nullptr);
        changed = true;
        break;
      }
    }
  }
}

void RunPass(Filter::ptr& root, const OptimizeContext& ctx) {
  struct Frame {
    Filter::ptr* slot;
    bool children_visited;
  };

  absl::InlinedVector<Frame, 16> stack;
  stack.push_back({&root, false});
  while (!stack.empty()) {
    auto& frame = stack.back();
    if (frame.children_visited) {
      RunRules(*frame.slot, ctx);
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

void RegisterRule(RuleDesc rule) {
  for (const auto tid : rule.targets) {
    RuleRegistry()[tid].push_back(rule);
  }
}

void InitOptimizeRules() {
  SDB_ASSERT(RuleRegistry().empty());
  optimizer::InitBooleanRules();
  optimizer::InitNegationRules();
  optimizer::InitTermsRules();
  optimizer::InitRangeRules();
  optimizer::InitLoweringRules();
}

void Optimize(Filter::ptr& root, const OptimizeContext& ctx) {
  if (!root) {
    return;
  }

  RunPass(root, ctx);
}

}  // namespace irs
