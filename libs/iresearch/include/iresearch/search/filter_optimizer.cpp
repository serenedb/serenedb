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
  struct Frame {
    Filter::ptr* slot;
    bool children_visited;
  };

  absl::InlinedVector<Frame, 16> stack;
  stack.emplace_back(&root, false);
  while (!stack.empty()) {
    auto& frame = stack.back();
    if (frame.children_visited) {
      RunRules(*frame.slot, ctx);
      stack.pop_back();
      continue;
    }
    frame.children_visited = true;
    for (auto& child : (**frame.slot).GetChildren()) {
      if (child) {
        stack.emplace_back(&child, false);
      }
    }
  }
}

}  // namespace irs
