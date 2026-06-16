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

#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/mixed_boolean_filter.hpp"
#include "iresearch/search/optimizer/boolean_rules.hpp"
#include "iresearch/search/optimizer/levenshtein_prefix_rules.hpp"
#include "iresearch/search/optimizer/lowering_rules.hpp"
#include "iresearch/search/optimizer/negation_rules.hpp"
#include "iresearch/search/optimizer/range_rules.hpp"
#include "iresearch/search/optimizer/terms_rules.hpp"

namespace irs {
namespace {

using Registry =
  sdb::containers::FlatHashMap<TypeInfo::type_id, std::vector<RuleDesc>>;

Registry& OptimizationRules() {
  static Registry gRules;
  return gRules;
}

void RunRules(Filter::ptr& slot, const OptimizeContext& ctx) {
  bool changed = true;
  const auto& optimizations = OptimizationRules();
  while (changed) {
    const auto it = optimizations.find(slot->type());
    changed = false;
    if (it == optimizations.end()) {
      return;
    }
    for (const auto& rule : it->second) {
      if (rule.apply(slot, ctx)) {
        SDB_ASSERT(slot);
        changed = true;
        break;
      }
    }
  }
}

void RunPass(Filter::ptr& root, const OptimizeContext& ctx) {
  TraverseFilter(root, [&](Filter::ptr& slot) { RunRules(slot, ctx); });
}

}  // namespace

void RegisterRule(RuleDesc rule) {
  auto& registry = OptimizationRules();
  for (const auto tid : rule.targets) {
    registry[tid].push_back(rule);
  }
}

void InitOptimizeRules() {
  SDB_ASSERT(OptimizationRules().empty());
  optimizer::InitBooleanRules();
  optimizer::InitNegationRules();
  optimizer::InitTermsRules();
  optimizer::InitRangeRules();
  optimizer::InitLevenshteinPrefixRules();
  optimizer::InitLoweringRules();
}

void Optimize(Filter::ptr& root, const OptimizeContext& ctx) {
  if (!root) {
    return;
  }
  RunPass(root, ctx);
  optimizer::LowerAutomatons(root, ctx);
}

}  // namespace irs
