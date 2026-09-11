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

#include <cstdlib>
#include <string>
#include <string_view>

#include "basics/down_cast.h"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/optimizer/boolean_rules.hpp"
#include "iresearch/search/optimizer/levenshtein_prefix_rules.hpp"
#include "iresearch/search/optimizer/lowering_rules.hpp"
#include "iresearch/search/optimizer/range_rules.hpp"
#include "iresearch/search/term_filter.hpp"

namespace irs {
namespace {

using Registry =
  sdb::containers::FlatHashMap<TypeInfo::type_id, std::vector<RuleDesc>>;

Registry& OptimizationRules() {
  static Registry gRules;
  return gRules;
}

void AssertValid([[maybe_unused]] const Filter& filter) {
  SDB_ASSERT(filter.type() != Type<BooleanFilter>::id() ||
             sdb::basics::downCast<BooleanFilter>(filter).Valid());
}

void RunRules(Filter::ptr& slot, const OptimizeContext& ctx) {
  AssertValid(*slot);
  bool changed = true;
  const auto& optimizations = OptimizationRules();
  while (changed) {
    const auto it = optimizations.find(slot->type());
    changed = false;
    if (it == optimizations.end()) {
      break;
    }
    for (const auto& rule : it->second) {
      if (rule.apply(slot, ctx)) {
        SDB_ASSERT(slot);
        AssertValid(*slot);
        changed = true;
        break;
      }
    }
  }
  AssertValid(*slot);
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

  optimizer::InitBooleanNormalizeTerms();

  optimizer::InitBooleanFlatten();

  optimizer::InitBooleanMinShouldMatch();
  optimizer::InitRangeDegenerate();
  optimizer::InitGranularRangeDegenerate();
  optimizer::InitEditDistanceSimplify();
  optimizer::InitPhraseSimplify();

  optimizer::InitBooleanAbsorb();
  optimizer::InitBooleanDedup();
  optimizer::InitBooleanNullMarker();
  optimizer::InitPhraseLower();
  optimizer::InitWildcardSimplify();
  optimizer::InitRegexpSimplify();

  optimizer::InitOrAcceptorFusion();
  optimizer::InitAndRangeMerge();
  optimizer::InitLevenshteinPrefixFusion();
  optimizer::InitNGramSimilarityLower();

  optimizer::InitBooleanSingleClause();
}

#ifdef SDB_DEV
namespace {

void AssertNoTermChild(Filter::ptr& root) {
  TraverseFilter(root, [](Filter::ptr& slot) {
    if (slot->type() != Type<BooleanFilter>::id()) {
      return;
    }
    const auto& node = sdb::basics::downCast<BooleanFilter>(*slot);
    for (const auto occur : kAllOccur) {
      for (const auto& child : node.Filters(occur)) {
        SDB_ASSERT(child->type() != Type<ByTerm>::id());
      }
    }
  });
}

}  // namespace
#endif

void Optimize(Filter::ptr& root, const OptimizeContext& ctx) {
  if (!root) {
    return;
  }
  RunPass(root, ctx);
  optimizer::LowerAutomatons(root, ctx);
  optimizer::FuseConjunctions(root, ctx);
#ifdef SDB_DEV
  AssertNoTermChild(root);
#endif
}

}  // namespace irs
