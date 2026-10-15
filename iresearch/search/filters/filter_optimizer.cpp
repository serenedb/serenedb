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

#include "iresearch/search/filters/boolean_filter.hpp"
#include "iresearch/search/filters/boolean_rules.hpp"
#include "iresearch/search/filters/deferral.hpp"
#include "iresearch/search/filters/geo_filter.hpp"
#include "iresearch/search/filters/levenshtein_prefix_rules.hpp"
#include "iresearch/search/filters/lowering_rules.hpp"
#include "iresearch/search/filters/range_rules.hpp"
#include "iresearch/search/filters/term_filter.hpp"
#include "iresearch/utils/down_cast.hpp"

namespace irs {
namespace {

using Registry =
  irs::containers::FlatHashMap<TypeInfo::type_id, std::vector<RuleDesc>>;

Registry& OptimizationRules() {
  static Registry gRules;
  return gRules;
}

void AssertValid([[maybe_unused]] const Filter& filter) {
  SDB_ASSERT(filter.type() != Type<BooleanFilter>::id() ||
             irs::utils::downCast<BooleanFilter>(filter).Valid());
}

}  // namespace

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

namespace {

void RunPass(Filter::ptr& root, const OptimizeContext& ctx) {
  if (!ctx.scored) {
    TraverseFilter(root, [&](Filter::ptr& slot, bool) { RunRules(slot, ctx); });
    return;
  }
  auto negated_ctx = ctx;
  negated_ctx.scored = false;
  TraverseFilter(root, [&](Filter::ptr& slot, bool negated) {
    RunRules(slot, negated ? negated_ctx : ctx);
  });
}

template<typename GeoType>
void PlanOnce(Filter& filter) {
  auto& options = *utils::downCast<GeoType>(filter).mutable_options();
  options.plan = PlanGeo(options);
}

void PlanGeoFilters(Filter::ptr& root) {
  TraverseFilter(root, [](Filter::ptr& slot, bool) {
    const auto type = slot->type();
    if (type == Type<GeoFilter>::id()) {
      PlanOnce<GeoFilter>(*slot);
    } else if (type == Type<GeoDistanceFilter>::id()) {
      PlanOnce<GeoDistanceFilter>(*slot);
    }
  });
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
  TraverseFilter(root, [](Filter::ptr& slot, bool) {
    if (slot->type() != Type<BooleanFilter>::id()) {
      return;
    }
    const auto& node = irs::utils::downCast<BooleanFilter>(*slot);
    for (const auto occur : kAllOccur) {
      for (const auto& child : node.Filters(occur)) {
        SDB_ASSERT(child->type() != Type<ByTerm>::id());
      }
    }
  });
}

void AssertDeferredAlive(Filter::ptr& root, const OptimizeContext& ctx) {
  if (!ctx.deferral) {
    return;
  }
  for (const auto& deferred : ctx.deferral->filters) {
    bool found = deferred.owned.get() == deferred.filter;
    TraverseFilter(root, [&](Filter::ptr& slot, bool) {
      found |= slot.get() == deferred.filter;
    });
    SDB_ASSERT(found);
  }
}

}  // namespace
#endif

void Optimize(Filter::ptr& root, const OptimizeContext& ctx) {
  if (!root) {
    return;
  }
  RunPass(root, ctx);
  optimizer::ApplyDeferral(root, ctx);
  optimizer::LowerAutomatons(root, ctx);
  optimizer::FuseConjunctions(root, ctx);
  PlanGeoFilters(root);
#ifdef SDB_DEV
  AssertNoTermChild(root);
  AssertDeferredAlive(root, ctx);
#endif
}

}  // namespace irs
