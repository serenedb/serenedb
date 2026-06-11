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
constexpr auto kDefaultRulesStorage = std::to_array({
  MakeRule<optimizer::ExclusionRule>(),
  MakeRule<optimizer::NotSimplifyRule>(),
  MakeRule<optimizer::AndEmptyRule>(),
  MakeRule<optimizer::OrEmptyRule>(),
  MakeRule<optimizer::OrMinMatchZeroRule>(),
  MakeRule<optimizer::OrUnsatRule>(),
  MakeRule<optimizer::AndAllFoldRule>(),
  MakeRule<optimizer::OrAllFoldRule>(),
  MakeRule<optimizer::FlattenAnd>(),
  MakeRule<optimizer::FlattenOr>(),
  MakeRule<optimizer::AndExclusionCoalesceRule>(),
  MakeRule<optimizer::OrAllRequiredRule>(),
  MakeRule<optimizer::ByTermsRule>(),
  MakeRule<optimizer::ByTermsMinMatchZeroRule>(),
  MakeRule<optimizer::ByTermsDegenerateRule>(),
  MakeRule<optimizer::SingleChildRule>(),
  MakeRule<optimizer::EmptyAndRule>(),
  MakeRule<optimizer::MixedDegenerateRule>(),
  MakeRule<optimizer::RangeDegenerateRule>(),
  MakeRule<optimizer::GranularRangeDegenerateRule>(),
  MakeRule<optimizer::NGramSimilarityLowerRule>(),
  MakeRule<optimizer::WildcardLowerRule>(),
  MakeRule<optimizer::RegexpLowerRule>(),
  MakeRule<optimizer::EditDistanceLowerRule>(),
  MakeRule<optimizer::PhraseLowerRule>(),
  MakeRule<optimizer::ExclusionDoubleNegationRule>(),
  MakeRule<optimizer::NotLowerRule>(),
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

std::vector<RuleDesc>& RuleRegistry() {
  static std::vector<RuleDesc> gRules(kDefaultRulesStorage.begin(),
                                      kDefaultRulesStorage.end());
  return gRules;
}

}  // namespace

constinit const std::span<const RuleDesc> kDefaultRules{kDefaultRulesStorage};

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
}

}  // namespace irs
