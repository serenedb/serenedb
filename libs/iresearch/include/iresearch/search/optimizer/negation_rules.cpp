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

#include "iresearch/search/optimizer/negation_rules.hpp"

#include <memory>

#include "iresearch/search/all_docs_provider.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/optimizer/common.hpp"

namespace irs::optimizer {
namespace {

struct NotSimplifyRule {
  static constexpr std::string_view kName = "not_simplify";
  static constexpr std::array kTargets{Type<Not>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct NotLowerRule {
  static constexpr std::string_view kName = "not_lower";
  static constexpr std::array kTargets{Type<Not>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct ExclusionRule {
  static constexpr std::string_view kName = "exclusion_simplify";
  static constexpr std::array kTargets{Type<Exclusion>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct ExclusionDoubleNegationRule {
  static constexpr std::string_view kName = "exclusion_double_negation";
  static constexpr std::array kTargets{Type<Exclusion>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

}  // namespace

bool NotSimplifyRule::Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
  size_t negations = 0;
  Filter::ptr* inner = &slot;
  while ((*inner)->type() == Type<Not>::id()) {
    auto& node = sdb::basics::downCast<Not>(**inner);
    SDB_ASSERT(node.Boost() == kNoBoost);
    if (!node.mutable_filter()) {
      return false;
    }
    ++negations;
    inner = &node.mutable_filter();
  }

  auto& base = *inner;
  const bool odd = (negations % 2) != 0;

  if (base->type() == Type<Empty>::id()) {
    slot = odd ? AllDocsProvider::Default(kNoBoost) : std::move(base);
    return true;
  }
  if (IsAllDocs(*base)) {
    slot = odd ? std::make_unique<Empty>() : std::move(base);
    return true;
  }
  if (negations < 2) {
    return false;
  }
  slot = odd ? std::make_unique<Not>(std::move(base)) : std::move(base);
  return true;
}

bool NotLowerRule::Apply(Filter::ptr& slot, const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<Not>(*slot);
  auto exclusion = std::make_unique<Exclusion>();
  if (auto& inner = node.mutable_filter(); inner) {
    exclusion->exclude(std::move(inner));
  }
  exclusion->boost(node.Boost());
  slot = std::move(exclusion);
  return true;
}

bool ExclusionRule::Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<Exclusion>(*slot);
  auto& incl = node.mutable_include();
  if (!node.mutable_excludes().empty()) {
    return false;
  }
  if (!incl) {
    slot = AllDocsProvider::Default(node.Boost());
    return true;
  }
  if (!TryFoldBoost(*incl, node.Boost(), ctx.scorer)) {
    return false;
  }
  slot = std::move(incl);
  return true;
}

bool ExclusionDoubleNegationRule::Apply(Filter::ptr& slot,
                                        const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<Exclusion>(*slot);
  if (node.mutable_include() || node.mutable_excludes().size() != 1) {
    return false;
  }
  Filter::ptr* inner = &node.mutable_excludes()[0];
  bool negated = true;
  size_t depth = 0;
  while ((*inner)->type() == Type<Exclusion>::id()) {
    auto& ex = sdb::basics::downCast<Exclusion>(**inner);
    if (ex.mutable_include() || ex.mutable_excludes().size() != 1) {
      break;
    }
    inner = &ex.mutable_excludes()[0];
    negated = !negated;
    ++depth;
  }
  if (depth == 0) {
    return false;
  }
  if (!negated) {
    if (!TryFoldBoost(**inner, node.Boost(), ctx.scorer)) {
      return false;
    }
    slot = std::move(*inner);
    return true;
  }
  auto exclusion = std::make_unique<Exclusion>();
  exclusion->exclude(std::move(*inner));
  exclusion->boost(node.Boost());
  slot = std::move(exclusion);
  return true;
}

void InitNegationRules() {
  RegisterRule<NotSimplifyRule>();
  RegisterRule<NotLowerRule>();
  RegisterRule<ExclusionRule>();
  RegisterRule<ExclusionDoubleNegationRule>();
}

}  // namespace irs::optimizer
