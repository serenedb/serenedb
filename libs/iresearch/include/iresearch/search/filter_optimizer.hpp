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

#pragma once

#include <absl/container/inlined_vector.h>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>

#include "iresearch/search/filter.hpp"

namespace irs {

struct OptimizeContext {
  bool scored = false;
  bool fuse_seekable_acceptors = false;
  bool fuse_acceptor_intersections = false;
  sdb::containers::FlatHashSet<irs::field_id> analyzed_fields;

  bool HasAnalyzer(irs::field_id field) const noexcept {
    return analyzed_fields.contains(field);
  }
};

template<typename Visit>
void TraverseFilter(Filter::ptr& root, Visit&& visit) {
  SDB_ASSERT(root);
  struct Frame {
    Filter::ptr* slot;
    bool children_visited;
  };

  absl::InlinedVector<Frame, 16> stack;
  stack.emplace_back(&root, false);
  while (!stack.empty()) {
    auto& frame = stack.back();
    if (frame.children_visited) {
      visit(*frame.slot);
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

struct RuleDesc {
  std::string_view name;
  std::span<const TypeInfo::type_id> targets;
  bool (*apply)(Filter::ptr& slot, const OptimizeContext& ctx);
};

template<typename Rule>
concept RuleLike = requires {
  { Rule::kName } -> std::convertible_to<std::string_view>;
  { std::span<const TypeInfo::type_id>{Rule::kTargets} };
  { Rule::kEnable } -> std::convertible_to<bool>;
  {
    &Rule::Apply
  } -> std::convertible_to<bool (*)(Filter::ptr&, const OptimizeContext&)>;
};

void RegisterRule(RuleDesc rule);

template<RuleLike Rule>
void RegisterRule() {
  if constexpr (Rule::kEnable) {
    auto r = RuleDesc{Rule::kName, Rule::kTargets, &Rule::Apply};
    RegisterRule(std::move(r));
  }
}

void InitOptimizeRules();

void Optimize(Filter::ptr& root, const OptimizeContext& ctx = {});

}  // namespace irs
