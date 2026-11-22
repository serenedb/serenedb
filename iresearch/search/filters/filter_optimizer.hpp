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

#include <absl/algorithm/container.h>
#include <absl/container/inlined_vector.h>

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/search/filters/deferral.hpp"
#include "iresearch/search/filters/filter.hpp"
#include "iresearch/utils/containers/flat_hash_set.hpp"

namespace irs {

struct OptimizeContext {
  bool scored = false;
  bool fuse_seekable_acceptors = false;
  bool fuse_acceptor_intersections = false;
  irs::containers::FlatHashSet<irs::field_id> analyzed_fields;
  const Scorer* scorer = nullptr;
  Deferral* deferral = nullptr;

  bool HasAnalyzer(irs::field_id field) const noexcept {
    return analyzed_fields.contains(field);
  }
};

namespace optimizer {

void ApplyDeferral(Filter::ptr& root, const OptimizeContext& ctx);

size_t EraseFilters(std::vector<Filter::ptr>& filters, auto predicate) {
  auto tail = filters.begin();
  for (auto it = tail, end = filters.end(); it != end; ++it) {
    if (!predicate(**it)) {
      if (tail != it) {
        std::iter_swap(tail, it);
      }
      ++tail;
    }
  }
  const auto erased = static_cast<size_t>(std::distance(tail, filters.end()));
  filters.erase(tail, filters.end());
  return erased;
}

bool AnyFilter(std::span<const Filter::ptr> filters, auto predicate) {
  return absl::c_any_of(
    filters, [&](const Filter::ptr& child) { return predicate(*child); });
}

}  // namespace optimizer

template<typename Visit>
void TraverseFilter(Filter::ptr& root, Visit&& visit) {
  SDB_ASSERT(root);
  struct Frame {
    Filter::ptr* slot;
    bool children_visited;
    bool negated;
  };

  absl::InlinedVector<Frame, 16> stack;
  stack.emplace_back(&root, false, false);
  while (!stack.empty()) {
    auto& frame = stack.back();
    if (frame.children_visited) {
      const auto* scorer = (**frame.slot).GetScorer();
      visit(*frame.slot, frame.negated);
      if (scorer != nullptr && *frame.slot) {
        const auto* lowered = (**frame.slot).GetScorer();
        if (lowered == nullptr || lowered == &DefaultConstScore()) {
          (**frame.slot).SetScorer(scorer);
        }
      }
      stack.pop_back();
      continue;
    }
    frame.children_visited = true;
    const bool negated = frame.negated;
    (**frame.slot).VisitChildren([&](Filter::ptr& child, bool child_negated) {
      if (child) {
        stack.emplace_back(&child, false, negated || child_negated);
      }
    });
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

void RunRules(Filter::ptr& slot, const OptimizeContext& ctx);

void Optimize(Filter::ptr& root, const OptimizeContext& ctx = {});

}  // namespace irs
