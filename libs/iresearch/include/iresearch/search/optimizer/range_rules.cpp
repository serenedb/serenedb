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

#include "iresearch/search/optimizer/range_rules.hpp"

#include <algorithm>
#include <memory>
#include <type_traits>
#include <utility>

#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/granular_range_filter.hpp"
#include "iresearch/search/range_filter.hpp"
#include "iresearch/search/search_range.hpp"
#include "iresearch/search/term_filter.hpp"

namespace irs::optimizer {
namespace {

struct RangeDegenerateRule {
  static constexpr std::string_view kName = "range_degenerate";
  static constexpr std::array kTargets{Type<ByRange>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct GranularRangeDegenerateRule {
  static constexpr std::string_view kName = "granular_range_degenerate";
  static constexpr std::array kTargets{Type<ByGranularRange>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct AndRangeMergeRule {
  static constexpr std::string_view kName = "and_range_merge";
  static constexpr std::array kTargets{Type<And>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

template<typename Range>
bool IsMinOnly(const Range& node) noexcept {
  const auto& rng = node.options().range;
  return rng.min_type != BoundType::Unbounded &&
         rng.max_type == BoundType::Unbounded;
}

template<typename Range>
bool IsMaxOnly(const Range& node) noexcept {
  const auto& rng = node.options().range;
  return rng.max_type != BoundType::Unbounded &&
         rng.min_type == BoundType::Unbounded;
}

template<typename Range>
const bstring& RangeBound(const Range& node, bool min) noexcept {
  const auto& rng = node.options().range;
  if constexpr (std::is_same_v<Range, ByRange>) {
    return min ? rng.min : rng.max;
  } else {
    return min ? rng.min.front() : rng.max.front();
  }
}

score_t MergedBoost(ScoreMergeType merge_type, score_t lo,
                    score_t hi) noexcept {
  switch (merge_type) {
    case ScoreMergeType::Max:
      return std::max(lo, hi);
    case ScoreMergeType::Noop:
      return kNoBoost;
    case ScoreMergeType::Sum:
      break;
  }
  return lo + hi;
}

template<typename Range>
Filter::ptr MergeRangeBounds(const Range& lo, const Range& hi,
                             ScoreMergeType merge_type) {
  const score_t boost = MergedBoost(merge_type, lo.Boost(), hi.Boost());
  if (RangeBound(lo, true) > RangeBound(hi, false)) {
    return std::make_unique<Empty>();
  }
  if (RangeBound(lo, true) == RangeBound(hi, false)) {
    if (lo.options().range.min_type == BoundType::Inclusive &&
        hi.options().range.max_type == BoundType::Inclusive) {
      auto by_term = std::make_unique<ByTerm>();
      *by_term->mutable_field_id() = lo.field_id();
      by_term->mutable_options()->term = RangeBound(lo, true);
      by_term->boost(boost);
      return by_term;
    }
    return std::make_unique<Empty>();
  }
  auto merged = std::make_unique<Range>();
  *merged->mutable_field_id() = lo.field_id();
  auto& options = *merged->mutable_options();
  options = lo.options();
  options.range.max = hi.options().range.max;
  options.range.max_type = hi.options().range.max_type;
  merged->boost(boost);
  return merged;
}

template<typename Range>
bool MergeComplementaryRanges(And& node, const OptimizeContext& ctx) {
  auto& children = node.mutable_filters();
  const auto is_range = [](const Filter::ptr& child) {
    return child->type() == Type<Range>::id();
  };
  std::vector<bool> consumed(children.size(), false);
  bool changed = false;
  for (size_t i = 0; i < children.size(); ++i) {
    if (consumed[i] || !is_range(children[i])) {
      continue;
    }
    auto& lo = sdb::basics::downCast<Range>(*children[i]);
    if (!IsMinOnly(lo)) {
      continue;
    }
    for (size_t j = 0; j < children.size(); ++j) {
      if (j == i || consumed[j] || !is_range(children[j])) {
        continue;
      }
      auto& hi = sdb::basics::downCast<Range>(*children[j]);
      if (!IsMaxOnly(hi) || hi.field_id() != lo.field_id()) {
        continue;
      }
      if (ctx.HasAnalyzer(lo.field_id())) {
        continue;
      }
      children[i] = MergeRangeBounds(lo, hi, node.merge_type());
      consumed[j] = true;
      changed = true;
      break;
    }
  }
  if (!changed) {
    return false;
  }
  auto out = children.begin();
  for (size_t i = 0; i < consumed.size(); ++i) {
    if (!consumed[i]) {
      *out++ = std::move(children[i]);
    }
  }
  children.erase(out, children.end());
  return true;
}

}  // namespace

bool RangeDegenerateRule::Apply(Filter::ptr& slot,
                                const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<ByRange>(*slot);
  const auto& rng = node.options().range;
  if (rng.min_type == BoundType::Unbounded ||
      rng.max_type == BoundType::Unbounded) {
    return false;
  }
  if (rng.min > rng.max) {
    slot = std::make_unique<Empty>();
    return true;
  }
  if (rng.min != rng.max) {
    return false;
  }
  if (rng.min_type == BoundType::Inclusive &&
      rng.max_type == BoundType::Inclusive) {
    auto by_term = std::make_unique<ByTerm>();
    *by_term->mutable_field_id() = node.field_id();
    by_term->mutable_options()->term = rng.min;
    by_term->boost(node.Boost());
    slot = std::move(by_term);
    return true;
  }
  slot = std::make_unique<Empty>();
  return true;
}

bool GranularRangeDegenerateRule::Apply(Filter::ptr& slot,
                                        const OptimizeContext& /*ctx*/) {
  auto& node = sdb::basics::downCast<ByGranularRange>(*slot);
  const auto& rng = node.options().range;
  if (rng.min.empty() || rng.max.empty()) {
    return false;
  }
  if (rng.min.front() > rng.max.front()) {
    slot = std::make_unique<Empty>();
    return true;
  }
  if (rng.min.front() != rng.max.front()) {
    return false;
  }
  if (rng.min_type == BoundType::Inclusive &&
      rng.max_type == BoundType::Inclusive) {
    auto by_term = std::make_unique<ByTerm>();
    *by_term->mutable_field_id() = node.field_id();
    by_term->mutable_options()->term = rng.min.front();
    by_term->boost(node.Boost());
    slot = std::move(by_term);
    return true;
  }
  slot = std::make_unique<Empty>();
  return true;
}

bool AndRangeMergeRule::Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
  auto& node = sdb::basics::downCast<And>(*slot);
  if (node.size() < 2) {
    return false;
  }
  const bool merged_range = MergeComplementaryRanges<ByRange>(node, ctx);
  const bool merged_granular =
    MergeComplementaryRanges<ByGranularRange>(node, ctx);
  return merged_range || merged_granular;
}

void InitRangeRules() {
  RegisterRule<RangeDegenerateRule>();
  RegisterRule<GranularRangeDegenerateRule>();
  RegisterRule<AndRangeMergeRule>();
}

}  // namespace irs::optimizer
