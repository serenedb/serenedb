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

#include "iresearch/search/filters/range_rules.hpp"

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/search/detail/search_range.hpp"
#include "iresearch/search/filters/boolean_filter.hpp"
#include "iresearch/search/filters/common.hpp"
#include "iresearch/search/filters/filter_optimizer.hpp"
#include "iresearch/search/filters/granular_range_filter.hpp"
#include "iresearch/search/filters/prefix_filter.hpp"
#include "iresearch/search/filters/range_filter.hpp"
#include "iresearch/search/filters/term_filter.hpp"

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
  static constexpr std::array kTargets{Type<BooleanFilter>::id()};
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

template<typename Range>
Filter::ptr MergeSameBounds(const Range& lhs, const Range& rhs, bool min,
                            ScoreMergeType merge_type, bool scored) {
  const auto& tighter =
    (RangeBound(lhs, min) < RangeBound(rhs, min)) == min ? rhs : lhs;
  const auto& looser = &tighter == &lhs ? rhs : lhs;
  auto merged = std::make_unique<Range>();
  *merged->mutable_field_id() = lhs.field_id();
  *merged->mutable_options() = tighter.options();
  if (RangeBound(lhs, min) == RangeBound(rhs, min)) {
    auto& rng = merged->mutable_options()->range;
    auto& bound_type = min ? rng.min_type : rng.max_type;
    const auto& other = looser.options().range;
    if ((min ? other.min_type : other.max_type) == BoundType::Exclusive) {
      bound_type = BoundType::Exclusive;
    }
  }
  merged->SetBoost(scored
                     ? MergedBoost(merge_type, lhs.GetBoost(), rhs.GetBoost())
                     : kNoBoost);
  return merged;
}

template<typename Range>
Filter::ptr MergeRangeBounds(const Range& lo, const Range& hi,
                             ScoreMergeType merge_type, bool scored) {
  const score_t boost =
    scored ? MergedBoost(merge_type, lo.GetBoost(), hi.GetBoost()) : kNoBoost;
  if (RangeBound(lo, true) > RangeBound(hi, false)) {
    return std::make_unique<Empty>();
  }
  if (RangeBound(lo, true) == RangeBound(hi, false)) {
    if (lo.options().range.min_type == BoundType::Inclusive &&
        hi.options().range.max_type == BoundType::Inclusive) {
      auto by_term = std::make_unique<ByTerm>();
      *by_term->mutable_field_id() = lo.field_id();
      by_term->mutable_options()->term = RangeBound(lo, true);
      by_term->SetBoost(boost);
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
  merged->SetBoost(boost);
  return merged;
}

template<typename Range>
bool MergeComplementaryRanges(BooleanFilter& node, const OptimizeContext& ctx) {
  auto& children = node.Bucket(Occur::Must).filters;
  const auto is_range = [](const Filter::ptr& child) {
    return child->type() == Type<Range>::id();
  };
  std::vector<bool> consumed(children.size(), false);
  const bool scores = !ScoreIsIgnored(node, ctx);
  bool changed = false;
  for (size_t i = 0; i < children.size(); ++i) {
    if (consumed[i] || !is_range(children[i])) {
      continue;
    }
    auto& lo = irs::utils::downCast<Range>(*children[i]);
    const bool min_only = IsMinOnly(lo);
    if (!min_only && !IsMaxOnly(lo)) {
      continue;
    }
    for (size_t j = 0; j < children.size(); ++j) {
      if (j == i || consumed[j] || !is_range(children[j])) {
        continue;
      }
      auto& hi = irs::utils::downCast<Range>(*children[j]);
      const bool same = min_only ? IsMinOnly(hi) : IsMaxOnly(hi);
      const bool complementary = min_only ? IsMaxOnly(hi) : IsMinOnly(hi);
      if ((!same && !complementary) || hi.field_id() != lo.field_id()) {
        continue;
      }
      if (scores && children[i]->GetScorer() != children[j]->GetScorer()) {
        continue;
      }
      if (ctx.HasAnalyzer(lo.field_id())) {
        continue;
      }
      const auto* scorer = children[i]->GetScorer();
      if (same) {
        children[i] =
          MergeSameBounds(lo, hi, min_only, node.MergeType(), scores);
      } else if (min_only) {
        children[i] = MergeRangeBounds(lo, hi, node.MergeType(), scores);
      } else {
        children[i] = MergeRangeBounds(hi, lo, node.MergeType(), scores);
      }
      children[i]->SetScorer(scorer);
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
      if (out != children.begin() + static_cast<ptrdiff_t>(i)) {
        *out = std::move(children[i]);
      }
      ++out;
    }
  }
  children.erase(out, children.end());
  return true;
}

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

bool Accepts(const ByRangeOptions& options, bytes_view term) noexcept {
  return RangeMinAcceptor{&options.range}(term) &&
         RangeMaxAcceptor{&options.range}(term);
}

const ByPrefixOptions* AsPrefix(const Filter& child) noexcept {
  return child.type() == Type<ByPrefix>::id()
           ? &irs::utils::downCast<ByPrefix>(child).options()
           : nullptr;
}

const ByRangeOptions* AsRange(const Filter& child) noexcept {
  return child.type() == Type<ByRange>::id()
           ? &irs::utils::downCast<ByRange>(child).options()
           : nullptr;
}

field_id FieldOf(const Filter& child) noexcept {
  if (AsPrefix(child) != nullptr) {
    return irs::utils::downCast<ByPrefix>(child).field_id();
  }
  if (AsRange(child) != nullptr) {
    return irs::utils::downCast<ByRange>(child).field_id();
  }
  return field_limits::invalid();
}

bool CoversTerm(const Filter& child, const TermClause& clause) noexcept {
  if (FieldOf(child) != clause.field) {
    return false;
  }
  if (const auto* prefix = AsPrefix(child); prefix != nullptr) {
    return bytes_view{clause.term}.starts_with(prefix->term);
  }
  const auto* range = AsRange(child);
  return range != nullptr && Accepts(*range, clause.term);
}

bool CoveredTerm(std::span<const Filter::ptr> filters,
                 const TermClause& clause) noexcept {
  return AnyFilter(
    filters, [&](const Filter& child) { return CoversTerm(child, clause); });
}

class PrefixIndex {
 public:
  bool Build(std::span<const Filter::ptr> filters, size_t min_count) {
    size_t count = 0;
    for (const auto& child : filters) {
      count += static_cast<size_t>(AsPrefix(*child) != nullptr);
    }
    if (count < min_count) {
      return false;
    }
    _map.reserve(count);
    for (const auto& child : filters) {
      if (const auto* prefix = AsPrefix(*child); prefix != nullptr) {
        _map.try_emplace(Key{FieldOf(*child), prefix->term},
                         Entry{child.get()});
      }
    }
    for (auto& [key, entry] : _map) {
      const auto term = key.second;
      for (size_t size = 0; size < term.size(); ++size) {
        const auto it = _map.find(Key{key.first, term.substr(0, size)});
        if (it != _map.end()) {
          entry.has_wider = true;
          it->second.has_narrower = true;
        }
      }
    }
    return true;
  }

  bool Subsumed(const Filter& child, bool required) const noexcept {
    const auto* prefix = AsPrefix(child);
    if (prefix == nullptr) {
      return false;
    }
    const auto it = _map.find(Key{FieldOf(child), prefix->term});
    SDB_ASSERT(it != _map.end());
    const auto& entry = it->second;
    return entry.first != &child ||
           (required ? entry.has_narrower : entry.has_wider);
  }

  bool Covers(const Filter& child) const noexcept {
    const auto* prefix = AsPrefix(child);
    if (prefix == nullptr) {
      return false;
    }
    const bytes_view term{prefix->term};
    const auto field = FieldOf(child);
    for (size_t size = 0; size <= term.size(); ++size) {
      if (_map.contains(Key{field, term.substr(0, size)})) {
        return true;
      }
    }
    return false;
  }

 private:
  struct Entry {
    const Filter* first{};
    bool has_wider{false};
    bool has_narrower{false};
  };
  using Key = std::pair<field_id, bytes_view>;

  absl::flat_hash_map<Key, Entry> _map;
};

bool DropSubsumed(BooleanFilter& node, const OptimizeContext& ctx,
                  bool& empty_node) {
  if (!ScoreIsIgnored(node, ctx)) {
    return false;
  }
  bool changed = false;
  auto& excluded = node.Bucket(Occur::MustNot);
  PrefixIndex excluded_index;
  const bool has_excluded = excluded_index.Build(excluded.filters, 1);
  for (const auto occur : {Occur::Must, Occur::Should, Occur::MustNot}) {
    if (occur == Occur::Should && node.MinShouldMatch() > 1) {
      continue;
    }
    auto& bucket = node.Bucket(occur);
    const bool required = occur == Occur::Must;
    const bool optional = occur == Occur::Should;

    if (occur != Occur::MustNot) {
      const auto before = bucket.terms.size();
      std::erase_if(bucket.terms, [&](const TermClause& clause) {
        return CoveredTerm(excluded.filters, clause);
      });
      if (bucket.terms.size() != before) {
        if (required) {
          empty_node = true;
          return true;
        }
        changed = true;
      }
      if (has_excluded &&
          EraseFilters(bucket.filters, [&](const Filter& child) {
            return excluded_index.Covers(child);
          }) != 0) {
        if (required) {
          empty_node = true;
          return true;
        }
        changed = true;
      }
    }

    if (required) {
      changed |=
        EraseFilters(bucket.filters, [&](const Filter& child) {
          return absl::c_any_of(bucket.terms, [&](const TermClause& clause) {
            return CoversTerm(child, clause);
          });
        }) != 0;
    } else {
      const auto before = bucket.terms.size();
      std::erase_if(bucket.terms, [&](const TermClause& clause) {
        return CoveredTerm(bucket.filters, clause);
      });
      changed |= bucket.terms.size() != before;
    }

    if (PrefixIndex index; index.Build(bucket.filters, 2)) {
      changed |= EraseFilters(bucket.filters, [&](const Filter& child) {
                   return index.Subsumed(child, required);
                 }) != 0;
    }
    if (optional) {
      auto& must = node.Bucket(Occur::Must);
      const auto before = bucket.terms.size();
      std::erase_if(bucket.terms, [&](const TermClause& clause) {
        return CoveredTerm(must.filters, clause);
      });
      changed |= bucket.terms.size() != before;
    }
  }
  return changed;
}

}  // namespace

bool RangeDegenerateRule::Apply(Filter::ptr& slot, const OptimizeContext&) {
  auto& node = irs::utils::downCast<ByRange>(*slot);
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
    by_term->SetBoost(node.GetBoost());
    by_term->SetScorer(node.GetScorer());
    slot = std::move(by_term);
    return true;
  }
  slot = std::make_unique<Empty>();
  return true;
}

bool GranularRangeDegenerateRule::Apply(Filter::ptr& slot,
                                        const OptimizeContext&) {
  auto& node = irs::utils::downCast<ByGranularRange>(*slot);
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
    by_term->SetBoost(node.GetBoost());
    by_term->SetScorer(node.GetScorer());
    slot = std::move(by_term);
    return true;
  }
  slot = std::make_unique<Empty>();
  return true;
}

bool AndRangeMergeRule::Apply(Filter::ptr& slot, const OptimizeContext& ctx) {
  auto& node = irs::utils::downCast<BooleanFilter>(*slot);
  bool empty_node = false;
  bool changed = DropSubsumed(node, ctx, empty_node);
  if (empty_node) {
    slot = std::make_unique<Empty>();
    return true;
  }
  if (node.Filters(Occur::Must).size() < 2) {
    return changed;
  }
  const bool merged_range = MergeComplementaryRanges<ByRange>(node, ctx);
  const bool merged_granular =
    MergeComplementaryRanges<ByGranularRange>(node, ctx);
  return changed || merged_range || merged_granular;
}

void InitRangeDegenerate() { RegisterRule<RangeDegenerateRule>(); }

void InitGranularRangeDegenerate() {
  RegisterRule<GranularRangeDegenerateRule>();
}

void InitAndRangeMerge() { RegisterRule<AndRangeMergeRule>(); }

}  // namespace irs::optimizer
