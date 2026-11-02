////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "nested_filter.hpp"

#include <absl/functional/overload.h>

#include <memory>
#include <utility>
#include <variant>

#include "iresearch/search/queries/nested_query.hpp"
#include "iresearch/search/scorers/scorer.hpp"

namespace irs {
namespace {

static_assert(std::variant_size_v<ByNestedOptions::MatchType> == 2);

const Scorer* GetOrder(const ByNestedOptions::MatchType& match,
                       const Scorer* scorer) noexcept {
  return std::visit(
    absl::Overload{[&](Match v) noexcept -> const Scorer* {
                     return kMatchNone == v ? nullptr : scorer;
                   },
                   [scorer](const MatchProvider&) noexcept -> const Scorer* {
                     return scorer;
                   }},
    match);
}

}  // namespace

bool IsValidMatch(const ByNestedOptions::MatchType& match) noexcept {
  return std::visit(
    absl::Overload{[](Match v) noexcept { return v.min <= v.max; },
                   [](const MatchProvider& v) { return nullptr != v; }},
    match);
}

PrepareCollector::ptr ByNestedFilter::MakeCollectorImpl(
  const Scorer* scorer, StatsArena& stats, uint32_t threads) const {
  auto& [parent, child, match, merge_type] = options();

  if (!parent || !child || !IsValidMatch(match)) {
    return nullptr;
  }

  auto compound = std::make_unique<CompoundCollector>(scorer);
  const auto* const order = GetOrder(match, scorer);
  compound->Add(order != nullptr ? child->MakeCollector(*order, stats, threads)
                                 : nullptr);
  return compound;
}

QueryBuilder::ptr ByNestedFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  auto& [parent, child, match, merge_type] = options();

  if (!parent || !child || !IsValidMatch(match)) {
    return QueryBuilder::Empty();
  }

  auto* compound =
    ctx.collector != nullptr ? ctx.collector->AsCompound() : nullptr;
  SDB_ASSERT(ctx.collector == nullptr || compound != nullptr);

  const auto sub_boost = ctx.boost * GetBoost();

  PrepareContext child_ctx = ctx;
  child_ctx.boost = sub_boost;
  child_ctx.collector = compound ? compound->Child(0) : nullptr;

  auto prepared_child = child->PrepareSegment(segment, child_ctx);

  if (!prepared_child) {
    return QueryBuilder::Empty();
  }

  auto query = memory::make_tracked<ByNestedQuery>(
    ctx.memory, segment, parent, std::move(prepared_child), merge_type, match,
    sub_boost);
  query->SetStats(ctx.Record());
  return query;
}

}  // namespace irs
