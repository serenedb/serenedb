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

#include "iresearch/search/filters/deferral.hpp"

#include <memory>
#include <utility>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/compiled_phrase.hpp"
#include "iresearch/search/filters/boolean_filter.hpp"
#include "iresearch/search/filters/common.hpp"
#include "iresearch/search/filters/filter_optimizer.hpp"
#include "iresearch/search/filters/geo_filter.hpp"
#include "iresearch/search/filters/phrase_filter.hpp"
#include "iresearch/search/filters/wildcard_ngram_filter.hpp"
#include "iresearch/utils/down_cast.hpp"

namespace irs::optimizer {
namespace {

using irs::utils::downCast;

duckdb::LogicalType StoredColumnType(const IndexReader& reader,
                                     field_id column) {
  for (const auto& segment : reader) {
    const auto* col_reader = segment.GetColReader();
    if (const auto* stored =
          col_reader ? col_reader->Column(column) : nullptr) {
      return stored->Type();
    }
  }
  return {};
}

Filter::ptr PartsConjunction(const ByPhrase& phrase,
                             const OptimizeContext& ctx) {
  const auto* scorer = ScorerOf(phrase, ctx);
  const bool constant = scorer && scorer->type() == Type<ConstantScore>::id();
  if (scorer && !constant && !IsUnscored(*scorer)) {
    return nullptr;
  }
  auto conjunction = std::make_unique<BooleanFilter>();
  for (const auto& info : phrase.options()) {
    conjunction->Add(PartFilter(phrase.field_id(), info.part, constant),
                     Occur::Must);
  }
  if (scorer) {
    conjunction->SetScorer(phrase.GetScorer());
    conjunction->SetBoost(phrase.GetBoost());
    if (constant) {
      conjunction->SetMergeType(ScoreMergeType::Max);
    }
  }
  return conjunction;
}

bool DeferPhrase(Filter::ptr& slot, const OptimizeContext& ctx) {
  const auto& phrase = downCast<ByPhrase>(*slot);
  const auto& options = phrase.options();
  const auto column = options.text();
  if (!field_limits::valid(column)) {
    return false;
  }
  auto type = StoredColumnType(ctx.deferral->reader, column);
  if (!type.IsValid()) {
    return false;
  }
  if (!IsStandalone(options) ||
      (options.slop() != 0 && HasPatternParts(options) &&
       options.explicit_pos())) {
    return false;
  }
  auto index = PartsConjunction(phrase, ctx);
  if (!index) {
    return false;
  }
  TraverseFilter(index, [&](Filter::ptr& node, bool) { RunRules(node, ctx); });
  ctx.deferral->filters.emplace_back(DeferredKind::Phrase, &phrase, column,
                                     std::move(type),
                                     std::exchange(slot, std::move(index)));
  return true;
}

template<typename GeoType, DeferredKind Kind>
bool DeferGeo(Filter::ptr& slot, const OptimizeContext& ctx) {
  auto& geo = downCast<GeoType>(*slot);
  auto& options = *geo.mutable_options();
  const auto column = options.store_field_id;
  auto type = StoredColumnType(ctx.deferral->reader, column);
  if (type.InternalType() != duckdb::PhysicalType::VARCHAR) {
    return false;
  }
  auto plan = PlanGeo(options);
  if (plan->kind != GeoPlan::Kind::Cells) {
    return false;
  }
  options.plan = std::move(plan);
  options.store_field_id = field_limits::invalid();
  ctx.deferral->filters.emplace_back(Kind, &geo, column, std::move(type));
  return true;
}

bool DeferWildcard(Filter::ptr& slot, const OptimizeContext& ctx) {
  auto& wildcard = downCast<ByWildcardNGram>(*slot);
  auto& options = *wildcard.mutable_options();
  if (!options.matcher) {
    return false;
  }
  const auto column = options.store_field_id;
  auto type = StoredColumnType(ctx.deferral->reader, column);
  if (!type.IsValid()) {
    return false;
  }
  options.store_field_id = field_limits::invalid();
  ctx.deferral->filters.emplace_back(DeferredKind::Wildcard, &wildcard, column,
                                     std::move(type));
  return true;
}

bool Defer(Filter::ptr& slot, const OptimizeContext& ctx) {
  const auto type = slot->type();
  if (type == Type<ByPhrase>::id()) {
    return DeferPhrase(slot, ctx);
  }
  if (type == Type<ByWildcardNGram>::id()) {
    return DeferWildcard(slot, ctx);
  }
  if (type == Type<GeoFilter>::id()) {
    return DeferGeo<GeoFilter, DeferredKind::Geo>(slot, ctx);
  }
  if (type == Type<GeoDistanceFilter>::id()) {
    return DeferGeo<GeoDistanceFilter, DeferredKind::GeoDistance>(slot, ctx);
  }
  return false;
}

}  // namespace

void ApplyDeferral(Filter::ptr& root, const OptimizeContext& ctx) {
  if (!ctx.deferral) {
    return;
  }
  if (root->type() != Type<BooleanFilter>::id()) {
    Defer(root, ctx);
    return;
  }
  bool deferred = false;
  for (auto& child :
       downCast<BooleanFilter>(*root).Bucket(Occur::Must).filters) {
    deferred |= Defer(child, ctx);
  }
  if (deferred) {
    RunRules(root, ctx);
  }
}

}  // namespace irs::optimizer
