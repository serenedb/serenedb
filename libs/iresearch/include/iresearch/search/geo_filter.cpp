////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/search/geo_filter.hpp"

#include <absl/base/internal/endian.h>
#include <s2/s2cap.h>
#include <s2/s2earth.h>
#include <s2/s2point_region.h>

#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>

#include "basics/down_cast.h"
#include "basics/log.h"
#include "basics/memory.hpp"
#include "geo/geo_params.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/geo_query.hpp"
#include "iresearch/search/geo_terms.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/types.hpp"

namespace irs {
namespace {

using namespace sdb::geo;

constexpr auto kSingletonCapEps = 2 * std::numeric_limits<double>::epsilon();

QueryBuilder::ptr MatchAll(const SubReader& segment,
                           const PrepareContext& ctx) {
  return irs::All{}.PrepareSegment(segment, ctx);
}

inline S2Cap FromPoint(const S2Point& origin) noexcept {
  return S2Cap{origin, S1Angle::Radians(kSingletonCapEps)};
}

inline S2Cap FromPoint(S2Point origin, double distance) noexcept {
  return {origin, S1Angle::Radians(MetersToRadians(distance))};
}

template<typename Options, typename Acceptor>
QueryBuilder::ptr MakeQuery(const SubReader& segment, const PrepareContext& ctx,
                            QueryBuilder::ptr&& cells, score_t boost,
                            const Options& options, Acceptor&& acceptor) {
  if (!cells || cells->Kind() == QueryKind::Empty) {
    return QueryBuilder::Empty();
  }
  const auto store_field_id = options.store_field_id;
  const auto* col_reader = segment.GetColReader();
  if (!col_reader || !col_reader->Column(store_field_id)) {
    return QueryBuilder::Empty();
  }
  const auto make = [&]<typename Parser>(Parser parser) -> QueryBuilder::ptr {
    auto query = memory::make_tracked<GeoQuery<Parser, Acceptor>>(
      ctx.memory, segment, std::move(cells), store_field_id, std::move(parser),
      std::forward<Acceptor>(acceptor), boost);
    query->SetStats(ctx.Record());
    return query;
  };
  switch (options.stored) {
    case StoredType::Source:
      if (options.source_is_wkb) {
        return make(SourceWkbParser{});
      }
      if (options.source_is_point) {
        return make(
          SourcePointParser{options.point_latitude, options.point_longitude});
      }
      return make(SourceJsonParser{});
    case StoredType::S2Region:
      return make(S2ShapeParser{});
    case StoredType::S2Point:
    case StoredType::S2Centroid:
      return make(S2PointParser{});
  }
  SDB_ASSERT(false);
  return QueryBuilder::Empty();
}

QueryBuilder::ptr PrepareCells(const SubReader& segment,
                               const PrepareContext& ctx,
                               std::span<const std::string> geo_terms,
                               irs::field_id id) {
  SDB_ASSERT(!geo_terms.empty());

  std::vector<std::string_view> sorted_terms(geo_terms.begin(),
                                             geo_terms.end());
  absl::c_sort(sorted_terms);
  SDB_ASSERT(std::unique(sorted_terms.begin(), sorted_terms.end()) ==
             sorted_terms.end());

  const auto* reader = segment.field(id);
  if (!reader) {
    return QueryBuilder::Empty();
  }
  auto terms = reader->iterator();
  if (!terms) [[unlikely]] {
    return QueryBuilder::Empty();
  }

  auto query = memory::make_tracked<MultiTermQuery>(
    ctx.memory, segment, ctx.memory, kNoBoost, ScoreMergeType::Noop);
  auto& state = query->State();
  state.Prepare(reader);
  for (const auto term : sorted_terms) {
    if (!terms->seek(ViewCast<byte_type>(term))) {
      continue;
    }
    state.Push(terms->cookie(), kNoBoost);
  }

  PrepareContext cells = ctx;
  cells.needs_terms = ctx.KeepsTerms();
  cells.collector = nullptr;
  return MultiTermQuery::Finish(std::move(query), cells);
}

std::pair<S2Cap, bool> GetBound(BoundType type, S2Point origin,
                                double distance) {
  if (BoundType::Unbounded == type) {
    return {S2Cap::Full(), true};
  }

  return {(0. == distance ? FromPoint(origin) : FromPoint(origin, distance)),
          BoundType::Inclusive == type};
}

BooleanFilter ExcludeCentre(irs::field_id id,
                            const GeoDistanceFilterOptions& options) {
  BooleanFilter root;
  auto excl = std::make_unique<GeoDistanceFilter>();
  *excl->mutable_field_id() = id;
  auto& opts = *excl->mutable_options();
  opts = options;
  opts.range.min = 0;
  opts.range.min_type = BoundType::Inclusive;
  opts.range.max = 0;
  opts.range.max_type = BoundType::Inclusive;
  root.Add(std::move(excl), Occur::MustNot);
  root.Add(std::make_unique<All>(), Occur::Must);
  return root;
}

QueryBuilder::ptr PrepareOpenInterval(const SubReader& segment,
                                      const PrepareContext& ctx,
                                      irs::field_id id,
                                      const GeoDistanceFilterOptions& options,
                                      bool greater) {
  const auto& range = options.range;
  const auto& origin = options.origin;

  const auto [dist, type] =
    greater ? std::forward_as_tuple(range.min, range.min_type)
            : std::forward_as_tuple(range.max, range.max_type);

  S2Cap bound;

  bool incl;

  if (dist < 0.) {
    bound = greater ? S2Cap::Full() : S2Cap::Empty();
  } else if (0. == dist) {
    switch (type) {
      case BoundType::Unbounded:
        incl = false;
        SDB_ASSERT(false);
        break;
      case BoundType::Inclusive:
        bound = greater ? S2Cap::Full() : FromPoint(origin);

        if (!bound.is_valid()) {
          return QueryBuilder::Empty();
        }

        incl = true;
        break;
      case BoundType::Exclusive:
        if (greater) {
          return ExcludeCentre(id, options).PrepareSegment(segment, ctx);
        } else {
          bound = S2Cap::Empty();
        }

        incl = false;
        break;
    }
  } else {
    std::tie(bound, incl) = GetBound(type, origin, dist);

    if (!bound.is_valid()) {
      return QueryBuilder::Empty();
    }

    if (greater) {
      bound = bound.Complement();
    }
  }

  SDB_ASSERT(bound.is_valid());

  if (bound.is_full()) {
    return MatchAll(segment, ctx);
  }

  if (bound.is_empty()) {
    return QueryBuilder::Empty();
  }

  const auto geo_terms =
    irs::geo_terms::QueryTerms(options.options, bound, options.prefix);

  if (geo_terms.empty()) {
    return QueryBuilder::Empty();
  }

  auto cells = PrepareCells(segment, ctx, geo_terms, id);

  if (incl) {
    return MakeQuery(segment, ctx, std::move(cells), ctx.boost, options,
                     GeoDistanceAcceptor<true>{bound});
  } else {
    return MakeQuery(segment, ctx, std::move(cells), ctx.boost, options,
                     GeoDistanceAcceptor<false>{bound});
  }
}

QueryBuilder::ptr PrepareInterval(const SubReader& segment,
                                  const PrepareContext& ctx, irs::field_id id,
                                  const GeoDistanceFilterOptions& options) {
  const auto& range = options.range;
  SDB_ASSERT(BoundType::Unbounded != range.min_type);
  SDB_ASSERT(BoundType::Unbounded != range.max_type);

  if (range.max < 0.) {
    return QueryBuilder::Empty();
  } else if (range.min < 0.) {
    return PrepareOpenInterval(segment, ctx, id, options, false);
  }

  const bool min_incl = range.min_type == BoundType::Inclusive;
  const bool max_incl = range.max_type == BoundType::Inclusive;

  if (math::ApproxEquals(range.min, range.max)) {
    if (!min_incl || !max_incl) {
      return QueryBuilder::Empty();
    }
  } else if (range.min > range.max) {
    return QueryBuilder::Empty();
  }

  const auto& origin = options.origin;

  if (0. == range.max && 0. == range.min) {
    SDB_ASSERT(min_incl);
    SDB_ASSERT(max_incl);

    const auto geo_terms =
      irs::geo_terms::QueryTerms(options.options, origin, options.prefix);

    if (geo_terms.empty()) {
      return QueryBuilder::Empty();
    }

    auto cells = PrepareCells(segment, ctx, geo_terms, id);

    return MakeQuery(segment, ctx, std::move(cells), ctx.boost, options,
                     GeoDistanceAcceptor<false>{FromPoint(origin)});
  }

  auto min_bound = FromPoint(origin, range.min);
  auto max_bound = FromPoint(origin, range.max);

  if (!min_bound.is_valid() || !max_bound.is_valid()) {
    return QueryBuilder::Empty();
  }

  S2RegionCoverer coverer(options.options);

  SDB_ASSERT(!min_bound.is_empty());
  SDB_ASSERT(!max_bound.is_empty());

  const auto ring = coverer.GetCovering(max_bound).Difference(
    coverer.GetInteriorCovering(min_bound));
  // S2CellUnion::Difference has no level cap: GetDifferenceInternal recurses
  // until cells are disjoint or fully contained, so `ring` can have cells
  // beyond options.max_level. Re-cover through GetQueryTerms so the coverer
  // enforces min/max level before GetQueryTermsForCanonicalCovering runs.
  const auto geo_terms =
    irs::geo_terms::QueryTerms(options.options, ring, options.prefix);

  if (geo_terms.empty()) {
    return QueryBuilder::Empty();
  }

  auto cells = PrepareCells(segment, ctx, geo_terms, id);

  switch (size_t(min_incl) + 2 * size_t(max_incl)) {
    case 0:
      return MakeQuery(
        segment, ctx, std::move(cells), ctx.boost, options,
        GeoDistanceRangeAcceptor<false, false>{min_bound, max_bound});
    case 1:
      return MakeQuery(
        segment, ctx, std::move(cells), ctx.boost, options,
        GeoDistanceRangeAcceptor<true, false>{min_bound, max_bound});
    case 2:
      return MakeQuery(
        segment, ctx, std::move(cells), ctx.boost, options,
        GeoDistanceRangeAcceptor<false, true>{min_bound, max_bound});
    case 3:
      return MakeQuery(
        segment, ctx, std::move(cells), ctx.boost, options,
        GeoDistanceRangeAcceptor<true, true>{min_bound, max_bound});
    default:
      SDB_ASSERT(false);
      return QueryBuilder::Empty();
  }
}

}  // namespace

QueryBuilder::ptr GeoFilter::PrepareSegment(const SubReader& segment,
                                            const PrepareContext& ctx) const {
  const auto& shape = options().shape;
  if (shape.empty()) {
    return QueryBuilder::Empty();
  }

  const auto& options = this->options();

  std::vector<std::string> geo_terms;
  const auto type = shape.type();
  if (type == ShapeContainer::Type::S2Point) {
    const auto& region = sdb::basics::downCast<S2PointRegion>(*shape.region());
    geo_terms = irs::geo_terms::QueryTerms(options.options, region.point(),
                                           options.prefix);
  } else {
    geo_terms =
      irs::geo_terms::QueryTerms(options.options, *shape.region(), {});
  }

  if (geo_terms.empty()) {
    return QueryBuilder::Empty();
  }

  auto cells = PrepareCells(segment, ctx, geo_terms, field_id());

  const auto boost = ctx.boost * this->GetBoost();

  switch (options.type) {
    case GeoFilterType::Intersects:
      return MakeQuery(segment, ctx, std::move(cells), boost, options,
                       GeoIntersectsAcceptor{&shape});
    case GeoFilterType::Contains:
      return MakeQuery(segment, ctx, std::move(cells), boost, options,
                       GeoContainsAcceptor{&shape});
    case GeoFilterType::IsContained:
      return MakeQuery(segment, ctx, std::move(cells), boost, options,
                       GeoIsContainedAcceptor{&shape});
  }
  SDB_ASSERT(false);
  return QueryBuilder::Empty();
}

PrepareCollector::ptr GeoFilter::MakeCollectorImpl(const Scorer* scorer,
                                                   StatsArena& stats,
                                                   uint32_t) const {
  return std::make_unique<AllCollector>(scorer, stats);
}

QueryBuilder::ptr GeoDistanceFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto& options = this->options();
  const auto& range = options.range;
  const auto lower_bound = BoundType::Unbounded != range.min_type;
  const auto upper_bound = BoundType::Unbounded != range.max_type;
  auto sub_ctx = ctx;
  sub_ctx.Boost(GetBoost());

  if (!lower_bound && !upper_bound) {
    return MatchAll(segment, sub_ctx);
  }
  if (lower_bound && upper_bound) {
    return PrepareInterval(segment, sub_ctx, field_id(), options);
  } else {
    return PrepareOpenInterval(segment, sub_ctx, field_id(), options,
                               lower_bound);
  }
}

PrepareCollector::ptr GeoDistanceFilter::MakeCollectorImpl(const Scorer* scorer,
                                                           StatsArena& stats,
                                                           uint32_t) const {
  return std::make_unique<AllCollector>(scorer, stats);
}

}  // namespace irs
