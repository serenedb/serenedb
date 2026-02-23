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

#include "iresearch/search/geo_filter.h"

#include <s2/s2cap.h>
#include <s2/s2earth.h>
#include <s2/s2point_region.h>

#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/logger/logger.h"
#include "basics/memory.hpp"
#include "geo/geo_json.h"
#include "geo/geo_params.h"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/column_existence_filter.hpp"
#include "iresearch/search/make_disjunction.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/vpack_utils.hpp"

namespace irs {
namespace {

using namespace sdb::geo;

// assume up to 2x machine epsilon in precision errors for singleton caps
constexpr auto kSingletonCapEps = 2 * std::numeric_limits<double>::epsilon();

using Disjunction =
  irs::DisjunctionIterator<ScoreAdapter, ScoreMergeType::Noop>;

// Return a filter matching all documents with a given geo field
irs::Filter::Query::ptr MatchAll(const irs::PrepareContext& ctx,
                                 std::string_view field) {
  // Return everything we've stored
  irs::ByColumnExistence filter;
  *filter.mutable_field() = field;

  return filter.prepare(ctx);
}

// Returns singleton S2Cap that tolerates precision errors
// TODO(mbkkt) Probably remove it
inline S2Cap FromPoint(const S2Point& origin) noexcept {
  return S2Cap{origin, S1Angle::Radians(kSingletonCapEps)};
}

inline S2Cap FromPoint(S2Point origin, double distance) noexcept {
  return {origin, S1Angle::Radians(MetersToRadians(distance))};
}

struct S2PointParser;

template<typename Parser, typename Acceptor>
class GeoIterator : public irs::DocIterator {
  // Two phase iterator is heavier than a usual disjunction
  static constexpr irs::CostAttr::Type kExtraCost = 2;

 public:
  GeoIterator(DocIterator::ptr&& approx, DocIterator::ptr&& column_it,
              Parser& parser, Acceptor& acceptor, FieldProperties field,
              const irs::byte_type* query_stats, irs::score_t boost)
    : _stats{query_stats},
      _boost{boost},
      _field{field},
      _approx{std::move(approx)},
      _column_it{std::move(column_it)},
      _stored_value{irs::get<irs::PayAttr>(*_column_it)},
      _acceptor{acceptor},
      _parser{parser} {
    std::get<irs::AttributePtr<irs::DocAttr>>(_attrs) =
      irs::GetMutable<irs::DocAttr>(_approx.get());

    std::get<irs::CostAttr>(_attrs).reset(
      [&]() noexcept { return kExtraCost * irs::CostAttr::extract(*_approx); });

    if constexpr (std::is_same_v<std::decay_t<Parser>, S2PointParser>) {
      // random, stub value but it should be unit length because assert
      _shape.reset(S2Point{1, 0, 0});
    }
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    SDB_ASSERT(ctx.scorer);
    return ctx.scorer->PrepareScorer({
      .segment = *ctx.segment,
      .field = _field,
      .doc_attrs = *this,
      .fetcher = ctx.fetcher,
      .stats = _stats,
      .boost = _boost,
    });
  }

  std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                      uint64_t* mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final {
    return FillBlockImpl(*this, min, max, mask, score, match);
  }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    CollectImpl(*this, scorer, fetcher, collector);
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  irs::doc_id_t value() const noexcept final {
    return std::get<irs::AttributePtr<irs::DocAttr>>(_attrs).ptr->value;
  }

  irs::doc_id_t advance() final {
    irs::doc_id_t value;
    while (!irs::doc_limits::eof(value = _approx->advance())) {
      if (Accept()) {
        return value;
      }
    }
    return irs::doc_limits::eof();
  }

  irs::doc_id_t seek(irs::doc_id_t target) final {
    auto* doc = std::get<irs::AttributePtr<irs::DocAttr>>(_attrs).ptr;

    if (target <= doc->value) {
      return doc->value;
    }

    if (irs::doc_limits::eof(_approx->seek(target))) {
      return irs::doc_limits::eof();
    }

    if (!Accept()) {
      return advance();
    }
    return doc->value;
  }

  uint32_t count() final { return CountImpl(*this); }

 private:
  bool Accept() {
    auto* doc = std::get<irs::AttributePtr<irs::DocAttr>>(_attrs).ptr;
    SDB_ASSERT(_column_it->value() < doc->value);

    if (doc->value != _column_it->seek(doc->value) ||
        _stored_value->value.empty()) {
      SDB_DEBUG("xxxxx", sdb::Logger::IRESEARCH,
                "Missing stored geo value, doc='", doc->value, "'");
      return false;
    }
    return _parser(_stored_value->value, _shape) && _acceptor(_shape);
  }

  using Attributes = std::tuple<irs::AttributePtr<DocAttr>, CostAttr>;

  const byte_type* _stats = nullptr;
  irs::score_t _boost = {};
  FieldProperties _field;

  ShapeContainer _shape;
  irs::DocIterator::ptr _approx;
  irs::DocIterator::ptr _column_it;
  const irs::PayAttr* _stored_value;
  Attributes _attrs;
  Acceptor& _acceptor;
  [[no_unique_address]] Parser _parser;
};

template<typename Parser, typename Acceptor>
irs::DocIterator::ptr MakeIterator(typename Disjunction::Adapters&& itrs,
                                   irs::DocIterator::ptr&& column_it,
                                   const irs::SubReader& reader,
                                   const irs::TermReader& field,
                                   const irs::byte_type* query_stats,
                                   irs::score_t boost, Parser& parser,
                                   Acceptor& acceptor) {
  if (itrs.empty() || !column_it) [[unlikely]] {
    return irs::DocIterator::empty();
  }

  return irs::memory::make_managed<GeoIterator<Parser, Acceptor>>(
    // TODO(mbkkt) by_terms? LazyBitsetIterator faster than disjunction
    irs::MakeDisjunction<Disjunction>({}, std::move(itrs)),
    std::move(column_it), parser, acceptor, field.meta(), query_stats, boost);
}

// Cached per reader query state
struct GeoState {
  explicit GeoState(irs::IResourceManager& memory) noexcept
    : states{{memory}} {}

  // Corresponding stored field
  const irs::ColumnReader* stored_field{};

  // Reader using for iterate over the terms
  const irs::TermReader* reader{};

  // Geo term states
  irs::ManagedVector<irs::SeekCookie::ptr> states;
};

using GeoStates = irs::StatesCache<GeoState>;

// Compiled GeoFilter
template<typename Parser, typename Acceptor>
class GeoQuery : public irs::Filter::Query {
 public:
  GeoQuery(GeoStates&& states, irs::bstring&& stats, Parser&& parser,
           Acceptor&& acceptor, irs::score_t boost) noexcept
    : _states{std::move(states)},
      _stats{std::move(stats)},
      _parser{std::move(parser)},
      _acceptor{std::move(acceptor)},
      _boost{boost} {}

  irs::DocIterator::ptr execute(const irs::ExecutionContext& ctx) const final {
    auto& segment = ctx.segment;
    const auto* state = _states.find(segment);

    if (!state) {
      return irs::DocIterator::empty();
    }

    auto* field = state->reader;
    SDB_ASSERT(field);

    typename Disjunction::Adapters itrs;
    itrs.reserve(state->states.size());

    for (auto& entry : state->states) {
      SDB_ASSERT(entry);
      auto& it = itrs.emplace_back(
        field->Iterator(IndexFeatures::None, {.cookie = entry.get()}));

      if (!it || irs::doc_limits::eof(it.value())) [[unlikely]] {
        itrs.pop_back();

        continue;
      }
    }

    auto column_it = state->stored_field->iterator(irs::ColumnHint::Normal);

    return MakeIterator(std::move(itrs), std::move(column_it), segment,
                        *state->reader, _stats.c_str(), Boost(), _parser,
                        _acceptor);
  }

  void visit(const irs::SubReader&, irs::PreparedStateVisitor&,
             irs::score_t) const final {}

  irs::score_t Boost() const noexcept final { return _boost; }

 private:
  GeoStates _states;
  irs::bstring _stats;
  [[no_unique_address]] Parser _parser;
  [[no_unique_address]] Acceptor _acceptor;
  irs::score_t _boost;
};

struct VPackParser {
  bool operator()(irs::bytes_view value, ShapeContainer& shape) const {
    SDB_ASSERT(!value.empty());
    return ParseShape<Parsing::FromIndex>(view_to_slice(value), shape, _cache,
                                          coding::Options::Invalid, nullptr);
  }

 private:
  mutable std::vector<S2LatLng> _cache;
};

struct S2ShapeParser {
  bool operator()(irs::bytes_view value, ShapeContainer& shape) const {
    SDB_ASSERT(!value.empty());
    Decoder decoder{value.data(), value.size()};
    auto r = shape.Decode(decoder, _cache);
    SDB_ASSERT(r);
    SDB_ASSERT(decoder.avail() == 0);
    return r;
  }

 private:
  mutable std::vector<S2Point> _cache;
};

struct S2PointParser {
  bool operator()(irs::bytes_view value, ShapeContainer& shape) const {
    SDB_ASSERT(!value.empty());
    SDB_ASSERT(shape.type() == ShapeContainer::Type::S2Point);
    Decoder decoder{value.data(), value.size()};
    S2Point point;
    const auto [r, tag] = DecodePoint(decoder, point);
    SDB_ASSERT(r);
    SDB_ASSERT(decoder.avail() == 0);
    sdb::basics::downCast<S2PointRegion>(*shape.region()) =
      S2PointRegion{point};
    shape.setCoding(static_cast<coding::Options>(coding::ToPoint(tag)));
    return r;
  }
};

// TODO(mbkkt) S2LaxShapeParser

template<bool MinIncl, bool MaxIncl>
struct GeoDistanceRangeAcceptor {
  S2Cap min;
  S2Cap max;

  bool operator()(const ShapeContainer& shape) const {
    const auto point = shape.centroid();

    return !(MinIncl ? min.InteriorContains(point) : min.Contains(point)) &&
           (MaxIncl ? max.Contains(point) : max.InteriorContains(point));
  }
};

template<bool Incl>
struct GeoDistanceAcceptor {
  S2Cap filter;

  bool operator()(const ShapeContainer& shape) const {
    const auto point = shape.centroid();

    return Incl ? filter.Contains(point) : filter.InteriorContains(point);
  }
};

template<typename Options, typename Acceptor>
irs::Filter::Query::ptr MakeQuery(irs::IResourceManager& manager,
                                  GeoStates&& states, irs::bstring&& stats,
                                  irs::score_t boost, const Options& options,
                                  Acceptor&& acceptor) {
  switch (options.stored) {
    case StoredType::VPack:
      return irs::memory::make_tracked<GeoQuery<VPackParser, Acceptor>>(
        manager, std::move(states), std::move(stats), VPackParser{},
        std::forward<Acceptor>(acceptor), boost);
    case StoredType::S2Region:
      return irs::memory::make_tracked<GeoQuery<S2ShapeParser, Acceptor>>(
        manager, std::move(states), std::move(stats), S2ShapeParser{},
        std::forward<Acceptor>(acceptor), boost);
    case StoredType::S2Point:
    case StoredType::S2Centroid:
      return irs::memory::make_tracked<GeoQuery<S2PointParser, Acceptor>>(
        manager, std::move(states), std::move(stats), S2PointParser{},
        std::forward<Acceptor>(acceptor), boost);
  }
  SDB_ASSERT(false);
  return irs::Filter::Query::empty();
}

std::pair<GeoStates, irs::bstring> PrepareStates(
  const irs::PrepareContext& ctx, std::span<const std::string> geo_terms,
  std::string_view field) {
  SDB_ASSERT(!geo_terms.empty());

  std::vector<std::string_view> sorted_terms(geo_terms.begin(),
                                             geo_terms.end());
  std::sort(sorted_terms.begin(), sorted_terms.end());
  SDB_ASSERT(std::unique(sorted_terms.begin(), sorted_terms.end()) ==
             sorted_terms.end());

  std::pair<GeoStates, irs::bstring> res{
    std::piecewise_construct,
    std::forward_as_tuple(ctx.memory, ctx.index.size()),
    std::forward_as_tuple(GetStatsSize(ctx.scorer), 0)};

  const auto size = sorted_terms.size();
  irs::FieldCollectors field_stats{ctx.scorer};
  irs::ManagedVector<irs::SeekCookie::ptr> term_states{{ctx.memory}};

  for (const auto& segment : ctx.index) {
    const auto* reader = segment.field(field);
    if (!reader) {
      continue;
    }
    const auto* stored_field = segment.column(field);
    if (!stored_field) {
      continue;
    }
    auto terms = reader->iterator(irs::SeekMode::NORMAL);
    if (!terms) [[unlikely]] {
      continue;
    }

    field_stats.collect(segment, *reader);
    term_states.reserve(size);

    for (const auto term : sorted_terms) {
      if (!terms->seek(irs::ViewCast<irs::byte_type>(term))) {
        continue;
      }
      terms->read();
      term_states.emplace_back(terms->cookie());
    }

    if (term_states.empty()) {
      continue;
    }

    auto& state = res.first.insert(segment);
    state.reader = reader;
    state.states = std::move(term_states);
    state.stored_field = stored_field;
    term_states.clear();
  }

  field_stats.finish(const_cast<irs::byte_type*>(res.second.data()));

  return res;
}

std::pair<S2Cap, bool> GetBound(irs::BoundType type, S2Point origin,
                                double distance) {
  if (irs::BoundType::Unbounded == type) {
    return {S2Cap::Full(), true};
  }

  return {(0. == distance ? FromPoint(origin) : FromPoint(origin, distance)),
          irs::BoundType::Inclusive == type};
}

irs::Filter::Query::ptr PrepareOpenInterval(
  const irs::PrepareContext& ctx, std::string_view field,
  const GeoDistanceFilterOptions& options, bool greater) {
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
      case irs::BoundType::Unbounded:
        incl = false;
        SDB_ASSERT(false);
        break;
      case irs::BoundType::Inclusive:
        bound = greater ? S2Cap::Full() : FromPoint(origin);

        if (!bound.is_valid()) {
          return irs::Filter::Query::empty();
        }

        incl = true;
        break;
      case irs::BoundType::Exclusive:
        if (greater) {
          // a full cap without a center
          irs::And root;
          {
            auto& column = root.add<irs::ByColumnExistence>();
            *column.mutable_field() = field;
          }
          {
            auto& excl = root.add<irs::Not>().filter<GeoDistanceFilter>();
            *excl.mutable_field() = field;
            auto& opts = *excl.mutable_options();
            opts = options;
            opts.range.min = 0;
            opts.range.min_type = irs::BoundType::Inclusive;
            opts.range.max = 0;
            opts.range.max_type = irs::BoundType::Inclusive;
          }

          return root.prepare(ctx);
        } else {
          bound = S2Cap::Empty();
        }

        incl = false;
        break;
    }
  } else {
    std::tie(bound, incl) = GetBound(type, origin, dist);

    if (!bound.is_valid()) {
      return irs::Filter::Query::empty();
    }

    if (greater) {
      bound = bound.Complement();
    }
  }

  SDB_ASSERT(bound.is_valid());

  if (bound.is_full()) {
    return MatchAll(ctx, field);
  }

  if (bound.is_empty()) {
    return irs::Filter::Query::empty();
  }

  S2RegionTermIndexer indexer(options.options);

  const auto geo_terms = indexer.GetQueryTerms(bound, options.prefix);

  if (geo_terms.empty()) {
    return irs::Filter::Query::empty();
  }

  auto [states, stats] = PrepareStates(ctx, geo_terms, field);

  if (incl) {
    return MakeQuery(ctx.memory, std::move(states), std::move(stats), ctx.boost,
                     options, GeoDistanceAcceptor<true>{bound});
  } else {
    return MakeQuery(ctx.memory, std::move(states), std::move(stats), ctx.boost,
                     options, GeoDistanceAcceptor<false>{bound});
  }
}

irs::Filter::Query::ptr PrepareInterval(
  const irs::PrepareContext& ctx, std::string_view field,
  const GeoDistanceFilterOptions& options) {
  const auto& range = options.range;
  SDB_ASSERT(irs::BoundType::Unbounded != range.min_type);
  SDB_ASSERT(irs::BoundType::Unbounded != range.max_type);

  if (range.max < 0.) {
    return irs::Filter::Query::empty();
  } else if (range.min < 0.) {
    return PrepareOpenInterval(ctx, field, options, false);
  }

  const bool min_incl = range.min_type == irs::BoundType::Inclusive;
  const bool max_incl = range.max_type == irs::BoundType::Inclusive;

  if (irs::math::ApproxEquals(range.min, range.max)) {
    if (!min_incl || !max_incl) {
      return irs::Filter::Query::empty();
    }
  } else if (range.min > range.max) {
    return irs::Filter::Query::empty();
  }

  const auto& origin = options.origin;

  if (0. == range.max && 0. == range.min) {
    SDB_ASSERT(min_incl);
    SDB_ASSERT(max_incl);

    S2RegionTermIndexer indexer(options.options);
    const auto geo_terms = indexer.GetQueryTerms(origin, options.prefix);

    if (geo_terms.empty()) {
      return irs::Filter::Query::empty();
    }

    auto [states, stats] = PrepareStates(ctx, geo_terms, field);

    return MakeQuery(ctx.memory, std::move(states), std::move(stats), ctx.boost,
                     options,
                     [bound = FromPoint(origin)](const ShapeContainer& shape) {
                       return bound.InteriorContains(shape.centroid());
                     });
  }

  auto min_bound = FromPoint(origin, range.min);
  auto max_bound = FromPoint(origin, range.max);

  if (!min_bound.is_valid() || !max_bound.is_valid()) {
    return irs::Filter::Query::empty();
  }

  S2RegionTermIndexer indexer(options.options);
  S2RegionCoverer coverer(options.options);

  SDB_ASSERT(!min_bound.is_empty());
  SDB_ASSERT(!max_bound.is_empty());

  const auto ring = coverer.GetCovering(max_bound).Difference(
    coverer.GetInteriorCovering(min_bound));
  const auto geo_terms =
    indexer.GetQueryTermsForCanonicalCovering(ring, options.prefix);

  if (geo_terms.empty()) {
    return irs::Filter::Query::empty();
  }

  auto [states, stats] = PrepareStates(ctx, geo_terms, field);

  switch (size_t(min_incl) + 2 * size_t(max_incl)) {
    case 0:
      return MakeQuery(
        ctx.memory, std::move(states), std::move(stats), ctx.boost, options,
        GeoDistanceRangeAcceptor<false, false>{min_bound, max_bound});
    case 1:
      return MakeQuery(
        ctx.memory, std::move(states), std::move(stats), ctx.boost, options,
        GeoDistanceRangeAcceptor<true, false>{min_bound, max_bound});
    case 2:
      return MakeQuery(
        ctx.memory, std::move(states), std::move(stats), ctx.boost, options,
        GeoDistanceRangeAcceptor<false, true>{min_bound, max_bound});
    case 3:
      return MakeQuery(
        ctx.memory, std::move(states), std::move(stats), ctx.boost, options,
        GeoDistanceRangeAcceptor<true, true>{min_bound, max_bound});
    default:
      SDB_ASSERT(false);
      return irs::Filter::Query::empty();
  }
}

}  // namespace

irs::Filter::Query::ptr GeoFilter::prepare(
  const irs::PrepareContext& ctx) const {
  auto& shape = const_cast<ShapeContainer&>(options().shape);
  if (shape.empty()) {
    return irs::Filter::Query::empty();
  }

  const auto& options = this->options();

  S2RegionTermIndexer indexer{options.options};
  std::vector<std::string> geo_terms;
  const auto type = shape.type();
  if (type == ShapeContainer::Type::S2Point) {
    const auto& region = sdb::basics::downCast<S2PointRegion>(*shape.region());
    geo_terms = indexer.GetQueryTerms(region.point(), options.prefix);
  } else {
    geo_terms = indexer.GetQueryTerms(*shape.region(), {});
  }

  if (geo_terms.empty()) {
    return irs::Filter::Query::empty();
  }

  auto [states, stats] = PrepareStates(ctx, geo_terms, field());

  const auto boost = ctx.boost * this->Boost();

  switch (options.type) {
    case GeoFilterType::Intersects:
      return MakeQuery(
        ctx.memory, std::move(states), std::move(stats), boost, options,
        [filter_shape = std::move(shape)](const ShapeContainer& indexed_shape) {
          return filter_shape.intersects(indexed_shape);
        });
    case GeoFilterType::Contains:
      return MakeQuery(
        ctx.memory, std::move(states), std::move(stats), boost, options,
        [filter_shape = std::move(shape)](const ShapeContainer& indexed_shape) {
          return filter_shape.contains(indexed_shape);
        });
    case GeoFilterType::IsContained:
      return MakeQuery(
        ctx.memory, std::move(states), std::move(stats), boost, options,
        [filter_shape = std::move(shape)](const ShapeContainer& indexed_shape) {
          return indexed_shape.contains(filter_shape);
        });
  }
  SDB_ASSERT(false);
  return irs::Filter::Query::empty();
}

irs::Filter::Query::ptr GeoDistanceFilter::prepare(
  const irs::PrepareContext& ctx) const {
  const auto& options = this->options();
  const auto& range = options.range;
  const auto lower_bound = irs::BoundType::Unbounded != range.min_type;
  const auto upper_bound = irs::BoundType::Unbounded != range.max_type;

  auto sub_ctx = ctx.Boost(Boost());

  if (!lower_bound && !upper_bound) {
    return MatchAll(sub_ctx, field());
  }
  if (lower_bound && upper_bound) {
    return PrepareInterval(sub_ctx, field(), options);
  } else {
    return PrepareOpenInterval(sub_ctx, field(), options, lower_bound);
  }
}

}  // namespace irs
