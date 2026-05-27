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
#include "basics/errors.h"
#include "basics/logger/logger.h"
#include "basics/memory.hpp"
#include "geo/geo_json.h"
#include "geo/geo_params.h"
#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/columnstore/read_context.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/column_collector.hpp"
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

using Disjunction = DisjunctionIterator<ScoreAdapter, ScoreMergeType::Noop>;

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
class GeoIterator : public DocIterator {
  // Two phase iterator is heavier than a usual disjunction
  static constexpr CostAttr::Type kExtraCost = 2;

 public:
  // Stored geo bytes now live in the columnstore as a typed BLOB column. The
  // iterator owns a BlobPointReader (per-doc fetch with row-group caching)
  // plus a one-row Vector<BLOB> that FetchRow lands the value into. The
  // existing parser API still expects bytes_view, so Accept() reads the
  // resulting string_t and forwards its bytes unchanged.
  GeoIterator(DocIterator::ptr&& approx,
              const columnstore::ColumnReader& stored_field,
              const columnstore::Reader& cs_reader, Parser& parser,
              Acceptor& acceptor, FieldProperties field,
              const byte_type* query_stats, score_t boost)
    : _stats{query_stats},
      _boost{boost},
      _field{field},
      _approx{std::move(approx)},
      _cursor{cs_reader, stored_field},
      _acceptor{acceptor},
      _parser{parser} {
    std::get<CostAttr>(_attrs).reset(
      [&]() noexcept { return kExtraCost * CostAttr::extract(*_approx); });

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

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  doc_id_t advance() final {
    while (true) {
      const auto doc = _approx->advance();
      if (doc_limits::eof(doc) || Accept(doc)) {
        return _doc = doc;
      }
    }
  }

  doc_id_t seek(doc_id_t target) final {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    const auto doc = _approx->seek(target);
    if (doc_limits::eof(doc) || Accept(doc)) {
      return _doc = doc;
    }
    return advance();
  }

  doc_id_t LazySeek(doc_id_t target) final {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }
    const auto doc = _approx->LazySeek(target);
    if (target != doc) {
      return doc;
    }
    if (doc_limits::eof(doc) || Accept(doc)) {
      return _doc = doc;
    }
    return doc + 1;
  }

  uint32_t count() final { return CountImpl(*this); }

 private:
  bool Accept(doc_id_t doc) {
    // Per-doc point fetch via cached cursor: same row group as the
    // previous doc reuses its pinned ColumnSegment + ColumnFetchState.
    // Empty span = row stored as null (analyzer didn't populate StoreAttr)
    // OR analyzer wrote zero bytes -- either way nothing to match.
    const auto bytes = _cursor.FetchDoc(doc);
    if (bytes.empty()) {
      SDB_DEBUG("xxxxx", sdb::Logger::IRESEARCH,
                "Missing stored geo value, doc='", doc, "'");
      return false;
    }
    return _parser(bytes, _shape) && _acceptor(_shape);
  }

  using Attributes = std::tuple<CostAttr>;

  const byte_type* _stats = nullptr;
  score_t _boost = {};
  FieldProperties _field;

  ShapeContainer _shape;
  DocIterator::ptr _approx;
  columnstore::ColumnReader::BlobPointReader _cursor;
  Attributes _attrs;
  Acceptor& _acceptor;
  [[no_unique_address]] Parser _parser;
};

template<typename Parser, typename Acceptor>
DocIterator::ptr MakeIterator(typename Disjunction::Adapters&& itrs,
                              const columnstore::ColumnReader& stored_field,
                              const columnstore::Reader& cs_reader,
                              const SubReader& reader, const TermReader& field,
                              const byte_type* query_stats, score_t boost,
                              Parser& parser, Acceptor& acceptor) {
  if (itrs.empty()) [[unlikely]] {
    return DocIterator::empty();
  }

  return memory::make_managed<GeoIterator<Parser, Acceptor>>(
    // TODO(mbkkt) by_terms? LazyBitsetIterator faster than disjunction
    MakeDisjunction<Disjunction>(
      {}, static_cast<irs::doc_id_t>(reader.docs_count()), std::move(itrs)),
    stored_field, cs_reader, parser, acceptor, field.meta(), query_stats,
    boost);
}

// Cached per reader query state
struct GeoState {
  explicit GeoState(IResourceManager& memory) noexcept : states{{memory}} {}

  // Columnstore reader for the BLOB column carrying the analyzer's per-doc
  // StoreAttr bytes. Resolved per-segment in PrepareStates via
  // SubReader::Column.
  const columnstore::ColumnReader* stored_field{};

  // Reader using for iterate over the terms
  const TermReader* reader{};

  // Geo term states
  ManagedVector<SeekCookie::ptr> states;
};

using GeoStates = StatesCacheImpl<GeoState>;

// Compiled GeoFilter
template<typename Parser, typename Acceptor>
class GeoQuery : public Filter::Query {
 public:
  GeoQuery(GeoStates&& states, bstring&& stats, Parser&& parser,
           Acceptor&& acceptor, score_t boost) noexcept
    : _states{std::move(states)},
      _stats{std::move(stats)},
      _parser{std::move(parser)},
      _acceptor{std::move(acceptor)},
      _boost{boost} {}

  DocIterator::ptr execute(const ExecutionContext& ctx) const final {
    auto& segment = ctx.segment;
    const auto* state = _states.find(segment);

    if (!state) {
      return DocIterator::empty();
    }

    auto* field = state->reader;
    SDB_ASSERT(field);

    const auto* cs_reader = segment.CsReader();
    if (!cs_reader) {
      return DocIterator::empty();
    }

    typename Disjunction::Adapters itrs;
    itrs.reserve(state->states.size());

    for (auto& entry : state->states) {
      SDB_ASSERT(entry);
      auto it = field->Iterator(IndexFeatures::None, {.cookie = entry.get()});
      if (!it || doc_limits::eof(it->value())) [[unlikely]] {
        continue;
      }
      itrs.emplace_back(std::move(it));
    }

    return MakeIterator(std::move(itrs), *state->stored_field, *cs_reader,
                        segment, *state->reader, _stats.c_str(), Boost(),
                        _parser, _acceptor);
  }

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  GeoStates _states;
  bstring _stats;
  [[no_unique_address]] Parser _parser;
  [[no_unique_address]] Acceptor _acceptor;
  score_t _boost;
};

struct VPackParser {
  bool operator()(bytes_view value, ShapeContainer& shape) const {
    SDB_ASSERT(!value.empty());
    return ParseShape<Parsing::FromIndex>(view_to_slice(value), shape, _cache,
                                          coding::Options::Invalid, nullptr);
  }

 private:
  mutable std::vector<S2LatLng> _cache;
};

struct S2ShapeParser {
  bool operator()(bytes_view value, ShapeContainer& shape) const {
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
  bool operator()(bytes_view value, ShapeContainer& shape) const {
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

template<typename Acceptor>
Filter::Query::ptr MakeQuery(IResourceManager& manager, GeoStates&& states,
                             bstring&& stats, score_t boost, StoredType stored,
                             Acceptor&& acceptor) {
  switch (stored) {
    case StoredType::VPack:
      return memory::make_tracked<GeoQuery<VPackParser, Acceptor>>(
        manager, std::move(states), std::move(stats), VPackParser{},
        std::forward<Acceptor>(acceptor), boost);
    case StoredType::S2Region:
      return memory::make_tracked<GeoQuery<S2ShapeParser, Acceptor>>(
        manager, std::move(states), std::move(stats), S2ShapeParser{},
        std::forward<Acceptor>(acceptor), boost);
    case StoredType::S2Point:
    case StoredType::S2Centroid:
      return memory::make_tracked<GeoQuery<S2PointParser, Acceptor>>(
        manager, std::move(states), std::move(stats), S2PointParser{},
        std::forward<Acceptor>(acceptor), boost);
  }
  SDB_ASSERT(false);
  return Filter::Query::empty();
}

template<typename Acceptor>
class GeoBufferImpl final : public Filter::ScoredBuffer {
 public:
  template<typename Options>
  GeoBufferImpl(const PrepareContext& ctx, std::string_view field,
                const Options& options, std::vector<std::string> geo_terms,
                Acceptor acceptor, score_t boost)
    : ScoredBuffer{ctx, boost},
      _field{field},
      _store_field_id{options.store_field_id},
      _stored{options.stored},
      _acceptor{std::move(acceptor)},
      _memory{&ctx.memory},
      _field_stats{ctx.scorer},
      _states{ctx.memory, ctx.index.size()},
      _stats(GetStatsSize(ctx.scorer), 0),
      _sorted_terms{std::move(geo_terms)},
      _term_states{{ctx.memory}} {
    absl::c_sort(_sorted_terms);
    SDB_ASSERT(std::unique(_sorted_terms.begin(), _sorted_terms.end()) ==
               _sorted_terms.end());
    SDB_ASSERT(_store_field_id != 0);
  }

  void PrepareSegment(const SubReader& segment) final {
    const auto* reader = segment.field(_field);
    if (!reader) {
      return;
    }
    const auto* stored_field = segment.Column(_store_field_id);
    if (!stored_field) {
      return;
    }
    auto terms = reader->iterator(SeekMode::NORMAL);
    if (!terms) [[unlikely]] {
      return;
    }

    _field_stats.collect(segment, *reader);

    _term_states.clear();
    _term_states.reserve(_sorted_terms.size());
    for (const auto& term : _sorted_terms) {
      if (!terms->seek(ViewCast<byte_type>(std::string_view{term}))) {
        continue;
      }
      terms->read();
      _term_states.emplace_back(terms->cookie());
    }
    if (_term_states.empty()) {
      return;
    }

    auto& state = _states.insert(segment);
    state.reader = reader;
    state.states = std::move(_term_states);
    state.stored_field = stored_field;
  }

  void Merge(PrepareBuffer&& other) final {
    auto& rhs = sdb::basics::downCast<GeoBufferImpl>(other);
    _field_stats.collect(std::move(rhs._field_stats));
    _states.Merge(std::move(rhs._states));
  }

  bool Empty() const noexcept final { return _states.empty(); }

  Filter::Query::ptr Compile(const PrepareContext& ctx) && final {
    _field_stats.finish(_stats.data());
    return MakeQuery(ctx.memory, std::move(_states), std::move(_stats), _boost,
                     _stored, std::move(_acceptor));
  }

 private:
  std::string _field;
  field_id _store_field_id;
  StoredType _stored;
  Acceptor _acceptor;
  IResourceManager* _memory;
  FieldCollectors _field_stats;
  GeoStates _states;
  bstring _stats;
  std::vector<std::string> _sorted_terms;
  ManagedVector<SeekCookie::ptr> _term_states;
};

template<typename Options, typename Acceptor>
std::unique_ptr<Filter::PrepareBuffer> MakeGeoBuffer(
  const PrepareContext& ctx, std::string_view field, const Options& options,
  std::vector<std::string> geo_terms, Acceptor&& acceptor, score_t boost) {
  return std::make_unique<GeoBufferImpl<std::decay_t<Acceptor>>>(
    ctx, field, options, std::move(geo_terms), std::forward<Acceptor>(acceptor),
    boost);
}

std::pair<S2Cap, bool> GetBound(BoundType type, S2Point origin,
                                double distance) {
  if (BoundType::Unbounded == type) {
    return {S2Cap::Full(), true};
  }

  return {(0. == distance ? FromPoint(origin) : FromPoint(origin, distance)),
          BoundType::Inclusive == type};
}

std::unique_ptr<Filter::PrepareBuffer> CreateOpenIntervalBuffer(
  const PrepareContext& ctx, std::string_view field,
  const GeoDistanceFilterOptions& options, bool greater, score_t boost) {
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
          return std::make_unique<Filter::EmptyBuffer>();
        }

        incl = true;
        break;
      case BoundType::Exclusive:
        if (greater) {
          auto root = std::make_unique<And>();
          root->boost(boost);
          auto& excl = root->add<Not>().filter<GeoDistanceFilter>();
          *excl.mutable_field() = field;
          auto& opts = *excl.mutable_options();
          opts = options;
          opts.range.min = 0;
          opts.range.min_type = BoundType::Inclusive;
          opts.range.max = 0;
          opts.range.max_type = BoundType::Inclusive;

          return root->CreateBuffer(ctx);
        } else {
          bound = S2Cap::Empty();
        }

        incl = false;
        break;
    }
  } else {
    std::tie(bound, incl) = GetBound(type, origin, dist);

    if (!bound.is_valid()) {
      return std::make_unique<Filter::EmptyBuffer>();
    }

    if (greater) {
      bound = bound.Complement();
    }
  }

  SDB_ASSERT(bound.is_valid());

  if (bound.is_full()) {
    All all;
    all.boost(boost);
    return all.CreateBuffer(ctx);
  }

  if (bound.is_empty()) {
    return std::make_unique<Filter::EmptyBuffer>();
  }

  S2RegionTermIndexer indexer(options.options);

  auto geo_terms = indexer.GetQueryTerms(bound, options.prefix);

  if (geo_terms.empty()) {
    return std::make_unique<Filter::EmptyBuffer>();
  }

  if (incl) {
    return MakeGeoBuffer(ctx, field, options, std::move(geo_terms),
                         GeoDistanceAcceptor<true>{bound}, boost);
  } else {
    return MakeGeoBuffer(ctx, field, options, std::move(geo_terms),
                         GeoDistanceAcceptor<false>{bound}, boost);
  }
}

std::unique_ptr<Filter::PrepareBuffer> CreateIntervalBuffer(
  const PrepareContext& ctx, std::string_view field,
  const GeoDistanceFilterOptions& options, score_t boost) {
  const auto& range = options.range;
  SDB_ASSERT(BoundType::Unbounded != range.min_type);
  SDB_ASSERT(BoundType::Unbounded != range.max_type);

  if (range.max < 0.) {
    return std::make_unique<Filter::EmptyBuffer>();
  } else if (range.min < 0.) {
    return CreateOpenIntervalBuffer(ctx, field, options, false, boost);
  }

  const bool min_incl = range.min_type == BoundType::Inclusive;
  const bool max_incl = range.max_type == BoundType::Inclusive;

  if (math::ApproxEquals(range.min, range.max)) {
    if (!min_incl || !max_incl) {
      return std::make_unique<Filter::EmptyBuffer>();
    }
  } else if (range.min > range.max) {
    return std::make_unique<Filter::EmptyBuffer>();
  }

  const auto& origin = options.origin;

  if (0. == range.max && 0. == range.min) {
    SDB_ASSERT(min_incl);
    SDB_ASSERT(max_incl);

    S2RegionTermIndexer indexer(options.options);
    auto geo_terms = indexer.GetQueryTerms(origin, options.prefix);

    if (geo_terms.empty()) {
      return std::make_unique<Filter::EmptyBuffer>();
    }

    return MakeGeoBuffer(
      ctx, field, options, std::move(geo_terms),
      [bound = FromPoint(origin)](const ShapeContainer& shape) {
        return bound.InteriorContains(shape.centroid());
      },
      boost);
  }

  auto min_bound = FromPoint(origin, range.min);
  auto max_bound = FromPoint(origin, range.max);

  if (!min_bound.is_valid() || !max_bound.is_valid()) {
    return std::make_unique<Filter::EmptyBuffer>();
  }

  S2RegionTermIndexer indexer(options.options);
  S2RegionCoverer coverer(options.options);

  SDB_ASSERT(!min_bound.is_empty());
  SDB_ASSERT(!max_bound.is_empty());

  const auto ring = coverer.GetCovering(max_bound).Difference(
    coverer.GetInteriorCovering(min_bound));
  auto geo_terms = indexer.GetQueryTerms(ring, options.prefix);

  if (geo_terms.empty()) {
    return std::make_unique<Filter::EmptyBuffer>();
  }

  switch (size_t(min_incl) + 2 * size_t(max_incl)) {
    case 0:
      return MakeGeoBuffer(
        ctx, field, options, std::move(geo_terms),
        GeoDistanceRangeAcceptor<false, false>{min_bound, max_bound}, boost);
    case 1:
      return MakeGeoBuffer(
        ctx, field, options, std::move(geo_terms),
        GeoDistanceRangeAcceptor<true, false>{min_bound, max_bound}, boost);
    case 2:
      return MakeGeoBuffer(
        ctx, field, options, std::move(geo_terms),
        GeoDistanceRangeAcceptor<false, true>{min_bound, max_bound}, boost);
    case 3:
      return MakeGeoBuffer(
        ctx, field, options, std::move(geo_terms),
        GeoDistanceRangeAcceptor<true, true>{min_bound, max_bound}, boost);
    default:
      SDB_ASSERT(false);
      return std::make_unique<Filter::EmptyBuffer>();
  }
}

}  // namespace

std::unique_ptr<Filter::PrepareBuffer> GeoFilter::CreateBuffer(
  const PrepareContext& ctx) const {
#ifdef SDB_DEV
  SDB_ASSERT(!_prepared.exchange(true, std::memory_order_relaxed));
#endif
  auto& shape = const_cast<ShapeContainer&>(options().shape);
  if (shape.empty()) {
    return std::make_unique<EmptyBuffer>();
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
    return std::make_unique<EmptyBuffer>();
  }

  switch (options.type) {
    case GeoFilterType::Intersects:
      return MakeGeoBuffer(
        ctx, field(), options, std::move(geo_terms),
        [filter_shape = std::move(shape)](const ShapeContainer& indexed_shape) {
          return filter_shape.intersects(indexed_shape);
        },
        Boost());
    case GeoFilterType::Contains:
      return MakeGeoBuffer(
        ctx, field(), options, std::move(geo_terms),
        [filter_shape = std::move(shape)](const ShapeContainer& indexed_shape) {
          return filter_shape.contains(indexed_shape);
        },
        Boost());
    case GeoFilterType::IsContained:
      return MakeGeoBuffer(
        ctx, field(), options, std::move(geo_terms),
        [filter_shape = std::move(shape)](const ShapeContainer& indexed_shape) {
          return indexed_shape.contains(filter_shape);
        },
        Boost());
  }
  SDB_ASSERT(false);
  return std::make_unique<EmptyBuffer>();
}

Filter::Query::ptr GeoFilter::prepare(const PrepareContext& ctx) const {
  auto buf = CreateBuffer(ctx);
  for (const auto& segment : ctx.index) {
    buf->PrepareSegment(segment);
  }
  return std::move(*buf).Compile(ctx);
}

std::unique_ptr<Filter::PrepareBuffer> GeoDistanceFilter::CreateBuffer(
  const PrepareContext& ctx) const {
  const auto& options = this->options();
  const auto& range = options.range;
  const auto lower_bound = BoundType::Unbounded != range.min_type;
  const auto upper_bound = BoundType::Unbounded != range.max_type;

  if (!lower_bound && !upper_bound) {
    All all;
    all.boost(Boost());
    return all.CreateBuffer(ctx);
  }
  if (lower_bound && upper_bound) {
    return CreateIntervalBuffer(ctx, field(), options, Boost());
  }
  return CreateOpenIntervalBuffer(ctx, field(), options, lower_bound, Boost());
}

Filter::Query::ptr GeoDistanceFilter::prepare(const PrepareContext& ctx) const {
  auto buf = CreateBuffer(ctx);
  for (const auto& segment : ctx.index) {
    buf->PrepareSegment(segment);
  }
  return std::move(*buf).Compile(ctx);
}

}  // namespace irs
