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
#include "basics/log.h"
#include "basics/memory.hpp"
#include "geo/geo_json.h"
#include "geo/geo_params.h"
#include "geo/wkb.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
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

namespace irs {
namespace {

using namespace sdb::geo;

// assume up to 2x machine epsilon in precision errors for singleton caps
constexpr auto kSingletonCapEps = 2 * std::numeric_limits<double>::epsilon();

using Disjunction = DisjunctionIterator<ScoreAdapter, ScoreMergeType::Noop>;

QueryBuilder::ptr MatchAll(const SubReader& segment,
                           const PrepareContext& ctx) {
  return irs::All{}.PrepareSegment(segment, ctx);
}

PrepareCollector::ptr MatchAllCollector(const Scorer* scorer) {
  return irs::All{}.MakeCollector(scorer);
}

PrepareCollector::ptr GeoCollector(const Scorer* scorer) {
  return std::make_unique<FieldPrepareCollector>(scorer);
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
class GeoIterator : public DocIterator {
  // Two phase iterator is heavier than a usual disjunction
  static constexpr CostAttr::Type kExtraCost = 2;

 public:
  GeoIterator(DocIterator::ptr&& approx, const ColumnReader& stored_field,
              const ColReader& col_reader, Parser& parser, Acceptor& acceptor,
              FieldProperties field, const byte_type* query_stats,
              score_t boost)
    : _stats{query_stats},
      _boost{boost},
      _field{field},
      _approx{std::move(approx)},
      _cursor{col_reader, stored_field},
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
      SDB_DEBUG(IRESEARCH, "Missing stored geo value, doc='", doc, "'");
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
  ColumnReader::BlobPointReader _cursor;
  Attributes _attrs;
  Acceptor& _acceptor;
  [[no_unique_address]] Parser _parser;
};

template<typename Parser, typename Acceptor>
DocIterator::ptr MakeIterator(typename Disjunction::Adapters&& itrs,
                              const ColumnReader& stored_field,
                              const ColReader& col_reader,
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
    stored_field, col_reader, parser, acceptor, field.meta(), query_stats,
    boost);
}

struct GeoState {
  explicit GeoState(IResourceManager& memory) noexcept : states{{memory}} {}

  const ColumnReader* stored_field{};
  const TermReader* reader{};
  ManagedVector<SeekCookie::ptr> states;
};

// Compiled GeoFilter
template<typename Parser, typename Acceptor>
class GeoQuery : public QueryBuilder {
 public:
  GeoQuery(const SubReader& segment, GeoState&& state, Parser&& parser,
           Acceptor&& acceptor, score_t boost) noexcept
    : QueryBuilder{segment},
      _state{std::move(state)},
      _parser{std::move(parser)},
      _acceptor{std::move(acceptor)},
      _boost{boost} {}

  DocIterator::ptr Execute(const ExecutionContext&,
                           const StatsBuffer& stats) const final {
    const auto& segment = _segment;

    if (!_state.reader) {
      return DocIterator::empty();
    }

    auto* field = _state.reader;

    const auto* col_reader = segment.GetColReader();
    if (!col_reader) {
      return DocIterator::empty();
    }

    typename Disjunction::Adapters itrs;
    itrs.reserve(_state.states.size());

    for (auto& entry : _state.states) {
      SDB_ASSERT(entry);
      auto it = field->Iterator(IndexFeatures::None, {.cookie = entry.get()});
      if (!it || doc_limits::eof(it->value())) [[unlikely]] {
        continue;
      }
      itrs.emplace_back(std::move(it));
    }

    return MakeIterator(std::move(itrs), *_state.stored_field, *col_reader,
                        segment, *_state.reader, stats.GetStats().data(),
                        Boost(), _parser, _acceptor);
  }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  GeoState _state;
  [[no_unique_address]] Parser _parser;
  [[no_unique_address]] Acceptor _acceptor;
  score_t _boost;
};

struct SourceJsonParser {
  SourceJsonParser() = default;
  // The parser/buffer hold only per-call scratch state, so copies and moves
  // start fresh. GeoIterator copy-constructs its Parser member per segment.
  SourceJsonParser(const SourceJsonParser&) noexcept {}
  SourceJsonParser(SourceJsonParser&&) noexcept {}
  SourceJsonParser& operator=(const SourceJsonParser&) = delete;
  SourceJsonParser& operator=(SourceJsonParser&&) = delete;

  bool operator()(bytes_view value, ShapeContainer& shape) const {
    SDB_ASSERT(!value.empty());
    const std::string_view json_str{reinterpret_cast<const char*>(value.data()),
                                    value.size()};
    _buffer.assign(json_str);
    _buffer.append(simdjson::SIMDJSON_PADDING, '\0');
    simdjson::padded_string_view padded_view{_buffer.data(), json_str.size(),
                                             _buffer.size()};
    simdjson::ondemand::document doc;
    if (_parser.iterate(padded_view).get(doc) != simdjson::SUCCESS) {
      return false;
    }
    simdjson::ondemand::value json;
    if (doc.get_value().get(json) != simdjson::SUCCESS) {
      return false;
    }
    return ParseShape<Parsing::FromIndex>(json, shape, _cache,
                                          coding::Options::Invalid, nullptr);
  }

 private:
  mutable simdjson::ondemand::parser _parser;
  mutable std::string _buffer;
  mutable std::vector<S2LatLng> _cache;
};

struct SourceWkbParser {
  bool operator()(bytes_view value, ShapeContainer& shape) const {
    SDB_ASSERT(!value.empty());
    const std::string_view bytes{reinterpret_cast<const char*>(value.data()),
                                 value.size()};
    shape = {};
    return sdb::geo::ParseShapeWKB(bytes, shape).ok();
  }
};

// Re-parses a geopoint source column (JSON text) with the same semantics as
// GeoPointAnalyzer::ParsePoint: a [lat, lng] array when both paths are empty,
// otherwise an object whose latitude/longitude live at the configured paths.
struct SourcePointParser {
  std::vector<std::string> latitude;
  std::vector<std::string> longitude;

  SourcePointParser() = default;
  SourcePointParser(std::vector<std::string> lat, std::vector<std::string> lng)
    : latitude{std::move(lat)}, longitude{std::move(lng)} {}
  SourcePointParser(const SourcePointParser& other)
    : latitude{other.latitude}, longitude{other.longitude} {}
  SourcePointParser(SourcePointParser&& other) noexcept
    : latitude{std::move(other.latitude)},
      longitude{std::move(other.longitude)} {}
  SourcePointParser& operator=(const SourcePointParser&) = delete;
  SourcePointParser& operator=(SourcePointParser&&) = delete;

  bool operator()(bytes_view value, ShapeContainer& shape) const {
    SDB_ASSERT(!value.empty());
    const std::string_view json_str{reinterpret_cast<const char*>(value.data()),
                                    value.size()};
    _buffer.assign(json_str);
    _buffer.append(simdjson::SIMDJSON_PADDING, '\0');
    simdjson::padded_string_view padded_view{_buffer.data(), json_str.size(),
                                             _buffer.size()};
    simdjson::ondemand::document doc;
    if (_parser.iterate(padded_view).get(doc) != simdjson::SUCCESS) {
      return false;
    }
    simdjson::ondemand::value json;
    if (doc.get_value().get(json) != simdjson::SUCCESS) {
      return false;
    }
    double lat, lng;
    if (latitude.empty()) {
      simdjson::ondemand::array array;
      if (json.get_array().get(array) != simdjson::SUCCESS) {
        return false;
      }
      double values[2];
      size_t i = 0;
      for (auto element : array) {
        if (i == 2) [[unlikely]] {
          return false;
        }
        if (element.get_double().get(values[i]) != simdjson::SUCCESS)
          [[unlikely]] {
          return false;
        }
        ++i;
      }
      if (i != 2) [[unlikely]] {
        return false;
      }
      lat = values[0];
      lng = values[1];
    } else {
      simdjson::ondemand::object object;
      if (json.get_object().get(object) != simdjson::SUCCESS) {
        return false;
      }
      auto find = [&object](std::span<const std::string> path,
                            double& out) -> bool {
        if (path.size() == 1) {
          return object.find_field_unordered(path.front())
                   .get_double()
                   .get(out) == simdjson::SUCCESS;
        }
        simdjson::ondemand::value current;
        if (object.find_field_unordered(path.front()).get(current) !=
            simdjson::SUCCESS) {
          return false;
        }
        for (size_t i = 1; i + 1 < path.size(); ++i) {
          simdjson::ondemand::object inner;
          if (current.get_object().get(inner) != simdjson::SUCCESS) {
            return false;
          }
          if (inner.find_field_unordered(path[i]).get(current) !=
              simdjson::SUCCESS) {
            return false;
          }
        }
        simdjson::ondemand::object inner;
        if (current.get_object().get(inner) != simdjson::SUCCESS) {
          return false;
        }
        return inner.find_field_unordered(path.back()).get_double().get(out) ==
               simdjson::SUCCESS;
      };
      if (!find(latitude, lat) || !find(longitude, lng)) {
        return false;
      }
    }
    shape.reset(S2LatLng::FromDegrees(lat, lng).Normalized().ToPoint(),
                coding::Options::Invalid);
    return true;
  }

 private:
  mutable simdjson::ondemand::parser _parser;
  mutable std::string _buffer;
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

template<typename Options, typename Acceptor>
QueryBuilder::ptr MakeQuery(const SubReader& segment, IResourceManager& manager,
                            GeoState&& state, score_t boost,
                            const Options& options, Acceptor&& acceptor) {
  switch (options.stored) {
    case StoredType::Source:
      if (options.source_is_wkb) {
        return memory::make_tracked<GeoQuery<SourceWkbParser, Acceptor>>(
          manager, segment, std::move(state), SourceWkbParser{},
          std::forward<Acceptor>(acceptor), boost);
      }
      if (options.source_is_point) {
        return memory::make_tracked<GeoQuery<SourcePointParser, Acceptor>>(
          manager, segment, std::move(state),
          SourcePointParser{options.point_latitude, options.point_longitude},
          std::forward<Acceptor>(acceptor), boost);
      }
      return memory::make_tracked<GeoQuery<SourceJsonParser, Acceptor>>(
        manager, segment, std::move(state), SourceJsonParser{},
        std::forward<Acceptor>(acceptor), boost);
    case StoredType::S2Region:
      return memory::make_tracked<GeoQuery<S2ShapeParser, Acceptor>>(
        manager, segment, std::move(state), S2ShapeParser{},
        std::forward<Acceptor>(acceptor), boost);
    case StoredType::S2Point:
    case StoredType::S2Centroid:
      return memory::make_tracked<GeoQuery<S2PointParser, Acceptor>>(
        manager, segment, std::move(state), S2PointParser{},
        std::forward<Acceptor>(acceptor), boost);
  }
  SDB_ASSERT(false);
  return QueryBuilder::Empty();
}

GeoState PrepareState(const SubReader& segment, const PrepareContext& ctx,
                      std::span<const std::string> geo_terms, irs::field_id id,
                      field_id store_field_id) {
  SDB_ASSERT(!geo_terms.empty());

  std::vector<std::string_view> sorted_terms(geo_terms.begin(),
                                             geo_terms.end());
  absl::c_sort(sorted_terms);
  SDB_ASSERT(std::unique(sorted_terms.begin(), sorted_terms.end()) ==
             sorted_terms.end());

  GeoState state{ctx.memory};

  SDB_ASSERT(irs::field_limits::valid(store_field_id));
  const auto* reader = segment.field(id);
  if (!reader) {
    return state;
  }
  const auto* stored_field = segment.Column(store_field_id);
  if (!stored_field) {
    return state;
  }
  auto terms = reader->iterator(SeekMode::NORMAL);
  if (!terms) [[unlikely]] {
    return state;
  }

  if (ctx.collector) {
    auto& collector =
      sdb::basics::downCast<FieldPrepareCollector>(*ctx.collector);
    collector.Field().Collect(*reader);
  }

  ManagedVector<SeekCookie::ptr> term_states{{ctx.memory}};
  term_states.reserve(sorted_terms.size());

  for (const auto term : sorted_terms) {
    if (!terms->seek(ViewCast<byte_type>(term))) {
      continue;
    }
    terms->read();
    term_states.emplace_back(terms->cookie());
  }

  if (term_states.empty()) {
    return state;
  }

  state.reader = reader;
  state.states = std::move(term_states);
  state.stored_field = stored_field;

  return state;
}

std::pair<S2Cap, bool> GetBound(BoundType type, S2Point origin,
                                double distance) {
  if (BoundType::Unbounded == type) {
    return {S2Cap::Full(), true};
  }

  return {(0. == distance ? FromPoint(origin) : FromPoint(origin, distance)),
          BoundType::Inclusive == type};
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
          // dist > 0: full cap minus the singleton center. Used to AND in
          // a ByColumnExistence gate on store_field_id; that's gone, so
          // rows without a stored geo value pass the Not-singleton check.
          Exclusion root;
          auto& excl = root.exclude<GeoDistanceFilter>();
          *excl.mutable_field_id() = id;
          auto& opts = *excl.mutable_options();
          opts = options;
          opts.range.min = 0;
          opts.range.min_type = BoundType::Inclusive;
          opts.range.max = 0;
          opts.range.max_type = BoundType::Inclusive;

          return root.PrepareSegment(segment, ctx);
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

  S2RegionTermIndexer indexer(options.options);

  const auto geo_terms = indexer.GetQueryTerms(bound, options.prefix);

  if (geo_terms.empty()) {
    return QueryBuilder::Empty();
  }

  auto state =
    PrepareState(segment, ctx, geo_terms, id, options.store_field_id);

  if (incl) {
    return MakeQuery(segment, ctx.memory, std::move(state), ctx.boost, options,
                     GeoDistanceAcceptor<true>{bound});
  } else {
    return MakeQuery(segment, ctx.memory, std::move(state), ctx.boost, options,
                     GeoDistanceAcceptor<false>{bound});
  }
}

PrepareCollector::ptr PrepareOpenIntervalCollector(
  const Scorer* scorer, irs::field_id id,
  const GeoDistanceFilterOptions& options, bool greater) {
  const auto& range = options.range;
  const auto& origin = options.origin;

  const auto [dist, type] =
    greater ? std::forward_as_tuple(range.min, range.min_type)
            : std::forward_as_tuple(range.max, range.max_type);

  S2Cap bound;

  if (dist < 0.) {
    bound = greater ? S2Cap::Full() : S2Cap::Empty();
  } else if (0. == dist) {
    switch (type) {
      case BoundType::Unbounded:
        SDB_ASSERT(false);
        bound = S2Cap::Empty();
        break;
      case BoundType::Inclusive:
        bound = greater ? S2Cap::Full() : FromPoint(origin);
        break;
      case BoundType::Exclusive:
        if (greater) {
          Exclusion root;
          auto& excl = root.exclude<GeoDistanceFilter>();
          *excl.mutable_field_id() = id;
          auto& opts = *excl.mutable_options();
          opts = options;
          opts.range.min = 0;
          opts.range.min_type = BoundType::Inclusive;
          opts.range.max = 0;
          opts.range.max_type = BoundType::Inclusive;

          return root.MakeCollector(scorer);
        } else {
          bound = S2Cap::Empty();
        }
        break;
    }
  } else {
    std::tie(bound, std::ignore) = GetBound(type, origin, dist);
    if (greater && bound.is_valid()) {
      bound = bound.Complement();
    }
  }

  if (bound.is_valid() && bound.is_full()) {
    return MatchAllCollector(scorer);
  }

  return GeoCollector(scorer);
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

    S2RegionTermIndexer indexer(options.options);
    const auto geo_terms = indexer.GetQueryTerms(origin, options.prefix);

    if (geo_terms.empty()) {
      return QueryBuilder::Empty();
    }

    auto state =
      PrepareState(segment, ctx, geo_terms, id, options.store_field_id);

    return MakeQuery(segment, ctx.memory, std::move(state), ctx.boost, options,
                     [bound = FromPoint(origin)](const ShapeContainer& shape) {
                       return bound.InteriorContains(shape.centroid());
                     });
  }

  auto min_bound = FromPoint(origin, range.min);
  auto max_bound = FromPoint(origin, range.max);

  if (!min_bound.is_valid() || !max_bound.is_valid()) {
    return QueryBuilder::Empty();
  }

  S2RegionTermIndexer indexer(options.options);
  S2RegionCoverer coverer(options.options);

  SDB_ASSERT(!min_bound.is_empty());
  SDB_ASSERT(!max_bound.is_empty());

  const auto ring = coverer.GetCovering(max_bound).Difference(
    coverer.GetInteriorCovering(min_bound));
  // S2CellUnion::Difference has no level cap: GetDifferenceInternal recurses
  // until cells are disjoint or fully contained, so `ring` can have cells
  // beyond options.max_level. Re-cover through GetQueryTerms so the coverer
  // enforces min/max level before GetQueryTermsForCanonicalCovering runs.
  const auto geo_terms = indexer.GetQueryTerms(ring, options.prefix);

  if (geo_terms.empty()) {
    return QueryBuilder::Empty();
  }

  auto state =
    PrepareState(segment, ctx, geo_terms, id, options.store_field_id);

  switch (size_t(min_incl) + 2 * size_t(max_incl)) {
    case 0:
      return MakeQuery(
        segment, ctx.memory, std::move(state), ctx.boost, options,
        GeoDistanceRangeAcceptor<false, false>{min_bound, max_bound});
    case 1:
      return MakeQuery(
        segment, ctx.memory, std::move(state), ctx.boost, options,
        GeoDistanceRangeAcceptor<true, false>{min_bound, max_bound});
    case 2:
      return MakeQuery(
        segment, ctx.memory, std::move(state), ctx.boost, options,
        GeoDistanceRangeAcceptor<false, true>{min_bound, max_bound});
    case 3:
      return MakeQuery(
        segment, ctx.memory, std::move(state), ctx.boost, options,
        GeoDistanceRangeAcceptor<true, true>{min_bound, max_bound});
    default:
      SDB_ASSERT(false);
      return QueryBuilder::Empty();
  }
}

PrepareCollector::ptr PrepareIntervalCollector(
  const Scorer* scorer, irs::field_id id,
  const GeoDistanceFilterOptions& options) {
  const auto& range = options.range;

  if (range.max < 0.) {
    return GeoCollector(scorer);
  } else if (range.min < 0.) {
    return PrepareOpenIntervalCollector(scorer, id, options, false);
  }

  return GeoCollector(scorer);
}

}  // namespace

QueryBuilder::ptr GeoFilter::PrepareSegment(const SubReader& segment,
                                            const PrepareContext& ctx) const {
  const auto& shape = options().shape;
  if (shape.empty()) {
    return QueryBuilder::Empty();
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
    return QueryBuilder::Empty();
  }

  auto state =
    PrepareState(segment, ctx, geo_terms, field_id(), options.store_field_id);

  const auto boost = ctx.boost * this->Boost();

  switch (options.type) {
    case GeoFilterType::Intersects:
      return MakeQuery(
        segment, ctx.memory, std::move(state), boost, options,
        [filter_shape = &shape](const ShapeContainer& indexed_shape) {
          return filter_shape->intersects(indexed_shape);
        });
    case GeoFilterType::Contains:
      return MakeQuery(
        segment, ctx.memory, std::move(state), boost, options,
        [filter_shape = &shape](const ShapeContainer& indexed_shape) {
          return filter_shape->contains(indexed_shape);
        });
    case GeoFilterType::IsContained:
      return MakeQuery(
        segment, ctx.memory, std::move(state), boost, options,
        [filter_shape = &shape](const ShapeContainer& indexed_shape) {
          return indexed_shape.contains(*filter_shape);
        });
  }
  SDB_ASSERT(false);
  return QueryBuilder::Empty();
}

PrepareCollector::ptr GeoFilter::MakeCollector(const Scorer* scorer) const {
  return GeoCollector(scorer);
}

QueryBuilder::ptr GeoDistanceFilter::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx) const {
  const auto& options = this->options();
  const auto& range = options.range;
  const auto lower_bound = BoundType::Unbounded != range.min_type;
  const auto upper_bound = BoundType::Unbounded != range.max_type;
  auto sub_ctx = ctx;
  sub_ctx.Boost(Boost());

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

PrepareCollector::ptr GeoDistanceFilter::MakeCollector(
  const Scorer* scorer) const {
  const auto& options = this->options();
  const auto& range = options.range;
  const auto lower_bound = BoundType::Unbounded != range.min_type;
  const auto upper_bound = BoundType::Unbounded != range.max_type;

  if (!lower_bound && !upper_bound) {
    return MatchAllCollector(scorer);
  }
  if (lower_bound && upper_bound) {
    return PrepareIntervalCollector(scorer, field_id(), options);
  } else {
    return PrepareOpenIntervalCollector(scorer, field_id(), options,
                                        lower_bound);
  }
}

}  // namespace irs
