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

#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/search/geo_filter.h>

#include <absl/strings/str_cat.h>
#include <frozen/map.h>
#include <s2/s2latlng.h>
#include <s2/s2point_region.h>
#include <vpack/builder.h>
#include <vpack/iterator.h>

#include <iresearch/utils/vpack_utils.hpp>
#include <magic_enum/magic_enum.hpp>
#include <string>

#include "basics/down_cast.h"
#include "basics/exceptions.h"
#include "basics/result.h"
#include "basics/logger/logger.h"
#include "geo/geo_json.h"
#include "geo/geo_params.h"
#include <iresearch/analysis/analyzers.hpp>

namespace magic_enum {

template<>
constexpr customize::customize_t
customize::enum_name<irs::analysis::GeoJsonAnalyzer::Type>(
  irs::analysis::GeoJsonAnalyzer::Type value) noexcept {
  switch (value) {
    case irs::analysis::GeoJsonAnalyzer::Type::Shape:
      return "shape";
    case irs::analysis::GeoJsonAnalyzer::Type::Centroid:
      return "centroid";
    case irs::analysis::GeoJsonAnalyzer::Type::Point:
      return "point";
  }
  return invalid_tag;
}

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<irs::analysis::GeoJsonAnalyzer::Coding>(
  irs::analysis::GeoJsonAnalyzer::Coding value) noexcept {
  switch (value) {
    case irs::analysis::GeoJsonAnalyzer::Coding::S2Point:
      return "s2Point";
    case irs::analysis::GeoJsonAnalyzer::Coding::S2LatLngF64:
      return "s2LatLngF64";
    case irs::analysis::GeoJsonAnalyzer::Coding::S2LatLngU32:
      return "s2LatLngU32";
    case irs::analysis::GeoJsonAnalyzer::Coding::VPack:
      return "vpack";
  }
  return invalid_tag;
}

}  // namespace magic_enum

namespace irs::analysis {

namespace {

using namespace sdb;
using namespace sdb::geo;

constexpr std::string_view kOptionsParam = "options";
constexpr std::string_view kMaxCellsParam = "maxCells";
constexpr std::string_view kMinLevelParam = "minLevel";
constexpr std::string_view kMaxLevelParam = "maxLevel";
constexpr std::string_view kLevelModParam = "modLevel";
constexpr std::string_view kOptimizeForSpaceParam = "optimizeForSpace";
constexpr std::string_view kLatitudeParam = "latitude";
constexpr std::string_view kLongitudeParam = "longitude";
constexpr std::string_view kTypeParam = "type";
constexpr std::string_view kFormatParam = "format";

Result GetBool(vpack::Slice object, std::string_view name, bool& output) {
  const auto value = object.get(name);
  if (value.isNone()) {
    return {};
  }
  if (!value.isBool()) {
    return {ERROR_BAD_PARAMETER, absl::StrCat("'", name, "' should be bool.")};
  }
  output = value.getBool();
  return {};
}

Result FromVPack(vpack::Slice object, sdb::geo::GeoOptions& options) {
  if (!object.isObject()) {
    return {
      ERROR_BAD_PARAMETER,
      absl::StrCat("Failed to parse '", kOptionsParam, "', expected Object.")};
  }
  auto get = [&]<typename T>(auto name, auto min, auto max,
                             T& output) -> Result {
    const auto value = object.get(name);
    if (value.isNone()) {
      return {};
    }
    auto error = [&] {
      return Result{
        ERROR_BAD_PARAMETER,
        absl::StrCat("'", name, "' out of bounds: [", min, "..", max, "].")};
    };
    if (!value.template isNumber<T>()) {
      return error();
    }
    const auto v = value.template getNumber<T>();
    if (v < min || max < v) {
      return error();
    }
    output = v;
    return {};
  };
  auto r = get(kMaxCellsParam, sdb::geo::GeoOptions::kMinCells, sdb::geo::GeoOptions::kMaxCells,
               options.max_cells);
  if (!r.ok()) {
    return r;
  }
  r = get(kMinLevelParam, sdb::geo::GeoOptions::kMinLevel, sdb::geo::GeoOptions::kMaxLevel,
          options.min_level);
  if (!r.ok()) {
    return r;
  }
  r = get(kMaxLevelParam, sdb::geo::GeoOptions::kMinLevel, sdb::geo::GeoOptions::kMaxLevel,
          options.max_level);
  if (!r.ok()) {
    return r;
  }
  r = get(kLevelModParam, sdb::geo::GeoOptions::kMinLevelMod, sdb::geo::GeoOptions::kMaxLevelMod,
          options.level_mod);
  if (!r.ok()) {
    return r;
  }
  if (options.min_level > options.max_level) {
    return {
      ERROR_BAD_PARAMETER,
      absl::StrCat("'", kMinLevelParam, "' should be less than or equal to '",
                   kMaxLevelParam, "'.")};
  }
  return GetBool(object, kOptimizeForSpaceParam, options.optimize_for_space);
}

Result FromVPack(vpack::Slice object, GeoPointAnalyzer::Options& options) {
  SDB_ASSERT(object.isObject());
  if (auto value = object.get(kOptionsParam); !value.isNone()) {
    auto r = FromVPack(value, options.options);
    if (!r.ok()) {
      return r;
    }
  }
  auto get = [&](auto name, auto& output) -> Result {
    SDB_ASSERT(output.empty());
    const auto value = object.get(name);
    if (value.isNone()) {
      return {};
    }
    auto error = [&] {
      return Result{ERROR_BAD_PARAMETER,
                    absl::StrCat("'", name, "' should be array of strings")};
    };
    if (!value.isArray()) {
      return error();
    }
    vpack::ArrayIterator it{value};
    output.reserve(it.size());
    for (; it.valid(); it.next()) {
      auto sub = *it;
      if (!sub.isString()) {
        output = {};
        return error();
      }
      output.emplace_back(sub.stringView());
    }
    return {};
  };
  auto r = get(kLatitudeParam, options.latitude);
  if (!r.ok()) {
    return r;
  }
  r = get(kLongitudeParam, options.longitude);
  if (!r.ok()) {
    return r;
  }
  if (options.latitude.empty() != options.longitude.empty()) {
    options.latitude = {};
    options.longitude = {};
    return {ERROR_BAD_PARAMETER,
            absl::StrCat("'", kLatitudeParam, "' and '", kLongitudeParam,
                         "' should be both empty or non-empty.")};
  }
  return {};
}

Result FromVPack(vpack::Slice object, GeoJsonAnalyzer::Options& options) {
  SDB_ASSERT(object.isObject());
  auto value = object.get(kOptionsParam);
  if (!value.isNone()) {
    auto r = FromVPack(value, options.options);
    if (!r.ok()) {
      return r;
    }
  }

  value = object.get(kTypeParam);
  if (!value.isNone()) {
    auto error = [&] {
      return Result{ERROR_BAD_PARAMETER,
                    absl::StrCat("'", kTypeParam,
                                 "' can be 'shape', 'centroid', 'point'.")};
    };
    if (!value.isString()) {
      return error();
    }
    const auto v =
      magic_enum::enum_cast<GeoJsonAnalyzer::Type>(value.stringView());
    if (!v) {
      return error();
    }
    options.type = *v;
  }

  value = object.get(kFormatParam);
  if (value.isNone()) {
    return {};
  }
  auto error = [&] {
    return Result{ERROR_BAD_PARAMETER,
                  absl::StrCat("'", kFormatParam,
                               "' should be one of strings: 'vpack', "
                               "'s2Point', 's2LatLngF64', 's2LatLngU32'.")};
  };
  if (!value.isString()) {
    return error();
  }
  const auto v =
    magic_enum::enum_cast<GeoJsonAnalyzer::Coding>(value.stringView());
  if (!v) {
    return error();
  }
  options.coding = *v;
  SDB_TRACE_IF("xxxxx", Logger::SEARCH,
               options.type == GeoJsonAnalyzer::Type::Centroid &&
                 options.coding != GeoJsonAnalyzer::Coding::S2Point &&
                 options.coding != GeoJsonAnalyzer::Coding::VPack,
               "It's probably not best idea to encode centroid not as S2Point, "
               "because it's computed as S2Point.");
  return {};
}

template<typename Analyzer>
bool ParseOptionsVPack(std::string_view args,
                       typename Analyzer::Options& options) {
  const auto object = irs::view_to_slice(args);
  Result r;
  if (!object.isObject()) {
    r = {ERROR_BAD_PARAMETER,
         "Cannot parse geo analyzer definition not from Object."};
  } else {
    r = FromVPack(object, options);
  }
  if (!r.ok()) {
    SDB_WARN("xxxxx", Logger::SEARCH,
             "Failed to deserialize options from JSON while constructing '",
             irs::Type<Analyzer>::name(), "' analyzer, error: '",
             r.errorMessage(), "'");
    return false;
  }
  return true;
}

template<typename Analyzer>
bool NormalizeImpl(std::string_view args, std::string& out) {
  typename Analyzer::Options options;
  if (!ParseOptionsVPack<Analyzer>(args, options)) {
    return false;
  }
  vpack::Builder root;
  ToVPack(root, options);
  out.resize(root.slice().byteSize());
  std::memcpy(&out[0], root.slice().begin(), out.size());
  return true;
}

void ToVPack(vpack::Builder& builder, const GeoOptions& options) {
  vpack::ObjectBuilder scope{&builder, kOptionsParam};
  builder.add(kMaxCellsParam, options.max_cells);
  builder.add(kMinLevelParam, options.min_level);
  builder.add(kMaxLevelParam, options.max_level);
}

struct S2AnalyzerData {
  sdb::geo::coding::Options coding;
  Encoder encoder;
};

template<typename Data>
class GeoJsonAnalyzerImpl final : public GeoJsonAnalyzer {
 public:
  
  explicit GeoJsonAnalyzerImpl(const Options& options)
    : GeoJsonAnalyzer{options} {
    if constexpr (kIsS2) {
      SDB_ASSERT(options.coding != Coding::VPack);
      _data.coding = sdb::geo::coding::Options{std::to_underlying(options.coding)};
      // should be enough space for type + size or S2Point
      _data.encoder.Ensure(30);
    }
  }

  bool reset(std::string_view value) final {
    if constexpr (kIsS2) {
      _data.encoder.clear();
      return ResetImpl(value, _data.coding, &_data.encoder);
    } else {
      return ResetImpl(value, geo::coding::Options::Invalid, nullptr);  
    }
  }

  void prepare(GeoFilterOptionsBase& options) const final {
    options.options = _indexer.options();
    if constexpr (kIsS2) {
      switch (_type) {
        case Type::Shape:
          options.stored = StoredType::S2Region;
          break;
        case Type::Point:
          options.stored = StoredType::S2Point;
          break;
        case Type::Centroid:
          options.stored = StoredType::S2Centroid;
          break;
      }
      options.coding = _data.coding;
    } else {
      options.stored = StoredType::VPack;
    }
  }

  irs::bytes_view StoreImpl(irs::Tokenizer* ctx, vpack::Slice slice) final;

 protected:
  static constexpr bool kIsS2 = std::is_same_v<Data, S2AnalyzerData>;
  Data _data;
};

}  // namespace

void ToVPack(vpack::Builder& builder,
             const GeoPointAnalyzer::Options& options) {
  auto add_array = [&](auto name, const auto& values) {
    vpack::ArrayBuilder scope{&builder, name};
    for (const auto& value : values) {
      builder.add(value);
    }
  };
  vpack::ObjectBuilder scope{&builder};
  ToVPack(builder, options.options);
  add_array(kLatitudeParam, options.latitude);
  add_array(kLongitudeParam, options.longitude);
}

void ToVPack(vpack::Builder& builder, const GeoJsonAnalyzer::Options& options) {
  vpack::ObjectBuilder scope{&builder};
  ToVPack(builder, options.options);
  builder.add(kTypeParam, magic_enum::enum_name(options.type));
  builder.add(kFormatParam, magic_enum::enum_name(options.coding));
}

GeoAnalyzer::GeoAnalyzer(const S2RegionTermIndexer::Options& options)
  : _indexer{options} {}

bool GeoAnalyzer::next() noexcept {
  if (_begin >= _end) {
    return false;
  }
  auto& value = *_begin++;
  std::get<irs::TermAttr>(_attrs).value = {
    reinterpret_cast<const irs::byte_type*>(value.data()), value.size()};
  return true;
}

void GeoAnalyzer::reset(std::vector<std::string>&& terms) noexcept {
  _terms = std::move(terms);
  _begin = _terms.data();
  _end = _begin + _terms.size();
}

bool GeoPointAnalyzer::normalize(std::string_view args, std::string& out) {
  return NormalizeImpl<GeoPointAnalyzer>(args, out);
}

irs::analysis::Analyzer::ptr GeoPointAnalyzer::make(std::string_view args) {
  Options options;
  if (!ParseOptionsVPack<GeoPointAnalyzer>(args, options)) {
    return {};
  }
  return std::make_unique<GeoPointAnalyzer>(options);
}

irs::bytes_view GeoPointAnalyzer::store(irs::Tokenizer* ctx,
                                        vpack::Slice slice) {
  SDB_ASSERT(ctx != nullptr);
  auto& impl = basics::downCast<GeoPointAnalyzer>(*ctx);
  // reuse already parsed point
  auto& point = impl._point;
#ifdef SDB_DEV
  S2LatLng slice_point;
  SDB_ASSERT(impl.ParsePoint(slice, slice_point));
  SDB_ASSERT(slice_point == point);
#endif
  impl._builder.clear();
  sdb::geo::PointToVPack(impl._builder, point);
  return irs::slice_to_view<irs::byte_type>(impl._builder.slice());
}

GeoPointAnalyzer::GeoPointAnalyzer(const Options& options)
  : GeoAnalyzer{S2Options(options.options, true)},
    _from_array{options.latitude.empty()},
    _latitude{options.latitude},
    _longitude{options.longitude} {
  SDB_ASSERT(_latitude.empty() == _longitude.empty());
}

bool GeoPointAnalyzer::reset(std::string_view value) {
  if (!ParsePoint(view_to_slice(value), _point)) {
    return false;
  }

  GeoAnalyzer::reset(_indexer.GetIndexTerms(_point.ToPoint(), {}));
  return true;
}

void GeoPointAnalyzer::prepare(GeoFilterOptionsBase& options) const {
  options.options = _indexer.options();
  options.stored = StoredType::VPack;
}

bool GeoPointAnalyzer::ParsePoint(vpack::Slice json, S2LatLng& point) const {
  vpack::Slice lat, lng;
  if (_from_array) {
    if (!json.isArray()) {
      return false;
    }
    vpack::ArrayIterator it{json};
    if (it.size() != 2) {
      return false;
    }
    lat = *it;
    it.next();
    lng = *it;
  } else {
    lat = json.get(_latitude);
    lng = json.get(_longitude);
  }
  if (!lat.isNumber<double>() || !lng.isNumber<double>()) [[unlikely]] {
    return false;
  }
  point =
    S2LatLng::FromDegrees(lat.getNumber<double>(), lng.getNumber<double>())
      .Normalized();
  return true;
}

bool GeoJsonAnalyzer::normalize(std::string_view args, std::string& out) {
  return NormalizeImpl<GeoJsonAnalyzer>(args, out);
}

irs::analysis::Analyzer::ptr GeoJsonAnalyzer::make(std::string_view args) {
  Options options;
  if (!ParseOptionsVPack<GeoJsonAnalyzer>(args, options)) {
    return {};
  }
  if (options.coding == Coding::VPack) {
    return std::make_unique<GeoJsonAnalyzerImpl<vpack::Builder>>(options);
  }
  return std::make_unique<GeoJsonAnalyzerImpl<S2AnalyzerData>>(options);
}

GeoJsonAnalyzer::GeoJsonAnalyzer(const Options& options)
  : GeoAnalyzer{S2Options(options.options, options.type != Type::Shape)},
    _type{options.type} {}

bool GeoJsonAnalyzer::ResetImpl(std::string_view value,
                                geo::coding::Options options,
                                Encoder* encoder) {
  const auto data = view_to_slice(value);
  if (_type != Type::Point) {
    const auto type = geo::json::ParseType(data);
    const bool without_serialization =
      _type == Type::Centroid && type != geo::json::Type::Point &&
      type != geo::json::Type::Unknown;  // UNKNOWN same as isArray for us
    if (!ParseShape<Parsing::GeoJson>(
          data, _shape, _cache,
          without_serialization ? geo::coding::Options::Invalid : options,
          without_serialization ? nullptr : encoder)) {
      return false;
    }
  } else if (!ParseShape<Parsing::OnlyPoint>(data, _shape, _cache, options,
                                             encoder)) {
    return false;
  }

  // TODO(mbkkt) use normal without allocation append interface
  _centroid = _shape.centroid();
  std::vector<std::string> geo_terms;
  const auto type = _shape.type();
  if (_type == Type::Centroid || type == geo::ShapeContainer::Type::S2Point) {
    geo_terms = _indexer.GetIndexTerms(_centroid, {});
  } else {
    geo_terms = _indexer.GetIndexTerms(*_shape.region(), {});
    if (!_shape.contains(_centroid)) {
      auto terms = _indexer.GetIndexTerms(_centroid, {});
      geo_terms.insert(geo_terms.end(), std::make_move_iterator(terms.begin()),
                       std::make_move_iterator(terms.end()));
      // TODO(mbkkt) Is it ok to not deduplicate this terms?
    }
  }
  GeoAnalyzer::reset(std::move(geo_terms));
  return true;
}

irs::bytes_view GeoJsonAnalyzer::store(irs::Tokenizer* ctx,
                                        vpack::Slice slice) {
                                          SDB_ASSERT(ctx != nullptr);
  auto& impl = basics::downCast<GeoJsonAnalyzer>(*ctx);
  return impl.StoreImpl(ctx, slice);
}

template<>
irs::bytes_view GeoJsonAnalyzerImpl<vpack::Builder>::StoreImpl(irs::Tokenizer* ctx,
                                        vpack::Slice slice) {
  SDB_ASSERT(ctx != nullptr);
  auto& impl = basics::downCast<GeoJsonAnalyzerImpl<vpack::Builder>>(*ctx);
  if (impl._type == Type::Centroid) {
    SDB_ASSERT(!impl._shape.empty());
    const S2LatLng centroid{impl._shape.centroid()};
    impl._data.clear();
    sdb::geo::PointToVPack(impl._data, centroid);
    slice = impl._data.slice();
  }
  auto data =  irs::slice_to_view<irs::byte_type>(slice);
  return data;
}

template<>
irs::bytes_view GeoJsonAnalyzerImpl<S2AnalyzerData>::StoreImpl(irs::Tokenizer* ctx, vpack::Slice slice) {
  SDB_ASSERT(ctx);
  auto& impl = basics::downCast<GeoJsonAnalyzerImpl<S2AnalyzerData>>(*ctx);
  if (impl._data.encoder.length() == 0) {
    SDB_ASSERT(impl._type == Type::Centroid);
    SDB_ASSERT(impl._data.coding != geo::coding::Options::Invalid);
    impl._data.encoder.put8(0);  // removed below
    if (geo::coding::IsOptionsS2(impl._data.coding)) {
      geo::EncodePoint(impl._data.encoder, impl._centroid);
    } else {
      S2LatLng lat_lng{impl._centroid};
      geo::EncodeLatLng(impl._data.encoder, lat_lng, impl._data.coding);
    }
  }
  auto data = irs::bytes_view{
    reinterpret_cast<const irs::byte_type*>(impl._data.encoder.base()),
    impl._data.encoder.length()};
  if (impl._type != Type::Shape) {
    data = data.substr(1);  // type isn't needed if it's only points
  }
  return data;
}

void GeoAnalyzer::init() {
  REGISTER_ANALYZER_VPACK(GeoPointAnalyzer, GeoPointAnalyzer::make,
                        GeoPointAnalyzer::normalize);
  REGISTER_ANALYZER_VPACK(GeoJsonAnalyzer, GeoJsonAnalyzer::make,
                        GeoJsonAnalyzer::normalize);
}

}  // namespace irs::analysis
