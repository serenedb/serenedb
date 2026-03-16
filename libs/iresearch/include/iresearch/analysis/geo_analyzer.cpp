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

#include "iresearch/analysis/geo_analyzer.hpp"

#include <absl/strings/str_cat.h>
#include <frozen/map.h>
#include <s2/s2latlng.h>
#include <s2/s2point_region.h>
#include <vpack/builder.h>
#include <vpack/iterator.h>
#include <vpack/serializer.h>

#include <magic_enum/magic_enum.hpp>
#include <string>

#include "basics/down_cast.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/result.h"
#include "geo/geo_json.h"
#include "geo/geo_params.h"
#include "iresearch/analysis/analyzers.hpp"
#include "iresearch/search/geo_filter.h"
#include "iresearch/utils/vpack_utils.hpp"

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

constexpr std::string_view kLatitudeParam = "latitude";
constexpr std::string_view kLongitudeParam = "longitude";

Result FromVPack(vpack::Slice object, GeoPointAnalyzer::Options& options) {
  SDB_ASSERT(object.isObject());
  auto r = vpack::ReadObjectNothrow(object, options,
                                    {
                                      .skip_unknown = true,
                                      .strict = false,
                                    });
  if (!r.ok()) {
    return {ERROR_BAD_PARAMETER,
            absl::StrCat("Failed to parse definition, ", r.errorMessage())};
  }

  r = options.options.Validate();
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
  auto r = vpack::ReadObjectNothrow(object, options,
                                    {
                                      .skip_unknown = true,
                                      .strict = false,
                                    });
  if (!r.ok()) {
    return {ERROR_BAD_PARAMETER,
            absl::StrCat("Failed to parse definition, ", r.errorMessage())};
  }

  r = options.options.Validate();
  if (!r.ok()) {
    return r;
  }

  return {};
}

template<typename Analyzer>
bool ParseOptionsVPack(std::string_view args,
                       typename Analyzer::Options& options) {
  const auto object = irs::view_to_slice(args);
  Result r;
  if (!object.isObject()) {
    r = {ERROR_BAD_PARAMETER, "Analyzer definition is not an Object"};
  } else {
    r = FromVPack(object, options);
  }
  if (!r.ok()) {
    SDB_WARN("xxxxx", Logger::SEARCH, "Failed to read options for '",
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
  vpack::WriteObject(root, options);
  out.resize(root.slice().byteSize());
  std::memcpy(&out[0], root.slice().begin(), out.size());
  return true;
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
      _data.coding =
        sdb::geo::coding::Options{std::to_underlying(options.coding)};
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
      type != geo::json::Type::Unknown;  // treat UNKNOWN as Array
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

  // TODO(mbkkt) try to avoid allocations in append
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
      // TODO(mbkkt) do we need terms deduplication?
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
irs::bytes_view GeoJsonAnalyzerImpl<vpack::Builder>::StoreImpl(
  irs::Tokenizer* ctx, vpack::Slice slice) {
  SDB_ASSERT(ctx != nullptr);
  auto& impl = basics::downCast<GeoJsonAnalyzerImpl<vpack::Builder>>(*ctx);
  if (impl._type == Type::Centroid) {
    SDB_ASSERT(!impl._shape.empty());
    const S2LatLng centroid{impl._shape.centroid()};
    impl._data.clear();
    sdb::geo::PointToVPack(impl._data, centroid);
    slice = impl._data.slice();
  }
  auto data = irs::slice_to_view<irs::byte_type>(slice);
  return data;
}

template<>
irs::bytes_view GeoJsonAnalyzerImpl<S2AnalyzerData>::StoreImpl(
  irs::Tokenizer* ctx, vpack::Slice slice) {
  SDB_ASSERT(ctx);
  auto& impl = basics::downCast<GeoJsonAnalyzerImpl<S2AnalyzerData>>(*ctx);
  if (impl._data.encoder.length() == 0) {
    SDB_ASSERT(impl._type == Type::Centroid);
    SDB_ASSERT(impl._data.coding != geo::coding::Options::Invalid);
    impl._data.encoder.put8(0);
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
    // For points we do not need type
    data = data.substr(1);
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
