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

#pragma once

#include <s2/s2latlng.h>
#include <s2/s2point_region.h>

#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "geo/coding.h"
#include "geo/shape_container.h"
#include "geo/wkb.h"
#include "iresearch/utils/string.hpp"

namespace irs {

struct SourceJsonParser {
  SourceJsonParser() = default;
  SourceJsonParser(const SourceJsonParser&) noexcept {}
  SourceJsonParser(SourceJsonParser&&) noexcept {}
  SourceJsonParser& operator=(const SourceJsonParser&) = delete;
  SourceJsonParser& operator=(SourceJsonParser&&) = delete;

  bool operator()(bytes_view value, sdb::geo::ShapeContainer& shape) const {
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
    return sdb::geo::ParseShape<sdb::geo::Parsing::FromIndex>(
      json, shape, _cache, sdb::geo::coding::Options::Invalid, nullptr);
  }

 private:
  mutable simdjson::ondemand::parser _parser;
  mutable std::string _buffer;
  mutable std::vector<S2LatLng> _cache;
};

struct SourceWkbParser {
  bool operator()(bytes_view value, sdb::geo::ShapeContainer& shape) const {
    SDB_ASSERT(!value.empty());
    const std::string_view bytes{reinterpret_cast<const char*>(value.data()),
                                 value.size()};
    shape = {};
    return sdb::geo::ParseShapeWKB(bytes, shape);
  }
};

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

  bool operator()(bytes_view value, sdb::geo::ShapeContainer& shape) const {
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
                sdb::geo::coding::Options::Invalid);
    return true;
  }

 private:
  mutable simdjson::ondemand::parser _parser;
  mutable std::string _buffer;
};

struct S2ShapeParser {
  bool operator()(bytes_view value, sdb::geo::ShapeContainer& shape) const {
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
  bool operator()(bytes_view value, sdb::geo::ShapeContainer& shape) const {
    SDB_ASSERT(!value.empty());
    SDB_ASSERT(shape.type() == sdb::geo::ShapeContainer::Type::S2Point);
    Decoder decoder{value.data(), value.size()};
    S2Point point;
    const auto [r, tag] = sdb::geo::DecodePoint(decoder, point);
    SDB_ASSERT(r);
    SDB_ASSERT(decoder.avail() == 0);
    sdb::basics::downCast<S2PointRegion>(*shape.region()) =
      S2PointRegion{point};
    shape.setCoding(
      static_cast<sdb::geo::coding::Options>(sdb::geo::coding::ToPoint(tag)));
    return r;
  }
};

}  // namespace irs
