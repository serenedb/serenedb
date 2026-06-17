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

#include <s2/s2cell_id.h>
#include <s2/s2point.h>
#include <s2/s2region.h>

#include <memory>
#include <vector>

#include "basics/result.h"
#include "geo/coding.h"

class S2Polyline;
class S2LatLngRect;
class S2Polygon;
class S2RegionCoverer;
class S2PointRegion;

namespace sdb::geo {

/// Thin wrapper around S2Region objects combined with
/// a type and helper methods to do intersect and contains
/// checks between all supported region types
class ShapeContainer final {
 public:
  enum class Type : uint8_t {
    Empty = 0,
    S2Point = 1,
    S2Polyline = 2,
    S2Polygon = 3,
    S2Multipoint = 4,
    S2Multipolyline = 5,
  };

  ShapeContainer(const ShapeContainer& other) = delete;
  ShapeContainer& operator=(const ShapeContainer& other) = delete;

  ShapeContainer() noexcept = default;
  ShapeContainer(ShapeContainer&& other) noexcept
    : _data{std::move(other._data)},
      _type{std::exchange(other._type, Type::Empty)},
      _options{std::exchange(other._options, coding::Options::Invalid)} {}
  ShapeContainer& operator=(ShapeContainer&& other) noexcept {
    std::swap(_data, other._data);
    std::swap(_type, other._type);
    std::swap(_options, other._options);
    return *this;
  }

  bool empty() const noexcept { return _type == Type::Empty; }

  S2Point centroid() const noexcept;

  bool contains(const S2Point& other) const;
  bool contains(const ShapeContainer& other) const;

  bool intersects(const ShapeContainer& other) const;

  bool equals(const ShapeContainer& other) const;

  double distanceFromCentroid(const S2Point& other) const noexcept;

  S2Region* region() noexcept { return _data.get(); }
  const S2Region* region() const noexcept { return _data.get(); }
  Type type() const noexcept { return _type; }

  void reset(std::unique_ptr<S2Region> region, Type type,
             coding::Options options = coding::Options::Invalid) noexcept;
  void reset(S2Point point, coding::Options options = coding::Options::Invalid);

  // Using s2 Encode/Decode
  void Encode(Encoder& encoder, coding::Options options) const;
  bool Decode(Decoder& decoder, std::vector<S2Point>& cache);

  void setCoding(coding::Options options) noexcept { _options = options; }

 private:
  template<ShapeContainer::Type Type, typename T>
  void decodeImpl(Decoder& decoder);

  std::unique_ptr<S2Region> _data;
  Type _type{Type::Empty};
  coding::Options _options{coding::Options::Invalid};
};

}  // namespace sdb::geo
