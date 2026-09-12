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

#include <s2/s2point.h>
#include <s2/s2region.h>
#include <s2/s2shape.h>

#include <exception>
#include <vector>

#include "iresearch/utils/geo/coding.hpp"

namespace irs::geo {

class S2MultiPointRegion final : public S2Region {
 public:
  ~S2MultiPointRegion() final = default;

  // The result is not unit length, so you may want to normalize it.
  S2Point GetCentroid() const noexcept;

  template<typename Region>
  bool Intersects(const Region& other) const noexcept {
    for (const auto& point : _impl) {
      if (other.Contains(point)) {
        return true;
      }
    }
    return false;
  }

  S2Region* Clone() const final;
  S2Cap GetCapBound() const final;
  S2LatLngRect GetRectBound() const final;
  void GetCellUnionBound(std::vector<S2CellId>* cell_ids) const final;
  bool Contains(const S2Cell& cell) const final;
  bool MayIntersect(const S2Cell& cell) const final;
  bool Contains(const S2Point& p) const final;

  void Encode(Encoder& encoder, coding::Options options) const;
  bool Decode(Decoder& decoder, uint8_t tag);

  auto& Impl() noexcept { return _impl; }
  const auto& Impl() const noexcept { return _impl; }

 private:
  std::vector<S2Point> _impl;
};

}  // namespace irs::geo
