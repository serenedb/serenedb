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

#include "geo/s2/multi_point_region.h"

#include <s2/s2cap.h>
#include <s2/s2cell.h>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>
#include <s2/s2latlng_rect_bounder.h>

#include "basics/exceptions.h"

namespace sdb::geo {

S2Point S2MultiPointRegion::GetCentroid() const noexcept {
  // copied from s2: S2Point GetCentroid(const S2Shape& shape);
  S2Point centroid;
  for (const auto& point : _impl) {
    centroid += point;
  }
  return centroid;
}

S2Region* S2MultiPointRegion::Clone() const {
  SDB_THROW(ERROR_NOT_IMPLEMENTED);
}

S2Cap S2MultiPointRegion::GetCapBound() const {
  return GetRectBound().GetCapBound();
}

S2LatLngRect S2MultiPointRegion::GetRectBound() const {
  S2LatLngRectBounder bounder;
  for (const auto& point : _impl) {
    bounder.AddPoint(point);
  }
  // TODO(mbkkt) Maybe cache it?
  return bounder.GetBound();
}

void S2MultiPointRegion::GetCellUnionBound(
  std::vector<S2CellId>* cell_ids) const {
  SDB_ASSERT(cell_ids);
  cell_ids->clear();
  cell_ids->reserve(4 * _impl.size());
  for (const auto& point : _impl) {
    S2CellId(point).AppendVertexNeighbors(S2::kMaxCellLevel - 1, cell_ids);
  }
}

bool S2MultiPointRegion::Contains(const S2Cell& cell) const { return false; }

bool S2MultiPointRegion::MayIntersect(const S2Cell& cell) const {
  for (const auto& point : _impl) {
    if (cell.Contains(point)) {
      return true;
    }
  }
  return false;
}

bool S2MultiPointRegion::Contains(const S2Point& p) const {
  for (const auto& point : _impl) {
    if (point == p) {
      return true;
    }
  }
  return false;
}

using namespace coding;

void S2MultiPointRegion::Encode(Encoder& encoder, Options options) const {
  SDB_ASSERT(IsOptionsS2(options));
  SDB_ASSERT(options != Options::S2PointShapeCompact ||
               options != Options::S2PointRegionCompact,
             "All vertices should be serialized at once.");
  SDB_ASSERT(encoder.avail() >= sizeof(uint8_t) + Varint::kMax64);
  encoder.put8(ToTag(Type::MultiPoint, options));
  encoder.put_varint64(_impl.size());
  EncodeVertices(encoder, _impl);
}

bool S2MultiPointRegion::Decode(Decoder& decoder, uint8_t tag) {
  uint64_t size = 0;
  if (!decoder.get_varint64(&size)) {
    return false;
  }
  _impl.resize(size);
  if (!DecodeVertices(decoder, _impl, tag)) {
    return false;
  }
  return true;
}

}  // namespace sdb::geo
