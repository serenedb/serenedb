////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2022 ArangoDB GmbH, Cologne, Germany
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

#include "geo/s2/multi_polyline_region.h"

#include <s2/s2cap.h>
#include <s2/s2latlng.h>
#include <s2/s2latlng_rect.h>
#include <s2/s2latlng_rect_bounder.h>
#include <s2/s2polyline_measures.h>

#include "basics/exceptions.h"

namespace sdb::geo {

S2Point S2MultiPolylineRegion::GetCentroid() const noexcept {
  // copied from s2: S2Point GetCentroid(const S2Shape& shape);
  S2Point centroid;
  for (const auto& polyline : _impl) {
    centroid += S2::GetCentroid(polyline.vertices_span());
  }
  return centroid;
}

S2Region* S2MultiPolylineRegion::Clone() const {
  SDB_THROW(ERROR_NOT_IMPLEMENTED);
}

S2Cap S2MultiPolylineRegion::GetCapBound() const {
  return GetRectBound().GetCapBound();
}

S2LatLngRect S2MultiPolylineRegion::GetRectBound() const {
  S2LatLngRectBounder bounder;
  for (const auto& polyline : _impl) {
    for (const auto& point : polyline.vertices_span()) {
      bounder.AddPoint(point);
    }
  }
  // TODO(mbkkt) Maybe cache it?
  return bounder.GetBound();
}

void S2MultiPolylineRegion::GetCellUnionBound(
  std::vector<S2CellId>* cell_ids) const {
  GetRectBound().GetCellUnionBound(cell_ids);
}

bool S2MultiPolylineRegion::Contains(const S2Cell& /*cell*/) const {
  return false;
}

bool S2MultiPolylineRegion::MayIntersect(const S2Cell& cell) const {
  for (const auto& polyline : _impl) {
    if (polyline.MayIntersect(cell)) {
      return true;
    }
  }
  return false;
}

bool S2MultiPolylineRegion::Contains(const S2Point& /*p*/) const {
  // S2MultiPolylineRegion doesn't have a Contains(S2Point) method, because
  // "containment" isn't numerically well-defined except at the polyline
  // vertices.
  return false;
}

using namespace coding;
/// num_lines <= 1:
///   varint(line[0]_num_vertices << 1 | 0)
///     line[0]_vertices_data
/// else:
///   varint(num_lines << 1 | 1)
///     varint(line[i]_num_vertices)
///     line[i]_vertices_data

void S2MultiPolylineRegion::Encode(Encoder& encoder, Options options) const {
  SDB_ASSERT(IsOptionsS2(options));
  SDB_ASSERT(options != Options::S2PointShapeCompact ||
               options != Options::S2PointRegionCompact,
             "All vertices should be serialized at once.");
  SDB_ASSERT(encoder.avail() >= sizeof(uint8_t) + Varint::kMax64);
  encoder.put8(ToTag(Type::MultiPolyline, options));
  switch (const auto num_polylines = _impl.size()) {
    case 0: {
      encoder.put_varint64(0);
    } break;
    case 1: {
      const auto vertices = _impl[0].vertices_span();
      SDB_ASSERT(!vertices.empty());
      encoder.put_varint64(vertices.size() * 2);
      EncodeVertices(encoder, vertices);
    } break;
    default: {
      encoder.Ensure((1 + num_polylines) * Varint::kMax64 +
                     2 * ToSize(options));
      encoder.put_varint64(num_polylines * 2 + 1);
      for (size_t i = 0; i != num_polylines; ++i) {
        const auto vertices = _impl[i].vertices_span();
        encoder.put_varint64(vertices.size());
        EncodeVertices(encoder, vertices);
      }
    } break;
  }
}

bool S2MultiPolylineRegion::Decode(Decoder& decoder, uint8_t tag,
                                   std::vector<S2Point>& cache) {
  _impl.clear();
  uint64_t size = 0;
  if (!decoder.get_varint64(&size)) {
    return false;
  }
  if (size == 0) {
    return true;
  } else if (size % 2 == 0) {
    cache.resize(static_cast<size_t>(size / 2));
    if (!DecodeVertices(decoder, cache, tag)) {
      return false;
    }
    _impl.emplace_back(cache, S2Debug::DISABLE);
    return true;
  }
  const auto num_polylines = static_cast<size_t>(size / 2);
  SDB_ASSERT(num_polylines >= 2);
  _impl.reserve(num_polylines);
  for (uint64_t i = 0; i != num_polylines; ++i) {
    if (!decoder.get_varint64(&size)) {
      return false;
    }
    cache.resize(size);
    if (!DecodeVertices(decoder, cache, tag)) {
      return false;
    }
    _impl.emplace_back(cache, S2Debug::DISABLE);
  }
  return true;
}

}  // namespace sdb::geo
