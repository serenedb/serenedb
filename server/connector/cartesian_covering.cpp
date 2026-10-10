////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "connector/cartesian_covering.h"

#include <spatial/modules/boost/boost_ops.hpp>

namespace sdb::connector {
namespace {

namespace bg = duckdb::bg;

using Rectangle = bg::model::box<duckdb::BoostPoint>;
using Segment = bg::model::segment<duckdb::BoostPoint>;

constexpr int kWidenSteps = 4;

double Widen(double value, double direction) {
  for (int step = 0; step < kWidenSteps; ++step) {
    value = std::nextafter(value, direction);
  }
  return value;
}

bool Intersects(const duckdb::BoostSingle& part, const Rectangle& rectangle) {
  return boost::variant2::visit(
    [&](const auto& shape) { return bg::intersects(shape, rectangle); }, part);
}

template<typename Ring>
bool RingTouches(const Ring& ring, const Rectangle& rectangle) {
  for (size_t i = 1; i < ring.size(); ++i) {
    if (bg::intersects(Segment{ring[i - 1], ring[i]}, rectangle)) {
      return true;
    }
  }
  return false;
}

bool Covers(const duckdb::BoostPolygon& polygon, const Rectangle& rectangle) {
  if (RingTouches(polygon.outer(), rectangle)) {
    return false;
  }
  for (const auto& ring : polygon.inners()) {
    if (RingTouches(ring, rectangle)) {
      return false;
    }
  }
  duckdb::BoostPoint center;
  bg::centroid(rectangle, center);
  return bg::within(center, polygon);
}

bool Covers(const duckdb::BoostSingle& part, const Rectangle& rectangle) {
  if (const auto* polygon =
        boost::variant2::get_if<duckdb::BoostPolygon>(&part)) {
    return Covers(*polygon, rectangle);
  }
  if (const auto* polygons =
        boost::variant2::get_if<duckdb::BoostMultiPolygon>(&part)) {
    for (const auto& polygon : *polygons) {
      if (Covers(polygon, rectangle)) {
        return true;
      }
    }
  }
  return false;
}

}  // namespace

std::vector<irs::curve::Cell> CoverCartesianGeometry(
  const duckdb::BoostGeometry& geometry, const irs::curve::Options& options) {
  if (duckdb::IsEmptyGeometry(geometry)) {
    return {};
  }
  Rectangle bounds;
  bg::assign_inverse(bounds);
  bool finite = true;
  for (const auto& part : geometry.Parts()) {
    boost::variant2::visit(
      [&](const auto& shape) {
        if (bg::is_empty(shape)) {
          return;
        }
        bg::for_each_point(shape, [&](const auto& point) {
          finite &= std::isfinite(point.x()) && std::isfinite(point.y());
        });
        Rectangle box;
        bg::envelope(shape, box);
        bg::expand(bounds, box);
      },
      part);
  }
  const auto x0 = bounds.min_corner().x();
  const auto y0 = bounds.min_corner().y();
  const auto x1 = bounds.max_corner().x();
  const auto y1 = bounds.max_corner().y();
  if (!finite ||
      std::max({std::abs(x0), std::abs(y0), std::abs(x1), std::abs(y1)}) >
        1e100 ||
      !duckdb::IsValid(geometry)) {
    return {irs::curve::Cell{}};
  }
  const irs::curve::Box box{
    {irs::curve::EncodeDouble(x0), irs::curve::EncodeDouble(y0)},
    {irs::curve::EncodeDouble(x1), irs::curve::EncodeDouble(y1)}};
  const bool areal = !geometry.IsCollection();
  return irs::curve::Cover(
    options,
    [&](const irs::curve::Cell& cell) {
      const auto relation = irs::curve::Classify(cell, box, 2);
      if (relation == irs::curve::Relation::Outside) {
        return irs::curve::Relation::Outside;
      }
      if (cell.min[0] <= box.min[0] && cell.Max(0) >= box.max[0] &&
          cell.min[1] <= box.min[1] && cell.Max(1) >= box.max[1]) {
        return irs::curve::Relation::Boundary;
      }
      const auto lower = [&](uint32_t axis) {
        return Widen(
          irs::curve::DecodeDouble(std::max(cell.min[axis], box.min[axis])),
          -std::numeric_limits<double>::infinity());
      };
      const auto upper = [&](uint32_t axis) {
        return Widen(
          irs::curve::DecodeDouble(std::min(cell.Max(axis), box.max[axis])),
          std::numeric_limits<double>::infinity());
      };
      const Rectangle rectangle{{lower(0), lower(1)}, {upper(0), upper(1)}};
      bool intersects = false;
      for (const auto& part : geometry.Parts()) {
        if (Intersects(part, rectangle)) {
          intersects = true;
          break;
        }
      }
      if (!intersects) {
        return irs::curve::Relation::Outside;
      }
      if (relation == irs::curve::Relation::Inside && areal &&
          Covers(geometry.Single(), rectangle)) {
        return irs::curve::Relation::Inside;
      }
      return irs::curve::Relation::Boundary;
    },
    [&](const irs::curve::Cell& cell) {
      long double size = 1;
      for (uint32_t axis = 0; axis < 2; ++axis) {
        const long double lo =
          irs::curve::DecodeDouble(std::max(cell.min[axis], box.min[axis]));
        const long double hi =
          irs::curve::DecodeDouble(std::min(cell.Max(axis), box.max[axis]));
        if (box.min[axis] != box.max[axis]) {
          size *= hi - lo;
        }
      }
      return size;
    });
}

}  // namespace sdb::connector
