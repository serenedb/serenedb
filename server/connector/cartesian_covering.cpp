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

#include <absl/algorithm/container.h>

#include <bit>
#include <spatial/modules/boost/boost_ops.hpp>
#include <spatial/modules/boost/boost_serde.hpp>

#include "connector/curve_index.h"

namespace sdb::connector {
namespace {

namespace bg = duckdb::bg;

using Rectangle = bg::model::box<duckdb::BoostPoint>;
using Segment = bg::model::segment<duckdb::BoostPoint>;

inline constexpr uint32_t kWidenSteps = 4;
inline constexpr size_t kClipBudgetBase = size_t{1} << 16;
inline constexpr size_t kClipBudgetPerEdge = 16;

double Widen(double value, bool up) {
  for (uint32_t step = 0; step < kWidenSteps; ++step) {
    if (value == 0) {
      value = up ? std::numeric_limits<double>::denorm_min()
                 : -std::numeric_limits<double>::denorm_min();
    } else {
      value = std::bit_cast<double>(std::bit_cast<uint64_t>(value) +
                                    ((value > 0) == up ? 1 : -1));
    }
  }
  return value;
}

class ShapeClassifier {
 public:
  ShapeClassifier(const duckdb::BoostGeometry& geometry,
                  const irs::curve::Box& box)
    : _box{box}, _areal{!geometry.IsCollection()} {
    for (const auto& part : geometry.Parts()) {
      boost::variant2::visit([&](const auto& shape) { Add(shape); }, part);
    }
    _clips.emplace_back(0, 0);
    _clip_budget = kClipBudgetBase + kClipBudgetPerEdge * _edges.size();
    double magnitude = 1;
    for (uint32_t axis = 0; axis < 2; ++axis) {
      magnitude = std::max({
        magnitude,
        std::abs(irs::curve::DecodeDouble(box.min[axis])),
        std::abs(irs::curve::DecodeDouble(box.max[axis])),
      });
    }
    _slack = 4 * std::numeric_limits<double>::epsilon() * magnitude;
  }

  irs::curve::Relation operator()(const irs::curve::Cell& cell,
                                  uint32_t& context) {
    const auto relation = irs::curve::Classify(cell, _box, 2);
    if (relation == irs::curve::Relation::Outside) {
      return irs::curve::Relation::Outside;
    }
    if (cell.min[0] <= _box.min[0] && cell.Max(0) >= _box.max[0] &&
        cell.min[1] <= _box.min[1] && cell.Max(1) >= _box.max[1]) {
      return irs::curve::Relation::Boundary;
    }
    const auto lower = [&](uint32_t axis) {
      return Widen(
        irs::curve::DecodeDouble(std::max(cell.min[axis], _box.min[axis])),
        false);
    };
    const auto upper = [&](uint32_t axis) {
      return Widen(
        irs::curve::DecodeDouble(std::min(cell.Max(axis), _box.max[axis])),
        true);
    };
    const Rectangle rectangle{{lower(0), lower(1)}, {upper(0), upper(1)}};
    const auto begin = _clipped.size();
    if (context == 0) {
      for (uint32_t edge = 0; edge < _edges.size(); ++edge) {
        if (Touches(_edges[edge], rectangle)) {
          _clipped.emplace_back(edge);
        }
      }
    } else {
      const auto clip = _clips[context];
      for (auto i = clip.begin; i < clip.end; ++i) {
        if (Touches(_edges[_clipped[i]], rectangle)) {
          _clipped.emplace_back(_clipped[i]);
        }
      }
    }
    const auto end = _clipped.size();
    if (begin == end) {
      if (!absl::c_any_of(_polygons, [&](const auto* polygon) {
            return Locate(rectangle.min_corner(), *polygon) >= 0;
          })) {
        return irs::curve::Relation::Outside;
      }
      if (relation == irs::curve::Relation::Inside && _areal) {
        duckdb::BoostPoint center;
        bg::centroid(rectangle, center);
        if (absl::c_any_of(_polygons, [&](const auto* polygon) {
              return Locate(center, *polygon) == 1;
            })) {
          return irs::curve::Relation::Inside;
        }
      }
    }
    if (end > _clip_budget) {
      _clipped.resize(begin);
      context = 0;
      return irs::curve::Relation::Boundary;
    }
    context = static_cast<uint32_t>(_clips.size());
    _clips.emplace_back(begin, end);
    return irs::curve::Relation::Boundary;
  }

 private:
  struct Edge {
    Segment segment;
    bool point;
  };

  struct Clip {
    size_t begin;
    size_t end;
  };

  bool Touches(const Edge& edge, const Rectangle& rectangle) const {
    const auto& a = edge.segment.first;
    const auto& b = edge.segment.second;
    if (std::max(a.x(), b.x()) < rectangle.min_corner().x() - _slack ||
        std::min(a.x(), b.x()) > rectangle.max_corner().x() + _slack ||
        std::max(a.y(), b.y()) < rectangle.min_corner().y() - _slack ||
        std::min(a.y(), b.y()) > rectangle.max_corner().y() + _slack) {
      return false;
    }
    return edge.point ? bg::covered_by(a, rectangle)
                      : bg::intersects(edge.segment, rectangle);
  }

  template<typename Ring>
  int32_t Locate(const duckdb::BoostPoint& point, const Ring& ring) const {
    using Winding = bg::strategy::within::cartesian_winding<>;
    if (ring.size() < 4) {
      return -1;
    }
    Winding::state_type state;
    for (size_t i = 1; i < ring.size(); ++i) {
      const auto& a = ring[i - 1];
      const auto& b = ring[i];
      if (std::max(a.x(), b.x()) < point.x() - _slack ||
          std::min(a.x(), b.x()) > point.x() + _slack) {
        continue;
      }
      if (!Winding::apply(point, a, b, state)) {
        break;
      }
    }
    return Winding::result(state);
  }

  int32_t Locate(const duckdb::BoostPoint& point,
                 const duckdb::BoostPolygon& polygon) const {
    const auto code = Locate(point, polygon.outer());
    if (code == 1) {
      for (const auto& ring : polygon.inners()) {
        if (const auto inner = Locate(point, ring); inner != -1) {
          return -inner;
        }
      }
    }
    return code;
  }

  void Add(const duckdb::BoostPoint& point) {
    _edges.emplace_back(Segment{point, point}, true);
  }

  template<typename Path>
  void AddPath(const Path& path) {
    if (path.size() == 1) {
      Add(path.front());
    }
    for (size_t i = 1; i < path.size(); ++i) {
      _edges.emplace_back(Segment{path[i - 1], path[i]}, false);
    }
  }

  void Add(const duckdb::BoostLinestring& line) { AddPath(line); }

  void Add(const duckdb::BoostPolygon& polygon) {
    AddPath(polygon.outer());
    for (const auto& ring : polygon.inners()) {
      AddPath(ring);
    }
    _polygons.emplace_back(&polygon);
  }

  template<typename Multi>
  void Add(const Multi& shapes) {
    for (const auto& shape : shapes) {
      Add(shape);
    }
  }

  const irs::curve::Box _box;
  const bool _areal;
  double _slack;
  size_t _clip_budget;
  std::vector<Edge> _edges;
  std::vector<const duckdb::BoostPolygon*> _polygons;
  std::vector<uint32_t> _clipped;
  std::vector<Clip> _clips;
};

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
    .min = {irs::curve::EncodeDouble(x0), irs::curve::EncodeDouble(y0)},
    .max = {irs::curve::EncodeDouble(x1), irs::curve::EncodeDouble(y1)},
  };
  return irs::curve::Cover(
    options, ShapeClassifier{geometry, box}, [&](const irs::curve::Cell& cell) {
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

std::vector<irs::curve::Cell> CoverCartesian(
  std::string_view wkb, const irs::curve::Options& options) {
  return CoverCartesianGeometry(
    duckdb::BoostSerde::Deserialize(wkb.data(), wkb.size()), options);
}

}  // namespace sdb::connector
