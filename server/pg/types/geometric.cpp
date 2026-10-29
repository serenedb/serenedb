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

#include "pg/types/geometric.h"

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>
#include <absl/strings/ascii.h>
#include <fast_float/fast_float.h>

#include <array>
#include <bit>
#include <cmath>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/main/client_context.hpp>
#include <functional>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <numbers>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "connector/common.h"
#include "pg/types/cast.h"
#include "pg/types/float.h"
#include "pg/types/function.h"
#include "query/config.h"

#pragma clang fp contract(off)

namespace sdb::pg {
namespace geo {
namespace {

using connector::AsView;

struct Point {
  double x;
  double y;
};

struct Line {
  double a;
  double b;
  double c;
};

struct Lseg {
  std::array<Point, 2> p;
};

struct Box {
  Point high;
  Point low;
};

struct Circle {
  Point center;
  double radius;
};

constexpr double kInfinity = std::numeric_limits<double>::infinity();
constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
constexpr double kEpsilon = 1.0E-06;
constexpr int kPointOnPolygon = std::numeric_limits<int>::max();
constexpr size_t kMaxRecursion = 4096;
constexpr int32_t kCirclePolygonPoints = 12;

double Add(double left, double right) {
  const auto result = left + right;
  if (std::isinf(result) && !std::isinf(left) && !std::isinf(right)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    ERR_MSG("value out of range: overflow"));
  }
  return result;
}

double Sub(double left, double right) {
  const auto result = left - right;
  if (std::isinf(result) && !std::isinf(left) && !std::isinf(right)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    ERR_MSG("value out of range: overflow"));
  }
  return result;
}

double Mul(double left, double right) {
  const auto result = left * right;
  if (std::isinf(result) && !std::isinf(left) && !std::isinf(right)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    ERR_MSG("value out of range: overflow"));
  }
  if (result == 0.0 && left != 0.0 && right != 0.0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    ERR_MSG("value out of range: underflow"));
  }
  return result;
}

double Div(double left, double right) {
  if (right == 0.0 && !std::isnan(left)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DIVISION_BY_ZERO),
                    ERR_MSG("division by zero"));
  }
  const auto result = left / right;
  if (std::isinf(result) && !std::isinf(left)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    ERR_MSG("value out of range: overflow"));
  }
  if (result == 0.0 && left != 0.0 && !std::isinf(right)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    ERR_MSG("value out of range: underflow"));
  }
  return result;
}

bool FloatEq(double left, double right) {
  return std::isnan(left) ? std::isnan(right)
                          : !std::isnan(right) && left == right;
}

bool FloatLt(double left, double right) {
  return !std::isnan(left) && (std::isnan(right) || left < right);
}

bool FloatGt(double left, double right) {
  return !std::isnan(right) && (std::isnan(left) || left > right);
}

double FloatMin(double left, double right) {
  return FloatLt(left, right) ? left : right;
}

double FloatMax(double left, double right) {
  return FloatGt(left, right) ? left : right;
}

double Hypot(double x, double y) {
  if (std::isinf(x) || std::isinf(y)) {
    return kInfinity;
  }
  if (std::isnan(x) || std::isnan(y)) {
    return kNaN;
  }
  x = std::fabs(x);
  y = std::fabs(y);
  if (x < y) {
    std::swap(x, y);
  }
  if (y == 0.0) {
    return x;
  }
  const auto ratio = y / x;
  const auto result = x * std::sqrt(1.0 + ratio * ratio);
  if (std::isinf(result)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    ERR_MSG("value out of range: overflow"));
  }
  return result;
}

bool FpZero(double value) { return std::fabs(value) <= kEpsilon; }

bool FpEq(double left, double right) {
  return left == right || std::fabs(left - right) <= kEpsilon;
}

bool FpNe(double left, double right) {
  return left != right && std::fabs(left - right) > kEpsilon;
}

bool FpLt(double left, double right) { return left + kEpsilon < right; }

bool FpLe(double left, double right) { return left <= right + kEpsilon; }

bool FpGt(double left, double right) { return left > right + kEpsilon; }

bool FpGe(double left, double right) { return left + kEpsilon >= right; }

template<typename T>
T ReadFixed(const duckdb::RecursiveUnifiedVectorFormat& value,
            duckdb::idx_t index) {
  std::array<double, sizeof(T) / sizeof(double)> fields{};
  for (size_t i = 0; i < fields.size(); ++i) {
    const auto& child = value.children[i].unified;
    fields[i] = duckdb::UnifiedVectorFormat::GetData<double>(
      child)[child.sel->get_index(index)];
  }
  return std::bit_cast<T>(fields);
}

duckdb::list_entry_t ListEntry(const duckdb::RecursiveUnifiedVectorFormat& list,
                               duckdb::idx_t index) {
  return duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(
    list.unified)[list.unified.sel->get_index(index)];
}

class PointsView {
 public:
  PointsView(const duckdb::RecursiveUnifiedVectorFormat& list,
             duckdb::idx_t index)
    : _points{&list.children[0]}, _entry{ListEntry(list, index)} {}

  duckdb::idx_t Size() const { return _entry.length; }

  Point operator[](duckdb::idx_t i) const {
    return ReadFixed<Point>(*_points,
                            _points->unified.sel->get_index(_entry.offset + i));
  }

 private:
  const duckdb::RecursiveUnifiedVectorFormat* _points;
  duckdb::list_entry_t _entry;
};

struct PathView {
  bool closed;
  PointsView points;
};

struct PolygonView {
  PointsView points;
};

struct Path {
  bool closed;
  std::vector<Point> points;
};

struct Polygon {
  std::vector<Point> points;
};

constexpr duckdb::idx_t kPathClosedChild = 0;
constexpr duckdb::idx_t kPathPointsChild = 1;
constexpr duckdb::idx_t kPolygonPointsChild = 0;

template<typename T>
T Read(const duckdb::RecursiveUnifiedVectorFormat& value, duckdb::idx_t index) {
  if constexpr (std::is_same_v<T, PathView>) {
    const auto& closed = value.children[kPathClosedChild].unified;
    return {
      .closed = duckdb::UnifiedVectorFormat::GetData<bool>(
        closed)[closed.sel->get_index(index)],
      .points = PointsView{value.children[kPathPointsChild], index},
    };
  } else if constexpr (std::is_same_v<T, PolygonView>) {
    return {.points = PointsView{value.children[kPolygonPointsChild], index}};
  } else {
    return ReadFixed<T>(value, index);
  }
}

template<typename T>
using GeometricArg = RecursiveArg<T, Read<T>>;

template<typename T>
  requires(std::is_same_v<T, Point> || std::is_same_v<T, Line> ||
           std::is_same_v<T, Lseg> || std::is_same_v<T, Box> ||
           std::is_same_v<T, Circle>)
void StoreValue(duckdb::Vector& vector, duckdb::idx_t row, const T& value) {
  const auto fields =
    std::bit_cast<std::array<double, sizeof(T) / sizeof(double)>>(value);
  auto& children = duckdb::StructVector::GetEntries(vector);
  for (size_t i = 0; i < fields.size(); ++i) {
    duckdb::FlatVector::GetDataMutable<double>(children[i])[row] = fields[i];
  }
}

void StoreValue(duckdb::Vector& vector, duckdb::idx_t row, const Path& value) {
  auto& children = duckdb::StructVector::GetEntries(vector);
  duckdb::FlatVector::GetDataMutable<bool>(children[kPathClosedChild])[row] =
    value.closed;
  StoreList<Point>(children[kPathPointsChild], row, value.points);
}

void StoreValue(duckdb::Vector& vector, duckdb::idx_t row,
                const Polygon& value) {
  StoreList<Point>(
    duckdb::StructVector::GetEntries(vector)[kPolygonPointsChild], row,
    value.points);
}

template<typename Transform>
std::vector<Point> MapPoints(const PointsView& points, Transform&& transform) {
  std::vector<Point> result;
  result.reserve(points.Size());
  for (duckdb::idx_t i = 0; i < points.Size(); ++i) {
    result.push_back(transform(points[i]));
  }
  return result;
}

std::optional<duckdb::idx_t> Previous(duckdb::idx_t size, bool closed,
                                      duckdb::idx_t i) {
  if (i > 0) {
    return i - 1;
  }
  if (!closed) {
    return std::nullopt;
  }
  return size - 1;
}

// Points.

Point ConstructPoint(double x, double y) { return {.x = x, .y = y}; }

bool PointEq(const Point& left, const Point& right) {
  if (std::isnan(left.x) || std::isnan(left.y) || std::isnan(right.x) ||
      std::isnan(right.y)) [[unlikely]] {
    return FloatEq(left.x, right.x) && FloatEq(left.y, right.y);
  }
  return FpEq(left.x, right.x) && FpEq(left.y, right.y);
}

bool PointNe(const Point& left, const Point& right) {
  return !PointEq(left, right);
}

double PointDistance(const Point& left, const Point& right) {
  return Hypot(Sub(left.x, right.x), Sub(left.y, right.y));
}

double PointSlope(const Point& left, const Point& right) {
  if (FpEq(left.x, right.x)) {
    return kInfinity;
  }
  if (FpEq(left.y, right.y)) {
    return 0.0;
  }
  return Div(Sub(left.y, right.y), Sub(left.x, right.x));
}

double PointInverseSlope(const Point& left, const Point& right) {
  if (FpEq(left.x, right.x)) {
    return 0.0;
  }
  if (FpEq(left.y, right.y)) {
    return kInfinity;
  }
  return Div(Sub(left.x, right.x), Sub(right.y, left.y));
}

Point PointAdd(const Point& left, const Point& right) {
  const auto x = Add(left.x, right.x);
  return {.x = x, .y = Add(left.y, right.y)};
}

Point PointSub(const Point& left, const Point& right) {
  const auto x = Sub(left.x, right.x);
  return {.x = x, .y = Sub(left.y, right.y)};
}

Point PointMul(const Point& left, const Point& right) {
  const auto x = Sub(Mul(left.x, right.x), Mul(left.y, right.y));
  return {.x = x, .y = Add(Mul(left.x, right.y), Mul(left.y, right.x))};
}

Point PointDiv(const Point& left, const Point& right) {
  const auto divisor = Add(Mul(right.x, right.x), Mul(right.y, right.y));
  const auto x = Div(Add(Mul(left.x, right.x), Mul(left.y, right.y)), divisor);
  return {
    .x = x,
    .y = Div(Sub(Mul(left.y, right.x), Mul(left.x, right.y)), divisor),
  };
}

bool PointLeft(const Point& left, const Point& right) {
  return FpLt(left.x, right.x);
}

bool PointRight(const Point& left, const Point& right) {
  return FpGt(left.x, right.x);
}

bool PointAbove(const Point& left, const Point& right) {
  return FpGt(left.y, right.y);
}

bool PointBelow(const Point& left, const Point& right) {
  return FpLt(left.y, right.y);
}

bool PointVertical(const Point& left, const Point& right) {
  return FpEq(left.x, right.x);
}

bool PointHorizontal(const Point& left, const Point& right) {
  return FpEq(left.y, right.y);
}

// Lines.

Line LineFromSlope(const Point& point, double slope) {
  if (std::isinf(slope)) {
    return {.a = -1.0, .b = 0.0, .c = point.x};
  }
  if (slope == 0) {
    return {.a = 0.0, .b = -1.0, .c = point.y};
  }
  auto c = Sub(point.y, Mul(slope, point.x));
  if (c == 0.0) {
    c = 0.0;
  }
  return {.a = slope, .b = -1.0, .c = c};
}

Line LineThrough(const Point& first, const Point& second) {
  if (PointEq(first, second)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("invalid line specification: must be two distinct points"));
  }
  return LineFromSlope(first, PointSlope(first, second));
}

double LineSlope(const Line& line) {
  if (FpZero(line.a)) {
    return 0.0;
  }
  if (FpZero(line.b)) {
    return kInfinity;
  }
  return Div(line.a, -line.b);
}

double LineInverseSlope(const Line& line) {
  if (FpZero(line.a)) {
    return kInfinity;
  }
  if (FpZero(line.b)) {
    return 0.0;
  }
  return Div(line.b, line.a);
}

bool LineIntersect(Point* result, const Line& l1, const Line& l2) {
  double x = 0;
  double y = 0;
  if (!FpZero(l1.b)) {
    if (FpEq(l2.a, Mul(l1.a, Div(l2.b, l1.b)))) {
      return false;
    }
    x = Div(Sub(Mul(l1.b, l2.c), Mul(l2.b, l1.c)),
            Sub(Mul(l1.a, l2.b), Mul(l2.a, l1.b)));
    y = Div(-Add(Mul(l1.a, x), l1.c), l1.b);
  } else if (!FpZero(l2.b)) {
    if (FpEq(l1.a, Mul(l2.a, Div(l1.b, l2.b)))) {
      return false;
    }
    x = Div(Sub(Mul(l2.b, l1.c), Mul(l1.b, l2.c)),
            Sub(Mul(l2.a, l1.b), Mul(l1.a, l2.b)));
    y = Div(-Add(Mul(l2.a, x), l2.c), l2.b);
  } else {
    return false;
  }
  if (x == 0.0) {
    x = 0.0;
  }
  if (y == 0.0) {
    y = 0.0;
  }
  if (result) {
    *result = {.x = x, .y = y};
  }
  return true;
}

bool LineContainsPoint(const Line& line, const Point& point) {
  return FpZero(Add(Add(Mul(line.a, point.x), Mul(line.b, point.y)), line.c));
}

double LineClosestPoint(Point* result, const Line& line, const Point& point) {
  Point closest{};
  if (!LineIntersect(&closest, LineFromSlope(point, LineInverseSlope(line)),
                     line)) {
    if (result) {
      *result = point;
    }
    return kNaN;
  }
  if (result) {
    *result = closest;
  }
  return PointDistance(closest, point);
}

bool LinesIntersect(const Line& l1, const Line& l2) {
  return LineIntersect(nullptr, l1, l2);
}

bool LinesParallel(const Line& l1, const Line& l2) {
  return !LineIntersect(nullptr, l1, l2);
}

bool LinesPerpendicular(const Line& l1, const Line& l2) {
  if (FpZero(l1.a)) {
    return FpZero(l2.b);
  }
  if (FpZero(l2.a)) {
    return FpZero(l1.b);
  }
  if (FpZero(l1.b)) {
    return FpZero(l2.a);
  }
  if (FpZero(l2.b)) {
    return FpZero(l1.a);
  }
  return FpEq(Div(Mul(l1.a, l2.a), Mul(l1.b, l2.b)), -1.0);
}

bool LineVertical(const Line& line) { return FpZero(line.b); }

bool LineHorizontal(const Line& line) { return FpZero(line.a); }

bool LinesEqual(const Line& l1, const Line& l2) {
  if (std::isnan(l1.a) || std::isnan(l1.b) || std::isnan(l1.c) ||
      std::isnan(l2.a) || std::isnan(l2.b) || std::isnan(l2.c)) [[unlikely]] {
    return FloatEq(l1.a, l2.a) && FloatEq(l1.b, l2.b) && FloatEq(l1.c, l2.c);
  }
  double ratio = 1.0;
  if (!FpZero(l2.a)) {
    ratio = Div(l1.a, l2.a);
  } else if (!FpZero(l2.b)) {
    ratio = Div(l1.b, l2.b);
  } else if (!FpZero(l2.c)) {
    ratio = Div(l1.c, l2.c);
  }
  return FpEq(l1.a, Mul(ratio, l2.a)) && FpEq(l1.b, Mul(ratio, l2.b)) &&
         FpEq(l1.c, Mul(ratio, l2.c));
}

double LineDistance(const Line& l1, const Line& l2) {
  if (LineIntersect(nullptr, l1, l2)) {
    return 0.0;
  }
  double ratio = 1.0;
  if (!FpZero(l1.a) && !std::isnan(l1.a) && !FpZero(l2.a) &&
      !std::isnan(l2.a)) {
    ratio = Div(l1.a, l2.a);
  } else if (!FpZero(l1.b) && !FpZero(l2.b)) {
    ratio = Div(l1.b, l2.b);
  }
  return Div(std::fabs(Sub(l1.c, Mul(ratio, l2.c))), Hypot(l1.a, l1.b));
}

std::optional<Point> LineIntersection(const Line& l1, const Line& l2) {
  Point result{};
  if (!LineIntersect(&result, l1, l2)) {
    return std::nullopt;
  }
  return result;
}

// Line segments.

Lseg LsegThrough(const Point& first, const Point& second) {
  return {.p = {first, second}};
}

double LsegSlope(const Lseg& lseg) { return PointSlope(lseg.p[0], lseg.p[1]); }

double LsegInverseSlope(const Lseg& lseg) {
  return PointInverseSlope(lseg.p[0], lseg.p[1]);
}

double LsegLength(const Lseg& lseg) {
  return PointDistance(lseg.p[0], lseg.p[1]);
}

bool LsegContainsPoint(const Lseg& lseg, const Point& point) {
  return FpEq(PointDistance(point, lseg.p[0]) + PointDistance(point, lseg.p[1]),
              PointDistance(lseg.p[0], lseg.p[1]));
}

bool LsegIntersectLine(Point* result, const Lseg& lseg, const Line& line) {
  Point point{};
  if (!LineIntersect(&point, LineFromSlope(lseg.p[0], LsegSlope(lseg)), line)) {
    return false;
  }
  if (!LsegContainsPoint(lseg, point)) {
    return false;
  }
  if (result) {
    if (PointEq(lseg.p[0], point)) {
      *result = lseg.p[0];
    } else if (PointEq(lseg.p[1], point)) {
      *result = lseg.p[1];
    } else {
      *result = point;
    }
  }
  return true;
}

bool LsegIntersectLseg(Point* result, const Lseg& l1, const Lseg& l2) {
  Point point{};
  if (!LsegIntersectLine(&point, l1, LineFromSlope(l2.p[0], LsegSlope(l2)))) {
    return false;
  }
  if (!LsegContainsPoint(l2, point)) {
    return false;
  }
  if (result) {
    *result = point;
  }
  return true;
}

double LsegClosestLine(Point* result, const Lseg& lseg, const Line& line) {
  if (LsegIntersectLine(result, lseg, line)) {
    return 0.0;
  }
  const auto first = LineClosestPoint(nullptr, line, lseg.p[0]);
  const auto second = LineClosestPoint(nullptr, line, lseg.p[1]);
  if (first < second) {
    if (result) {
      *result = lseg.p[0];
    }
    return first;
  }
  if (result) {
    *result = lseg.p[1];
  }
  return second;
}

double LsegClosestPoint(Point* result, const Lseg& lseg, const Point& point) {
  Point closest{};
  LsegClosestLine(
    &closest, lseg,
    LineFromSlope(point, PointInverseSlope(lseg.p[0], lseg.p[1])));
  if (result) {
    *result = closest;
  }
  return PointDistance(closest, point);
}

double LsegClosestLseg(Point* result, const Lseg& on, const Lseg& to) {
  if (LsegIntersectLseg(result, on, to)) {
    return 0.0;
  }
  Point point{};
  auto distance = LsegClosestPoint(result, on, to.p[0]);
  auto candidate = LsegClosestPoint(&point, on, to.p[1]);
  if (FloatLt(candidate, distance)) {
    distance = candidate;
    if (result) {
      *result = point;
    }
  }
  for (const auto& end : on.p) {
    candidate = LsegClosestPoint(nullptr, to, end);
    if (FloatLt(candidate, distance)) {
      distance = candidate;
      if (result) {
        *result = end;
      }
    }
  }
  return distance;
}

bool LsegsIntersect(const Lseg& l1, const Lseg& l2) {
  return LsegIntersectLseg(nullptr, l1, l2);
}

bool LsegsParallel(const Lseg& l1, const Lseg& l2) {
  return FpEq(LsegSlope(l1), LsegSlope(l2));
}

bool LsegsPerpendicular(const Lseg& l1, const Lseg& l2) {
  return FpEq(LsegSlope(l1), LsegInverseSlope(l2));
}

bool LsegVertical(const Lseg& lseg) { return FpEq(lseg.p[0].x, lseg.p[1].x); }

bool LsegHorizontal(const Lseg& lseg) { return FpEq(lseg.p[0].y, lseg.p[1].y); }

bool LsegsEqual(const Lseg& l1, const Lseg& l2) {
  return PointEq(l1.p[0], l2.p[0]) && PointEq(l1.p[1], l2.p[1]);
}

bool LsegsDiffer(const Lseg& l1, const Lseg& l2) {
  return !PointEq(l1.p[0], l2.p[0]) || !PointEq(l1.p[1], l2.p[1]);
}

double LsegDistance(const Lseg& l1, const Lseg& l2) {
  return LsegClosestLseg(nullptr, l1, l2);
}

Point LsegCenter(const Lseg& lseg) {
  const auto x = Div(Add(lseg.p[0].x, lseg.p[1].x), 2.0);
  return {.x = x, .y = Div(Add(lseg.p[0].y, lseg.p[1].y), 2.0)};
}

std::optional<Point> LsegIntersection(const Lseg& l1, const Lseg& l2) {
  Point result{};
  if (!LsegIntersectLseg(&result, l1, l2)) {
    return std::nullopt;
  }
  return result;
}

// Boxes.

Box MakeBox(const Point& first, const Point& second) {
  const bool x = FloatGt(first.x, second.x);
  const bool y = FloatGt(first.y, second.y);
  return {
    .high = {.x = x ? first.x : second.x, .y = y ? first.y : second.y},
    .low = {.x = x ? second.x : first.x, .y = y ? second.y : first.y},
  };
}

Box Reorder(Box box) {
  if (FloatLt(box.high.x, box.low.x)) {
    std::swap(box.high.x, box.low.x);
  }
  if (FloatLt(box.high.y, box.low.y)) {
    std::swap(box.high.y, box.low.y);
  }
  return box;
}

Box PointBox(const Point& point) { return {.high = point, .low = point}; }

bool BoxSame(const Box& b1, const Box& b2) {
  return PointEq(b1.high, b2.high) && PointEq(b1.low, b2.low);
}

bool BoxOverlap(const Box& b1, const Box& b2) {
  return FpLe(b1.low.x, b2.high.x) && FpLe(b2.low.x, b1.high.x) &&
         FpLe(b1.low.y, b2.high.y) && FpLe(b2.low.y, b1.high.y);
}

bool BoxLeft(const Box& b1, const Box& b2) { return FpLt(b1.high.x, b2.low.x); }

bool BoxOverLeft(const Box& b1, const Box& b2) {
  return FpLe(b1.high.x, b2.high.x);
}

bool BoxRight(const Box& b1, const Box& b2) {
  return FpGt(b1.low.x, b2.high.x);
}

bool BoxOverRight(const Box& b1, const Box& b2) {
  return FpGe(b1.low.x, b2.low.x);
}

bool BoxBelow(const Box& b1, const Box& b2) {
  return FpLt(b1.high.y, b2.low.y);
}

bool BoxOverBelow(const Box& b1, const Box& b2) {
  return FpLe(b1.high.y, b2.high.y);
}

bool BoxAbove(const Box& b1, const Box& b2) {
  return FpGt(b1.low.y, b2.high.y);
}

bool BoxOverAbove(const Box& b1, const Box& b2) {
  return FpGe(b1.low.y, b2.low.y);
}

bool BoxContainsBox(const Box& outer, const Box& inner) {
  return FpGe(outer.high.x, inner.high.x) && FpLe(outer.low.x, inner.low.x) &&
         FpGe(outer.high.y, inner.high.y) && FpLe(outer.low.y, inner.low.y);
}

bool BoxContainedBy(const Box& inner, const Box& outer) {
  return BoxContainsBox(outer, inner);
}

bool BoxBelowEq(const Box& b1, const Box& b2) {
  return FpLe(b1.high.y, b2.low.y);
}

bool BoxAboveEq(const Box& b1, const Box& b2) {
  return FpGe(b1.low.y, b2.high.y);
}

double BoxWidth(const Box& box) { return Sub(box.high.x, box.low.x); }

double BoxHeight(const Box& box) { return Sub(box.high.y, box.low.y); }

double BoxArea(const Box& box) { return Mul(BoxWidth(box), BoxHeight(box)); }

Point BoxCenter(const Box& box) {
  const auto x = Div(Add(box.high.x, box.low.x), 2.0);
  return {.x = x, .y = Div(Add(box.high.y, box.low.y), 2.0)};
}

double BoxDistance(const Box& b1, const Box& b2) {
  return PointDistance(BoxCenter(b1), BoxCenter(b2));
}

std::optional<Box> BoxIntersection(const Box& b1, const Box& b2) {
  if (!BoxOverlap(b1, b2)) {
    return std::nullopt;
  }
  return Box{
    .high = {.x = FloatMin(b1.high.x, b2.high.x),
             .y = FloatMin(b1.high.y, b2.high.y)},
    .low = {.x = FloatMax(b1.low.x, b2.low.x),
            .y = FloatMax(b1.low.y, b2.low.y)},
  };
}

Lseg BoxDiagonal(const Box& box) { return {.p = {box.high, box.low}}; }

Box BoxAddPoint(const Box& box, const Point& point) {
  return {.high = PointAdd(box.high, point), .low = PointAdd(box.low, point)};
}

Box BoxSubPoint(const Box& box, const Point& point) {
  return {.high = PointSub(box.high, point), .low = PointSub(box.low, point)};
}

Box BoxMulPoint(const Box& box, const Point& point) {
  const auto high = PointMul(box.high, point);
  return MakeBox(high, PointMul(box.low, point));
}

Box BoxDivPoint(const Box& box, const Point& point) {
  const auto high = PointDiv(box.high, point);
  return MakeBox(high, PointDiv(box.low, point));
}

Box BoundBox(const Box& b1, const Box& b2) {
  return {
    .high = {.x = FloatMax(b1.high.x, b2.high.x),
             .y = FloatMax(b1.high.y, b2.high.y)},
    .low = {.x = FloatMin(b1.low.x, b2.low.x),
            .y = FloatMin(b1.low.y, b2.low.y)},
  };
}

bool BoxContainsPoint(const Box& box, const Point& point) {
  return box.high.x >= point.x && box.low.x <= point.x &&
         box.high.y >= point.y && box.low.y <= point.y;
}

bool PointInBox(const Point& point, const Box& box) {
  return BoxContainsPoint(box, point);
}

std::array<Lseg, 4> BoxSides(const Box& box) {
  const Point upper_left{.x = box.low.x, .y = box.high.y};
  const Point lower_right{.x = box.high.x, .y = box.low.y};
  return {{{.p = {box.low, upper_left}},
           {.p = {box.high, upper_left}},
           {.p = {box.low, lower_right}},
           {.p = {box.high, lower_right}}}};
}

double BoxClosestPoint(Point* result, const Box& box, const Point& point) {
  if (BoxContainsPoint(box, point)) {
    if (result) {
      *result = point;
    }
    return 0.0;
  }
  const auto sides = BoxSides(box);
  auto distance = LsegClosestPoint(result, sides[0], point);
  for (size_t i = 1; i < sides.size(); ++i) {
    Point closest{};
    const auto candidate = LsegClosestPoint(&closest, sides[i], point);
    if (FloatLt(candidate, distance)) {
      distance = candidate;
      if (result) {
        *result = closest;
      }
    }
  }
  return distance;
}

bool BoxIntersectLseg(Point* result, const Box& box, const Lseg& lseg) {
  const Box bounds{
    .high = {.x = FloatMax(lseg.p[0].x, lseg.p[1].x),
             .y = FloatMax(lseg.p[0].y, lseg.p[1].y)},
    .low = {.x = FloatMin(lseg.p[0].x, lseg.p[1].x),
            .y = FloatMin(lseg.p[0].y, lseg.p[1].y)},
  };
  if (!BoxOverlap(bounds, box)) {
    return false;
  }
  if (result) {
    LsegClosestPoint(result, lseg, BoxCenter(box));
  }
  if (BoxContainsPoint(box, lseg.p[0]) || BoxContainsPoint(box, lseg.p[1])) {
    return true;
  }
  return absl::c_any_of(BoxSides(box), [&](const Lseg& side) {
    return LsegIntersectLseg(nullptr, side, lseg);
  });
}

double BoxClosestLseg(Point* result, const Box& box, const Lseg& lseg) {
  if (BoxIntersectLseg(result, box, lseg)) {
    return 0.0;
  }
  const auto sides = BoxSides(box);
  auto distance = LsegClosestLseg(result, sides[0], lseg);
  for (size_t i = 1; i < sides.size(); ++i) {
    Point closest{};
    const auto candidate = LsegClosestLseg(&closest, sides[i], lseg);
    if (FloatLt(candidate, distance)) {
      distance = candidate;
      if (result) {
        *result = closest;
      }
    }
  }
  return distance;
}

bool LineIntersectsBox(const Line& line, const Box& box) {
  const Point upper_left{.x = box.low.x, .y = box.high.y};
  const Point lower_right{.x = box.high.x, .y = box.low.y};
  const std::array<Lseg, 4> sides{{{.p = {box.low, upper_left}},
                                   {.p = {box.high, upper_left}},
                                   {.p = {box.high, lower_right}},
                                   {.p = {box.low, lower_right}}}};
  return absl::c_any_of(sides, [&](const Lseg& side) {
    return LsegIntersectLine(nullptr, side, line);
  });
}

bool LsegInBox(const Lseg& lseg, const Box& box) {
  return BoxContainsPoint(box, lseg.p[0]) && BoxContainsPoint(box, lseg.p[1]);
}

bool LsegIntersectsBox(const Lseg& lseg, const Box& box) {
  return BoxIntersectLseg(nullptr, box, lseg);
}

bool LsegOnLine(const Lseg& lseg, const Line& line) {
  return LineContainsPoint(line, lseg.p[0]) &&
         LineContainsPoint(line, lseg.p[1]);
}

bool LsegIntersectsLine(const Lseg& lseg, const Line& line) {
  return LsegIntersectLine(nullptr, lseg, line);
}

bool PointOnLine(const Point& point, const Line& line) {
  return LineContainsPoint(line, point);
}

bool PointOnLseg(const Point& point, const Lseg& lseg) {
  return LsegContainsPoint(lseg, point);
}

// Distances and closest points between different kinds.

double PointLineDistance(const Point& point, const Line& line) {
  return LineClosestPoint(nullptr, line, point);
}

double LinePointDistance(const Line& line, const Point& point) {
  return LineClosestPoint(nullptr, line, point);
}

double PointLsegDistance(const Point& point, const Lseg& lseg) {
  return LsegClosestPoint(nullptr, lseg, point);
}

double LsegPointDistance(const Lseg& lseg, const Point& point) {
  return LsegClosestPoint(nullptr, lseg, point);
}

double PointBoxDistance(const Point& point, const Box& box) {
  return BoxClosestPoint(nullptr, box, point);
}

double BoxPointDistance(const Box& box, const Point& point) {
  return BoxClosestPoint(nullptr, box, point);
}

double LsegLineDistance(const Lseg& lseg, const Line& line) {
  return LsegClosestLine(nullptr, lseg, line);
}

double LineLsegDistance(const Line& line, const Lseg& lseg) {
  return LsegClosestLine(nullptr, lseg, line);
}

double LsegBoxDistance(const Lseg& lseg, const Box& box) {
  return BoxClosestLseg(nullptr, box, lseg);
}

double BoxLsegDistance(const Box& box, const Lseg& lseg) {
  return BoxClosestLseg(nullptr, box, lseg);
}

template<auto Closest, typename Target, typename Source>
std::optional<Point> ClosestPoint(const Target& target, const Source& source) {
  Point result{};
  if (std::isnan(Closest(&result, target, source))) {
    return std::nullopt;
  }
  return result;
}

std::optional<Point> PointLineClosest(const Point& point, const Line& line) {
  return ClosestPoint<&LineClosestPoint>(line, point);
}

std::optional<Point> PointLsegClosest(const Point& point, const Lseg& lseg) {
  return ClosestPoint<&LsegClosestPoint>(lseg, point);
}

std::optional<Point> PointBoxClosest(const Point& point, const Box& box) {
  return ClosestPoint<&BoxClosestPoint>(box, point);
}

std::optional<Point> LsegBoxClosest(const Lseg& lseg, const Box& box) {
  return ClosestPoint<&BoxClosestLseg>(box, lseg);
}

std::optional<Point> LineLsegClosest(const Line& line, const Lseg& lseg) {
  if (LsegSlope(lseg) == LineSlope(line)) {
    return std::nullopt;
  }
  return ClosestPoint<&LsegClosestLine>(lseg, line);
}

std::optional<Point> LsegLsegClosest(const Lseg& l1, const Lseg& l2) {
  if (LsegSlope(l1) == LsegSlope(l2)) {
    return std::nullopt;
  }
  return ClosestPoint<&LsegClosestLseg>(l2, l1);
}

// Points inside polygons and closed paths.

int LsegCrossing(double x, double y, double prev_x, double prev_y) {
  if (FpZero(y)) {
    if (FpZero(x)) {
      return kPointOnPolygon;
    }
    if (FpGt(x, 0)) {
      if (FpZero(prev_y)) {
        return FpGt(prev_x, 0.0) ? 0 : kPointOnPolygon;
      }
      return FpLt(prev_y, 0.0) ? 1 : -1;
    }
    if (FpZero(prev_y)) {
      return FpLt(prev_x, 0.0) ? 0 : kPointOnPolygon;
    }
    return 0;
  }
  const bool above = FpGt(y, 0.0);
  const int y_sign = above ? 1 : -1;
  if (FpZero(prev_y)) {
    return FpLt(prev_x, 0.0) ? 0 : y_sign;
  }
  if (above ? FpGt(prev_y, 0.0) : FpLt(prev_y, 0.0)) {
    return 0;
  }
  if (FpGe(x, 0.0) && FpGt(prev_x, 0.0)) {
    return 2 * y_sign;
  }
  if (FpLt(x, 0.0) && FpLe(prev_x, 0.0)) {
    return 0;
  }
  const auto z = Sub(Mul(Sub(x, prev_x), y), Mul(Sub(y, prev_y), x));
  if (FpZero(z)) {
    return kPointOnPolygon;
  }
  if (above ? FpGt(z, 0.0) : FpLt(z, 0.0)) {
    return 0;
  }
  return 2 * y_sign;
}

int PointInside(const Point& point, const PointsView& points) {
  SDB_ASSERT(points.Size() > 0);
  const auto first = points[0];
  const auto x0 = Sub(first.x, point.x);
  const auto y0 = Sub(first.y, point.y);
  auto prev_x = x0;
  auto prev_y = y0;
  int total = 0;
  for (duckdb::idx_t i = 1; i < points.Size(); ++i) {
    const auto current = points[i];
    const auto x = Sub(current.x, point.x);
    const auto y = Sub(current.y, point.y);
    const auto cross = LsegCrossing(x, y, prev_x, prev_y);
    if (cross == kPointOnPolygon) {
      return 2;
    }
    total += cross;
    prev_x = x;
    prev_y = y;
  }
  const auto cross = LsegCrossing(x0, y0, prev_x, prev_y);
  if (cross == kPointOnPolygon) {
    return 2;
  }
  total += cross;
  return total != 0 ? 1 : 0;
}

// Paths.

std::optional<double> PathArea(const PathView& path) {
  if (!path.closed) {
    return std::nullopt;
  }
  const auto& points = path.points;
  double area = 0.0;
  for (duckdb::idx_t i = 0; i < points.Size(); ++i) {
    const auto current = points[i];
    const auto next = points[(i + 1) % points.Size()];
    area = Add(area, Mul(current.x, next.y));
    area = Sub(area, Mul(current.y, next.x));
  }
  return Div(std::fabs(area), 2.0);
}

bool PathIsClosed(const PathView& path) { return path.closed; }

bool PathIsOpen(const PathView& path) { return !path.closed; }

int32_t PathPointCount(const PathView& path) {
  return static_cast<int32_t>(path.points.Size());
}

Path PathClose(const PathView& path) {
  return {.closed = true, .points = MapPoints(path.points, std::identity{})};
}

Path PathOpen(const PathView& path) {
  return {.closed = false, .points = MapPoints(path.points, std::identity{})};
}

Box PointsBounds(const PointsView& points) {
  SDB_ASSERT(points.Size() > 0);
  Box box{.high = points[0], .low = points[0]};
  for (duckdb::idx_t i = 1; i < points.Size(); ++i) {
    const auto point = points[i];
    box.high.x = FloatMax(point.x, box.high.x);
    box.high.y = FloatMax(point.y, box.high.y);
    box.low.x = FloatMin(point.x, box.low.x);
    box.low.y = FloatMin(point.y, box.low.y);
  }
  return box;
}

bool PathsIntersect(const PathView& p1, const PathView& p2) {
  if (!BoxOverlap(PointsBounds(p1.points), PointsBounds(p2.points))) {
    return false;
  }
  for (duckdb::idx_t i = 0; i < p1.points.Size(); ++i) {
    const auto previous = Previous(p1.points.Size(), p1.closed, i);
    if (!previous) {
      continue;
    }
    for (duckdb::idx_t j = 0; j < p2.points.Size(); ++j) {
      const auto other = Previous(p2.points.Size(), p2.closed, j);
      if (!other) {
        continue;
      }
      if (LsegIntersectLseg(nullptr,
                            {.p = {p1.points[*previous], p1.points[i]}},
                            {.p = {p2.points[*other], p2.points[j]}})) {
        return true;
      }
    }
  }
  return false;
}

std::optional<double> PathDistance(const PathView& p1, const PathView& p2) {
  std::optional<double> result;
  for (duckdb::idx_t i = 0; i < p1.points.Size(); ++i) {
    const auto previous = Previous(p1.points.Size(), p1.closed, i);
    if (!previous) {
      continue;
    }
    for (duckdb::idx_t j = 0; j < p2.points.Size(); ++j) {
      const auto other = Previous(p2.points.Size(), p2.closed, j);
      if (!other) {
        continue;
      }
      const auto distance =
        LsegClosestLseg(nullptr, {.p = {p1.points[*previous], p1.points[i]}},
                        {.p = {p2.points[*other], p2.points[j]}});
      if (!result || FloatLt(distance, *result)) {
        result = distance;
      }
    }
  }
  return result;
}

double PathLength(const PathView& path) {
  double result = 0.0;
  for (duckdb::idx_t i = 0; i < path.points.Size(); ++i) {
    if (const auto previous = Previous(path.points.Size(), path.closed, i)) {
      result =
        Add(result, PointDistance(path.points[*previous], path.points[i]));
    }
  }
  return result;
}

std::optional<Path> PathConcat(const PathView& p1, const PathView& p2) {
  if (p1.closed || p2.closed) {
    return std::nullopt;
  }
  auto points = MapPoints(p1.points, std::identity{});
  points.reserve(p1.points.Size() + p2.points.Size());
  for (duckdb::idx_t i = 0; i < p2.points.Size(); ++i) {
    points.push_back(p2.points[i]);
  }
  return Path{.closed = false, .points = std::move(points)};
}

template<auto Apply>
Path PathMap(const PathView& path, const Point& point) {
  return {.closed = path.closed,
          .points = MapPoints(path.points, [&](const Point& vertex) {
            return Apply(vertex, point);
          })};
}

double PointPathDistance(const Point& point, const PathView& path) {
  std::optional<double> result;
  for (duckdb::idx_t i = 0; i < path.points.Size(); ++i) {
    const auto previous = Previous(path.points.Size(), path.closed, i);
    if (!previous) {
      continue;
    }
    const auto distance = LsegClosestPoint(
      nullptr, {.p = {path.points[*previous], path.points[i]}}, point);
    if (!result || FloatLt(distance, *result)) {
      result = distance;
    }
  }
  return result.value_or(0.0);
}

double PathPointDistance(const PathView& path, const Point& point) {
  return PointPathDistance(point, path);
}

bool PointOnPath(const Point& point, const PathView& path) {
  const auto& points = path.points;
  if (!path.closed) {
    auto previous = PointDistance(point, points[0]);
    for (duckdb::idx_t i = 0; i + 1 < points.Size(); ++i) {
      const auto next = PointDistance(point, points[i + 1]);
      if (FpEq(Add(previous, next), PointDistance(points[i], points[i + 1]))) {
        return true;
      }
      previous = next;
    }
    return false;
  }
  return PointInside(point, points) != 0;
}

bool PathContainsPoint(const PathView& path, const Point& point) {
  return PointOnPath(point, path);
}

Polygon PathPolygon(const PathView& path) {
  if (!path.closed) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("open path cannot be converted to polygon"));
  }
  return {.points = MapPoints(path.points, std::identity{})};
}

// Polygons.

int32_t PolygonPointCount(const PolygonView& polygon) {
  return static_cast<int32_t>(polygon.points.Size());
}

Box PolygonBounds(const PolygonView& polygon) {
  const auto& points = polygon.points;
  SDB_ASSERT(points.Size() > 0);
  auto low = points[0];
  auto high = low;
  for (duckdb::idx_t i = 1; i < points.Size(); ++i) {
    const auto point = points[i];
    if (FloatLt(point.x, low.x)) {
      low.x = point.x;
    }
    if (FloatGt(point.x, high.x)) {
      high.x = point.x;
    }
    if (FloatLt(point.y, low.y)) {
      low.y = point.y;
    }
    if (FloatGt(point.y, high.y)) {
      high.y = point.y;
    }
  }
  return {.high = high, .low = low};
}

bool PolygonLeft(const PolygonView& a, const PolygonView& b) {
  return PolygonBounds(a).high.x < PolygonBounds(b).low.x;
}

bool PolygonOverLeft(const PolygonView& a, const PolygonView& b) {
  return PolygonBounds(a).high.x <= PolygonBounds(b).high.x;
}

bool PolygonRight(const PolygonView& a, const PolygonView& b) {
  return PolygonBounds(a).low.x > PolygonBounds(b).high.x;
}

bool PolygonOverRight(const PolygonView& a, const PolygonView& b) {
  return PolygonBounds(a).low.x >= PolygonBounds(b).low.x;
}

bool PolygonBelow(const PolygonView& a, const PolygonView& b) {
  return PolygonBounds(a).high.y < PolygonBounds(b).low.y;
}

bool PolygonOverBelow(const PolygonView& a, const PolygonView& b) {
  return PolygonBounds(a).high.y <= PolygonBounds(b).high.y;
}

bool PolygonAbove(const PolygonView& a, const PolygonView& b) {
  return PolygonBounds(a).low.y > PolygonBounds(b).high.y;
}

bool PolygonOverAbove(const PolygonView& a, const PolygonView& b) {
  return PolygonBounds(a).low.y >= PolygonBounds(b).low.y;
}

bool PolygonSame(const PolygonView& a, const PolygonView& b) {
  const auto& p1 = a.points;
  const auto& p2 = b.points;
  const auto count = p1.Size();
  if (count != p2.Size()) {
    return false;
  }
  for (duckdb::idx_t i = 0; i < count; ++i) {
    if (!PointEq(p2[i], p1[0])) {
      continue;
    }
    duckdb::idx_t matched = 1;
    for (auto j = i + 1; matched < count; ++matched, ++j) {
      if (!PointEq(p2[j % count], p1[matched])) {
        break;
      }
    }
    if (matched == count) {
      return true;
    }
    matched = 1;
    for (auto j = i + count - 1; matched < count; ++matched, --j) {
      if (!PointEq(p2[j % count], p1[matched])) {
        break;
      }
    }
    if (matched == count) {
      return true;
    }
  }
  return false;
}

bool PolygonsOverlap(const PolygonView& a, const PolygonView& b) {
  if (!BoxOverlap(PolygonBounds(a), PolygonBounds(b))) {
    return false;
  }
  const auto& pa = a.points;
  const auto& pb = b.points;
  auto previous_a = pa[pa.Size() - 1];
  for (duckdb::idx_t ia = 0; ia < pa.Size(); ++ia) {
    const auto current_a = pa[ia];
    auto previous_b = pb[pb.Size() - 1];
    for (duckdb::idx_t ib = 0; ib < pb.Size(); ++ib) {
      const auto current_b = pb[ib];
      if (LsegIntersectLseg(nullptr, {.p = {previous_a, current_a}},
                            {.p = {previous_b, current_b}})) {
        return true;
      }
      previous_b = current_b;
    }
    previous_a = current_a;
  }
  return PointInside(pa[0], pb) != 0 || PointInside(pb[0], pa) != 0;
}

bool LsegInsidePolygon(const Point& a, const Point& b, const PointsView& poly,
                       duckdb::idx_t start, size_t depth);

bool TouchedLsegInsidePolygon(const Point& a, const Point& b, const Lseg& s,
                              const PointsView& poly, duckdb::idx_t start,
                              size_t depth) {
  const Lseg t{.p = {a, b}};
  if (PointEq(a, s.p[0])) {
    if (LsegContainsPoint(t, s.p[1])) {
      return LsegInsidePolygon(b, s.p[1], poly, start, depth);
    }
  } else if (PointEq(a, s.p[1])) {
    if (LsegContainsPoint(t, s.p[0])) {
      return LsegInsidePolygon(b, s.p[0], poly, start, depth);
    }
  } else if (LsegContainsPoint(t, s.p[0])) {
    return LsegInsidePolygon(b, s.p[0], poly, start, depth);
  } else if (LsegContainsPoint(t, s.p[1])) {
    return LsegInsidePolygon(b, s.p[1], poly, start, depth);
  }
  return true;
}

bool LsegInsidePolygon(const Point& a, const Point& b, const PointsView& poly,
                       duckdb::idx_t start, size_t depth) {
  if (depth > kMaxRecursion) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_STATEMENT_TOO_COMPLEX),
                    ERR_MSG("stack depth limit exceeded"));
  }
  const Lseg t{.p = {a, b}};
  Lseg s{.p = {poly[start == 0 ? poly.Size() - 1 : start - 1], {}}};
  bool inside = true;
  bool intersection = false;
  for (auto i = start; i < poly.Size() && inside; ++i) {
    s.p[1] = poly[i];
    Point point{};
    if (LsegContainsPoint(s, t.p[0])) {
      if (LsegContainsPoint(s, t.p[1])) {
        return true;
      }
      inside =
        TouchedLsegInsidePolygon(t.p[0], t.p[1], s, poly, i + 1, depth + 1);
    } else if (LsegContainsPoint(s, t.p[1])) {
      inside =
        TouchedLsegInsidePolygon(t.p[1], t.p[0], s, poly, i + 1, depth + 1);
    } else if (LsegIntersectLseg(&point, t, s)) {
      intersection = true;
      inside = LsegInsidePolygon(t.p[0], point, poly, i + 1, depth + 1) &&
               LsegInsidePolygon(t.p[1], point, poly, i + 1, depth + 1);
    }
    s.p[0] = s.p[1];
  }
  if (inside && !intersection) {
    const auto x = Div(Add(t.p[0].x, t.p[1].x), 2.0);
    inside =
      PointInside({.x = x, .y = Div(Add(t.p[0].y, t.p[1].y), 2.0)}, poly) != 0;
  }
  return inside;
}

bool PolygonContains(const PolygonView& outer, const PolygonView& inner) {
  if (!BoxContainsBox(PolygonBounds(outer), PolygonBounds(inner))) {
    return false;
  }
  const auto& points = inner.points;
  auto previous = points[points.Size() - 1];
  for (duckdb::idx_t i = 0; i < points.Size(); ++i) {
    const auto current = points[i];
    if (!LsegInsidePolygon(previous, current, outer.points, 0, 0)) {
      return false;
    }
    previous = current;
  }
  return true;
}

bool PolygonContainedBy(const PolygonView& inner, const PolygonView& outer) {
  return PolygonContains(outer, inner);
}

bool PolygonContainsPoint(const PolygonView& polygon, const Point& point) {
  return PointInside(point, polygon.points) != 0;
}

bool PointInPolygon(const Point& point, const PolygonView& polygon) {
  return PointInside(point, polygon.points) != 0;
}

double PolygonDistance(const PolygonView& a, const PolygonView& b) {
  if (PolygonsOverlap(a, b)) {
    return 0.0;
  }
  return *PathDistance({.closed = true, .points = a.points},
                       {.closed = true, .points = b.points});
}

double PointPolygonDistance(const Point& point, const PolygonView& polygon) {
  const auto& points = polygon.points;
  if (PointInside(point, points) != 0) {
    return 0.0;
  }
  auto result = LsegClosestPoint(
    nullptr, {.p = {points[0], points[points.Size() - 1]}}, point);
  for (duckdb::idx_t i = 0; i + 1 < points.Size(); ++i) {
    const auto distance =
      LsegClosestPoint(nullptr, {.p = {points[i], points[i + 1]}}, point);
    if (FloatLt(distance, result)) {
      result = distance;
    }
  }
  return result;
}

double PolygonPointDistance(const PolygonView& polygon, const Point& point) {
  return PointPolygonDistance(point, polygon);
}

Circle PolygonCircle(const PolygonView& polygon) {
  const auto& points = polygon.points;
  SDB_ASSERT(points.Size() > 0);
  const auto count = static_cast<double>(points.Size());
  Point center{.x = 0, .y = 0};
  for (duckdb::idx_t i = 0; i < points.Size(); ++i) {
    center = PointAdd(center, points[i]);
  }
  center.x = Div(center.x, count);
  center.y = Div(center.y, count);
  double radius = 0;
  for (duckdb::idx_t i = 0; i < points.Size(); ++i) {
    radius = Add(radius, PointDistance(points[i], center));
  }
  return {.center = center, .radius = Div(radius, count)};
}

Point PolygonCenter(const PolygonView& polygon) {
  return PolygonCircle(polygon).center;
}

Path PolygonPath(const PolygonView& polygon) {
  return {.closed = true, .points = MapPoints(polygon.points, std::identity{})};
}

Polygon BoxPolygon(const Box& box) {
  return {.points = {box.low,
                     {.x = box.low.x, .y = box.high.y},
                     box.high,
                     {.x = box.high.x, .y = box.low.y}}};
}

// Circles.

double CircleArea(const Circle& circle) {
  return Mul(Mul(circle.radius, circle.radius), std::numbers::pi);
}

bool CircleSame(const Circle& c1, const Circle& c2) {
  return ((std::isnan(c1.radius) && std::isnan(c2.radius)) ||
          FpEq(c1.radius, c2.radius)) &&
         PointEq(c1.center, c2.center);
}

bool CircleOverlap(const Circle& c1, const Circle& c2) {
  return FpLe(PointDistance(c1.center, c2.center), Add(c1.radius, c2.radius));
}

bool CircleOverLeft(const Circle& c1, const Circle& c2) {
  return FpLe(Add(c1.center.x, c1.radius), Add(c2.center.x, c2.radius));
}

bool CircleLeft(const Circle& c1, const Circle& c2) {
  return FpLt(Add(c1.center.x, c1.radius), Sub(c2.center.x, c2.radius));
}

bool CircleRight(const Circle& c1, const Circle& c2) {
  return FpGt(Sub(c1.center.x, c1.radius), Add(c2.center.x, c2.radius));
}

bool CircleOverRight(const Circle& c1, const Circle& c2) {
  return FpGe(Sub(c1.center.x, c1.radius), Sub(c2.center.x, c2.radius));
}

bool CircleContainedBy(const Circle& c1, const Circle& c2) {
  return FpLe(PointDistance(c1.center, c2.center), Sub(c2.radius, c1.radius));
}

bool CircleContains(const Circle& c1, const Circle& c2) {
  return FpLe(PointDistance(c1.center, c2.center), Sub(c1.radius, c2.radius));
}

bool CircleBelow(const Circle& c1, const Circle& c2) {
  return FpLt(Add(c1.center.y, c1.radius), Sub(c2.center.y, c2.radius));
}

bool CircleAbove(const Circle& c1, const Circle& c2) {
  return FpGt(Sub(c1.center.y, c1.radius), Add(c2.center.y, c2.radius));
}

bool CircleOverBelow(const Circle& c1, const Circle& c2) {
  return FpLe(Add(c1.center.y, c1.radius), Add(c2.center.y, c2.radius));
}

bool CircleOverAbove(const Circle& c1, const Circle& c2) {
  return FpGe(Sub(c1.center.y, c1.radius), Sub(c2.center.y, c2.radius));
}

Circle CircleAddPoint(const Circle& circle, const Point& point) {
  return {.center = PointAdd(circle.center, point), .radius = circle.radius};
}

Circle CircleSubPoint(const Circle& circle, const Point& point) {
  return {.center = PointSub(circle.center, point), .radius = circle.radius};
}

Circle CircleMulPoint(const Circle& circle, const Point& point) {
  const auto center = PointMul(circle.center, point);
  return {.center = center,
          .radius = Mul(circle.radius, Hypot(point.x, point.y))};
}

Circle CircleDivPoint(const Circle& circle, const Point& point) {
  const auto center = PointDiv(circle.center, point);
  return {.center = center,
          .radius = Div(circle.radius, Hypot(point.x, point.y))};
}

double CircleDiameter(const Circle& circle) { return Mul(circle.radius, 2.0); }

double CircleRadius(const Circle& circle) { return circle.radius; }

double CircleDistance(const Circle& c1, const Circle& c2) {
  const auto result =
    Sub(PointDistance(c1.center, c2.center), Add(c1.radius, c2.radius));
  return result < 0.0 ? 0.0 : result;
}

bool CircleContainsPoint(const Circle& circle, const Point& point) {
  return PointDistance(circle.center, point) <= circle.radius;
}

bool PointInCircle(const Point& point, const Circle& circle) {
  return PointDistance(circle.center, point) <= circle.radius;
}

double PointCircleDistance(const Point& point, const Circle& circle) {
  const auto result = Sub(PointDistance(point, circle.center), circle.radius);
  return result < 0.0 ? 0.0 : result;
}

double CirclePointDistance(const Circle& circle, const Point& point) {
  return PointCircleDistance(point, circle);
}

double CirclePolygonDistance(const Circle& circle, const PolygonView& polygon) {
  const auto result =
    Sub(PointPolygonDistance(circle.center, polygon), circle.radius);
  return result < 0.0 ? 0.0 : result;
}

double PolygonCircleDistance(const PolygonView& polygon, const Circle& circle) {
  return CirclePolygonDistance(circle, polygon);
}

Point CircleCenter(const Circle& circle) { return circle.center; }

Circle MakeCircle(const Point& center, double radius) {
  return {.center = center, .radius = radius};
}

Box CircleBox(const Circle& circle) {
  const auto delta = Div(circle.radius, std::sqrt(2.0));
  const auto high_x = Add(circle.center.x, delta);
  const auto low_x = Sub(circle.center.x, delta);
  const auto high_y = Add(circle.center.y, delta);
  return {.high = {.x = high_x, .y = high_y},
          .low = {.x = low_x, .y = Sub(circle.center.y, delta)}};
}

Circle BoxCircle(const Box& box) {
  const auto center = BoxCenter(box);
  return {.center = center, .radius = PointDistance(center, box.high)};
}

Polygon CirclePolygonWith(int32_t count, const Circle& circle) {
  if (FpZero(circle.radius)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("cannot convert circle with radius zero to polygon"));
  }
  if (count < 2) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("must request at least 2 points"));
  }
  constexpr int64_t kHeader = 40;
  constexpr int64_t kMaxAllocSize = 0x3fffffff;
  const auto size = kHeader + int64_t{16} * count;
  if (size > std::numeric_limits<int32_t>::max()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    ERR_MSG("too many points requested"));
  }
  if (size > kMaxAllocSize) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("invalid memory alloc request size ", size));
  }
  const auto step = Div(2.0 * std::numbers::pi, count);
  std::vector<Point> points;
  points.reserve(count);
  for (int32_t i = 0; i < count; ++i) {
    const auto angle = Mul(step, i);
    const auto x = Sub(circle.center.x, Mul(circle.radius, std::cos(angle)));
    points.push_back(
      {.x = x, .y = Add(circle.center.y, Mul(circle.radius, std::sin(angle)))});
  }
  return {.points = std::move(points)};
}

Polygon CirclePolygon(const Circle& circle) {
  return CirclePolygonWith(kCirclePolygonPoints, circle);
}

template<typename T, double (*Measure)(const T&),
         bool (*Compare)(double, double)>
bool CompareMeasures(const T& left, const T& right) {
  return Compare(Measure(left), Measure(right));
}

template<auto Compare>
bool ComparePointCounts(const PathView& left, const PathView& right) {
  return Compare(left.points.Size(), right.points.Size());
}

// Text input.

class Input {
 public:
  Input(std::string_view text, std::string_view type)
    : _text{text}, _type{type} {}

  char Peek() const { return At(_pos); }

  bool AtEnd() const { return Peek() == '\0'; }

  void SkipSpaces() {
    while (absl::ascii_isspace(static_cast<unsigned char>(Peek()))) {
      ++_pos;
    }
  }

  bool Consume(char c) {
    if (Peek() != c) {
      return false;
    }
    ++_pos;
    return true;
  }

  bool Fail() {
    return Fail(SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid input syntax for type ", _type, ": \"", _text, "\"")));
  }

  bool Fail(irs::pg::SqlErrorData error) {
    _error = std::move(error);
    return false;
  }

  std::optional<irs::pg::SqlErrorData> TakeError() {
    if (!_error) {
      Fail();
    }
    return std::move(_error);
  }

  bool Number(double& value) {
    static constexpr fast_float::parse_options kOptions{
      fast_float::chars_format::general |
      fast_float::chars_format::allow_leading_plus};
    SkipSpaces();
    if (AtEnd()) {
      return Fail();
    }
    const auto* begin = _text.data() + _pos;
    const auto [end, error] = fast_float::from_chars_advanced(
      begin, _text.data() + _text.size(), value, kOptions);
    if (error == std::errc::result_out_of_range) {
      return Fail(SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
        ERR_MSG("\"", std::string_view{begin, end},
                "\" is out of range for type double precision")));
    }
    if (error != std::errc{}) {
      return Fail();
    }
    _pos = static_cast<size_t>(end - _text.data());
    SkipSpaces();
    return true;
  }

  bool Pair(Point& point) {
    SkipSpaces();
    const bool parenthesized = Consume('(');
    if (!Number(point.x)) {
      return false;
    }
    if (!Consume(',')) {
      return Fail();
    }
    if (!Number(point.y)) {
      return false;
    }
    if (parenthesized) {
      if (!Consume(')')) {
        return Fail();
      }
      SkipSpaces();
    }
    return true;
  }

  bool Points(bool open_allowed, std::span<Point> points, bool& open) {
    SkipSpaces();
    open = Peek() == '[';
    bool delimited = open;
    if (open) {
      if (!open_allowed) {
        return Fail();
      }
      ++_pos;
    } else if (Peek() == '(') {
      auto next = _pos + 1;
      while (absl::ascii_isspace(static_cast<unsigned char>(At(next)))) {
        ++next;
      }
      delimited = At(next) == '(' || _text.rfind('(') == _pos;
      if (delimited) {
        _pos = next;
      }
    }
    for (auto& point : points) {
      if (!Pair(point)) {
        return false;
      }
      Consume(',');
    }
    if (delimited) {
      if (!Consume(')') && (!open || !Consume(']'))) {
        return Fail();
      }
      SkipSpaces();
    }
    return true;
  }

  bool SingleParenthesis() {
    if (Peek() != '(' || _text.rfind('(') != _pos) {
      return false;
    }
    ++_pos;
    return true;
  }

  bool DoubleParenthesis() {
    auto next = _pos + 1;
    while (absl::ascii_isspace(static_cast<unsigned char>(At(next)))) {
      ++next;
    }
    if (Peek() != '(' || At(next) != '(') {
      return false;
    }
    _pos = next;
    return true;
  }

  int PointCount() const {
    const auto commas = absl::c_count(_text, ',');
    return commas % 2 ? static_cast<int>((commas + 1) / 2) : -1;
  }

 private:
  char At(size_t pos) const { return pos < _text.size() ? _text[pos] : '\0'; }

  std::string_view _text;
  std::string_view _type;
  size_t _pos = 0;
  std::optional<irs::pg::SqlErrorData> _error;
};

std::optional<irs::pg::SqlErrorData> Parse(std::string_view text,
                                           Point& point) {
  Input input{text, kPointAlias};
  if (input.Pair(point) && input.AtEnd()) {
    return std::nullopt;
  }
  return input.TakeError();
}

std::optional<irs::pg::SqlErrorData> Parse(std::string_view text, Lseg& lseg) {
  Input input{text, kLsegAlias};
  bool open = false;
  if (input.Points(true, lseg.p, open) && input.AtEnd()) {
    return std::nullopt;
  }
  return input.TakeError();
}

std::optional<irs::pg::SqlErrorData> Parse(std::string_view text, Box& box) {
  Input input{text, kBoxAlias};
  std::array<Point, 2> corners{};
  bool open = false;
  if (!input.Points(false, corners, open) || !input.AtEnd()) {
    return input.TakeError();
  }
  box = Reorder({.high = corners[0], .low = corners[1]});
  return std::nullopt;
}

std::optional<irs::pg::SqlErrorData> Parse(std::string_view text, Line& line) {
  Input input{text, kLineAlias};
  input.SkipSpaces();
  if (input.Consume('{')) {
    if (!input.Number(line.a) || !input.Consume(',') || !input.Number(line.b) ||
        !input.Consume(',') || !input.Number(line.c) || !input.Consume('}')) {
      return input.TakeError();
    }
    input.SkipSpaces();
    if (!input.AtEnd()) {
      return input.TakeError();
    }
    if (FpZero(line.a) && FpZero(line.b)) {
      return SQL_ERROR_DATA(
        ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
        ERR_MSG("invalid line specification: A and B cannot both be zero"));
    }
    return std::nullopt;
  }
  Lseg lseg{};
  bool open = false;
  if (!input.Points(true, lseg.p, open) || !input.AtEnd()) {
    return input.TakeError();
  }
  if (PointEq(lseg.p[0], lseg.p[1])) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid line specification: must be two distinct points"));
  }
  line = LineFromSlope(lseg.p[0], LsegSlope(lseg));
  return std::nullopt;
}

std::optional<irs::pg::SqlErrorData> Parse(std::string_view text, Path& path) {
  Input input{text, kPathAlias};
  const auto count = input.PointCount();
  if (count <= 0) {
    return input.TakeError();
  }
  input.SkipSpaces();
  const bool parenthesized = input.SingleParenthesis();
  path.points.resize(count);
  bool open = false;
  if (!input.Points(true, path.points, open)) {
    return input.TakeError();
  }
  if (parenthesized) {
    if (!input.Consume(')')) {
      return input.TakeError();
    }
    input.SkipSpaces();
  }
  if (!input.AtEnd()) {
    return input.TakeError();
  }
  path.closed = !open;
  return std::nullopt;
}

std::optional<irs::pg::SqlErrorData> Parse(std::string_view text,
                                           Polygon& polygon) {
  Input input{text, kPolygonAlias};
  const auto count = input.PointCount();
  if (count <= 0) {
    return input.TakeError();
  }
  polygon.points.resize(count);
  bool open = false;
  if (input.Points(false, polygon.points, open) && input.AtEnd()) {
    return std::nullopt;
  }
  return input.TakeError();
}

std::optional<irs::pg::SqlErrorData> Parse(std::string_view text,
                                           Circle& circle) {
  Input input{text, kCircleAlias};
  input.SkipSpaces();
  const bool delimited = input.Consume('<') || input.DoubleParenthesis();
  if (!input.Pair(circle.center)) {
    return input.TakeError();
  }
  input.Consume(',');
  if (!input.Number(circle.radius)) {
    return input.TakeError();
  }
  if (circle.radius < 0.0) {
    return input.TakeError();
  }
  if (delimited && !input.Consume(')') && !input.Consume('>')) {
    return input.TakeError();
  }
  input.SkipSpaces();
  if (!input.AtEnd()) {
    return input.TakeError();
  }
  return std::nullopt;
}

// Values read from storage or the binary protocol.

std::optional<irs::pg::SqlErrorData> Check(Point&) { return std::nullopt; }

std::optional<irs::pg::SqlErrorData> Check(Lseg&) { return std::nullopt; }

std::optional<irs::pg::SqlErrorData> Check(Box& box) {
  box = Reorder(box);
  return std::nullopt;
}

std::optional<irs::pg::SqlErrorData> Check(Line& line) {
  if (FpZero(line.a) && FpZero(line.b)) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
      ERR_MSG("invalid line specification: A and B cannot both be zero"));
  }
  return std::nullopt;
}

std::optional<irs::pg::SqlErrorData> Check(Circle& circle) {
  if (circle.radius < 0.0) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
      ERR_MSG("invalid radius in external \"circle\" value"));
  }
  return std::nullopt;
}

std::optional<irs::pg::SqlErrorData> Check(Path& path) {
  if (path.points.empty()) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
      ERR_MSG("invalid number of points in external \"path\" value"));
  }
  return std::nullopt;
}

std::optional<irs::pg::SqlErrorData> Check(Polygon& polygon) {
  if (polygon.points.empty()) {
    return SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_INVALID_BINARY_REPRESENTATION),
      ERR_MSG("invalid number of points in external \"polygon\" value"));
  }
  return std::nullopt;
}

constexpr int32_t kMaxBinaryPoints =
  (std::numeric_limits<int32_t>::max() - 40) /
  static_cast<int32_t>(sizeof(Point));

double LoadDouble(const char* data) {
  return std::bit_cast<double>(absl::big_endian::Load64(data));
}

void StoreDouble(uint8_t* data, double value) {
  absl::big_endian::Store64(data, std::bit_cast<uint64_t>(value));
}

template<typename T>
bool RecvFixed(std::string_view data, T& value) {
  std::array<double, sizeof(T) / sizeof(double)> fields{};
  if (data.size() != sizeof(fields)) {
    return false;
  }
  for (size_t i = 0; i < fields.size(); ++i) {
    fields[i] = LoadDouble(data.data() + i * sizeof(double));
  }
  value = std::bit_cast<T>(fields);
  return true;
}

bool RecvPoints(std::string_view data, std::vector<Point>& points) {
  if (data.size() < sizeof(int32_t)) {
    return false;
  }
  const auto count = absl::big_endian::Load<int32_t>(data.data());
  data.remove_prefix(sizeof(int32_t));
  if (count <= 0 || count >= kMaxBinaryPoints ||
      data.size() != static_cast<size_t>(count) * sizeof(Point)) {
    return false;
  }
  points.resize(count);
  for (auto& point : points) {
    point = {.x = LoadDouble(data.data()),
             .y = LoadDouble(data.data() + sizeof(double))};
    data.remove_prefix(sizeof(Point));
  }
  return true;
}

bool Recv(std::string_view data, Point& value) {
  return RecvFixed(data, value);
}

bool Recv(std::string_view data, Line& value) { return RecvFixed(data, value); }

bool Recv(std::string_view data, Lseg& value) { return RecvFixed(data, value); }

bool Recv(std::string_view data, Box& value) { return RecvFixed(data, value); }

bool Recv(std::string_view data, Circle& value) {
  return RecvFixed(data, value);
}

bool Recv(std::string_view data, Path& value) {
  if (data.empty()) {
    return false;
  }
  value.closed = data[0] != 0;
  return RecvPoints(data.substr(1), value.points);
}

bool Recv(std::string_view data, Polygon& value) {
  return RecvPoints(data, value.points);
}

// Text and binary output.

class TextWriter {
 public:
  TextWriter(int8_t extra_float_digits,
             absl::FunctionRef<void(std::string_view)> write)
    : _extra_float_digits{extra_float_digits}, _write{write} {}

  void Text(std::string_view text) { _write(text); }

  void Number(double value) {
    char buffer[utils::kNumberStrMaxLen];
    _write({buffer, _extra_float_digits > 0
                      ? WriteFloat<true>(value, _extra_float_digits, buffer)
                      : WriteFloat<false>(value, _extra_float_digits, buffer)});
  }

  void Pair(const Point& point) {
    Text("(");
    Number(point.x);
    Text(",");
    Number(point.y);
    Text(")");
  }

  void Points(const PointsView& points) {
    for (duckdb::idx_t i = 0; i < points.Size(); ++i) {
      if (i != 0) {
        Text(",");
      }
      Pair(points[i]);
    }
  }

 private:
  int8_t _extra_float_digits;
  absl::FunctionRef<void(std::string_view)> _write;
};

void Write(TextWriter& writer, const Point& point) { writer.Pair(point); }

void Write(TextWriter& writer, const Line& line) {
  writer.Text("{");
  writer.Number(line.a);
  writer.Text(",");
  writer.Number(line.b);
  writer.Text(",");
  writer.Number(line.c);
  writer.Text("}");
}

void Write(TextWriter& writer, const Lseg& lseg) {
  writer.Text("[");
  writer.Pair(lseg.p[0]);
  writer.Text(",");
  writer.Pair(lseg.p[1]);
  writer.Text("]");
}

void Write(TextWriter& writer, const Box& box) {
  writer.Pair(box.high);
  writer.Text(",");
  writer.Pair(box.low);
}

void Write(TextWriter& writer, const PathView& path) {
  writer.Text(path.closed ? "(" : "[");
  writer.Points(path.points);
  writer.Text(path.closed ? ")" : "]");
}

void Write(TextWriter& writer, const PolygonView& polygon) {
  writer.Text("(");
  writer.Points(polygon.points);
  writer.Text(")");
}

void Write(TextWriter& writer, const Circle& circle) {
  writer.Text("<");
  writer.Pair(circle.center);
  writer.Text(",");
  writer.Number(circle.radius);
  writer.Text(">");
}

template<typename T>
void SendFixed(const T& value, absl::FunctionRef<uint8_t*(size_t)> alloc) {
  const auto fields =
    std::bit_cast<std::array<double, sizeof(T) / sizeof(double)>>(value);
  auto* out = alloc(sizeof(fields));
  for (const auto field : fields) {
    StoreDouble(out, field);
    out += sizeof(double);
  }
}

void SendPoints(const PointsView& points, uint8_t* out) {
  absl::big_endian::Store32(out, static_cast<uint32_t>(points.Size()));
  out += sizeof(int32_t);
  for (duckdb::idx_t i = 0; i < points.Size(); ++i) {
    const auto point = points[i];
    StoreDouble(out, point.x);
    StoreDouble(out + sizeof(double), point.y);
    out += sizeof(Point);
  }
}

void Send(const Point& value, absl::FunctionRef<uint8_t*(size_t)> alloc) {
  SendFixed(value, alloc);
}

void Send(const Line& value, absl::FunctionRef<uint8_t*(size_t)> alloc) {
  SendFixed(value, alloc);
}

void Send(const Lseg& value, absl::FunctionRef<uint8_t*(size_t)> alloc) {
  SendFixed(value, alloc);
}

void Send(const Box& value, absl::FunctionRef<uint8_t*(size_t)> alloc) {
  SendFixed(value, alloc);
}

void Send(const Circle& value, absl::FunctionRef<uint8_t*(size_t)> alloc) {
  SendFixed(value, alloc);
}

void Send(const PathView& value, absl::FunctionRef<uint8_t*(size_t)> alloc) {
  auto* out = alloc(1 + sizeof(int32_t) + value.points.Size() * sizeof(Point));
  *out = value.closed ? 1 : 0;
  SendPoints(value.points, out + 1);
}

void Send(const PolygonView& value, absl::FunctionRef<uint8_t*(size_t)> alloc) {
  SendPoints(value.points,
             alloc(sizeof(int32_t) + value.points.Size() * sizeof(Point)));
}

// Kinds.

template<Geometric K>
struct Kind;

template<>
struct Kind<Geometric::Point> {
  using Value = Point;
  using View = Point;
};

template<>
struct Kind<Geometric::Line> {
  using Value = Line;
  using View = Line;
};

template<>
struct Kind<Geometric::Lseg> {
  using Value = Lseg;
  using View = Lseg;
};

template<>
struct Kind<Geometric::Box> {
  using Value = Box;
  using View = Box;
};

template<>
struct Kind<Geometric::Path> {
  using Value = Path;
  using View = PathView;
};

template<>
struct Kind<Geometric::Polygon> {
  using Value = Polygon;
  using View = PolygonView;
};

template<>
struct Kind<Geometric::Circle> {
  using Value = Circle;
  using View = Circle;
};

Path ToValue(const PathView& path) {
  return {.closed = path.closed,
          .points = MapPoints(path.points, std::identity{})};
}

Polygon ToValue(const PolygonView& polygon) {
  return {.points = MapPoints(polygon.points, std::identity{})};
}

template<typename T>
T ToValue(const T& value) {
  return value;
}

duckdb::LogicalType Doubles(std::span<const std::string_view> names) {
  duckdb::child_list_t<duckdb::LogicalType> children;
  for (const auto name : names) {
    children.emplace_back(std::string{name}, duckdb::LogicalType::DOUBLE);
  }
  return duckdb::LogicalType::STRUCT(std::move(children));
}

duckdb::LogicalType PointList() {
  constexpr std::array<std::string_view, 2> kFields = {"x", "y"};
  return duckdb::LogicalType::LIST(Doubles(kFields));
}

duckdb::LogicalType StorageType(Geometric kind) {
  switch (kind) {
    case Geometric::Point: {
      constexpr std::array<std::string_view, 2> kFields = {"x", "y"};
      return Doubles(kFields);
    }
    case Geometric::Line: {
      constexpr std::array<std::string_view, 3> kFields = {"a", "b", "c"};
      return Doubles(kFields);
    }
    case Geometric::Lseg: {
      constexpr std::array<std::string_view, 4> kFields = {"x1", "y1", "x2",
                                                           "y2"};
      return Doubles(kFields);
    }
    case Geometric::Box: {
      constexpr std::array<std::string_view, 4> kFields = {"high_x", "high_y",
                                                           "low_x", "low_y"};
      return Doubles(kFields);
    }
    case Geometric::Path:
      return duckdb::LogicalType::STRUCT({
        {"closed", duckdb::LogicalType::BOOLEAN},
        {"points", PointList()},
      });
    case Geometric::Polygon:
      return duckdb::LogicalType::STRUCT({{"points", PointList()}});
    case Geometric::Circle: {
      constexpr std::array<std::string_view, 3> kFields = {"x", "y", "radius"};
      return Doubles(kFields);
    }
  }
  SDB_UNREACHABLE();
}

struct Alias {
  std::string_view name;
  Geometric kind;
};

constexpr std::array<Alias, 7> kAliases = {{
  {kPointAlias, Geometric::Point},
  {kLineAlias, Geometric::Line},
  {kLsegAlias, Geometric::Lseg},
  {kBoxAlias, Geometric::Box},
  {kPathAlias, Geometric::Path},
  {kPolygonAlias, Geometric::Polygon},
  {kCircleAlias, Geometric::Circle},
}};

duckdb::LogicalType MakeType(Geometric kind) {
  const auto alias = absl::c_find_if(
    kAliases, [&](const Alias& entry) { return entry.kind == kind; });
  return StorageType(kind).WithAlias(std::string{alias->name});
}

// Casts.

template<Geometric K>
bool TextToGeometric(duckdb::Vector& source, duckdb::Vector& result,
                     duckdb::idx_t count, duckdb::CastParameters& parameters) {
  bool converted = true;
  const auto texts = source.Values<duckdb::string_t>();
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto text = texts[row];
    if (text.IsValid()) {
      auto error = ParseGeometric(K, AsView(text.GetValue()), result, row);
      if (!error) {
        continue;
      }
      InputError(std::move(*error), parameters);
      converted = false;
    }
    duckdb::FlatVector::SetNull(result, row, true);
  }
  return converted;
}

struct TextCastData final : duckdb::BoundCastData {
  explicit TextCastData(int8_t extra_float_digits)
    : extra_float_digits{extra_float_digits} {}

  duckdb::unique_ptr<duckdb::BoundCastData> Copy() const final {
    return duckdb::make_uniq<TextCastData>(extra_float_digits);
  }

  int8_t extra_float_digits;
};

template<Geometric K>
bool GeometricToText(duckdb::Vector& source, duckdb::Vector& result,
                     duckdb::idx_t count, duckdb::CastParameters& parameters) {
  const auto extra_float_digits =
    parameters.cast_data->Cast<TextCastData>().extra_float_digits;
  duckdb::RecursiveUnifiedVectorFormat values;
  duckdb::Vector::RecursiveToUnifiedFormat(source, values);
  auto writer = duckdb::FlatVector::Writer<duckdb::string_t>(result, count);
  std::string text;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto index = values.unified.sel->get_index(row);
    if (!values.unified.validity.RowIsValid(index)) {
      writer.WriteNull();
      continue;
    }
    text.clear();
    WriteGeometric(K, values, index, extra_float_digits,
                   [&](std::string_view part) { text.append(part); });
    writer.WriteValue(
      duckdb::string_t{text.data(), static_cast<uint32_t>(text.size())});
  }
  return true;
}

template<Geometric K>
duckdb::BoundCastInfo BindGeometricToText(duckdb::BindCastInput& input,
                                          const duckdb::LogicalType&,
                                          const duckdb::LogicalType&) {
  return {GeometricToText<K>,
          duckdb::make_uniq<TextCastData>(static_cast<int8_t>(
            GetSignedIntSetting(*input.context, "extra_float_digits")))};
}

bool HasNullField(const duckdb::RecursiveUnifiedVectorFormat& value,
                  duckdb::idx_t index) {
  for (const auto& child : value.children) {
    const auto child_index = child.unified.sel->get_index(index);
    if (!child.unified.validity.RowIsValid(child_index)) {
      return true;
    }
    if (child.logical_type.id() == duckdb::LogicalTypeId::LIST) {
      const auto entry =
        duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(
          child.unified)[child_index];
      const auto& points = child.children[0];
      for (duckdb::idx_t i = 0; i < entry.length; ++i) {
        const auto point = points.unified.sel->get_index(entry.offset + i);
        if (!points.unified.validity.RowIsValid(point) ||
            HasNullField(points, point)) {
          return true;
        }
      }
    }
  }
  return false;
}

template<Geometric K>
bool StorageToGeometric(duckdb::Vector& source, duckdb::Vector& result,
                        duckdb::idx_t count,
                        duckdb::CastParameters& parameters) {
  duckdb::RecursiveUnifiedVectorFormat values;
  duckdb::Vector::RecursiveToUnifiedFormat(source, values);
  bool converted = true;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto index = values.unified.sel->get_index(row);
    if (values.unified.validity.RowIsValid(index) &&
        !HasNullField(values, index)) {
      auto value = ToValue(Read<typename Kind<K>::View>(values, index));
      auto error = Check(value);
      if (!error) {
        StoreValue(result, row, value);
        continue;
      }
      InputError(std::move(*error), parameters);
      converted = false;
    }
    duckdb::FlatVector::SetNull(result, row, true);
  }
  return converted;
}

// Registration.

template<typename T>
struct Sql;

template<>
struct Sql<double> {
  using Arg = ScalarArg<double>;
  static duckdb::LogicalType Type() { return duckdb::LogicalType::DOUBLE; }
};

template<>
struct Sql<int32_t> {
  using Arg = ScalarArg<int32_t>;
  static duckdb::LogicalType Type() { return duckdb::LogicalType::INTEGER; }
};

template<>
struct Sql<bool> {
  static duckdb::LogicalType Type() { return duckdb::LogicalType::BOOLEAN; }
};

template<typename T>
struct Sql<std::optional<T>> : Sql<T> {};

template<>
struct Sql<Point> {
  using Arg = GeometricArg<Point>;
  static duckdb::LogicalType Type() { return POINT(); }
};

template<>
struct Sql<Line> {
  using Arg = GeometricArg<Line>;
  static duckdb::LogicalType Type() { return LINE(); }
};

template<>
struct Sql<Lseg> {
  using Arg = GeometricArg<Lseg>;
  static duckdb::LogicalType Type() { return LSEG(); }
};

template<>
struct Sql<Box> {
  using Arg = GeometricArg<Box>;
  static duckdb::LogicalType Type() { return BOX(); }
};

template<>
struct Sql<Circle> {
  using Arg = GeometricArg<Circle>;
  static duckdb::LogicalType Type() { return CIRCLE(); }
};

template<>
struct Sql<PathView> {
  using Arg = GeometricArg<PathView>;
  static duckdb::LogicalType Type() { return PATH(); }
};

template<>
struct Sql<Path> {
  static duckdb::LogicalType Type() { return PATH(); }
};

template<>
struct Sql<PolygonView> {
  using Arg = GeometricArg<PolygonView>;
  static duckdb::LogicalType Type() { return POLYGON(); }
};

template<>
struct Sql<Polygon> {
  static duckdb::LogicalType Type() { return POLYGON(); }
};

template<auto Fn>
duckdb::ScalarFunction Function() {
  return []<typename R, typename... A>(R (*)(A...)) {
    duckdb::scalar_function_t execute;
    if constexpr (sizeof...(A) == 1) {
      execute = Unary<typename Sql<std::remove_cvref_t<A>>::Arg..., Fn>;
    } else {
      static_assert(sizeof...(A) == 2);
      execute = Binary<typename Sql<std::remove_cvref_t<A>>::Arg..., Fn>;
    }
    return Fallible({{Sql<std::remove_cvref_t<A>>::Type()...},
                     Sql<R>::Type(),
                     std::move(execute)});
  }(Fn);
}

template<auto Fn>
void RegisterCast(duckdb::CastFunctionSet& casts) {
  [&]<typename R, typename A>(R (*)(const A&)) {
    casts.RegisterCastFunction(Sql<A>::Type(), Sql<R>::Type(),
                               UnaryCast<typename Sql<A>::Arg, Fn>, -1);
  }(Fn);
}

void Operator(Functions& functions, std::string_view symbol,
              std::string_view procedure, duckdb::ScalarFunction function) {
  Add(functions, procedure, function);
  Add(functions, symbol, std::move(function));
}

void Division(Functions& functions, std::string_view procedure,
              const duckdb::ScalarFunction& function) {
  Add(functions, procedure, function);
  AddDivision(functions, function);
}

template<Geometric K>
void RegisterType(duckdb::CastFunctionSet& casts) {
  const auto type = MakeType(K);
  RegisterTextInput(casts, type, TextToGeometric<K>, InitInput);
  casts.RegisterCastFunction(type, duckdb::LogicalType::VARCHAR,
                             BindGeometricToText<K>, 50);
  RegisterStorageCast<StorageToGeometric<K>>(casts, StorageType(K), type);
}

duckdb::BoundCastInfo BindTextToBoxes(duckdb::BindCastInput& input,
                                      const duckdb::LogicalType&,
                                      const duckdb::LogicalType& target) {
  return duckdb::DefaultCasts::StringToListCast(input, target, ';');
}

void RegisterCasts(duckdb::CastFunctionSet& casts) {
  RegisterType<Geometric::Point>(casts);
  RegisterType<Geometric::Line>(casts);
  RegisterType<Geometric::Lseg>(casts);
  RegisterType<Geometric::Box>(casts);
  RegisterTextInput(casts, duckdb::LogicalType::LIST(MakeType(Geometric::Box)),
                    BindTextToBoxes);
  RegisterType<Geometric::Path>(casts);
  RegisterType<Geometric::Polygon>(casts);
  RegisterType<Geometric::Circle>(casts);
  RegisterCast<&PointBox>(casts);
  RegisterCast<&LsegCenter>(casts);
  RegisterCast<&PathPolygon>(casts);
  RegisterCast<&BoxCenter>(casts);
  RegisterCast<&BoxDiagonal>(casts);
  RegisterCast<&BoxPolygon>(casts);
  RegisterCast<&BoxCircle>(casts);
  RegisterCast<&PolygonCenter>(casts);
  RegisterCast<&PolygonPath>(casts);
  RegisterCast<&PolygonBounds>(casts);
  RegisterCast<&PolygonCircle>(casts);
  RegisterCast<&CircleCenter>(casts);
  RegisterCast<&CircleBox>(casts);
  RegisterCast<&CirclePolygon>(casts);
}

void RegisterPointFunctions(Functions& functions) {
  Add(functions, "point", Function<&ConstructPoint>());
  Add(functions, "point", Function<&BoxCenter>());
  Add(functions, "point", Function<&CircleCenter>());
  Add(functions, "point", Function<&LsegCenter>());
  Add(functions, "point", Function<&PolygonCenter>());
  Add(functions, "slope", Function<&PointSlope>());
  Add(functions, "isvertical", Function<&PointVertical>());
  Add(functions, "ishorizontal", Function<&PointHorizontal>());
  Operator(functions, "+", "point_add", Function<&PointAdd>());
  Operator(functions, "-", "point_sub", Function<&PointSub>());
  Operator(functions, "*", "point_mul", Function<&PointMul>());
  Division(functions, "point_div", Function<&PointDiv>());
  Operator(functions, "<->", "point_distance", Function<&PointDistance>());
  Operator(functions, "<<", "point_left", Function<&PointLeft>());
  Operator(functions, ">>", "point_right", Function<&PointRight>());
  Operator(functions, ">^", "point_above", Function<&PointAbove>());
  Add(functions, "|>>", Function<&PointAbove>());
  Operator(functions, "<^", "point_below", Function<&PointBelow>());
  Add(functions, "<<|", Function<&PointBelow>());
  Operator(functions, "?|", "point_vert", Function<&PointVertical>());
  Operator(functions, "?-", "point_horiz", Function<&PointHorizontal>());
  Operator(functions, "~=", "point_eq", Function<&PointEq>());
  Add(functions, "point_ne", Function<&PointNe>());
  Operator(functions, "<@", "on_pl", Function<&PointOnLine>());
  Operator(functions, "<@", "on_ps", Function<&PointOnLseg>());
  Operator(functions, "<@", "on_pb", Function<&PointInBox>());
  Operator(functions, "<@", "on_ppath", Function<&PointOnPath>());
  Operator(functions, "<@", "pt_contained_poly", Function<&PointInPolygon>());
  Operator(functions, "<@", "pt_contained_circle", Function<&PointInCircle>());
  Operator(functions, "<->", "dist_pl", Function<&PointLineDistance>());
  Operator(functions, "<->", "dist_ps", Function<&PointLsegDistance>());
  Operator(functions, "<->", "dist_pb", Function<&PointBoxDistance>());
  Operator(functions, "<->", "dist_ppath", Function<&PointPathDistance>());
  Operator(functions, "<->", "dist_ppoly", Function<&PointPolygonDistance>());
  Operator(functions, "<->", "dist_pc", Function<&PointCircleDistance>());
  Operator(functions, "##", "close_pl", Function<&PointLineClosest>());
  Operator(functions, "##", "close_ps", Function<&PointLsegClosest>());
  Operator(functions, "##", "close_pb", Function<&PointBoxClosest>());
}

void RegisterLineFunctions(Functions& functions) {
  Add(functions, "line", Function<&LineThrough>());
  Add(functions, "isvertical", Function<&LineVertical>());
  Add(functions, "ishorizontal", Function<&LineHorizontal>());
  Add(functions, "isparallel", Function<&LinesParallel>());
  Add(functions, "isperp", Function<&LinesPerpendicular>());
  Operator(functions, "?#", "line_intersect", Function<&LinesIntersect>());
  Operator(functions, "?||", "line_parallel", Function<&LinesParallel>());
  Operator(functions, "?-|", "line_perp", Function<&LinesPerpendicular>());
  Operator(functions, "?|", "line_vertical", Function<&LineVertical>());
  Add(functions, "line_horizontal", Function<&LineHorizontal>());
  Add(functions, "line_eq", Function<&LinesEqual>());
  Operator(functions, "<->", "line_distance", Function<&LineDistance>());
  Operator(functions, "#", "line_interpt", Function<&LineIntersection>());
  Operator(functions, "<->", "dist_lp", Function<&LinePointDistance>());
  Operator(functions, "<->", "dist_ls", Function<&LineLsegDistance>());
  Operator(functions, "##", "close_ls", Function<&LineLsegClosest>());
  Operator(functions, "?#", "inter_lb", Function<&LineIntersectsBox>());
}

void RegisterLsegFunctions(Functions& functions) {
  Add(functions, "lseg", Function<&LsegThrough>());
  Add(functions, "lseg", Function<&BoxDiagonal>());
  Add(functions, "length", Function<&LsegLength>());
  Add(functions, "isvertical", Function<&LsegVertical>());
  Add(functions, "ishorizontal", Function<&LsegHorizontal>());
  Add(functions, "isparallel", Function<&LsegsParallel>());
  Add(functions, "isperp", Function<&LsegsPerpendicular>());
  Operator(functions, "@-@", "lseg_length", Function<&LsegLength>());
  Operator(functions, "?#", "lseg_intersect", Function<&LsegsIntersect>());
  Operator(functions, "?||", "lseg_parallel", Function<&LsegsParallel>());
  Operator(functions, "?-|", "lseg_perp", Function<&LsegsPerpendicular>());
  Operator(functions, "?|", "lseg_vertical", Function<&LsegVertical>());
  Add(functions, "lseg_horizontal", Function<&LsegHorizontal>());
  Add(functions, "lseg_eq", Function<&LsegsEqual>());
  Add(functions, "lseg_ne", Function<&LsegsDiffer>());
  Add(functions, "lseg_lt",
      Function<&CompareMeasures<Lseg, &LsegLength, &FpLt>>());
  Add(functions, "lseg_le",
      Function<&CompareMeasures<Lseg, &LsegLength, &FpLe>>());
  Add(functions, "lseg_gt",
      Function<&CompareMeasures<Lseg, &LsegLength, &FpGt>>());
  Add(functions, "lseg_ge",
      Function<&CompareMeasures<Lseg, &LsegLength, &FpGe>>());
  Operator(functions, "<->", "lseg_distance", Function<&LsegDistance>());
  Operator(functions, "@@", "lseg_center", Function<&LsegCenter>());
  Operator(functions, "#", "lseg_interpt", Function<&LsegIntersection>());
  Operator(functions, "<->", "dist_sp", Function<&LsegPointDistance>());
  Operator(functions, "<->", "dist_sl", Function<&LsegLineDistance>());
  Operator(functions, "<->", "dist_sb", Function<&LsegBoxDistance>());
  Operator(functions, "##", "close_lseg", Function<&LsegLsegClosest>());
  Operator(functions, "##", "close_sb", Function<&LsegBoxClosest>());
  Operator(functions, "<@", "on_sl", Function<&LsegOnLine>());
  Operator(functions, "<@", "on_sb", Function<&LsegInBox>());
  Operator(functions, "?#", "inter_sl", Function<&LsegIntersectsLine>());
  Operator(functions, "?#", "inter_sb", Function<&LsegIntersectsBox>());
}

void RegisterBoxFunctions(Functions& functions) {
  Add(functions, "box", Function<&PointBox>());
  Add(functions, "box", Function<&MakeBox>());
  Add(functions, "box", Function<&PolygonBounds>());
  Add(functions, "box", Function<&CircleBox>());
  Add(functions, "area", Function<&BoxArea>());
  Add(functions, "width", Function<&BoxWidth>());
  Add(functions, "height", Function<&BoxHeight>());
  Add(functions, "center", Function<&BoxCenter>());
  Add(functions, "diagonal", Function<&BoxDiagonal>());
  Add(functions, "bound_box", Function<&BoundBox>());
  Operator(functions, "~=", "box_same", Function<&BoxSame>());
  Operator(functions, "&&", "box_overlap", Function<&BoxOverlap>());
  Add(functions, "?#", Function<&BoxOverlap>());
  Operator(functions, "<<", "box_left", Function<&BoxLeft>());
  Operator(functions, "&<", "box_overleft", Function<&BoxOverLeft>());
  Operator(functions, ">>", "box_right", Function<&BoxRight>());
  Operator(functions, "&>", "box_overright", Function<&BoxOverRight>());
  Operator(functions, "<<|", "box_below", Function<&BoxBelow>());
  Operator(functions, "&<|", "box_overbelow", Function<&BoxOverBelow>());
  Operator(functions, "|>>", "box_above", Function<&BoxAbove>());
  Operator(functions, "|&>", "box_overabove", Function<&BoxOverAbove>());
  Operator(functions, "<@", "box_contained", Function<&BoxContainedBy>());
  Operator(functions, "@>", "box_contain", Function<&BoxContainsBox>());
  Operator(functions, "<^", "box_below_eq", Function<&BoxBelowEq>());
  Operator(functions, ">^", "box_above_eq", Function<&BoxAboveEq>());
  Add(functions, "box_lt", Function<&CompareMeasures<Box, &BoxArea, &FpLt>>());
  Add(functions, "box_le", Function<&CompareMeasures<Box, &BoxArea, &FpLe>>());
  Add(functions, "box_eq", Function<&CompareMeasures<Box, &BoxArea, &FpEq>>());
  Add(functions, "box_gt", Function<&CompareMeasures<Box, &BoxArea, &FpGt>>());
  Add(functions, "box_ge", Function<&CompareMeasures<Box, &BoxArea, &FpGe>>());
  Operator(functions, "<->", "box_distance", Function<&BoxDistance>());
  Operator(functions, "@@", "box_center", Function<&BoxCenter>());
  Operator(functions, "#", "box_intersect", Function<&BoxIntersection>());
  Operator(functions, "+", "box_add", Function<&BoxAddPoint>());
  Operator(functions, "-", "box_sub", Function<&BoxSubPoint>());
  Operator(functions, "*", "box_mul", Function<&BoxMulPoint>());
  Division(functions, "box_div", Function<&BoxDivPoint>());
  Operator(functions, "@>", "box_contain_pt", Function<&BoxContainsPoint>());
  Operator(functions, "<->", "dist_bp", Function<&BoxPointDistance>());
  Operator(functions, "<->", "dist_bs", Function<&BoxLsegDistance>());
}

void RegisterPathFunctions(Functions& functions) {
  Add(functions, "path", Function<&PolygonPath>());
  Add(functions, "area", Function<&PathArea>());
  Add(functions, "length", Function<&PathLength>());
  Add(functions, "npoints", Function<&PathPointCount>());
  Add(functions, "isclosed", Function<&PathIsClosed>());
  Add(functions, "isopen", Function<&PathIsOpen>());
  Add(functions, "pclose", Function<&PathClose>());
  Add(functions, "popen", Function<&PathOpen>());
  Operator(functions, "@-@", "path_length", Function<&PathLength>());
  Operator(functions, "#", "path_npoints", Function<&PathPointCount>());
  Operator(functions, "?#", "path_inter", Function<&PathsIntersect>());
  Operator(functions, "<->", "path_distance", Function<&PathDistance>());
  Operator(functions, "+", "path_add", Function<&PathConcat>());
  Operator(functions, "+", "path_add_pt", Function<&PathMap<&PointAdd>>());
  Operator(functions, "-", "path_sub_pt", Function<&PathMap<&PointSub>>());
  Operator(functions, "*", "path_mul_pt", Function<&PathMap<&PointMul>>());
  Division(functions, "path_div_pt", Function<&PathMap<&PointDiv>>());
  Operator(functions, "@>", "path_contain_pt", Function<&PathContainsPoint>());
  Operator(functions, "<->", "dist_pathp", Function<&PathPointDistance>());
  Add(functions, "path_n_lt", Function<&ComparePointCounts<std::less<>{}>>());
  Add(functions, "path_n_le",
      Function<&ComparePointCounts<std::less_equal<>{}>>());
  Add(functions, "path_n_eq",
      Function<&ComparePointCounts<std::equal_to<>{}>>());
  Add(functions, "path_n_gt",
      Function<&ComparePointCounts<std::greater<>{}>>());
  Add(functions, "path_n_ge",
      Function<&ComparePointCounts<std::greater_equal<>{}>>());
}

void RegisterPolygonFunctions(Functions& functions) {
  Add(functions, "polygon", Function<&BoxPolygon>());
  Add(functions, "polygon", Function<&CirclePolygon>());
  Add(functions, "polygon", Function<&CirclePolygonWith>());
  Add(functions, "polygon", Function<&PathPolygon>());
  Add(functions, "npoints", Function<&PolygonPointCount>());
  Operator(functions, "#", "poly_npoints", Function<&PolygonPointCount>());
  Operator(functions, "<<", "poly_left", Function<&PolygonLeft>());
  Operator(functions, "&<", "poly_overleft", Function<&PolygonOverLeft>());
  Operator(functions, ">>", "poly_right", Function<&PolygonRight>());
  Operator(functions, "&>", "poly_overright", Function<&PolygonOverRight>());
  Operator(functions, "<<|", "poly_below", Function<&PolygonBelow>());
  Operator(functions, "&<|", "poly_overbelow", Function<&PolygonOverBelow>());
  Operator(functions, "|>>", "poly_above", Function<&PolygonAbove>());
  Operator(functions, "|&>", "poly_overabove", Function<&PolygonOverAbove>());
  Operator(functions, "~=", "poly_same", Function<&PolygonSame>());
  Operator(functions, "&&", "poly_overlap", Function<&PolygonsOverlap>());
  Operator(functions, "@>", "poly_contain", Function<&PolygonContains>());
  Operator(functions, "<@", "poly_contained", Function<&PolygonContainedBy>());
  Operator(functions, "@>", "poly_contain_pt",
           Function<&PolygonContainsPoint>());
  Operator(functions, "<->", "poly_distance", Function<&PolygonDistance>());
  Operator(functions, "@@", "poly_center", Function<&PolygonCenter>());
  Operator(functions, "<->", "dist_polyp", Function<&PolygonPointDistance>());
  Operator(functions, "<->", "dist_polyc", Function<&PolygonCircleDistance>());
}

void RegisterCircleFunctions(Functions& functions) {
  Add(functions, "circle", Function<&BoxCircle>());
  Add(functions, "circle", Function<&MakeCircle>());
  Add(functions, "circle", Function<&PolygonCircle>());
  Add(functions, "area", Function<&CircleArea>());
  Add(functions, "center", Function<&CircleCenter>());
  Add(functions, "diameter", Function<&CircleDiameter>());
  Add(functions, "radius", Function<&CircleRadius>());
  Operator(functions, "~=", "circle_same", Function<&CircleSame>());
  Operator(functions, "&&", "circle_overlap", Function<&CircleOverlap>());
  Operator(functions, "&<", "circle_overleft", Function<&CircleOverLeft>());
  Operator(functions, "<<", "circle_left", Function<&CircleLeft>());
  Operator(functions, ">>", "circle_right", Function<&CircleRight>());
  Operator(functions, "&>", "circle_overright", Function<&CircleOverRight>());
  Operator(functions, "<@", "circle_contained", Function<&CircleContainedBy>());
  Operator(functions, "@>", "circle_contain", Function<&CircleContains>());
  Operator(functions, "<<|", "circle_below", Function<&CircleBelow>());
  Operator(functions, "|>>", "circle_above", Function<&CircleAbove>());
  Operator(functions, "&<|", "circle_overbelow", Function<&CircleOverBelow>());
  Operator(functions, "|&>", "circle_overabove", Function<&CircleOverAbove>());
  Add(functions, "circle_eq",
      Function<&CompareMeasures<Circle, &CircleArea, &FpEq>>());
  Add(functions, "circle_ne",
      Function<&CompareMeasures<Circle, &CircleArea, &FpNe>>());
  Add(functions, "circle_lt",
      Function<&CompareMeasures<Circle, &CircleArea, &FpLt>>());
  Add(functions, "circle_le",
      Function<&CompareMeasures<Circle, &CircleArea, &FpLe>>());
  Add(functions, "circle_gt",
      Function<&CompareMeasures<Circle, &CircleArea, &FpGt>>());
  Add(functions, "circle_ge",
      Function<&CompareMeasures<Circle, &CircleArea, &FpGe>>());
  Operator(functions, "+", "circle_add_pt", Function<&CircleAddPoint>());
  Operator(functions, "-", "circle_sub_pt", Function<&CircleSubPoint>());
  Operator(functions, "*", "circle_mul_pt", Function<&CircleMulPoint>());
  Division(functions, "circle_div_pt", Function<&CircleDivPoint>());
  Operator(functions, "<->", "circle_distance", Function<&CircleDistance>());
  Operator(functions, "@>", "circle_contain_pt",
           Function<&CircleContainsPoint>());
  Operator(functions, "<->", "dist_cpoint", Function<&CirclePointDistance>());
  Operator(functions, "<->", "dist_cpoly", Function<&CirclePolygonDistance>());
  Operator(functions, "@@", "circle_center", Function<&CircleCenter>());
}

}  // namespace
}  // namespace geo

duckdb::LogicalType POINT() {
  static const auto kType = geo::MakeType(Geometric::Point);
  return kType;
}

duckdb::LogicalType LINE() {
  static const auto kType = geo::MakeType(Geometric::Line);
  return kType;
}

duckdb::LogicalType LSEG() {
  static const auto kType = geo::MakeType(Geometric::Lseg);
  return kType;
}

duckdb::LogicalType BOX() {
  static const auto kType = geo::MakeType(Geometric::Box);
  return kType;
}

duckdb::LogicalType PATH() {
  static const auto kType = geo::MakeType(Geometric::Path);
  return kType;
}

duckdb::LogicalType POLYGON() {
  static const auto kType = geo::MakeType(Geometric::Polygon);
  return kType;
}

duckdb::LogicalType CIRCLE() {
  static const auto kType = geo::MakeType(Geometric::Circle);
  return kType;
}

std::optional<Geometric> GeometricOf(const duckdb::LogicalType& type) {
  if (type.id() != duckdb::LogicalTypeId::STRUCT || !type.HasAlias()) {
    return std::nullopt;
  }
  const auto& name = type.GetAlias();
  for (const auto& alias : geo::kAliases) {
    if (name == alias.name) {
      return alias.kind;
    }
  }
  return std::nullopt;
}

std::optional<irs::pg::SqlErrorData> ParseGeometric(Geometric kind,
                                                    std::string_view text,
                                                    duckdb::Vector& result,
                                                    duckdb::idx_t row) {
  return VisitGeometric(
    kind, [&]<Geometric K>() -> std::optional<irs::pg::SqlErrorData> {
      typename geo::Kind<K>::Value value{};
      if (auto error = geo::Parse(text, value)) {
        return error;
      }
      geo::StoreValue(result, row, value);
      return std::nullopt;
    });
}

bool RecvGeometric(Geometric kind, std::string_view data,
                   duckdb::Vector& result, duckdb::idx_t row) {
  return VisitGeometric(kind, [&]<Geometric K>() {
    typename geo::Kind<K>::Value value{};
    if (!geo::Recv(data, value) || geo::Check(value)) {
      return false;
    }
    geo::StoreValue(result, row, value);
    return true;
  });
}

void WriteGeometric(Geometric kind,
                    const duckdb::RecursiveUnifiedVectorFormat& value,
                    duckdb::idx_t index, int8_t extra_float_digits,
                    absl::FunctionRef<void(std::string_view)> write) {
  geo::TextWriter writer{extra_float_digits, write};
  VisitGeometric(kind, [&]<Geometric K>() {
    geo::Write(writer, geo::Read<typename geo::Kind<K>::View>(value, index));
  });
}

void SendGeometric(Geometric kind,
                   const duckdb::RecursiveUnifiedVectorFormat& value,
                   duckdb::idx_t index,
                   absl::FunctionRef<uint8_t*(size_t)> alloc) {
  VisitGeometric(kind, [&]<Geometric K>() {
    geo::Send(geo::Read<typename geo::Kind<K>::View>(value, index), alloc);
  });
}

void RegisterGeometric(duckdb::CastFunctionSet& casts, Functions& functions) {
  geo::RegisterCasts(casts);
  geo::RegisterPointFunctions(functions);
  geo::RegisterLineFunctions(functions);
  geo::RegisterLsegFunctions(functions);
  geo::RegisterBoxFunctions(functions);
  geo::RegisterPathFunctions(functions);
  geo::RegisterPolygonFunctions(functions);
  geo::RegisterCircleFunctions(functions);
}

}  // namespace sdb::pg
