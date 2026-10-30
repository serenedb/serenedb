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

#pragma once

#include <absl/functional/function_ref.h>

#include <cstddef>
#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <optional>
#include <string_view>

#include "pg/types/function.h"

namespace sdb::pg {

inline constexpr std::string_view kPointAlias = "point";
inline constexpr std::string_view kLineAlias = "line";
inline constexpr std::string_view kLsegAlias = "lseg";
inline constexpr std::string_view kBoxAlias = "box";
inline constexpr std::string_view kPathAlias = "path";
inline constexpr std::string_view kPolygonAlias = "polygon";
inline constexpr std::string_view kCircleAlias = "circle";

enum class Geometric : uint8_t {
  Point,
  Line,
  Lseg,
  Box,
  Path,
  Polygon,
  Circle,
};

duckdb::LogicalType POINT();
duckdb::LogicalType LINE();
duckdb::LogicalType LSEG();
duckdb::LogicalType BOX();
duckdb::LogicalType PATH();
duckdb::LogicalType POLYGON();
duckdb::LogicalType CIRCLE();

std::optional<Geometric> GeometricOf(const duckdb::LogicalType& type);

template<typename Visit>
decltype(auto) VisitGeometric(Geometric kind, Visit&& visit) {
  switch (kind) {
    case Geometric::Point:
      return visit.template operator()<Geometric::Point>();
    case Geometric::Line:
      return visit.template operator()<Geometric::Line>();
    case Geometric::Lseg:
      return visit.template operator()<Geometric::Lseg>();
    case Geometric::Box:
      return visit.template operator()<Geometric::Box>();
    case Geometric::Path:
      return visit.template operator()<Geometric::Path>();
    case Geometric::Polygon:
      return visit.template operator()<Geometric::Polygon>();
    case Geometric::Circle:
      return visit.template operator()<Geometric::Circle>();
  }
  SDB_UNREACHABLE();
}

std::optional<irs::pg::SqlErrorData> ParseGeometric(Geometric kind,
                                                    std::string_view text,
                                                    duckdb::Vector& result,
                                                    duckdb::idx_t row);

bool RecvGeometric(Geometric kind, std::string_view data,
                   duckdb::Vector& result, duckdb::idx_t row);

void WriteGeometric(Geometric kind,
                    const duckdb::RecursiveUnifiedVectorFormat& value,
                    duckdb::idx_t index, int8_t extra_float_digits,
                    absl::FunctionRef<void(std::string_view)> write);

void SendGeometric(Geometric kind,
                   const duckdb::RecursiveUnifiedVectorFormat& value,
                   duckdb::idx_t index,
                   absl::FunctionRef<uint8_t*(size_t)> alloc);

void RegisterGeometric(duckdb::CastFunctionSet& casts, Functions& functions);

}  // namespace sdb::pg
