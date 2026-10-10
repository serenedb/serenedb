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

#include "connector/functions/curve.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/parser/parsed_data/create_coordinate_system_info.hpp>

#include "connector/curve_index.h"
#include "connector/geo_validate.h"

namespace sdb::connector {
namespace {

void BoxContains(duckdb::DataChunk& args, duckdb::ExpressionState&,
                 duckdb::Vector& result) {
  for (const auto& arg : args.data) {
    if (arg.GetType().id() == duckdb::LogicalTypeId::SQLNULL) {
      result.Reference(duckdb::Value{duckdb::LogicalType::BOOLEAN},
                       duckdb::count_t{args.size()});
      return;
    }
  }
  const auto& point_type = args.data[0].GetType();
  ValidateCurveBounds(point_type, args.data[1].GetType(),
                      args.data[2].GetType());
  const auto dimensions =
    static_cast<uint32_t>(duckdb::StructType::GetChildCount(point_type));
  const CurveTuples points{args.data[0], point_type};
  const CurveTuples lower{args.data[1], point_type};
  const CurveTuples upper{args.data[2], point_type};
  auto* matches = duckdb::FlatVector::GetDataMutable<bool>(result);
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    if (!points.IsValid(row) || !lower.IsValid(row) || !upper.IsValid(row)) {
      duckdb::FlatVector::SetNull(result, row, true);
      continue;
    }
    bool match = true;
    bool unknown = false;
    for (uint32_t axis = 0; axis < dimensions; ++axis) {
      const bool has_min = lower.IsValid(row, axis);
      const bool has_max = upper.IsValid(row, axis);
      if (!has_min && !has_max) {
        continue;
      }
      if (!points.IsValid(row, axis)) {
        unknown = true;
        continue;
      }
      const auto value = points.Get(row, axis);
      match &= (!has_min || value >= lower.Get(row, axis)) &&
               (!has_max || value <= upper.Get(row, axis));
    }
    if (match && unknown) {
      duckdb::FlatVector::SetNull(result, row, true);
      continue;
    }
    matches[row] = match;
  }
}

}  // namespace

void RegisterCurveFunctions(duckdb::ExtensionLoader& loader) {
  duckdb::CreateCoordinateSystemInfo crs{
    duckdb::Identifier{kCartesianCRS}, "SDB", "CARTESIAN", {}, {}};
  crs.internal = true;
  crs.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  loader.RegisterCoordinateSystem(crs);
  duckdb::ScalarFunction function{
    "sdb_box_contains",
    {duckdb::LogicalType::ANY, duckdb::LogicalType::ANY,
     duckdb::LogicalType::ANY},
    duckdb::LogicalType::BOOLEAN,
    BoxContains};
  function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  loader.RegisterFunction(function);
}

}  // namespace sdb::connector
