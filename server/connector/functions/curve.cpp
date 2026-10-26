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
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/parser/parsed_data/create_coordinate_system_info.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>

#include "connector/curve_index.h"
#include "connector/geo_validate.h"

namespace sdb::connector {
namespace {

duckdb::unique_ptr<duckdb::FunctionData> CurveBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& function = input.GetBoundFunction();
  auto& args = input.GetArguments();
  auto spec = BindCurveSpec(input.GetClientContext(),
                            function.GetDefinition()->GetSignature(), args);
  auto& types = function.GetArguments();
  for (size_t i = 0; i < args.size(); ++i) {
    types[i] = args[i]->GetReturnType();
  }
  return spec;
}

void CurveFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                   duckdb::Vector& result) {
  const auto& spec = state.expr.Cast<duckdb::BoundFunctionExpression>()
                       .BindInfo()
                       ->Cast<CurveSpec>();
  EncodeCurveCodes(spec, args, result);
}

}  // namespace

void RegisterCurveFunctions(duckdb::ExtensionLoader& loader) {
  duckdb::CreateCoordinateSystemInfo crs{
    duckdb::Identifier{kCartesianCRS}, "SDB", "CARTESIAN", {}, {}};
  crs.internal = true;
  crs.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  loader.RegisterCoordinateSystem(crs);
  duckdb::ScalarFunction function{
    duckdb::Identifier{kCurveFunction}, duckdb::vector<duckdb::LogicalType>{},
    duckdb::LogicalType::BLOB, CurveFunction, CurveBind};
  function.GetSignature()
    .AddPositionalOnly("point", duckdb::LogicalTypeId::STRUCT)
    .AddKeywordOnly("kind", duckdb::LogicalType::VARCHAR,
                    duckdb::Value{"morton"})
    .AddKeywordOnly("max_level", duckdb::LogicalType::INTEGER,
                    duckdb::Value{duckdb::LogicalType::INTEGER})
    .AddKeywordOnly("level_step", duckdb::LogicalType::INTEGER,
                    duckdb::Value{duckdb::LogicalType::INTEGER})
    .AddKeywordOnly("max_cells", duckdb::LogicalType::INTEGER,
                    duckdb::Value{duckdb::LogicalType::INTEGER})
    .AddKeywordOnly("lower", duckdb::LogicalTypeId::STRUCT, duckdb::Value{})
    .AddKeywordOnly("upper", duckdb::LogicalTypeId::STRUCT, duckdb::Value{});
  function.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  loader.RegisterFunction(function);
}

}  // namespace sdb::connector
