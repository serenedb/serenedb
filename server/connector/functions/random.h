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

#include <cstdint>
#include <duckdb/common/random_engine.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>

namespace sdb::connector {

struct RandomState final : duckdb::FunctionLocalState {
  explicit RandomState(uint64_t seed) { random.SetSeed(seed); }

  duckdb::RandomEngine random;
};

inline duckdb::unique_ptr<duckdb::FunctionLocalState> InitRandomState(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData*) {
  auto& random = duckdb::RandomEngine::Get(state.GetContext());
  duckdb::lock_guard<duckdb::mutex> guard{random.lock};
  return duckdb::make_uniq<RandomState>(random.NextRandomInteger64());
}

}  // namespace sdb::connector
