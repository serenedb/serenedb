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

#include "connector/functions/math.h"

#include <cmath>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <random>

namespace sdb::connector {
namespace {

void ErfFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                 duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<double, double>(
    args.data[0], result, args.size(),
    [](double x) -> double { return std::erf(x); });
}

void ErfcFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                  duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<double, double>(
    args.data[0], result, args.size(),
    [](double x) -> double { return std::erfc(x); });
}

void RandomNormalFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                          duckdb::Vector& result) {
  thread_local std::mt19937_64 gen{std::random_device{}()};
  auto& mean_vec = args.data[0];
  auto& stddev_vec = args.data[1];
  duckdb::BinaryExecutor::Execute<double, double, double>(
    mean_vec, stddev_vec, result, args.size(),
    [](double mean, double stddev) -> double {
      std::normal_distribution<double> dist(mean, stddev);
      return dist(gen);
    });
}

}  // namespace

void RegisterPgMathFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  loader.RegisterFunction(duckdb::ScalarFunction{"erf",
                                                 {duckdb::LogicalType::DOUBLE},
                                                 duckdb::LogicalType::DOUBLE,
                                                 ErfFunction});

  loader.RegisterFunction(duckdb::ScalarFunction{"erfc",
                                                 {duckdb::LogicalType::DOUBLE},
                                                 duckdb::LogicalType::DOUBLE,
                                                 ErfcFunction});

  {
    duckdb::ScalarFunction func{
      "random_normal",
      {duckdb::LogicalType::DOUBLE, duckdb::LogicalType::DOUBLE},
      duckdb::LogicalType::DOUBLE,
      RandomNormalFunction};
    func.SetVolatile();
    loader.RegisterFunction(func);
  }
}

}  // namespace sdb::connector
