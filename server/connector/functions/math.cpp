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
#include <duckdb/common/types/interval.hpp>
#include <duckdb/common/types/timestamp.hpp>
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

// div(y, x) -> bigint — PG-compatible, ported from Velox PgDiv
void DivIntFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<int64_t, int64_t, int64_t>(
    args.data[0], args.data[1], result, args.size(),
    [](int64_t y, int64_t x) -> int64_t {
      if (x == 0) {
        throw duckdb::InvalidInputException("division by zero");
      }
      if (y == std::numeric_limits<int64_t>::min() && x == -1) {
        throw duckdb::InvalidInputException("bigint out of range");
      }
      return y / x;
    });
}

void DivDoubleFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<double, double, int64_t>(
    args.data[0], args.data[1], result, args.size(),
    [](double y, double x) -> int64_t {
      if (x == 0.0) {
        throw duckdb::InvalidInputException("division by zero");
      }
      double truncated = std::trunc(y / x);
      if (truncated < static_cast<double>(std::numeric_limits<int64_t>::min()) ||
          truncated > static_cast<double>(std::numeric_limits<int64_t>::max())) {
        throw duckdb::InvalidInputException("bigint out of range");
      }
      return static_cast<int64_t>(truncated);
    });
}

// date_bin(stride, source, origin) -> timestamp
// Ported from Velox PgDateBin — same logic, DuckDB types.
void DateBinFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  duckdb::TernaryExecutor::Execute<duckdb::interval_t, duckdb::timestamp_t,
                                   duckdb::timestamp_t, duckdb::timestamp_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [](duckdb::interval_t stride, duckdb::timestamp_t source,
       duckdb::timestamp_t origin) -> duckdb::timestamp_t {
      if (stride.micros <= 0 && stride.days <= 0 && stride.months <= 0) {
        throw duckdb::InvalidInputException(
          "stride must be greater than zero");
      }
      if (stride.months != 0) {
        throw duckdb::InvalidInputException(
          "timestamps cannot be binned into intervals containing "
          "months or years");
      }

      int64_t stride_us =
        stride.micros + static_cast<int64_t>(stride.days) * 86400'000'000LL;
      if (stride_us <= 0) {
        throw duckdb::InvalidInputException(
          "stride must be greater than zero");
      }

      int64_t src_us = source.value;
      int64_t orig_us = origin.value;

      int64_t diff = src_us - orig_us;
      // Floor division.
      int64_t bin = diff >= 0
                      ? (diff / stride_us) * stride_us
                      : ((diff - stride_us + 1) / stride_us) * stride_us;

      return duckdb::timestamp_t(orig_us + bin);
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

  // div(y, x) -> bigint
  {
    duckdb::ScalarFunctionSet div_set("div");
    div_set.AddFunction(duckdb::ScalarFunction{
      "div", {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT},
      duckdb::LogicalType::BIGINT, DivIntFunction});
    div_set.AddFunction(duckdb::ScalarFunction{
      "div", {duckdb::LogicalType::DOUBLE, duckdb::LogicalType::DOUBLE},
      duckdb::LogicalType::BIGINT, DivDoubleFunction});
    loader.RegisterFunction(div_set);
  }

  // date_bin(stride interval, source timestamp, origin timestamp) -> timestamp
  loader.RegisterFunction(duckdb::ScalarFunction{
    "date_bin",
    {duckdb::LogicalType::INTERVAL, duckdb::LogicalType::TIMESTAMP,
     duckdb::LogicalType::TIMESTAMP},
    duckdb::LogicalType::TIMESTAMP, DateBinFunction});

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
