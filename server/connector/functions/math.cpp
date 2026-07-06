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

#include <absl/strings/str_format.h>

#include <cmath>
#include <cstdint>
#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/interval.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <limits>
#include <random>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

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

// div(y, x) -> bigint -- PG-compatible, ported from Velox PgDiv
void DivIntFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<int64_t, int64_t, int64_t>(
    args.data[0], args.data[1], result, args.size(),
    [](int64_t y, int64_t x) -> int64_t {
      if (x == 0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DIVISION_BY_ZERO),
                        ERR_MSG("division by zero"));
      }
      if (y == std::numeric_limits<int64_t>::min() && x == -1) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        ERR_MSG("bigint out of range"));
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
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DIVISION_BY_ZERO),
                        ERR_MSG("division by zero"));
      }
      double truncated = std::trunc(y / x);
      if (truncated <
            static_cast<double>(std::numeric_limits<int64_t>::min()) ||
          truncated >
            static_cast<double>(std::numeric_limits<int64_t>::max())) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        ERR_MSG("bigint out of range"));
      }
      return static_cast<int64_t>(truncated);
    });
}

// gcd(a, b) -> int32 -- PG-compatible, ported from Velox PgGcd
void GcdInt32Function(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<int32_t, int32_t, int32_t>(
    args.data[0], args.data[1], result, args.size(),
    [](int32_t a, int32_t b) -> int32_t {
      if (a == std::numeric_limits<int32_t>::min() ||
          b == std::numeric_limits<int32_t>::min()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        ERR_MSG("integer out of range"));
      }
      return static_cast<int32_t>(std::gcd(a, b));
    });
}

// gcd(a, b) -> int64 -- PG-compatible, ported from Velox PgGcd
void GcdInt64Function(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<int64_t, int64_t, int64_t>(
    args.data[0], args.data[1], result, args.size(),
    [](int64_t a, int64_t b) -> int64_t {
      if (a == std::numeric_limits<int64_t>::min() ||
          b == std::numeric_limits<int64_t>::min()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        ERR_MSG("bigint out of range"));
      }
      return std::gcd(a, b);
    });
}

// lcm helper -- ported from Velox PgLcm
template<typename S>
S LcmImpl(S s1, S s2, const char* msg) {
  if (s1 == 0 || s2 == 0) {
    return 0;
  }
  using U = std::make_unsigned_t<S>;
  auto abs_impl = [](S s) -> U {
    if (s >= 0) {
      return s;
    }
    if (s == std::numeric_limits<S>::min()) {
      return -static_cast<U>(s);
    }
    return -s;
  };
  U u1 = abs_impl(s1) / std::gcd(abs_impl(s1), abs_impl(s2));
  U u2 = abs_impl(s2);
  U ur;
  if (__builtin_mul_overflow(u1, u2, &ur) ||
      ur > static_cast<U>(std::numeric_limits<S>::max())) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), ERR_MSG(msg));
  }
  return static_cast<S>(ur);
}

void LcmInt32Function(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<int32_t, int32_t, int32_t>(
    args.data[0], args.data[1], result, args.size(),
    [](int32_t a, int32_t b) -> int32_t {
      return LcmImpl(a, b, "integer out of range");
    });
}

void LcmInt64Function(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<int64_t, int64_t, int64_t>(
    args.data[0], args.data[1], result, args.size(),
    [](int64_t a, int64_t b) -> int64_t {
      return LcmImpl(a, b, "bigint out of range");
    });
}

// log(base, value) -> double -- PG-compatible, ported from Velox PgLogBase
void LogBaseFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<double, double, double>(
    args.data[0], args.data[1], result, args.size(),
    [](double base, double value) -> double {
      if (value == 0.0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
                        ERR_MSG("cannot take logarithm of zero"));
      }
      if (value < 0.0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
                        ERR_MSG("cannot take logarithm of a negative number"));
      }
      if (base == 0.0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
                        ERR_MSG("cannot take logarithm of zero"));
      }
      if (base < 0.0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
                        ERR_MSG("cannot take logarithm of a negative number"));
      }
      if (base == 1.0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DIVISION_BY_ZERO),
                        ERR_MSG("division by zero"));
      }
      return std::log(value) / std::log(base);
    });
}

// cotd(x) -> double -- PG-compatible, ported from Velox PgCotD
constexpr double kDegreesToRadians = M_PI / 180.0;

void CotDFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                  duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<double, double>(
    args.data[0], result, args.size(), [](double x) -> double {
      double t = std::tan(x * kDegreesToRadians);
      if (t == 0.0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DIVISION_BY_ZERO),
                        ERR_MSG("division by zero"));
      }
      return 1.0 / t;
    });
}

// make_timestamp(year, month, day, hour, min, sec) -> timestamp
// PG-compatible, ported from Velox PgMakeTimestamp
void MakeTimestampFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                           duckdb::Vector& result) {
  auto count = args.size();
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto result_data =
    duckdb::FlatVector::GetDataMutable<duckdb::timestamp_t>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);

  duckdb::UnifiedVectorFormat vdata[6];
  for (int i = 0; i < 6; ++i) {
    args.data[i].ToUnifiedFormat(count, vdata[i]);
  }

  for (duckdb::idx_t row = 0; row < count; ++row) {
    bool any_null = false;
    for (int i = 0; i < 6; ++i) {
      auto idx = vdata[i].sel->get_index(row);
      if (!vdata[i].validity.RowIsValid(idx)) {
        any_null = true;
        break;
      }
    }
    if (any_null) {
      validity.SetInvalid(row);
      continue;
    }

    auto year = duckdb::UnifiedVectorFormat::GetData<int32_t>(
      vdata[0])[vdata[0].sel->get_index(row)];
    auto month = duckdb::UnifiedVectorFormat::GetData<int32_t>(
      vdata[1])[vdata[1].sel->get_index(row)];
    auto day = duckdb::UnifiedVectorFormat::GetData<int32_t>(
      vdata[2])[vdata[2].sel->get_index(row)];
    auto hour = duckdb::UnifiedVectorFormat::GetData<int32_t>(
      vdata[3])[vdata[3].sel->get_index(row)];
    auto min = duckdb::UnifiedVectorFormat::GetData<int32_t>(
      vdata[4])[vdata[4].sel->get_index(row)];
    auto sec = duckdb::UnifiedVectorFormat::GetData<double>(
      vdata[5])[vdata[5].sel->get_index(row)];

    duckdb::date_t date;
    if (!duckdb::Date::TryFromDate(year, month, day, date)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DATETIME_FIELD_OVERFLOW),
        ERR_MSG(absl::StrFormat("date field value out of range: %d-%02d-%02d",
                                year, month, day)));
    }
    if (hour < 0 || hour > 23 || min < 0 || min > 59 || sec < 0.0 ||
        sec > 60.0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATETIME_FIELD_OVERFLOW),
                      ERR_MSG("date/time field value out of range"));
    }
    int32_t whole_sec = static_cast<int32_t>(sec);
    double frac = sec - whole_sec;
    int64_t micros = duckdb::Date::EpochMicroseconds(date) +
                     static_cast<int64_t>(hour) * 3600'000'000LL +
                     static_cast<int64_t>(min) * 60'000'000LL +
                     static_cast<int64_t>(whole_sec) * 1'000'000LL +
                     static_cast<int64_t>(frac * 1'000'000);
    result_data[row] = micros;
  }
}

// date_bin(stride, source, origin) -> timestamp
// Ported from Velox PgDateBin -- same logic, DuckDB types.
void DateBinFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  duckdb::TernaryExecutor::Execute<duckdb::interval_t, duckdb::timestamp_t,
                                   duckdb::timestamp_t, duckdb::timestamp_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [](duckdb::interval_t stride, duckdb::timestamp_t source,
       duckdb::timestamp_t origin) -> duckdb::timestamp_t {
      if (stride.micros <= 0 && stride.days <= 0 && stride.months <= 0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("stride must be greater than zero"));
      }
      if (stride.months != 0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG("timestamps cannot be binned into intervals "
                                "containing months or years"));
      }

      int64_t stride_us =
        stride.micros + static_cast<int64_t>(stride.days) * 86400'000'000LL;
      if (stride_us <= 0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("stride must be greater than zero"));
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
      "div",
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT},
      duckdb::LogicalType::BIGINT,
      DivIntFunction});
    div_set.AddFunction(duckdb::ScalarFunction{
      "div",
      {duckdb::LogicalType::DOUBLE, duckdb::LogicalType::DOUBLE},
      duckdb::LogicalType::BIGINT,
      DivDoubleFunction});
    loader.RegisterFunction(div_set);
  }

  // gcd(a, b) -> int32/int64
  {
    duckdb::ScalarFunctionSet gcd_set("gcd");
    gcd_set.AddFunction(duckdb::ScalarFunction{
      "gcd",
      {duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER},
      duckdb::LogicalType::INTEGER,
      GcdInt32Function});
    gcd_set.AddFunction(duckdb::ScalarFunction{
      "gcd",
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT},
      duckdb::LogicalType::BIGINT,
      GcdInt64Function});
    loader.RegisterFunction(gcd_set);
  }

  // lcm(a, b) -> int32/int64
  {
    duckdb::ScalarFunctionSet lcm_set("lcm");
    lcm_set.AddFunction(duckdb::ScalarFunction{
      "lcm",
      {duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER},
      duckdb::LogicalType::INTEGER,
      LcmInt32Function});
    lcm_set.AddFunction(duckdb::ScalarFunction{
      "lcm",
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT},
      duckdb::LogicalType::BIGINT,
      LcmInt64Function});
    loader.RegisterFunction(lcm_set);
  }

  // log(base, value) -> double (2-arg PG-compatible override)
  {
    duckdb::ScalarFunctionSet log_set("log");
    log_set.AddFunction(duckdb::ScalarFunction{
      "log",
      {duckdb::LogicalType::DOUBLE, duckdb::LogicalType::DOUBLE},
      duckdb::LogicalType::DOUBLE,
      LogBaseFunction});
    loader.RegisterFunction(log_set);
  }

  // cotd(x) -> double
  loader.RegisterFunction(duckdb::ScalarFunction{"cotd",
                                                 {duckdb::LogicalType::DOUBLE},
                                                 duckdb::LogicalType::DOUBLE,
                                                 CotDFunction});

  // make_timestamp(year, month, day, hour, min, sec) -> timestamp
  loader.RegisterFunction(duckdb::ScalarFunction{
    "make_timestamp",
    {duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER,
     duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER,
     duckdb::LogicalType::INTEGER, duckdb::LogicalType::DOUBLE},
    duckdb::LogicalType::TIMESTAMP,
    MakeTimestampFunction});

  // date_bin(stride interval, source timestamp, origin timestamp) -> timestamp
  loader.RegisterFunction(duckdb::ScalarFunction{
    "date_bin",
    {duckdb::LogicalType::INTERVAL, duckdb::LogicalType::TIMESTAMP,
     duckdb::LogicalType::TIMESTAMP},
    duckdb::LogicalType::TIMESTAMP,
    DateBinFunction});

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
