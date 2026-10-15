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

#include <boost/multiprecision/cpp_int.hpp>
#include <cmath>
#include <cstdint>
#include <duckdb/common/operator/add.hpp>
#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/decimal.hpp>
#include <duckdb/common/types/interval.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/common/vector_operations/ternary_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/function_binder.hpp>
#include <duckdb/function/scalar/date_bucket_rewrite.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/optimizer/expression_rewriter.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <icu-datefunc.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <optional>
#include <random>
#include <string_view>
#include <type_traits>
#include <utility>

namespace sdb::connector {
namespace {

constexpr uint8_t kNumericWidth = 18;
constexpr uint8_t kNumericScale = 3;

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
      if (!origin.IsFinite()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        ERR_MSG("origin out of range"));
      }
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
      if (!source.IsFinite()) {
        return source;
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

constexpr int32_t kDaysPerMonth = 30;

int32_t AddIntervalField(int32_t field, int64_t delta) {
  const auto sum = static_cast<int64_t>(field) + delta;
  if (sum < std::numeric_limits<int32_t>::min() ||
      sum > std::numeric_limits<int32_t>::max()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                    ERR_MSG("interval out of range"));
  }
  return static_cast<int32_t>(sum);
}

void MonthsFromDays(duckdb::interval_t& span) {
  const auto months = span.days / kDaysPerMonth;
  span.days -= months * kDaysPerMonth;
  span.months = AddIntervalField(span.months, months);
}

void DaysFromMicros(duckdb::interval_t& span) {
  const auto days = span.micros / duckdb::Interval::MICROS_PER_DAY;
  span.micros -= days * duckdb::Interval::MICROS_PER_DAY;
  span.days = AddIntervalField(span.days, days);
}

void AlignDaysToMicros(duckdb::interval_t& span) {
  if (span.days > 0 && span.micros < 0) {
    span.micros += duckdb::Interval::MICROS_PER_DAY;
    --span.days;
  } else if (span.days < 0 && span.micros > 0) {
    span.micros -= duckdb::Interval::MICROS_PER_DAY;
    ++span.days;
  }
}

duckdb::interval_t JustifyDays(duckdb::interval_t span) {
  MonthsFromDays(span);
  if (span.months > 0 && span.days < 0) {
    span.days += kDaysPerMonth;
    --span.months;
  } else if (span.months < 0 && span.days > 0) {
    span.days -= kDaysPerMonth;
    ++span.months;
  }
  return span;
}

duckdb::interval_t JustifyHours(duckdb::interval_t span) {
  DaysFromMicros(span);
  AlignDaysToMicros(span);
  return span;
}

duckdb::interval_t JustifyInterval(duckdb::interval_t span) {
  if ((span.days > 0 && span.micros > 0) ||
      (span.days < 0 && span.micros < 0)) {
    MonthsFromDays(span);
  }
  DaysFromMicros(span);
  MonthsFromDays(span);
  if (span.months > 0 &&
      (span.days < 0 || (span.days == 0 && span.micros < 0))) {
    span.days += kDaysPerMonth;
    --span.months;
  } else if (span.months < 0 &&
             (span.days > 0 || (span.days == 0 && span.micros > 0))) {
    span.days -= kDaysPerMonth;
    ++span.months;
  }
  AlignDaysToMicros(span);
  return span;
}

template<duckdb::interval_t (*Justify)(duckdb::interval_t)>
void JustifyFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                     duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::interval_t, duckdb::interval_t>(
    args.data[0], result, args.size(), Justify);
}

[[noreturn]] void ThrowFloatOverflow() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                  ERR_MSG("value out of range: overflow"));
}

[[noreturn]] void ThrowFloatUnderflow() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                  ERR_MSG("value out of range: underflow"));
}

double PgExp(double x) {
  if (std::isnan(x)) {
    return x;
  }
  if (std::isinf(x)) {
    return x > 0 ? x : 0;
  }
  const auto result = std::exp(x);
  if (std::isinf(result)) {
    ThrowFloatOverflow();
  }
  if (result == 0.0) {
    ThrowFloatUnderflow();
  }
  return result;
}

template<double (*Log)(double)>
double PgLog(double x) {
  if (x == 0.0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
                    ERR_MSG("cannot take logarithm of zero"));
  }
  if (x < 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
                    ERR_MSG("cannot take logarithm of a negative number"));
  }
  const auto result = Log(x);
  if (std::isinf(result) && !std::isinf(x)) {
    ThrowFloatOverflow();
  }
  if (result == 0.0 && x != 1.0) {
    ThrowFloatUnderflow();
  }
  return result;
}

template<double (*Function)(double)>
void DoubleFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<double, double>(args.data[0], result,
                                                 args.size(), Function);
}

duckdb::LogicalType DecimalArgument(duckdb::BindScalarFunctionInput& input,
                                    duckdb::idx_t index) {
  uint8_t width = 0;
  uint8_t scale = 0;
  if (!input.GetArguments()[index]->GetReturnType().GetDecimalProperties(
        width, scale) ||
      width == 0) {
    width = kNumericWidth;
    scale = kNumericScale;
  }
  const auto type = duckdb::LogicalType::DECIMAL(width, scale);
  input.GetBoundFunction().GetArguments()[index] = type;
  return type;
}

duckdb::unique_ptr<duckdb::FunctionData> BindScale(
  duckdb::BindScalarFunctionInput& input) {
  DecimalArgument(input, 0);
  return nullptr;
}

duckdb::unique_ptr<duckdb::Expression> ScaleExpression(
  duckdb::FunctionBindExpressionInput& input) {
  const auto scale =
    duckdb::DecimalType::GetScale(input.children[0]->GetReturnType());
  return duckdb::ExpressionRewriter::ConstantOrNull(
    input.context, std::move(input.children), duckdb::Value::INTEGER(scale));
}

template<typename T>
void MinScaleFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  const int32_t scale = duckdb::DecimalType::GetScale(args.data[0].GetType());
  duckdb::UnaryExecutor::Execute<T, int32_t>(
    args.data[0], result, args.size(), [&](T value) {
      int32_t min_scale = scale;
      for (; min_scale > 0 && value % T(10) == T(0); --min_scale) {
        value /= T(10);
      }
      return min_scale;
    });
}

using Int256 = boost::multiprecision::int256_t;

Int256 Pow10(uint8_t exponent) {
  Int256 power = 1;
  for (uint8_t i = 0; i < exponent; ++i) {
    power *= 10;
  }
  return power;
}

Int256 ToInt256(duckdb::hugeint_t value) {
  return Int256{value.upper} * (Int256{1} << 64) + Int256{value.lower};
}

duckdb::hugeint_t ToHugeint(const Int256& value) {
  const Int256 magnitude = boost::multiprecision::abs(value);
  const duckdb::hugeint_t result{
    static_cast<int64_t>(static_cast<uint64_t>(magnitude >> 64)),
    static_cast<uint64_t>(magnitude & std::numeric_limits<uint64_t>::max())};
  return value < 0 ? -result : result;
}

void DivTruncFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  const auto dividend_scale =
    duckdb::DecimalType::GetScale(args.data[0].GetType());
  const auto divisor_scale =
    duckdb::DecimalType::GetScale(args.data[1].GetType());
  const auto dividend_factor = Pow10(divisor_scale);
  const auto divisor_factor = Pow10(dividend_scale);
  const auto limit = Pow10(duckdb::Decimal::MAX_WIDTH_DECIMAL);
  duckdb::BinaryExecutor::Execute<duckdb::hugeint_t, duckdb::hugeint_t,
                                  duckdb::hugeint_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::hugeint_t dividend, duckdb::hugeint_t divisor) {
      if (divisor == duckdb::hugeint_t{0}) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DIVISION_BY_ZERO),
                        ERR_MSG("division by zero"));
      }
      const Int256 quotient = ToInt256(dividend) * dividend_factor /
                              (ToInt256(divisor) * divisor_factor);
      if (boost::multiprecision::abs(quotient) >= limit) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                        ERR_MSG("value overflows numeric format"));
      }
      return ToHugeint(quotient);
    });
}

duckdb::unique_ptr<duckdb::FunctionData> BindDivTrunc(
  duckdb::BindScalarFunctionInput& input) {
  auto& function = input.GetBoundFunction();
  for (duckdb::idx_t i = 0; i < 2; ++i) {
    function.GetArguments()[i] = duckdb::LogicalType::DECIMAL(
      duckdb::Decimal::MAX_WIDTH_DECIMAL,
      duckdb::DecimalType::GetScale(DecimalArgument(input, i)));
  }
  return nullptr;
}

duckdb::unique_ptr<duckdb::FunctionData> BindMinScale(
  duckdb::BindScalarFunctionInput& input) {
  const auto physical = DecimalArgument(input, 0).InternalType();
  auto& function = input.GetBoundFunction();
  if (physical == duckdb::PhysicalType::INT16) {
    function.SetFunctionCallback(MinScaleFunction<int16_t>);
  } else if (physical == duckdb::PhysicalType::INT32) {
    function.SetFunctionCallback(MinScaleFunction<int32_t>);
  } else if (physical == duckdb::PhysicalType::INT64) {
    function.SetFunctionCallback(MinScaleFunction<int64_t>);
  } else {
    function.SetFunctionCallback(MinScaleFunction<duckdb::hugeint_t>);
  }
  return nullptr;
}

duckdb::unique_ptr<duckdb::Expression> BindDateSubtract(
  duckdb::FunctionBindExpressionInput& input) {
  return duckdb::FunctionBinder{input.context}.BindScalarFunction(
    duckdb::Identifier{DEFAULT_SCHEMA}, duckdb::Identifier{"-"},
    std::move(input.children), true);
}

duckdb::TZCalendar SessionCalendar(duckdb::ExpressionState& state) {
  const auto& info = state.expr.Cast<duckdb::BoundFunctionExpression>()
                       .BindInfo()
                       ->Cast<duckdb::ICUDateFunc::BindData>();
  return {*info.calendar, info.cal_setting};
}

void DateSubtractInZoneFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto& zones = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<duckdb::ICUDateFunc::CalendarCacheState>();
  auto calendar = SessionCalendar(state);
  const auto subtract = [&](duckdb::timestamp_tz_t instant,
                            duckdb::interval_t span) {
    return duckdb::ICUDateFunc::Sub(calendar, instant, span);
  };
  auto& zone = args.data[2];
  if (zone.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
    if (duckdb::ConstantVector::IsNull(zone)) {
      result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
      duckdb::ConstantVector::SetNull(result, true);
      return;
    }
    zones.SetTimeZone(calendar.GetCalendar(),
                      *duckdb::ConstantVector::GetData<duckdb::string_t>(zone));
    duckdb::BinaryExecutor::Execute<duckdb::timestamp_tz_t, duckdb::interval_t,
                                    duckdb::timestamp_tz_t>(
      args.data[0], args.data[1], result, args.size(), subtract);
    return;
  }
  duckdb::TernaryExecutor::Execute<duckdb::timestamp_tz_t, duckdb::interval_t,
                                   duckdb::string_t, duckdb::timestamp_tz_t>(
    args.data[0], args.data[1], zone, result, args.size(),
    [&](duckdb::timestamp_tz_t instant, duckdb::interval_t span,
        duckdb::string_t name) {
      zones.SetTimeZone(calendar.GetCalendar(), name);
      return subtract(instant, span);
    });
}

template<typename T>
std::optional<bool> Overlaps(std::optional<T> start1, std::optional<T> end1,
                             std::optional<T> start2, std::optional<T> end2) {
  if (!start1 || (end1 && *start1 > *end1)) {
    std::swap(start1, end1);
  }
  if (!start2 || (end2 && *start2 > *end2)) {
    std::swap(start2, end2);
  }
  if (!start1 || !start2) {
    return std::nullopt;
  }
  if (*start1 > *start2) {
    if (!end2) {
      return std::nullopt;
    }
    if (*start1 < *end2) {
      return true;
    }
    if (!end1) {
      return std::nullopt;
    }
    return false;
  }
  if (*start1 < *start2) {
    if (!end1) {
      return std::nullopt;
    }
    if (*start2 < *end1) {
      return true;
    }
    if (!end2) {
      return std::nullopt;
    }
    return false;
  }
  if (!end1 || !end2) {
    return std::nullopt;
  }
  return true;
}

template<typename T>
std::optional<T> PeriodStart(const auto& start) {
  if (!start.IsValid()) {
    return std::nullopt;
  }
  return start.GetValue();
}

template<typename T, typename End>
std::optional<T> PeriodEnd(const auto& start, const auto& end,
                           const auto& add) {
  if (!end.IsValid()) {
    return std::nullopt;
  }
  if constexpr (std::is_same_v<End, duckdb::interval_t>) {
    if (!start.IsValid()) {
      return std::nullopt;
    }
    return add(start.GetValue(), end.GetValue());
  } else {
    return end.GetValue();
  }
}

template<typename T, typename End1, typename End2>
void ExecuteOverlaps(duckdb::DataChunk& args, duckdb::Vector& result,
                     const auto& add) {
  const auto start1 = args.data[0].Values<T>();
  const auto end1 = args.data[1].Values<End1>();
  const auto start2 = args.data[2].Values<T>();
  const auto end2 = args.data[3].Values<End2>();
  auto out = duckdb::FlatVector::Writer<bool>(result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto overlaps =
      Overlaps<T>(PeriodStart<T>(start1[row]),
                  PeriodEnd<T, End1>(start1[row], end1[row], add),
                  PeriodStart<T>(start2[row]),
                  PeriodEnd<T, End2>(start2[row], end2[row], add));
    if (overlaps) {
      out.WriteValue(*overlaps);
    } else {
      out.WriteNull();
    }
  }
}

template<typename T, typename End1, typename End2>
void OverlapsFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  ExecuteOverlaps<T, End1, End2>(
    args, result, [](auto start, duckdb::interval_t length) {
      if constexpr (std::is_same_v<decltype(start), duckdb::dtime_t>) {
        return duckdb::AddTimeOperator::Operation<
          duckdb::dtime_t, duckdb::interval_t, duckdb::dtime_t>(start, length);
      } else {
        return duckdb::AddOperator::Operation<
          duckdb::timestamp_t, duckdb::interval_t, duckdb::timestamp_t>(start,
                                                                        length);
      }
    });
}

template<typename End1, typename End2>
void OverlapsInSessionZoneFunction(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  auto calendar = SessionCalendar(state);
  ExecuteOverlaps<duckdb::timestamp_tz_t, End1, End2>(
    args, result, [&](duckdb::timestamp_tz_t start, duckdb::interval_t length) {
      return duckdb::ICUDateFunc::Add(calendar, start, length);
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
  {
    duckdb::ScalarFunction func{
      "date_bin",
      {duckdb::LogicalType::INTERVAL, duckdb::LogicalType::TIMESTAMP,
       duckdb::LogicalType::TIMESTAMP},
      duckdb::LogicalType::TIMESTAMP,
      DateBinFunction};
    func.SetBucketRewriteCallback(duckdb::DateBinBucketRewrite);
    loader.RegisterFunction(func);
  }

  struct Justify {
    std::string_view name;
    duckdb::scalar_function_t function;
  };
  for (const auto& [name, function] : {
         Justify{"justify_days", JustifyFunction<JustifyDays>},
         Justify{"justify_hours", JustifyFunction<JustifyHours>},
         Justify{"justify_interval", JustifyFunction<JustifyInterval>},
       }) {
    loader.RegisterFunction(
      duckdb::ScalarFunction{duckdb::Identifier{name},
                             {duckdb::LogicalType::INTERVAL},
                             duckdb::LogicalType::INTERVAL,
                             function});
  }

  {
    duckdb::ScalarFunction in_session_zone{
      "date_subtract",
      {duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::INTERVAL},
      duckdb::LogicalType::TIMESTAMP_TZ,
      nullptr};
    in_session_zone.SetBindExpressionCallback(BindDateSubtract);
    in_session_zone.SetStability(
      duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY);
    duckdb::ScalarFunction in_zone{
      "date_subtract",
      {duckdb::LogicalType::TIMESTAMP_TZ, duckdb::LogicalType::INTERVAL,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::TIMESTAMP_TZ,
      DateSubtractInZoneFunction,
      duckdb::ICUDateFunc::Bind,
      nullptr,
      duckdb::ICUDateFunc::InitCalendarCache};
    in_zone.SetFallible();
    duckdb::ScalarFunctionSet set{"date_subtract"};
    set.AddFunction(std::move(in_session_zone));
    set.AddFunction(std::move(in_zone));
    loader.RegisterFunction(std::move(set));
  }

  {
    using duckdb::dtime_t;
    using duckdb::dtime_tz_t;
    using duckdb::interval_t;
    using duckdb::timestamp_t;
    using duckdb::timestamp_tz_t;
    const auto time = duckdb::LogicalType::TIME;
    const auto timetz = duckdb::LogicalType::TIME_TZ;
    const auto timestamp = duckdb::LogicalType::TIMESTAMP;
    const auto timestamptz = duckdb::LogicalType::TIMESTAMP_TZ;
    const auto interval = duckdb::LogicalType::INTERVAL;
    duckdb::ScalarFunctionSet set{"overlaps"};
    const auto add = [&](const duckdb::LogicalType& bound,
                         const duckdb::LogicalType& end1,
                         const duckdb::LogicalType& end2,
                         duckdb::scalar_function_t function,
                         duckdb::bind_scalar_function_t bind) {
      duckdb::ScalarFunction overload{"overlaps",
                                      {bound, end1, bound, end2},
                                      duckdb::LogicalType::BOOLEAN,
                                      std::move(function),
                                      bind};
      overload.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      if (bind) {
        overload.SetStability(
          duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY);
      }
      if (end1 == interval || end2 == interval) {
        overload.SetFallible();
      }
      set.AddFunction(std::move(overload));
    };
    add(timetz, timetz, timetz,
        OverlapsFunction<dtime_tz_t, dtime_tz_t, dtime_tz_t>, nullptr);
    add(time, time, time, OverlapsFunction<dtime_t, dtime_t, dtime_t>, nullptr);
    add(time, interval, interval,
        OverlapsFunction<dtime_t, interval_t, interval_t>, nullptr);
    add(time, time, interval, OverlapsFunction<dtime_t, dtime_t, interval_t>,
        nullptr);
    add(time, interval, time, OverlapsFunction<dtime_t, interval_t, dtime_t>,
        nullptr);
    add(timestamp, timestamp, timestamp,
        OverlapsFunction<timestamp_t, timestamp_t, timestamp_t>, nullptr);
    add(timestamp, interval, interval,
        OverlapsFunction<timestamp_t, interval_t, interval_t>, nullptr);
    add(timestamp, timestamp, interval,
        OverlapsFunction<timestamp_t, timestamp_t, interval_t>, nullptr);
    add(timestamp, interval, timestamp,
        OverlapsFunction<timestamp_t, interval_t, timestamp_t>, nullptr);
    add(timestamptz, timestamptz, timestamptz,
        OverlapsFunction<timestamp_tz_t, timestamp_tz_t, timestamp_tz_t>,
        nullptr);
    add(timestamptz, interval, interval,
        OverlapsInSessionZoneFunction<interval_t, interval_t>,
        duckdb::ICUDateFunc::Bind);
    add(timestamptz, timestamptz, interval,
        OverlapsInSessionZoneFunction<timestamp_tz_t, interval_t>,
        duckdb::ICUDateFunc::Bind);
    add(timestamptz, interval, timestamptz,
        OverlapsInSessionZoneFunction<interval_t, timestamp_tz_t>,
        duckdb::ICUDateFunc::Bind);
    loader.RegisterFunction(std::move(set));
  }

  {
    duckdb::ScalarFunction func{
      "random_normal",
      {duckdb::LogicalType::DOUBLE, duckdb::LogicalType::DOUBLE},
      duckdb::LogicalType::DOUBLE,
      RandomNormalFunction};
    func.SetVolatile();
    loader.RegisterFunction(func);
  }

  struct DoubleScalar {
    std::string_view name;
    duckdb::scalar_function_t function;
  };
  for (const auto& [name, function] : {
         DoubleScalar{"dexp", DoubleFunction<PgExp>},
         DoubleScalar{"dlog1", DoubleFunction<PgLog<std::log>>},
         DoubleScalar{"dlog10", DoubleFunction<PgLog<std::log10>>},
         DoubleScalar{"dround", DoubleFunction<std::rint>},
         DoubleScalar{"dtrunc", DoubleFunction<std::trunc>},
       }) {
    loader.RegisterFunction(
      duckdb::ScalarFunction{duckdb::Identifier{name},
                             {duckdb::LogicalType::DOUBLE},
                             duckdb::LogicalType::DOUBLE,
                             function});
  }

  {
    duckdb::ScalarFunction scale{"scale",
                                 {duckdb::LogicalTypeId::DECIMAL},
                                 duckdb::LogicalType::INTEGER,
                                 nullptr,
                                 BindScale};
    scale.SetBindExpressionCallback(ScaleExpression);
    loader.RegisterFunction(scale);
  }
  loader.RegisterFunction(
    duckdb::ScalarFunction{"min_scale",
                           {duckdb::LogicalTypeId::DECIMAL},
                           duckdb::LogicalType::INTEGER,
                           nullptr,
                           BindMinScale});
  for (const auto name : {"div", "numeric_div_trunc"}) {
    loader.RegisterFunction(duckdb::ScalarFunction{
      name,
      {duckdb::LogicalTypeId::DECIMAL, duckdb::LogicalTypeId::DECIMAL},
      duckdb::LogicalType::DECIMAL(duckdb::Decimal::MAX_WIDTH_DECIMAL, 0),
      DivTruncFunction,
      BindDivTrunc});
  }
}

}  // namespace sdb::connector
