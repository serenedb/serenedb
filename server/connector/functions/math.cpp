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

#include <absl/algorithm/container.h>
#include <absl/strings/str_format.h>

#include <algorithm>
#include <array>
#include <boost/multiprecision/cpp_int.hpp>
#include <cmath>
#include <cstdint>
#include <duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp>
#include <duckdb/common/enums/date_part_specifier.hpp>
#include <duckdb/common/operator/add.hpp>
#include <duckdb/common/operator/comparison_operators.hpp>
#include <duckdb/common/random_engine.hpp>
#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/decimal.hpp>
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/types/interval.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/common/vector_operations/ternary_executor.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/create_sort_key.hpp>
#include <duckdb/function/function_binder.hpp>
#include <duckdb/function/scalar/date_bucket_rewrite.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/optimizer/expression_rewriter.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_binder.hpp>
#include <icu-datefunc.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <numbers>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

#include "connector/functions/duckdb_aliases.h"

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

void DroundFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<double, double>(
    args.data[0], result, args.size(),
    [](double x) -> double { return std::rint(x); });
}

struct RandomNormalLocalState : duckdb::FunctionLocalState {
  explicit RandomNormalLocalState(uint64_t seed) : random_engine{0} {
    random_engine.SetSeed(seed);
  }

  double Next(double mean, double stddev) {
    const double u1 = 1.0 - random_engine.NextRandom();
    const double u2 = random_engine.NextRandom();
    return stddev * std::sqrt(-2.0 * std::log(u1)) *
             std::sin(2.0 * std::numbers::pi * u2) +
           mean;
  }

  duckdb::RandomEngine random_engine;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> RandomNormalInitLocalState(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData*) {
  auto& random_engine = duckdb::RandomEngine::Get(state.GetContext());
  duckdb::lock_guard<duckdb::mutex> guard(random_engine.lock);
  return duckdb::make_uniq<RandomNormalLocalState>(
    random_engine.NextRandomInteger64());
}

void RandomNormalFunction(duckdb::DataChunk& args,
                          duckdb::ExpressionState& state,
                          duckdb::Vector& result) {
  auto& lstate = duckdb::ExecuteFunctionState::GetFunctionState(state)
                   ->Cast<RandomNormalLocalState>();
  const auto count = args.size();
  std::array<duckdb::UnifiedVectorFormat, 2> params;
  for (duckdb::idx_t p = 0; p < args.ColumnCount(); ++p) {
    args.data[p].ToUnifiedFormat(params[p]);
  }
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto out = duckdb::FlatVector::Writer<double>(result, count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    std::array<double, 2> value{0.0, 1.0};
    bool valid = true;
    for (duckdb::idx_t p = 0; p < args.ColumnCount(); ++p) {
      const auto idx = params[p].sel->get_index(i);
      if (!params[p].validity.RowIsValid(idx)) {
        valid = false;
        break;
      }
      value[p] = duckdb::UnifiedVectorFormat::GetData<double>(params[p])[idx];
    }
    if (valid) {
      out.WriteValue(lstate.Next(value[0], value[1]));
    } else {
      out.WriteNull();
    }
  }
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

duckdb::hugeint_t DecimalGcd(duckdb::hugeint_t a, duckdb::hugeint_t b) {
  a = duckdb::Hugeint::Abs(a);
  b = duckdb::Hugeint::Abs(b);
  while (b != 0) {
    a = std::exchange(b, a % b);
  }
  return a;
}

duckdb::hugeint_t DecimalLcm(duckdb::hugeint_t a, duckdb::hugeint_t b) {
  if (a == 0 || b == 0) {
    return 0;
  }
  duckdb::hugeint_t lcm;
  if (!duckdb::Hugeint::TryMultiply(duckdb::Hugeint::Abs(a) / DecimalGcd(a, b),
                                    duckdb::Hugeint::Abs(b), lcm) ||
      lcm >=
        duckdb::Hugeint::POWERS_OF_TEN[duckdb::Decimal::MAX_WIDTH_DECIMAL]) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    ERR_MSG("numeric field overflow"));
  }
  return lcm;
}

template<auto Apply>
void DecimalGcdFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  duckdb::BinaryExecutor::Execute<duckdb::hugeint_t, duckdb::hugeint_t,
                                  duckdb::hugeint_t>(
    args.data[0], args.data[1], result, args.size(), Apply);
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

duckdb::LogicalType DecimalArgument(duckdb::BindScalarFunctionInput& input,
                                    duckdb::idx_t index) {
  uint8_t width = 0;
  uint8_t scale = 0;
  if (!input.GetArguments()[index]->GetReturnType().GetDecimalProperties(
        width, scale)) {
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

duckdb::unique_ptr<duckdb::FunctionData> BindDecimalGcd(
  duckdb::BindScalarFunctionInput& input) {
  const auto type = duckdb::LogicalType::DECIMAL(
    duckdb::Decimal::MAX_WIDTH_DECIMAL,
    std::max(duckdb::DecimalType::GetScale(DecimalArgument(input, 0)),
             duckdb::DecimalType::GetScale(DecimalArgument(input, 1))));
  auto& function = input.GetBoundFunction();
  function.GetArguments()[0] = type;
  function.GetArguments()[1] = type;
  function.SetReturnType(type);
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
int32_t MinScale(T value, int32_t scale) {
  for (; scale > 0 && value % T(10) == T(0); --scale) {
    value /= T(10);
  }
  return scale;
}

template<typename T>
void MinScaleFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  const int32_t scale = duckdb::DecimalType::GetScale(args.data[0].GetType());
  duckdb::UnaryExecutor::Execute<T, int32_t>(
    args.data[0], result, args.size(),
    [&](T value) { return MinScale(value, scale); });
}

duckdb::unique_ptr<duckdb::FunctionData> BindTrimScale(
  duckdb::BindScalarFunctionInput& input) {
  input.GetBoundFunction().SetReturnType(DecimalArgument(input, 0));
  return nullptr;
}

duckdb::unique_ptr<duckdb::Expression> TrimScaleExpression(
  duckdb::FunctionBindExpressionInput& input) {
  auto& child = input.children[0];
  if (!child->IsFoldable()) {
    return std::move(child);
  }
  const auto value =
    duckdb::ExpressionExecutor::EvaluateScalar(input.context, *child);
  const auto width = duckdb::DecimalType::GetWidth(value.type());
  const auto scale = duckdb::DecimalType::GetScale(value.type());
  const auto trimmed =
    static_cast<uint8_t>(MinScale(duckdb::IntegralValue::Get(value), scale));
  const auto type = duckdb::LogicalType::DECIMAL(
    std::max<uint8_t>(width - scale + trimmed, 1), trimmed);
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    value.DefaultCastAs(type));
}

using Int256 = boost::multiprecision::int256_t;

Int256 Pow10(uint8_t exponent) {
  return boost::multiprecision::pow(Int256{10}, exponent);
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

void DateTruncInZoneFunction(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  auto& zones = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<duckdb::ICUDateFunc::CalendarCacheState>();
  auto calendar = SessionCalendar(state);
  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::timestamp_tz_t,
                                   duckdb::string_t, duckdb::timestamp_tz_t>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t part, duckdb::timestamp_tz_t instant,
        duckdb::string_t name) {
      if (!instant.IsFinite()) {
        return instant;
      }
      const auto truncate =
        duckdb::ICUDateFunc::TruncationFactory(duckdb::GetDatePartSpecifier(
          std::string_view{part.GetData(), part.GetSize()}));
      zones.SetTimeZone(calendar.GetCalendar(), name);
      auto micros =
        duckdb::ICUDateFunc::SetTime(calendar.GetCalendar(), instant);
      truncate(calendar.GetCalendar(), micros);
      return duckdb::ICUDateFunc::GetTimeUnsafe(calendar.GetCalendar(), micros);
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

template<typename T>
bool ClampGreater(T left, T right) {
  if constexpr (std::is_floating_point_v<T>) {
    return ((left > right) | (left != left)) & (right == right);
  } else {
    return duckdb::GreaterThan::Operation(left, right);
  }
}

template<typename T>
T ClampBetween(T value, T min, T max) {
  const auto lower = ClampGreater(min, value) ? min : value;
  return ClampGreater(lower, max) ? max : lower;
}

[[noreturn]] void ThrowClampBounds() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("clamp minimum cannot be greater than maximum"));
}

template<typename T>
bool TryClampConstantBounds(duckdb::DataChunk& args, duckdb::Vector& result) {
  auto& value = args.data[0];
  auto& min = args.data[1];
  auto& max = args.data[2];
  if (value.GetVectorType() != duckdb::VectorType::FLAT_VECTOR ||
      min.GetVectorType() != duckdb::VectorType::CONSTANT_VECTOR ||
      max.GetVectorType() != duckdb::VectorType::CONSTANT_VECTOR ||
      duckdb::ConstantVector::IsNull(min) ||
      duckdb::ConstantVector::IsNull(max)) {
    return false;
  }
  const auto min_value = duckdb::ConstantVector::GetData<T>(min)[0];
  const auto max_value = duckdb::ConstantVector::GetData<T>(max)[0];
  if (ClampGreater(min_value, max_value)) {
    return false;
  }
  const auto count = args.size();
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  if (result.size() != count) {
    duckdb::FlatVector::SetSize(result, count);
  }
  auto& mask = duckdb::FlatVector::ValidityMutable(result);
  mask.Reset(count);
  mask.Combine(duckdb::FlatVector::Validity(value), count);
  const auto* value_data = duckdb::FlatVector::GetData<T>(value);
  auto* result_data = duckdb::FlatVector::GetDataMutable<T>(result);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    result_data[i] = ClampBetween(value_data[i], min_value, max_value);
  }
  return true;
}

template<typename T>
void ClampFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                   duckdb::Vector& result) {
  if constexpr (!std::is_same_v<T, duckdb::string_t>) {
    if (TryClampConstantBounds<T>(args, result)) {
      return;
    }
  }
  duckdb::TernaryExecutor::Execute<T, T, T, T>(
    args.data[0], args.data[1], args.data[2], result,
    [](T value, T min, T max) {
      if (ClampGreater(min, max)) {
        ThrowClampBounds();
      }
      return ClampBetween(value, min, max);
    });
  if constexpr (std::is_same_v<T, duckdb::string_t>) {
    for (auto& input : args.data) {
      duckdb::StringVector::AddHeapReference(result, input);
    }
  }
}

struct ClampSortKeyState final : duckdb::FunctionLocalState {
  ClampSortKeyState() {
    keys.Initialize(duckdb::Allocator::DefaultAllocator(),
                    {duckdb::LogicalType::BLOB, duckdb::LogicalType::BLOB,
                     duckdb::LogicalType::BLOB});
  }

  duckdb::DataChunk keys;
  duckdb::OrderModifiers modifiers{duckdb::OrderType::ASCENDING,
                                   duckdb::OrderByNullType::NULLS_LAST};
};

duckdb::unique_ptr<duckdb::FunctionLocalState> ClampSortKeyInit(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<ClampSortKeyState>();
}

void ClampSortKeyFunction(duckdb::DataChunk& args,
                          duckdb::ExpressionState& state,
                          duckdb::Vector& result) {
  auto& local = duckdb::ExecuteFunctionState::GetFunctionState(state)
                  ->Cast<ClampSortKeyState>();
  local.keys.Reset();
  auto result_type = duckdb::VectorType::CONSTANT_VECTOR;
  std::array<duckdb::UnifiedVectorFormat, 3> formats;
  for (duckdb::idx_t c = 0; c < formats.size(); ++c) {
    if (args.data[c].GetVectorType() != duckdb::VectorType::CONSTANT_VECTOR) {
      result_type = duckdb::VectorType::FLAT_VECTOR;
    }
    args.data[c].ToUnifiedFormat(formats[c]);
    duckdb::CreateSortKeyHelpers::CreateSortKey(args.data[c], local.modifiers,
                                                local.keys.data[c]);
  }
  auto value_keys = local.keys.data[0].Values<duckdb::string_t>();
  auto min_keys = local.keys.data[1].Values<duckdb::string_t>();
  auto max_keys = local.keys.data[2].Values<duckdb::string_t>();
  auto& result_mask = duckdb::FlatVector::ValidityMutable(result);
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto valid = absl::c_all_of(formats, [&](const auto& format) {
      return format.validity.RowIsValid(format.sel->get_index(row));
    });
    if (!valid) {
      result_mask.SetInvalid(row);
      continue;
    }
    const auto min = min_keys[row].GetValue();
    const auto max = max_keys[row].GetValue();
    if (ClampGreater(min, max)) {
      ThrowClampBounds();
    }
    duckdb::CreateSortKeyHelpers::DecodeSortKey(
      ClampBetween(value_keys[row].GetValue(), min, max), result, row,
      local.modifiers);
  }
  result.SetVectorType(result_type);
}

duckdb::unique_ptr<duckdb::FunctionData> ClampBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& context = input.GetClientContext();
  auto& arguments = input.GetArguments();
  auto type = duckdb::ExpressionBinder::GetExpressionReturnType(*arguments[0]);
  for (duckdb::idx_t i = 1; i < arguments.size(); ++i) {
    const auto arg_type =
      duckdb::ExpressionBinder::GetExpressionReturnType(*arguments[i]);
    if (!duckdb::LogicalType::TryGetMaxLogicalType(context, type, arg_type,
                                                   type)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                      ERR_MSG("clamp types ", type.ToString(), " and ",
                              arg_type.ToString(), " cannot be matched"));
    }
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::UNKNOWN:
      throw duckdb::ParameterNotResolvedException();
    case duckdb::LogicalTypeId::INTEGER_LITERAL:
      type = duckdb::IntegerLiteral::GetType(type);
      break;
    case duckdb::LogicalTypeId::STRING_LITERAL:
      type = duckdb::LogicalType::VARCHAR;
      break;
    default:
      break;
  }
  auto& function = input.GetBoundFunction();
  for (auto& arg : function.GetArguments()) {
    arg = type;
  }
  function.SetReturnType(type);
  const auto physical_type = type.id() == duckdb::LogicalTypeId::TIME_TZ
                               ? duckdb::PhysicalType::INVALID
                               : type.InternalType();
  switch (physical_type) {
    case duckdb::PhysicalType::BOOL:
    case duckdb::PhysicalType::INT8:
      function.SetFunctionCallback(ClampFunction<int8_t>);
      break;
    case duckdb::PhysicalType::INT16:
      function.SetFunctionCallback(ClampFunction<int16_t>);
      break;
    case duckdb::PhysicalType::INT32:
      function.SetFunctionCallback(ClampFunction<int32_t>);
      break;
    case duckdb::PhysicalType::INT64:
      function.SetFunctionCallback(ClampFunction<int64_t>);
      break;
    case duckdb::PhysicalType::INT128:
      function.SetFunctionCallback(ClampFunction<duckdb::hugeint_t>);
      break;
    case duckdb::PhysicalType::UINT8:
      function.SetFunctionCallback(ClampFunction<uint8_t>);
      break;
    case duckdb::PhysicalType::UINT16:
      function.SetFunctionCallback(ClampFunction<uint16_t>);
      break;
    case duckdb::PhysicalType::UINT32:
      function.SetFunctionCallback(ClampFunction<uint32_t>);
      break;
    case duckdb::PhysicalType::UINT64:
      function.SetFunctionCallback(ClampFunction<uint64_t>);
      break;
    case duckdb::PhysicalType::UINT128:
      function.SetFunctionCallback(ClampFunction<duckdb::uhugeint_t>);
      break;
    case duckdb::PhysicalType::FLOAT:
      function.SetFunctionCallback(ClampFunction<float>);
      break;
    case duckdb::PhysicalType::DOUBLE:
      function.SetFunctionCallback(ClampFunction<double>);
      break;
    case duckdb::PhysicalType::VARCHAR:
      function.SetFunctionCallback(ClampFunction<duckdb::string_t>);
      break;
    default:
      function.SetFunctionCallback(ClampSortKeyFunction);
      function.SetInitStateCallback(ClampSortKeyInit);
      break;
  }
  return nullptr;
}

}  // namespace

void RegisterPgMathFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  RegisterScalarAlias(loader, "xor", "#");
  RegisterScalarAlias(loader, "sqrt", "|/");
  RegisterScalarAlias(loader, "cbrt", "||/");
  RegisterScalarAlias(loader, "exp", "dexp");
  RegisterScalarAlias(loader, "ln", "dlog1");
  RegisterScalarAlias(loader, "log10", "dlog10");
  RegisterScalarAlias(loader, "sqrt", "dsqrt");

  {
    duckdb::ScalarFunctionSet dtrunc{"dtrunc"};
    for (const auto& function :
         loader.GetFunction(duckdb::Identifier{"trunc"}).functions.functions) {
      const auto& parameters = function->GetSignature().GetParameters();
      if (parameters.size() == 1 &&
          parameters[0].GetType() == duckdb::LogicalType::DOUBLE) {
        dtrunc.AddFunction(function);
      }
    }
    loader.RegisterFunction(dtrunc);
  }

  loader.RegisterFunction(duckdb::ScalarFunction{"dround",
                                                 {duckdb::LogicalType::DOUBLE},
                                                 duckdb::LogicalType::DOUBLE,
                                                 DroundFunction});

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
    gcd_set.AddFunction(duckdb::ScalarFunction{
      "gcd",
      {duckdb::LogicalTypeId::DECIMAL, duckdb::LogicalTypeId::DECIMAL},
      duckdb::LogicalType::DECIMAL(duckdb::Decimal::MAX_WIDTH_DECIMAL, 0),
      DecimalGcdFunction<&DecimalGcd>,
      BindDecimalGcd});
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
    duckdb::ScalarFunction lcm_decimal{
      "lcm",
      {duckdb::LogicalTypeId::DECIMAL, duckdb::LogicalTypeId::DECIMAL},
      duckdb::LogicalType::DECIMAL(duckdb::Decimal::MAX_WIDTH_DECIMAL, 0),
      DecimalGcdFunction<&DecimalLcm>,
      BindDecimalGcd};
    lcm_decimal.SetFallible();
    lcm_set.AddFunction(std::move(lcm_decimal));
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
    duckdb::ScalarFunction in_zone{
      "date_trunc",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::TIMESTAMP_TZ,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::TIMESTAMP_TZ,
      DateTruncInZoneFunction,
      duckdb::ICUDateFunc::Bind,
      nullptr,
      duckdb::ICUDateFunc::InitCalendarCache};
    in_zone.SetFallible();
    loader.AddFunctionOverload(std::move(in_zone));
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
    duckdb::ScalarFunctionSet random_normal_set("random_normal");
    for (duckdb::idx_t arity = 0; arity <= 2; ++arity) {
      duckdb::ScalarFunction func{
        "random_normal",
        duckdb::vector<duckdb::LogicalType>(arity, duckdb::LogicalType::DOUBLE),
        duckdb::LogicalType::DOUBLE, RandomNormalFunction};
      func.SetInitStateCallback(RandomNormalInitLocalState);
      func.SetVolatile();
      random_normal_set.AddFunction(func);
    }
    loader.RegisterFunction(random_normal_set);
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
  {
    duckdb::ScalarFunction trim_scale{"trim_scale",
                                      {duckdb::LogicalTypeId::DECIMAL},
                                      duckdb::LogicalTypeId::DECIMAL,
                                      nullptr,
                                      BindTrimScale};
    trim_scale.SetBindExpressionCallback(TrimScaleExpression);
    loader.RegisterFunction(trim_scale);
  }
  {
    duckdb::ScalarFunction div_trunc{
      "numeric_div_trunc",
      {duckdb::LogicalTypeId::DECIMAL, duckdb::LogicalTypeId::DECIMAL},
      duckdb::LogicalType::DECIMAL(duckdb::Decimal::MAX_WIDTH_DECIMAL, 0),
      DivTruncFunction,
      BindDivTrunc};
    div_trunc.SetFallible();
    loader.RegisterFunction(std::move(div_trunc));
  }

  {
    duckdb::ScalarFunction func{
      "clamp",
      {duckdb::LogicalType::ANY, duckdb::LogicalType::ANY,
       duckdb::LogicalType::ANY},
      duckdb::LogicalType::ANY,
      nullptr,
      ClampBind};
    func.SetFallible();
    loader.RegisterFunction(func);
  }
}

}  // namespace sdb::connector
