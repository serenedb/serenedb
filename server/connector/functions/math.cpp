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

#include <array>
#include <cmath>
#include <cstdint>
#include <duckdb/common/operator/comparison_operators.hpp>
#include <duckdb/common/random_engine.hpp>
#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/interval.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/vector_operations/generic_executor.hpp>
#include <duckdb/common/vector_operations/ternary_executor.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/create_sort_key.hpp>
#include <duckdb/function/scalar/date_bucket_rewrite.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_binder.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <numbers>
#include <type_traits>

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
