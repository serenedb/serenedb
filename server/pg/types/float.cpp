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

#include "pg/types/float.h"

#include <absl/strings/numbers.h>
#include <fmt/format.h>

#include <cstdlib>
#include <duckdb/common/vector_operations/unary_executor.hpp>

#include "query/config.h"

namespace sdb::pg {
namespace {

template<typename T>
constexpr T kFixedMin = 1e-4;
template<>
constexpr float kFixedMin<float> = 1e-4F;

template<typename T>
constexpr T kFixedLimit = 1e15;
template<>
constexpr float kFixedLimit<float> = 1e6F;

template<typename T>
char* WriteShortest(T value, char* buffer) {
  if (std::signbit(value)) {
    value = -value;
    *buffer++ = '-';
  }
  if (value < kFixedLimit<T>) [[likely]] {
    const auto integer = static_cast<uint64_t>(value);
    if (value == static_cast<T>(integer)) [[likely]] {
      return absl::numbers_internal::FastIntToBuffer(integer, buffer);
    }
  }
  const auto decimal = fmt::detail::dragonbox::to_decimal(value);
  char digits[absl::numbers_internal::kFastToBufferSize];
  const auto length = static_cast<int>(
    absl::numbers_internal::FastIntToBuffer(decimal.significand, digits) -
    digits);
  const int point = decimal.exponent + length;
  if (value >= kFixedMin<T> && value < kFixedLimit<T>) {
    if (point <= 0) {
      *buffer++ = '0';
      *buffer++ = '.';
      std::memset(buffer, '0', static_cast<size_t>(-point));
      buffer += -point;
      std::memcpy(buffer, digits, static_cast<size_t>(length));
      return buffer + length;
    }
    SDB_ASSERT(point < length);
    std::memcpy(buffer, digits, static_cast<size_t>(point));
    buffer[point] = '.';
    std::memcpy(buffer + point + 1, digits + point,
                static_cast<size_t>(length - point));
    return buffer + length + 1;
  }
  *buffer++ = digits[0];
  if (length > 1) {
    *buffer++ = '.';
    std::memcpy(buffer, digits + 1, static_cast<size_t>(length - 1));
    buffer += length - 1;
  }
  *buffer++ = 'e';
  int exponent = point - 1;
  *buffer++ = exponent < 0 ? '-' : '+';
  exponent = std::abs(exponent);
  if (exponent >= 100) {
    *buffer++ = static_cast<char>('0' + exponent / 100);
    exponent %= 100;
  }
  *buffer++ = static_cast<char>('0' + exponent / 10);
  *buffer++ = static_cast<char>('0' + exponent % 10);
  return buffer;
}

struct FloatTextCastData final : duckdb::BoundCastData {
  explicit FloatTextCastData(int8_t extra_float_digits)
    : extra_float_digits{extra_float_digits} {}

  duckdb::unique_ptr<duckdb::BoundCastData> Copy() const final {
    return duckdb::make_uniq<FloatTextCastData>(extra_float_digits);
  }

  int8_t extra_float_digits;
};

template<typename T, bool Precise>
bool FloatToText(duckdb::Vector& source, duckdb::Vector& result,
                 duckdb::idx_t count, duckdb::CastParameters& parameters) {
  const auto extra_float_digits =
    parameters.cast_data->Cast<FloatTextCastData>().extra_float_digits;
  duckdb::UnaryExecutor::Execute<T, duckdb::string_t>(
    source, result, count, [&](T value) {
      char buffer[kNumberStrMaxLen];
      return duckdb::StringVector::AddString(
        result, buffer, WriteFloat<Precise>(value, extra_float_digits, buffer));
    });
  return true;
}

template<typename T>
duckdb::BoundCastInfo BindFloatToText(duckdb::BindCastInput& input,
                                      const duckdb::LogicalType&,
                                      const duckdb::LogicalType&) {
  const auto extra_float_digits = static_cast<int8_t>(
    GetSignedIntSetting(*input.context, "extra_float_digits"));
  return {extra_float_digits > 0 ? FloatToText<T, true> : FloatToText<T, false>,
          duckdb::make_uniq<FloatTextCastData>(extra_float_digits)};
}

}  // namespace

void RegisterFloat(duckdb::CastFunctionSet& casts) {
  const duckdb::LogicalType text{duckdb::LogicalType::VARCHAR};
  const duckdb::LogicalType real{duckdb::LogicalType::FLOAT};
  const duckdb::LogicalType precise{duckdb::LogicalType::DOUBLE};
  casts.RegisterCastFunction(real, text, BindFloatToText<float>,
                             casts.ImplicitCastCost(nullptr, real, text));
  casts.RegisterCastFunction(precise, text, BindFloatToText<double>,
                             casts.ImplicitCastCost(nullptr, precise, text));
}

char* WriteShortestFloat(float value, char* buffer) {
  return WriteShortest(value, buffer);
}

char* WriteShortestFloat(double value, char* buffer) {
  return WriteShortest(value, buffer);
}

}  // namespace sdb::pg
