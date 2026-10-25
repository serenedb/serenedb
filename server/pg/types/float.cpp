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

#include <absl/algorithm/container.h>
#include <absl/strings/numbers.h>

#include <algorithm>
#include <cfloat>
#include <charconv>
#include <cmath>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string_view>

namespace sdb::pg {
namespace {

size_t WriteText(std::string_view text, char* buffer) {
  return static_cast<size_t>(absl::c_copy(text, buffer) - buffer);
}

template<typename T, T kFixedMin, T kFixedLimit>
size_t WriteShortest(T value, char* buffer) {
  if (std::isnan(value)) {
    return WriteText("NaN", buffer);
  }
  char* out = buffer;
  if (std::signbit(value)) {
    *out++ = '-';
    value = -value;
  }
  if (std::isinf(value)) {
    return static_cast<size_t>(out - buffer) + WriteText("Infinity", out);
  }
  if (value < kFixedLimit) {
    const auto integral = static_cast<uint64_t>(value);
    if (static_cast<T>(integral) == value) {
      return static_cast<size_t>(
        absl::numbers_internal::FastIntToBuffer(integral, out) - buffer);
    }
  }
  const auto format = value >= kFixedMin && value < kFixedLimit
                        ? std::chars_format::fixed
                        : std::chars_format::scientific;
  const auto [end, error] =
    std::to_chars(out, buffer + kFloatText, value, format);
  SDB_ASSERT(error == std::errc{});
  return static_cast<size_t>(end - buffer);
}

template<typename T, int kDigits>
size_t WriteRounded(T value, int8_t extra_float_digits, char* buffer) {
  if (std::isnan(value)) {
    return WriteText("NaN", buffer);
  }
  if (std::isinf(value)) {
    return WriteText(value < 0 ? "-Infinity" : "Infinity", buffer);
  }
  const auto [end, error] = std::to_chars(
    buffer, buffer + kFloatText, value, std::chars_format::general,
    std::max(1, kDigits + extra_float_digits));
  SDB_ASSERT(error == std::errc{});
  return static_cast<size_t>(end - buffer);
}

}  // namespace

size_t WriteFloatShortest(float value, char* buffer) {
  return WriteShortest<float, 1e-4F, 1e6F>(value, buffer);
}

size_t WriteFloatShortest(double value, char* buffer) {
  return WriteShortest<double, 1e-4, 1e15>(value, buffer);
}

size_t WriteFloatRounded(float value, int8_t extra_float_digits, char* buffer) {
  return WriteRounded<float, FLT_DIG>(value, extra_float_digits, buffer);
}

size_t WriteFloatRounded(double value, int8_t extra_float_digits,
                         char* buffer) {
  return WriteRounded<double, DBL_DIG>(value, extra_float_digits, buffer);
}

void ThrowFloatOverflow() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                  ERR_MSG("value out of range: overflow"));
}

void ThrowFloatUnderflow() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                  ERR_MSG("value out of range: underflow"));
}

void ThrowDivisionByZero() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DIVISION_BY_ZERO),
                  ERR_MSG("division by zero"));
}

}  // namespace sdb::pg
