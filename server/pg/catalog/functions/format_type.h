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

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace sdb::pg {

inline constexpr int32_t kVarHdrSz = 4;

inline constexpr int32_t kIntervalMonth = 1 << 1;
inline constexpr int32_t kIntervalYear = 1 << 2;
inline constexpr int32_t kIntervalDay = 1 << 3;
inline constexpr int32_t kIntervalHour = 1 << 10;
inline constexpr int32_t kIntervalMinute = 1 << 11;
inline constexpr int32_t kIntervalSecond = 1 << 12;
inline constexpr int32_t kIntervalFullRange = 0x7FFF;
inline constexpr int32_t kIntervalFullPrecision = 0xFFFF;

struct IntervalRange {
  int32_t mask;
  std::string_view fields;
};

inline constexpr std::array kIntervalRanges{
  IntervalRange{kIntervalYear, "year"},
  IntervalRange{kIntervalMonth, "month"},
  IntervalRange{kIntervalDay, "day"},
  IntervalRange{kIntervalHour, "hour"},
  IntervalRange{kIntervalMinute, "minute"},
  IntervalRange{kIntervalSecond, "second"},
  IntervalRange{kIntervalYear | kIntervalMonth, "year to month"},
  IntervalRange{kIntervalDay | kIntervalHour, "day to hour"},
  IntervalRange{kIntervalDay | kIntervalHour | kIntervalMinute,
                "day to minute"},
  IntervalRange{
    kIntervalDay | kIntervalHour | kIntervalMinute | kIntervalSecond,
    "day to second"},
  IntervalRange{kIntervalHour | kIntervalMinute, "hour to minute"},
  IntervalRange{kIntervalHour | kIntervalMinute | kIntervalSecond,
                "hour to second"},
  IntervalRange{kIntervalMinute | kIntervalSecond, "minute to second"},
};

inline constexpr auto kIntervalFields = std::span{kIntervalRanges}.first<6>();

const IntervalRange* FindIntervalRange(int32_t typmod);

constexpr int32_t NumericTypmod(int32_t precision, int32_t scale) {
  return ((precision << 16) | (scale & 0x7FF)) + kVarHdrSz;
}

constexpr int32_t NumericTypmodPrecision(int32_t typmod) {
  return ((typmod - kVarHdrSz) >> 16) & 0xFFFF;
}

constexpr int32_t NumericTypmodScale(int32_t typmod) {
  return (((typmod - kVarHdrSz) & 0x7FF) ^ 1024) - 1024;
}

std::optional<int32_t> CharMaxLength(int64_t typid, int32_t typmod);
std::optional<int32_t> CharOctetLength(int64_t typid, int32_t typmod);
std::optional<int32_t> NumericPrecision(int64_t typid, int32_t typmod);
std::optional<int32_t> NumericPrecisionRadix(int64_t typid, int32_t typmod);
std::optional<int32_t> NumericScale(int64_t typid, int32_t typmod);
std::optional<int32_t> DatetimePrecision(int64_t typid, int32_t typmod);
std::optional<std::string> IntervalType(int64_t typid, int32_t typmod);

}  // namespace sdb::pg
