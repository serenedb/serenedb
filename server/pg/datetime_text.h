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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

namespace sdb::pg {

inline constexpr int32_t kMinFastDays = -719162;
inline constexpr int32_t kMaxFastDays = 2932896;
inline constexpr int64_t kMicrosPerDay = 86'400'000'000;
inline constexpr size_t kMaxTimestampText =
  sizeof("9999-12-31 23:59:59.999999") - 1;

struct CivilDate {
  int32_t year;
  uint32_t month;
  uint32_t day;
};

constexpr CivilDate DaysToCivil(int32_t days) noexcept {
  constexpr uint32_t kEras = 82;
  constexpr uint32_t kShift = 719468 + 146097 * kEras;
  const uint32_t shifted = static_cast<uint32_t>(days) + kShift;
  const uint32_t n1 = 4 * shifted + 3;
  const uint32_t century = n1 / 146097;
  const uint32_t n2 = 4 * (n1 % 146097 / 4) + 3;
  const uint64_t p2 = uint64_t{2939745} * n2;
  const uint32_t year_of_century = static_cast<uint32_t>(p2 >> 32);
  const uint32_t day_of_year = static_cast<uint32_t>(p2) / 2939745 / 4;
  const uint32_t n3 = 2141 * day_of_year + 197913;
  const uint32_t month = n3 >> 16;
  const uint32_t day = (n3 & 0xFFFF) / 2141;
  const uint32_t jan_feb = day_of_year >= 306;
  return {
    static_cast<int32_t>(100 * century + year_of_century - 400 * kEras +
                         jan_feb),
    jan_feb ? month - 12 : month,
    day + 1,
  };
}

inline constexpr char kDigitPairs[] =
  "00010203040506070809101112131415161718192021222324252627282930313233343536"
  "37383940414243444546474849505152535455565758596061626364656667686970717273"
  "7475767778798081828384858687888990919293949596979899";

inline char* WriteDigitPair(char* out, uint32_t value) noexcept {
  std::memcpy(out, kDigitPairs + 2 * value, 2);
  return out + 2;
}

inline size_t FormatDate(char* out, int32_t days) noexcept {
  const auto date = DaysToCivil(days);
  const auto year = static_cast<uint32_t>(date.year);
  WriteDigitPair(out, year / 100);
  WriteDigitPair(out + 2, year % 100);
  out[4] = '-';
  WriteDigitPair(out + 5, date.month);
  out[7] = '-';
  WriteDigitPair(out + 8, date.day);
  return 10;
}

inline constexpr int64_t kNanosPerDay = kMicrosPerDay * 1'000;
inline constexpr size_t kMaxTimestampNsText =
  sizeof("9999-12-31 23:59:59.999999999") - 1;

template<int64_t kPerDay>
constexpr void SplitDays(int64_t value, int64_t offset, int64_t& days,
                         int64_t& time) noexcept {
  days = value / kPerDay;
  time = value % kPerDay;
  if (time < 0) {
    time += kPerDay;
    --days;
  }
  time += offset;
  if (time >= kPerDay) {
    time -= kPerDay;
    ++days;
  } else if (time < 0) {
    time += kPerDay;
    --days;
  }
}

template<uint32_t kFractionDigits>
size_t FormatTimestampParts(char* out, int64_t days, uint64_t time) noexcept {
  constexpr uint64_t kPerSecond =
    kFractionDigits == 9 ? 1'000'000'000 : 1'000'000;
  if (days < kMinFastDays || days > kMaxFastDays) {
    return 0;
  }
  FormatDate(out, static_cast<int32_t>(days));
  out[10] = ' ';
  const auto seconds = static_cast<uint32_t>(time / kPerSecond);
  auto fraction = time % kPerSecond;
  WriteDigitPair(out + 11, seconds / 3600);
  out[13] = ':';
  WriteDigitPair(out + 14, seconds / 60 % 60);
  out[16] = ':';
  WriteDigitPair(out + 17, seconds % 60);
  if (fraction == 0) {
    return 19;
  }
  out[19] = '.';
  char* digits = out + 20;
  if constexpr (kFractionDigits == 9) {
    *digits++ = static_cast<char>('0' + fraction / 100'000'000);
    fraction %= 100'000'000;
    digits =
      WriteDigitPair(digits, static_cast<uint32_t>(fraction / 1'000'000));
    fraction %= 1'000'000;
  }
  WriteDigitPair(digits, static_cast<uint32_t>(fraction / 10000));
  WriteDigitPair(digits + 2, static_cast<uint32_t>(fraction / 100 % 100));
  WriteDigitPair(digits + 4, static_cast<uint32_t>(fraction % 100));
  size_t size = 20 + kFractionDigits;
  while (out[size - 1] == '0') {
    --size;
  }
  return size;
}

inline size_t FormatTimestamp(char* out, int64_t micros) noexcept {
  int64_t days = 0;
  int64_t time = 0;
  SplitDays<kMicrosPerDay>(micros, 0, days, time);
  return FormatTimestampParts<6>(out, days, static_cast<uint64_t>(time));
}

}  // namespace sdb::pg
