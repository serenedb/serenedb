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

#include "connector/functions/formatting.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <duckdb/common/operator/add.hpp>
#include <duckdb/common/operator/multiply.hpp>
#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/decimal.hpp>
#include <duckdb/common/types/interval.hpp>
#include <duckdb/common/types/time.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/vector_operations/binary_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <icu-datefunc.hpp>
#include <icu-zone-lut.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "connector/common.h"

namespace sdb::connector {
namespace {

constexpr int32_t kNumMaxItemSize = 8;
constexpr int32_t kMaxRomanLength = 15;
constexpr int32_t kHoursPerDay = 24;
constexpr int32_t kMonthsPerYear = 12;
constexpr int32_t kDaysPerMonth = 30;
constexpr int32_t kMinsPerHour = 60;
constexpr int32_t kSecsPerMinute = 60;
constexpr int32_t kSecsPerHour = 3600;
constexpr int32_t kMaxTzDispHour = 15;
constexpr int64_t kUsecsPerSec = 1'000'000;
constexpr int64_t kUsecsPerMinute = 60 * kUsecsPerSec;
constexpr int64_t kUsecsPerHour = 60 * kUsecsPerMinute;
constexpr int64_t kUsecsPerDay = 24 * kUsecsPerHour;
constexpr int32_t kUnixEpochJulianDay = 2440588;
constexpr int32_t kPostgresEpochJulianDay = 2451545;
constexpr int32_t kDateEndJulianDay = 2147483494;
constexpr int64_t kPostgresEpochUsecs =
  int64_t{kPostgresEpochJulianDay - kUnixEpochJulianDay} * kUsecsPerDay;
constexpr int64_t kMinTimestampUsecs =
  int64_t{-kUnixEpochJulianDay} * kUsecsPerDay;
constexpr int32_t kMaxTimestampPrecision = 6;
constexpr int32_t kKeywordIndexSize = '~' - ' ';
constexpr size_t kZoneTokenMaxLength = 10;
constexpr uint8_t kNumericWidth = duckdb::Decimal::MAX_WIDTH_DECIMAL;
constexpr uint8_t kNumericDefaultScale = 3;
constexpr int32_t kNumericMinSignificantDigits = 16;

constexpr std::string_view kMonthsFull[] = {
  "January", "February", "March",     "April",   "May",      "June",
  "July",    "August",   "September", "October", "November", "December",
};
constexpr std::string_view kMonthsShort[] = {
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
};
constexpr std::string_view kDaysFull[] = {
  "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
};
constexpr std::string_view kDaysShort[] = {
  "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat",
};
constexpr std::string_view kRomanMonthsUpper[] = {
  "XII", "XI", "X", "IX", "VIII", "VII", "VI", "V", "IV", "III", "II", "I",
};
constexpr std::string_view kRomanMonthsLower[] = {
  "xii", "xi", "x", "ix", "viii", "vii", "vi", "v", "iv", "iii", "ii", "i",
};
constexpr std::string_view kMeridiemShort[] = {"am", "pm", "AM", "PM"};
constexpr std::string_view kMeridiemLong[] = {"a.m.", "p.m.", "A.M.", "P.M."};
constexpr std::string_view kEraShort[] = {"ad", "bc", "AD", "BC"};
constexpr std::string_view kEraLong[] = {"a.d.", "b.c.", "A.D.", "B.C."};
constexpr std::string_view kRomanOnes[] = {
  "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX",
};
constexpr std::string_view kRomanTens[] = {
  "X", "XX", "XXX", "XL", "L", "LX", "LXX", "LXXX", "XC",
};
constexpr std::string_view kRomanHundreds[] = {
  "C", "CC", "CCC", "CD", "D", "DC", "DCC", "DCCC", "CM",
};
constexpr std::string_view kUtcZoneIds[] = {
  "UTC",       "Etc/UTC",       "UCT",  "Etc/UCT",
  "Universal", "Etc/Universal", "Zulu", "Etc/Zulu",
};
constexpr std::string_view kGmtZoneIds[] = {
  "GMT",       "Etc/GMT", "GMT0",      "Etc/GMT0",  "GMT+0",
  "Etc/GMT+0", "GMT-0",   "Etc/GMT-0", "Greenwich", "Etc/Greenwich",
};
constexpr std::string_view kZeroOffsetAbbreviations[] = {
  "gmt", "uct", "ut", "utc", "z", "zulu",
};
constexpr std::array<int64_t, kMaxTimestampPrecision + 1> kTimestampScales = {
  1000000, 100000, 10000, 1000, 100, 10, 1,
};
constexpr std::array<int64_t, kMaxTimestampPrecision + 1> kTimestampOffsets = {
  500000, 50000, 5000, 500, 50, 5, 0,
};

int32_t DateToJulian(int32_t year, int32_t month, int32_t day) {
  int64_t shifted_year = year;
  int64_t shifted_month = month;
  if (month > 2) {
    shifted_month += 1;
    shifted_year += 4800;
  } else {
    shifted_month += 13;
    shifted_year += 4799;
  }
  const int64_t century = shifted_year / 100;
  const int64_t julian = shifted_year * 365 - 32167 + shifted_year / 4 -
                         century + century / 4 + 7834 * shifted_month / 256 +
                         day;
  return static_cast<int32_t>(julian);
}

void JulianToDate(int32_t julian_day, int32_t& year, int32_t& month,
                  int32_t& day) {
  auto julian = static_cast<uint32_t>(julian_day);
  julian += 32044;
  uint32_t quad = julian / 146097;
  const uint32_t extra = (julian - quad * 146097) * 4 + 3;
  julian += 60 + quad * 3 + extra / 146097;
  quad = julian / 1461;
  julian -= quad * 1461;
  auto y = static_cast<int32_t>(julian * 4 / 1461);
  julian = (y != 0 ? (julian + 305) % 365 : (julian + 306) % 366) + 123;
  y += static_cast<int32_t>(quad * 4);
  year = y - 4800;
  quad = julian * 2141 / 65536;
  day = static_cast<int32_t>(julian - 7834 * quad / 256);
  month = static_cast<int32_t>((quad + 10) % kMonthsPerYear + 1);
}

int32_t JulianToWeekday(int32_t julian_day) {
  julian_day += 1;
  julian_day %= 7;
  if (julian_day < 0) {
    julian_day += 7;
  }
  return julian_day;
}

int32_t IsoWeekToJulian(int32_t year, int32_t week) {
  const int32_t day4 = DateToJulian(year, 1, 4);
  const int32_t day0 = JulianToWeekday(day4 - 1);
  return static_cast<int32_t>((int64_t{week} - 1) * 7 + day4 - day0);
}

void IsoWeekDateToDate(int32_t week, int32_t weekday, int32_t& year,
                       int32_t& month, int32_t& day) {
  const int64_t julian_day =
    int64_t{IsoWeekToJulian(year, week)} + (weekday > 1 ? weekday - 2 : 6);
  JulianToDate(static_cast<int32_t>(julian_day), year, month, day);
}

int32_t WrappingNegate(int32_t value) {
  return static_cast<int32_t>(0U - static_cast<uint32_t>(value));
}

struct IsoWeek {
  int32_t year;
  int32_t week;
};

IsoWeek DateToIsoWeek(int32_t year, int32_t month, int32_t day) {
  const int32_t day_number = DateToJulian(year, month, day);
  int32_t start = IsoWeekToJulian(year, 1);
  if (day_number < start) {
    start = IsoWeekToJulian(--year, 1);
  }
  int32_t week = (day_number - start) / 7 + 1;
  if (week >= 52) {
    if (const int32_t next = IsoWeekToJulian(year + 1, 1); day_number >= next) {
      ++year;
      week = (day_number - next) / 7 + 1;
    }
  }
  return {year, week};
}

int32_t DateToIsoYearDay(int32_t year, int32_t month, int32_t day) {
  return DateToJulian(year, month, day) -
         IsoWeekToJulian(DateToIsoWeek(year, month, day).year, 1) + 1;
}

bool IsValidJulian(int32_t year, int32_t month) {
  return (year > -4713 || (year == -4713 && month >= 11)) &&
         (year < 5874898 || (year == 5874898 && month < 6));
}

int64_t FloorDivide(int64_t value, int64_t divisor) {
  const auto quotient = value / divisor;
  return (value % divisor != 0 && value < 0) ? quotient - 1 : quotient;
}

size_t Utf8SequenceLength(char lead) {
  const auto c = static_cast<unsigned char>(lead);
  if ((c & 0xe0) == 0xc0) {
    return 2;
  }
  if ((c & 0xf0) == 0xe0) {
    return 3;
  }
  if ((c & 0xf8) == 0xf0) {
    return 4;
  }
  return 1;
}

size_t CharLength(std::string_view str) {
  return std::min(Utf8SequenceLength(str.front()), str.size());
}

size_t CharLength(const char* str) {
  const auto length = Utf8SequenceLength(*str);
  size_t i = 1;
  while (i < length && str[i] != '\0') {
    ++i;
  }
  return i;
}

bool IsSeparatorChar(char c) {
  return c > 0x20 && c < 0x7f && !absl::ascii_isalnum(c);
}

enum class NodeType : uint8_t {
  kAction,
  kChar,
  kSeparator,
  kSpace,
};

enum class DateMode : uint8_t {
  kNone,
  kGregorian,
  kIsoWeek,
};

enum class SuffixType : uint8_t {
  kPrefix,
  kPostfix,
};

constexpr uint8_t kSuffixFillMode = 0x01;
constexpr uint8_t kSuffixOrdinalUpper = 0x02;
constexpr uint8_t kSuffixOrdinalLower = 0x04;
constexpr uint8_t kSuffixSpell = 0x08;
constexpr uint8_t kSuffixTranslate = 0x10;

struct KeySuffix {
  std::string_view name;
  uint8_t id;
  SuffixType type;
};

constexpr KeySuffix kDchSuffixes[] = {
  {"FM", kSuffixFillMode, SuffixType::kPrefix},
  {"fm", kSuffixFillMode, SuffixType::kPrefix},
  {"TM", kSuffixTranslate, SuffixType::kPrefix},
  {"tm", kSuffixTranslate, SuffixType::kPrefix},
  {"TH", kSuffixOrdinalUpper, SuffixType::kPostfix},
  {"th", kSuffixOrdinalLower, SuffixType::kPostfix},
  {"SP", kSuffixSpell, SuffixType::kPostfix},
};

enum class DchKey : uint8_t {
  kAdPeriods,
  kAmPeriods,
  kAd,
  kAm,
  kBcPeriods,
  kBc,
  kCc,
  kDayUpper,
  kDdd,
  kDd,
  kDyUpper,
  kDayCapitalized,
  kDyCapitalized,
  kD,
  kFf1,
  kFf2,
  kFf3,
  kFf4,
  kFf5,
  kFf6,
  kFx,
  kHh24,
  kHh12,
  kHh,
  kIddd,
  kId,
  kIw,
  kIyyy,
  kIyy,
  kIy,
  kI,
  kJ,
  kMi,
  kMm,
  kMonthUpper,
  kMonUpper,
  kMs,
  kMonthCapitalized,
  kMonCapitalized,
  kOf,
  kPmPeriods,
  kPm,
  kQ,
  kRmUpper,
  kSsss,
  kSs,
  kTzh,
  kTzm,
  kTzUpper,
  kUs,
  kWw,
  kW,
  kYCommaYyy,
  kYyyy,
  kYyy,
  kYy,
  kY,
  kAdPeriodsLower,
  kAmPeriodsLower,
  kAdLower,
  kAmLower,
  kBcPeriodsLower,
  kBcLower,
  kDayLower,
  kDyLower,
  kMonthLower,
  kMonLower,
  kPmPeriodsLower,
  kPmLower,
  kRmLower,
  kTzLower,
};

struct DchKeyword {
  std::string_view name;
  DchKey id;
  bool is_digit;
  DateMode date_mode;
};

constexpr DchKeyword kDchKeywords[] = {
  {"A.D.", DchKey::kAdPeriods, false, DateMode::kNone},
  {"A.M.", DchKey::kAmPeriods, false, DateMode::kNone},
  {"AD", DchKey::kAd, false, DateMode::kNone},
  {"AM", DchKey::kAm, false, DateMode::kNone},
  {"B.C.", DchKey::kBcPeriods, false, DateMode::kNone},
  {"BC", DchKey::kBc, false, DateMode::kNone},
  {"CC", DchKey::kCc, true, DateMode::kNone},
  {"DAY", DchKey::kDayUpper, false, DateMode::kNone},
  {"DDD", DchKey::kDdd, true, DateMode::kGregorian},
  {"DD", DchKey::kDd, true, DateMode::kGregorian},
  {"DY", DchKey::kDyUpper, false, DateMode::kNone},
  {"Day", DchKey::kDayCapitalized, false, DateMode::kNone},
  {"Dy", DchKey::kDyCapitalized, false, DateMode::kNone},
  {"D", DchKey::kD, true, DateMode::kGregorian},
  {"FF1", DchKey::kFf1, true, DateMode::kNone},
  {"FF2", DchKey::kFf2, true, DateMode::kNone},
  {"FF3", DchKey::kFf3, true, DateMode::kNone},
  {"FF4", DchKey::kFf4, true, DateMode::kNone},
  {"FF5", DchKey::kFf5, true, DateMode::kNone},
  {"FF6", DchKey::kFf6, true, DateMode::kNone},
  {"FX", DchKey::kFx, false, DateMode::kNone},
  {"HH24", DchKey::kHh24, true, DateMode::kNone},
  {"HH12", DchKey::kHh12, true, DateMode::kNone},
  {"HH", DchKey::kHh, true, DateMode::kNone},
  {"IDDD", DchKey::kIddd, true, DateMode::kIsoWeek},
  {"ID", DchKey::kId, true, DateMode::kIsoWeek},
  {"IW", DchKey::kIw, true, DateMode::kIsoWeek},
  {"IYYY", DchKey::kIyyy, true, DateMode::kIsoWeek},
  {"IYY", DchKey::kIyy, true, DateMode::kIsoWeek},
  {"IY", DchKey::kIy, true, DateMode::kIsoWeek},
  {"I", DchKey::kI, true, DateMode::kIsoWeek},
  {"J", DchKey::kJ, true, DateMode::kNone},
  {"MI", DchKey::kMi, true, DateMode::kNone},
  {"MM", DchKey::kMm, true, DateMode::kGregorian},
  {"MONTH", DchKey::kMonthUpper, false, DateMode::kGregorian},
  {"MON", DchKey::kMonUpper, false, DateMode::kGregorian},
  {"MS", DchKey::kMs, true, DateMode::kNone},
  {"Month", DchKey::kMonthCapitalized, false, DateMode::kGregorian},
  {"Mon", DchKey::kMonCapitalized, false, DateMode::kGregorian},
  {"OF", DchKey::kOf, false, DateMode::kNone},
  {"P.M.", DchKey::kPmPeriods, false, DateMode::kNone},
  {"PM", DchKey::kPm, false, DateMode::kNone},
  {"Q", DchKey::kQ, true, DateMode::kNone},
  {"RM", DchKey::kRmUpper, false, DateMode::kGregorian},
  {"SSSSS", DchKey::kSsss, true, DateMode::kNone},
  {"SSSS", DchKey::kSsss, true, DateMode::kNone},
  {"SS", DchKey::kSs, true, DateMode::kNone},
  {"TZH", DchKey::kTzh, false, DateMode::kNone},
  {"TZM", DchKey::kTzm, true, DateMode::kNone},
  {"TZ", DchKey::kTzUpper, false, DateMode::kNone},
  {"US", DchKey::kUs, true, DateMode::kNone},
  {"WW", DchKey::kWw, true, DateMode::kGregorian},
  {"W", DchKey::kW, true, DateMode::kGregorian},
  {"Y,YYY", DchKey::kYCommaYyy, true, DateMode::kGregorian},
  {"YYYY", DchKey::kYyyy, true, DateMode::kGregorian},
  {"YYY", DchKey::kYyy, true, DateMode::kGregorian},
  {"YY", DchKey::kYy, true, DateMode::kGregorian},
  {"Y", DchKey::kY, true, DateMode::kGregorian},
  {"a.d.", DchKey::kAdPeriodsLower, false, DateMode::kNone},
  {"a.m.", DchKey::kAmPeriodsLower, false, DateMode::kNone},
  {"ad", DchKey::kAdLower, false, DateMode::kNone},
  {"am", DchKey::kAmLower, false, DateMode::kNone},
  {"b.c.", DchKey::kBcPeriodsLower, false, DateMode::kNone},
  {"bc", DchKey::kBcLower, false, DateMode::kNone},
  {"cc", DchKey::kCc, true, DateMode::kNone},
  {"day", DchKey::kDayLower, false, DateMode::kNone},
  {"ddd", DchKey::kDdd, true, DateMode::kGregorian},
  {"dd", DchKey::kDd, true, DateMode::kGregorian},
  {"dy", DchKey::kDyLower, false, DateMode::kNone},
  {"d", DchKey::kD, true, DateMode::kGregorian},
  {"ff1", DchKey::kFf1, true, DateMode::kNone},
  {"ff2", DchKey::kFf2, true, DateMode::kNone},
  {"ff3", DchKey::kFf3, true, DateMode::kNone},
  {"ff4", DchKey::kFf4, true, DateMode::kNone},
  {"ff5", DchKey::kFf5, true, DateMode::kNone},
  {"ff6", DchKey::kFf6, true, DateMode::kNone},
  {"fx", DchKey::kFx, false, DateMode::kNone},
  {"hh24", DchKey::kHh24, true, DateMode::kNone},
  {"hh12", DchKey::kHh12, true, DateMode::kNone},
  {"hh", DchKey::kHh, true, DateMode::kNone},
  {"iddd", DchKey::kIddd, true, DateMode::kIsoWeek},
  {"id", DchKey::kId, true, DateMode::kIsoWeek},
  {"iw", DchKey::kIw, true, DateMode::kIsoWeek},
  {"iyyy", DchKey::kIyyy, true, DateMode::kIsoWeek},
  {"iyy", DchKey::kIyy, true, DateMode::kIsoWeek},
  {"iy", DchKey::kIy, true, DateMode::kIsoWeek},
  {"i", DchKey::kI, true, DateMode::kIsoWeek},
  {"j", DchKey::kJ, true, DateMode::kNone},
  {"mi", DchKey::kMi, true, DateMode::kNone},
  {"mm", DchKey::kMm, true, DateMode::kGregorian},
  {"month", DchKey::kMonthLower, false, DateMode::kGregorian},
  {"mon", DchKey::kMonLower, false, DateMode::kGregorian},
  {"ms", DchKey::kMs, true, DateMode::kNone},
  {"of", DchKey::kOf, false, DateMode::kNone},
  {"p.m.", DchKey::kPmPeriodsLower, false, DateMode::kNone},
  {"pm", DchKey::kPmLower, false, DateMode::kNone},
  {"q", DchKey::kQ, true, DateMode::kNone},
  {"rm", DchKey::kRmLower, false, DateMode::kGregorian},
  {"sssss", DchKey::kSsss, true, DateMode::kNone},
  {"ssss", DchKey::kSsss, true, DateMode::kNone},
  {"ss", DchKey::kSs, true, DateMode::kNone},
  {"tzh", DchKey::kTzh, false, DateMode::kNone},
  {"tzm", DchKey::kTzm, true, DateMode::kNone},
  {"tz", DchKey::kTzLower, false, DateMode::kNone},
  {"us", DchKey::kUs, true, DateMode::kNone},
  {"ww", DchKey::kWw, true, DateMode::kGregorian},
  {"w", DchKey::kW, true, DateMode::kGregorian},
  {"y,yyy", DchKey::kYCommaYyy, true, DateMode::kGregorian},
  {"yyyy", DchKey::kYyyy, true, DateMode::kGregorian},
  {"yyy", DchKey::kYyy, true, DateMode::kGregorian},
  {"yy", DchKey::kYy, true, DateMode::kGregorian},
  {"y", DchKey::kY, true, DateMode::kGregorian},
};

enum class NumKey : uint8_t {
  kComma,
  kDecimalPoint,
  kZero,
  kNine,
  kBlank,
  kIsoCurrency,
  kLocaleDecimal,
  kEeee,
  kFillMode,
  kGroupSeparator,
  kCurrency,
  kMinus,
  kPlus,
  kAngleBrackets,
  kRomanUpper,
  kPlusMinus,
  kSpell,
  kLocaleSign,
  kOrdinalUpper,
  kMultiplier,
  kRomanLower,
  kOrdinalLower,
};

struct NumKeyword {
  std::string_view name;
  NumKey id;
};

constexpr NumKeyword kNumKeywords[] = {
  {",", NumKey::kComma},         {".", NumKey::kDecimalPoint},
  {"0", NumKey::kZero},          {"9", NumKey::kNine},
  {"B", NumKey::kBlank},         {"C", NumKey::kIsoCurrency},
  {"D", NumKey::kLocaleDecimal}, {"EEEE", NumKey::kEeee},
  {"FM", NumKey::kFillMode},     {"G", NumKey::kGroupSeparator},
  {"L", NumKey::kCurrency},      {"MI", NumKey::kMinus},
  {"PL", NumKey::kPlus},         {"PR", NumKey::kAngleBrackets},
  {"RN", NumKey::kRomanUpper},   {"SG", NumKey::kPlusMinus},
  {"SP", NumKey::kSpell},        {"S", NumKey::kLocaleSign},
  {"TH", NumKey::kOrdinalUpper}, {"V", NumKey::kMultiplier},
  {"b", NumKey::kBlank},         {"c", NumKey::kIsoCurrency},
  {"d", NumKey::kLocaleDecimal}, {"eeee", NumKey::kEeee},
  {"fm", NumKey::kFillMode},     {"g", NumKey::kGroupSeparator},
  {"l", NumKey::kCurrency},      {"mi", NumKey::kMinus},
  {"pl", NumKey::kPlus},         {"pr", NumKey::kAngleBrackets},
  {"rn", NumKey::kRomanLower},   {"sg", NumKey::kPlusMinus},
  {"sp", NumKey::kSpell},        {"s", NumKey::kLocaleSign},
  {"th", NumKey::kOrdinalLower}, {"v", NumKey::kMultiplier},
};

using KeywordIndex = std::array<int16_t, kKeywordIndexSize>;

template<typename Keyword, size_t N>
constexpr KeywordIndex BuildKeywordIndex(const Keyword (&keywords)[N]) {
  KeywordIndex index;
  for (auto& position : index) {
    position = -1;
  }
  for (size_t i = 0; i < N; ++i) {
    auto& position =
      index[static_cast<unsigned char>(keywords[i].name.front()) - ' '];
    if (position < 0) {
      position = static_cast<int16_t>(i);
    }
  }
  return index;
}

constexpr KeywordIndex kDchIndex = BuildKeywordIndex(kDchKeywords);
constexpr KeywordIndex kNumIndex = BuildKeywordIndex(kNumKeywords);

template<typename Keyword, size_t N>
const Keyword* SearchKeyword(std::string_view str, const Keyword (&keywords)[N],
                             const KeywordIndex& index) {
  const auto c = static_cast<unsigned char>(str.front());
  if (c <= ' ' || c >= '~') {
    return nullptr;
  }
  const auto position = index[c - ' '];
  if (position < 0) {
    return nullptr;
  }
  for (auto i = static_cast<size_t>(position);
       i < N && keywords[i].name.front() == str.front(); ++i) {
    if (str.starts_with(keywords[i].name)) {
      return &keywords[i];
    }
  }
  return nullptr;
}

const KeySuffix* SearchSuffix(std::string_view str, SuffixType type) {
  const auto* it = absl::c_find_if(kDchSuffixes, [&](const KeySuffix& s) {
    return s.type == type && str.starts_with(s.name);
  });
  return it == std::end(kDchSuffixes) ? nullptr : it;
}

template<typename Keyword>
struct FormatNode {
  NodeType type;
  uint8_t suffix;
  uint8_t length;
  std::array<char, 4> chars;
  const Keyword* key;

  std::string_view Character() const { return {chars.data(), length}; }
};

using DchNode = FormatNode<DchKeyword>;
using NumNode = FormatNode<NumKeyword>;

template<typename Keyword>
FormatNode<Keyword> MakeCharNode(NodeType type, std::string_view str) {
  FormatNode<Keyword> node{
    .type = type,
    .suffix = 0,
    .length = static_cast<uint8_t>(str.size()),
    .chars = {},
    .key = nullptr,
  };
  absl::c_copy(str, node.chars.begin());
  return node;
}

constexpr int32_t kNumDecimal = 1 << 0;
constexpr int32_t kNumZero = 1 << 1;
constexpr int32_t kNumBlank = 1 << 2;
constexpr int32_t kNumFillMode = 1 << 3;
constexpr int32_t kNumLocaleSign = 1 << 4;
constexpr int32_t kNumBracket = 1 << 5;
constexpr int32_t kNumMinus = 1 << 6;
constexpr int32_t kNumPlus = 1 << 7;
constexpr int32_t kNumRoman = 1 << 8;
constexpr int32_t kNumMulti = 1 << 9;
constexpr int32_t kNumEeee = 1 << 10;

enum class LocaleSign : uint8_t {
  kNone,
  kPre,
  kPost,
};

struct NumDesc {
  int32_t pre = 0;
  int32_t post = 0;
  int32_t flag = 0;
  int32_t pre_lsign_num = 0;
  int32_t multi = 0;
  int32_t zero_start = 0;
  int32_t zero_end = 0;
  LocaleSign lsign = LocaleSign::kNone;

  bool Has(int32_t mask) const { return (flag & mask) != 0; }
};

[[noreturn]] void ThrowNumberFormatError(std::string_view message) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR), ERR_MSG(message));
}

void PrepareNumDesc(NumDesc& num, NumKey id) {
  if (num.Has(kNumEeee) && id != NumKey::kEeee) {
    ThrowNumberFormatError("\"EEEE\" must be the last pattern used");
  }
  switch (id) {
    case NumKey::kNine:
      if (num.Has(kNumBracket)) {
        ThrowNumberFormatError("\"9\" must be ahead of \"PR\"");
      }
      if (num.Has(kNumMulti)) {
        ++num.multi;
        break;
      }
      if (num.Has(kNumDecimal)) {
        ++num.post;
      } else {
        ++num.pre;
      }
      break;
    case NumKey::kZero:
      if (num.Has(kNumBracket)) {
        ThrowNumberFormatError("\"0\" must be ahead of \"PR\"");
      }
      if (!num.Has(kNumZero) && !num.Has(kNumDecimal)) {
        num.flag |= kNumZero;
        num.zero_start = num.pre + 1;
      }
      if (!num.Has(kNumDecimal)) {
        ++num.pre;
      } else {
        ++num.post;
      }
      num.zero_end = num.pre + num.post;
      break;
    case NumKey::kBlank:
      if (num.pre == 0 && num.post == 0 && !num.Has(kNumZero)) {
        num.flag |= kNumBlank;
      }
      break;
    case NumKey::kLocaleDecimal:
    case NumKey::kDecimalPoint:
      if (num.Has(kNumDecimal)) {
        ThrowNumberFormatError("multiple decimal points");
      }
      if (num.Has(kNumMulti)) {
        ThrowNumberFormatError("cannot use \"V\" and decimal point together");
      }
      num.flag |= kNumDecimal;
      break;
    case NumKey::kFillMode:
      num.flag |= kNumFillMode;
      break;
    case NumKey::kLocaleSign:
      if (num.Has(kNumLocaleSign)) {
        ThrowNumberFormatError("cannot use \"S\" twice");
      }
      if (num.Has(kNumPlus) || num.Has(kNumMinus) || num.Has(kNumBracket)) {
        ThrowNumberFormatError(
          "cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together");
      }
      if (!num.Has(kNumDecimal)) {
        num.lsign = LocaleSign::kPre;
        num.pre_lsign_num = num.pre;
        num.flag |= kNumLocaleSign;
      } else if (num.lsign == LocaleSign::kNone) {
        num.lsign = LocaleSign::kPost;
        num.flag |= kNumLocaleSign;
      }
      break;
    case NumKey::kMinus:
      if (num.Has(kNumLocaleSign)) {
        ThrowNumberFormatError("cannot use \"S\" and \"MI\" together");
      }
      num.flag |= kNumMinus;
      break;
    case NumKey::kPlus:
      if (num.Has(kNumLocaleSign)) {
        ThrowNumberFormatError("cannot use \"S\" and \"PL\" together");
      }
      num.flag |= kNumPlus;
      break;
    case NumKey::kPlusMinus:
      if (num.Has(kNumLocaleSign)) {
        ThrowNumberFormatError("cannot use \"S\" and \"SG\" together");
      }
      num.flag |= kNumMinus | kNumPlus;
      break;
    case NumKey::kAngleBrackets:
      if (num.Has(kNumLocaleSign) || num.Has(kNumPlus) || num.Has(kNumMinus)) {
        ThrowNumberFormatError(
          "cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together");
      }
      num.flag |= kNumBracket;
      break;
    case NumKey::kRomanUpper:
    case NumKey::kRomanLower:
      if (num.Has(kNumRoman)) {
        ThrowNumberFormatError("cannot use \"RN\" twice");
      }
      num.flag |= kNumRoman;
      break;
    case NumKey::kMultiplier:
      if (num.Has(kNumDecimal)) {
        ThrowNumberFormatError("cannot use \"V\" and decimal point together");
      }
      num.flag |= kNumMulti;
      break;
    case NumKey::kEeee:
      if (num.Has(kNumEeee)) {
        ThrowNumberFormatError("cannot use \"EEEE\" twice");
      }
      if (num.Has(kNumBlank) || num.Has(kNumFillMode) ||
          num.Has(kNumLocaleSign) || num.Has(kNumBracket) ||
          num.Has(kNumMinus) || num.Has(kNumPlus) || num.Has(kNumRoman) ||
          num.Has(kNumMulti)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("\"EEEE\" is incompatible with other formats"),
                        ERR_DETAIL("\"EEEE\" may only be used together with "
                                   "digit and decimal point patterns."));
      }
      num.flag |= kNumEeee;
      break;
    case NumKey::kComma:
    case NumKey::kIsoCurrency:
    case NumKey::kGroupSeparator:
    case NumKey::kCurrency:
    case NumKey::kSpell:
    case NumKey::kOrdinalUpper:
    case NumKey::kOrdinalLower:
      break;
  }
  if (num.Has(kNumRoman) && (num.flag & ~(kNumRoman | kNumFillMode)) != 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("\"RN\" is incompatible with other formats"),
      ERR_DETAIL("\"RN\" may only be used together with \"FM\"."));
  }
}

template<typename Keyword>
struct ParsedFormat {
  std::vector<FormatNode<Keyword>> nodes;
  NumDesc num;
};

template<typename Keyword>
ParsedFormat<Keyword> ParseFormat(std::string_view str) {
  constexpr bool kDateTime = std::is_same_v<Keyword, DchKeyword>;
  ParsedFormat<Keyword> parsed;
  while (!str.empty()) {
    uint8_t suffix = 0;
    if constexpr (kDateTime) {
      if (const auto* prefix = SearchSuffix(str, SuffixType::kPrefix)) {
        suffix |= prefix->id;
        str.remove_prefix(prefix->name.size());
      }
    }
    if (str.empty()) {
      break;
    }
    const Keyword* key = nullptr;
    if constexpr (kDateTime) {
      key = SearchKeyword(str, kDchKeywords, kDchIndex);
    } else {
      key = SearchKeyword(str, kNumKeywords, kNumIndex);
    }
    if (key) {
      auto& node = parsed.nodes.emplace_back(FormatNode<Keyword>{
        .type = NodeType::kAction,
        .suffix = suffix,
        .length = 0,
        .chars = {},
        .key = key,
      });
      str.remove_prefix(key->name.size());
      if constexpr (kDateTime) {
        if (const auto* postfix = SearchSuffix(str, SuffixType::kPostfix)) {
          node.suffix |= postfix->id;
          str.remove_prefix(postfix->name.size());
        }
      } else {
        PrepareNumDesc(parsed.num, key->id);
      }
      continue;
    }
    if (str.front() == '"') {
      str.remove_prefix(1);
      while (!str.empty()) {
        if (str.front() == '"') {
          str.remove_prefix(1);
          break;
        }
        if (str.front() == '\\' && str.size() > 1) {
          str.remove_prefix(1);
        }
        const auto length = CharLength(str);
        parsed.nodes.push_back(
          MakeCharNode<Keyword>(NodeType::kChar, str.substr(0, length)));
        str.remove_prefix(length);
      }
      continue;
    }
    if (str.size() > 1 && str[0] == '\\' && str[1] == '"') {
      str.remove_prefix(1);
    }
    auto type = NodeType::kChar;
    if (kDateTime && IsSeparatorChar(str.front())) {
      type = NodeType::kSeparator;
    } else if (absl::ascii_isspace(str.front())) {
      type = NodeType::kSpace;
    }
    const auto length = CharLength(str);
    parsed.nodes.push_back(MakeCharNode<Keyword>(type, str.substr(0, length)));
    str.remove_prefix(length);
  }
  return parsed;
}

bool HasOrdinal(uint8_t suffix) {
  return (suffix & (kSuffixOrdinalUpper | kSuffixOrdinalLower)) != 0;
}

std::string_view OrdinalSuffix(std::string_view number, bool upper) {
  auto last = number.back();
  if (!absl::ascii_isdigit(last)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                    ERR_MSG("\"", number, "\" is not a number"));
  }
  if (number.size() > 1 && number[number.size() - 2] == '1') {
    last = '0';
  }
  if (last == '1') {
    return upper ? "ST" : "st";
  }
  if (last == '2') {
    return upper ? "ND" : "nd";
  }
  if (last == '3') {
    return upper ? "RD" : "rd";
  }
  return upper ? "TH" : "th";
}

void AppendNumber(std::string& out, int64_t value, int32_t width) {
  std::array<char, 20> digits;
  auto magnitude =
    value < 0 ? 0 - static_cast<uint64_t>(value) : static_cast<uint64_t>(value);
  size_t count = 0;
  do {
    digits[count++] = static_cast<char>('0' + magnitude % 10);
    magnitude /= 10;
  } while (magnitude != 0);
  const auto length = static_cast<int32_t>(count) + (value < 0 ? 1 : 0);
  if (value < 0) {
    out.push_back('-');
  }
  if (width > length) {
    out.append(width - length, '0');
  }
  while (count > 0) {
    out.push_back(digits[--count]);
  }
}

void AppendOrdinal(std::string& out, size_t start, uint8_t suffix) {
  if (HasOrdinal(suffix)) {
    out.append(OrdinalSuffix(std::string_view{out}.substr(start),
                             (suffix & kSuffixOrdinalUpper) != 0));
  }
}

void AppendOrdinalNumber(std::string& out, int64_t value, int32_t width,
                         uint8_t suffix) {
  const auto start = out.size();
  AppendNumber(out, value, width);
  AppendOrdinal(out, start, suffix);
}

int32_t SignedWidth(bool fill_mode, int64_t value, int32_t width) {
  if (fill_mode) {
    return 0;
  }
  return value >= 0 ? width : width + 1;
}

enum class LetterCase : uint8_t {
  kUpper,
  kAsIs,
  kLower,
};

void AppendName(std::string& out, std::string_view name, LetterCase letter_case,
                size_t width) {
  const auto start = out.size();
  out.append(name);
  switch (letter_case) {
    case LetterCase::kUpper:
      for (auto i = start; i < out.size(); ++i) {
        out[i] = absl::ascii_toupper(out[i]);
      }
      break;
    case LetterCase::kAsIs:
      break;
    case LetterCase::kLower:
      for (auto i = start; i < out.size(); ++i) {
        out[i] = absl::ascii_tolower(out[i]);
      }
      break;
  }
  if (name.size() < width) {
    out.append(width - name.size(), ' ');
  }
}

struct TmToChar {
  int32_t tm_sec = 0;
  int32_t tm_min = 0;
  int64_t tm_hour = 0;
  int32_t tm_mday = 1;
  int32_t tm_mon = 1;
  int32_t tm_year = 0;
  int32_t tm_wday = 0;
  int32_t tm_yday = 0;
  int64_t tm_gmtoff = 0;
  int32_t fsec = 0;
  std::string_view tzn;
};

void RejectInterval(bool is_interval) {
  if (is_interval) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_DATETIME_FORMAT),
      ERR_MSG("invalid format specification for an interval value"),
      ERR_HINT("Intervals are not tied to specific calendar dates."));
  }
}

int32_t AdjustYear(int32_t year, bool is_interval) {
  if (is_interval) {
    return year;
  }
  return year <= 0 ? -(year - 1) : year;
}

int32_t ReportedYear(const TmToChar& tm, bool is_interval, bool iso_year) {
  return AdjustYear(iso_year
                      ? DateToIsoWeek(tm.tm_year, tm.tm_mon, tm.tm_mday).year
                      : tm.tm_year,
                    is_interval);
}

size_t FullNameWidth(const DchNode& node) {
  return (node.suffix & (kSuffixTranslate | kSuffixFillMode)) != 0 ? 0 : 9;
}

void AppendMonth(std::string& out, const TmToChar& tm, bool is_interval,
                 std::span<const std::string_view> names,
                 LetterCase letter_case, size_t width) {
  RejectInterval(is_interval);
  if (tm.tm_mon) {
    AppendName(out, names[tm.tm_mon - 1], letter_case, width);
  }
}

void AppendDay(std::string& out, const TmToChar& tm, bool is_interval,
               std::span<const std::string_view> names, LetterCase letter_case,
               size_t width) {
  RejectInterval(is_interval);
  AppendName(out, names[tm.tm_wday], letter_case, width);
}

void AppendRomanMonth(std::string& out, const DchNode& node, const TmToChar& tm,
                      std::span<const std::string_view> months) {
  if (!tm.tm_mon && !tm.tm_year) {
    return;
  }
  int32_t month = 0;
  if (tm.tm_mon == 0) {
    month = tm.tm_year >= 0 ? 0 : kMonthsPerYear - 1;
  } else if (tm.tm_mon < 0) {
    month = -1 * (tm.tm_mon + 1);
  } else {
    month = kMonthsPerYear - tm.tm_mon;
  }
  AppendName(out, months[month], LetterCase::kAsIs,
             (node.suffix & kSuffixFillMode) != 0 ? 0 : 4);
}

void DchToChar(std::span<const DchNode> nodes, bool is_interval,
               const TmToChar& tm, std::string& out) {
  const bool pm = tm.tm_hour % kHoursPerDay >= kHoursPerDay / 2;
  const auto gmtoff = std::abs(static_cast<int32_t>(tm.tm_gmtoff));
  for (const auto& node : nodes) {
    if (node.type != NodeType::kAction) {
      out.append(node.Character());
      continue;
    }
    const bool fill_mode = (node.suffix & kSuffixFillMode) != 0;
    switch (node.key->id) {
      case DchKey::kAmPeriods:
      case DchKey::kPmPeriods:
        out.append(pm ? "P.M." : "A.M.");
        break;
      case DchKey::kAm:
      case DchKey::kPm:
        out.append(pm ? "PM" : "AM");
        break;
      case DchKey::kAmPeriodsLower:
      case DchKey::kPmPeriodsLower:
        out.append(pm ? "p.m." : "a.m.");
        break;
      case DchKey::kAmLower:
      case DchKey::kPmLower:
        out.append(pm ? "pm" : "am");
        break;
      case DchKey::kHh:
      case DchKey::kHh12: {
        const auto hour = tm.tm_hour % (kHoursPerDay / 2);
        AppendOrdinalNumber(out, hour == 0 ? kHoursPerDay / 2 : hour,
                            SignedWidth(fill_mode, tm.tm_hour, 2), node.suffix);
        break;
      }
      case DchKey::kHh24:
        AppendOrdinalNumber(out, tm.tm_hour,
                            SignedWidth(fill_mode, tm.tm_hour, 2), node.suffix);
        break;
      case DchKey::kMi:
        AppendOrdinalNumber(out, tm.tm_min,
                            SignedWidth(fill_mode, tm.tm_min, 2), node.suffix);
        break;
      case DchKey::kSs:
        AppendOrdinalNumber(out, tm.tm_sec,
                            SignedWidth(fill_mode, tm.tm_sec, 2), node.suffix);
        break;
      case DchKey::kFf1:
        AppendOrdinalNumber(out, tm.fsec / 100000, 1, node.suffix);
        break;
      case DchKey::kFf2:
        AppendOrdinalNumber(out, tm.fsec / 10000, 2, node.suffix);
        break;
      case DchKey::kFf3:
      case DchKey::kMs:
        AppendOrdinalNumber(out, tm.fsec / 1000, 3, node.suffix);
        break;
      case DchKey::kFf4:
        AppendOrdinalNumber(out, tm.fsec / 100, 4, node.suffix);
        break;
      case DchKey::kFf5:
        AppendOrdinalNumber(out, tm.fsec / 10, 5, node.suffix);
        break;
      case DchKey::kFf6:
      case DchKey::kUs:
        AppendOrdinalNumber(out, tm.fsec, 6, node.suffix);
        break;
      case DchKey::kSsss:
        AppendOrdinalNumber(
          out,
          tm.tm_hour * kSecsPerHour + tm.tm_min * kSecsPerMinute + tm.tm_sec, 0,
          node.suffix);
        break;
      case DchKey::kTzLower:
        RejectInterval(is_interval);
        for (const char c : tm.tzn) {
          out.push_back(absl::ascii_tolower(c));
        }
        break;
      case DchKey::kTzUpper:
        RejectInterval(is_interval);
        out.append(tm.tzn);
        break;
      case DchKey::kTzh:
        RejectInterval(is_interval);
        out.push_back(tm.tm_gmtoff >= 0 ? '+' : '-');
        AppendNumber(out, gmtoff / kSecsPerHour, 2);
        break;
      case DchKey::kTzm:
        RejectInterval(is_interval);
        AppendNumber(out, gmtoff % kSecsPerHour / kSecsPerMinute, 2);
        break;
      case DchKey::kOf:
        RejectInterval(is_interval);
        out.push_back(tm.tm_gmtoff >= 0 ? '+' : '-');
        AppendNumber(out, gmtoff / kSecsPerHour, fill_mode ? 0 : 2);
        if (gmtoff % kSecsPerHour != 0) {
          out.push_back(':');
          AppendNumber(out, gmtoff % kSecsPerHour / kSecsPerMinute, 2);
        }
        break;
      case DchKey::kAdPeriods:
      case DchKey::kBcPeriods:
        RejectInterval(is_interval);
        out.append(tm.tm_year <= 0 ? "B.C." : "A.D.");
        break;
      case DchKey::kAd:
      case DchKey::kBc:
        RejectInterval(is_interval);
        out.append(tm.tm_year <= 0 ? "BC" : "AD");
        break;
      case DchKey::kAdPeriodsLower:
      case DchKey::kBcPeriodsLower:
        RejectInterval(is_interval);
        out.append(tm.tm_year <= 0 ? "b.c." : "a.d.");
        break;
      case DchKey::kAdLower:
      case DchKey::kBcLower:
        RejectInterval(is_interval);
        out.append(tm.tm_year <= 0 ? "bc" : "ad");
        break;
      case DchKey::kMonthUpper:
        AppendMonth(out, tm, is_interval, kMonthsFull, LetterCase::kUpper,
                    FullNameWidth(node));
        break;
      case DchKey::kMonthCapitalized:
        AppendMonth(out, tm, is_interval, kMonthsFull, LetterCase::kAsIs,
                    FullNameWidth(node));
        break;
      case DchKey::kMonthLower:
        AppendMonth(out, tm, is_interval, kMonthsFull, LetterCase::kLower,
                    FullNameWidth(node));
        break;
      case DchKey::kMonUpper:
        AppendMonth(out, tm, is_interval, kMonthsShort, LetterCase::kUpper, 0);
        break;
      case DchKey::kMonCapitalized:
        AppendMonth(out, tm, is_interval, kMonthsShort, LetterCase::kAsIs, 0);
        break;
      case DchKey::kMonLower:
        AppendMonth(out, tm, is_interval, kMonthsShort, LetterCase::kLower, 0);
        break;
      case DchKey::kMm:
        AppendOrdinalNumber(out, tm.tm_mon,
                            SignedWidth(fill_mode, tm.tm_mon, 2), node.suffix);
        break;
      case DchKey::kDayUpper:
        AppendDay(out, tm, is_interval, kDaysFull, LetterCase::kUpper,
                  FullNameWidth(node));
        break;
      case DchKey::kDayCapitalized:
        AppendDay(out, tm, is_interval, kDaysFull, LetterCase::kAsIs,
                  FullNameWidth(node));
        break;
      case DchKey::kDayLower:
        AppendDay(out, tm, is_interval, kDaysFull, LetterCase::kLower,
                  FullNameWidth(node));
        break;
      case DchKey::kDyUpper:
        AppendDay(out, tm, is_interval, kDaysShort, LetterCase::kUpper, 0);
        break;
      case DchKey::kDyCapitalized:
        AppendDay(out, tm, is_interval, kDaysShort, LetterCase::kAsIs, 0);
        break;
      case DchKey::kDyLower:
        AppendDay(out, tm, is_interval, kDaysShort, LetterCase::kLower, 0);
        break;
      case DchKey::kDdd:
        AppendOrdinalNumber(out, tm.tm_yday, fill_mode ? 0 : 3, node.suffix);
        break;
      case DchKey::kIddd:
        AppendOrdinalNumber(out,
                            DateToIsoYearDay(tm.tm_year, tm.tm_mon, tm.tm_mday),
                            fill_mode ? 0 : 3, node.suffix);
        break;
      case DchKey::kDd:
        AppendOrdinalNumber(out, tm.tm_mday, fill_mode ? 0 : 2, node.suffix);
        break;
      case DchKey::kD:
        RejectInterval(is_interval);
        AppendOrdinalNumber(out, tm.tm_wday + 1, 0, node.suffix);
        break;
      case DchKey::kId:
        RejectInterval(is_interval);
        AppendOrdinalNumber(out, tm.tm_wday == 0 ? 7 : tm.tm_wday, 0,
                            node.suffix);
        break;
      case DchKey::kWw:
        AppendOrdinalNumber(out, (tm.tm_yday - 1) / 7 + 1, fill_mode ? 0 : 2,
                            node.suffix);
        break;
      case DchKey::kIw:
        AppendOrdinalNumber(
          out, DateToIsoWeek(tm.tm_year, tm.tm_mon, tm.tm_mday).week,
          fill_mode ? 0 : 2, node.suffix);
        break;
      case DchKey::kQ:
        if (tm.tm_mon) {
          AppendOrdinalNumber(out, (tm.tm_mon - 1) / 3 + 1, 0, node.suffix);
        }
        break;
      case DchKey::kCc: {
        int32_t century = 0;
        if (is_interval) {
          century = tm.tm_year / 100;
        } else if (tm.tm_year > 0) {
          century = (tm.tm_year - 1) / 100 + 1;
        } else {
          century = tm.tm_year / 100 - 1;
        }
        const bool two_digits = century <= 99 && century >= -99;
        AppendOrdinalNumber(out, century,
                            two_digits ? SignedWidth(fill_mode, century, 2) : 0,
                            node.suffix);
        break;
      }
      case DchKey::kYCommaYyy: {
        const auto start = out.size();
        const int32_t year = AdjustYear(tm.tm_year, is_interval);
        const int32_t millennia = year / 1000;
        AppendNumber(out, millennia, 0);
        out.push_back(',');
        AppendNumber(out, year - millennia * 1000, 3);
        AppendOrdinal(out, start, node.suffix);
        break;
      }
      case DchKey::kYyyy:
      case DchKey::kIyyy:
        AppendOrdinalNumber(
          out, ReportedYear(tm, is_interval, node.key->id == DchKey::kIyyy),
          SignedWidth(fill_mode, AdjustYear(tm.tm_year, is_interval), 4),
          node.suffix);
        break;
      case DchKey::kYyy:
      case DchKey::kIyy:
        AppendOrdinalNumber(
          out,
          ReportedYear(tm, is_interval, node.key->id == DchKey::kIyy) % 1000,
          SignedWidth(fill_mode, AdjustYear(tm.tm_year, is_interval), 3),
          node.suffix);
        break;
      case DchKey::kYy:
      case DchKey::kIy:
        AppendOrdinalNumber(
          out, ReportedYear(tm, is_interval, node.key->id == DchKey::kIy) % 100,
          SignedWidth(fill_mode, AdjustYear(tm.tm_year, is_interval), 2),
          node.suffix);
        break;
      case DchKey::kY:
      case DchKey::kI:
        AppendOrdinalNumber(
          out, ReportedYear(tm, is_interval, node.key->id == DchKey::kI) % 10,
          1, node.suffix);
        break;
      case DchKey::kRmUpper:
        AppendRomanMonth(out, node, tm, kRomanMonthsUpper);
        break;
      case DchKey::kRmLower:
        AppendRomanMonth(out, node, tm, kRomanMonthsLower);
        break;
      case DchKey::kW:
        AppendOrdinalNumber(out, (tm.tm_mday - 1) / 7 + 1, 0, node.suffix);
        break;
      case DchKey::kJ:
        AppendOrdinalNumber(
          out, DateToJulian(tm.tm_year, tm.tm_mon, tm.tm_mday), 0, node.suffix);
        break;
      case DchKey::kFx:
        break;
    }
  }
}

enum class Clock : uint8_t {
  k24Hour,
  k12Hour,
};

struct TmFromChar {
  DateMode mode = DateMode::kNone;
  int32_t hh = 0;
  int32_t pm = 0;
  int32_t mi = 0;
  int32_t ss = 0;
  int32_t ssss = 0;
  int32_t d = 0;
  int32_t dd = 0;
  int32_t ddd = 0;
  int32_t mm = 0;
  int32_t ms = 0;
  int32_t year = 0;
  int32_t bc = 0;
  int32_t ww = 0;
  int32_t w = 0;
  int32_t cc = 0;
  int32_t j = 0;
  int32_t us = 0;
  int32_t yysz = 0;
  Clock clock = Clock::k24Hour;
  int32_t tzsign = 0;
  int32_t tzh = 0;
  int32_t tzm = 0;
  int32_t ff = 0;
  bool has_tz = false;
};

bool IsNextSeparator(std::span<const DchNode> nodes, size_t index) {
  if (HasOrdinal(nodes[index].suffix)) {
    return true;
  }
  if (index + 1 == nodes.size()) {
    return true;
  }
  const auto& next = nodes[index + 1];
  if (next.type == NodeType::kAction) {
    return !next.key->is_digit;
  }
  return !(next.length == 1 && absl::ascii_isdigit(next.chars[0]));
}

int32_t AdjustPartialYearTo2020(int32_t year) {
  if (year < 70) {
    return year + 2000;
  }
  if (year < 100) {
    return year + 1900;
  }
  if (year < 520) {
    return year + 2000;
  }
  if (year < 1000) {
    return year + 1000;
  }
  return year;
}

void FromCharSetMode(TmFromChar& tmfc, DateMode mode) {
  if (mode == DateMode::kNone) {
    return;
  }
  if (tmfc.mode == DateMode::kNone) {
    tmfc.mode = mode;
  } else if (tmfc.mode != mode) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_DATETIME_FORMAT),
                    ERR_MSG("invalid combination of date conventions"),
                    ERR_HINT("Do not mix Gregorian and ISO week date "
                             "conventions in a formatting template."));
  }
}

void FromCharSetInt(int32_t& dest, int32_t value, const DchNode& node) {
  if (dest != 0 && dest != value) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_DATETIME_FORMAT),
      ERR_MSG("conflicting values for \"", node.key->name,
              "\" field in formatting string"),
      ERR_DETAIL(
        "This value contradicts a previous setting for the same field type."));
  }
  dest = value;
}

int32_t FromCharParseIntLength(int32_t* dest, const char*& src, int32_t length,
                               std::span<const DchNode> nodes, size_t index) {
  const auto& node = nodes[index];
  const char* init = src;
  while (*src != '\0' && absl::ascii_isspace(*src)) {
    ++src;
  }
  const auto remaining = std::strlen(src);
  const std::string copy{src, std::min<size_t>(length, remaining)};
  auto used = static_cast<int32_t>(remaining);
  long result = 0;
  if ((node.suffix & kSuffixFillMode) || IsNextSeparator(nodes, index)) {
    char* end = nullptr;
    errno = 0;
    result = std::strtol(init, &end, 10);
    src = end;
  } else {
    if (used < length) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_DATETIME_FORMAT),
        ERR_MSG("source string too short for \"", node.key->name,
                "\" formatting field"),
        ERR_DETAIL("Field requires ", length, " characters, but only ", used,
                   " remain."),
        ERR_HINT("If your source string is not fixed-width, try using the "
                 "\"FM\" modifier."));
    }
    char* last = nullptr;
    errno = 0;
    result = std::strtol(copy.c_str(), &last, 10);
    used = static_cast<int32_t>(last - copy.c_str());
    if (used > 0 && used < length) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_DATETIME_FORMAT),
        ERR_MSG("invalid value \"", copy, "\" for \"", node.key->name, "\""),
        ERR_DETAIL("Field requires ", length, " characters, but only ", used,
                   " could be parsed."),
        ERR_HINT("If your source string is not fixed-width, try using the "
                 "\"FM\" modifier."));
    }
    src += used;
  }
  if (src == init) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_DATETIME_FORMAT),
      ERR_MSG("invalid value \"", copy, "\" for \"", node.key->name, "\""),
      ERR_DETAIL("Value must be an integer."));
  }
  if (errno == ERANGE || result < std::numeric_limits<int32_t>::min() ||
      result > std::numeric_limits<int32_t>::max()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                    ERR_MSG("value for \"", node.key->name,
                            "\" in source string is out of range"),
                    ERR_DETAIL("Value must be in the range ",
                               std::numeric_limits<int32_t>::min(), " to ",
                               std::numeric_limits<int32_t>::max(), "."));
  }
  if (dest) {
    FromCharSetInt(*dest, static_cast<int32_t>(result), node);
  }
  return static_cast<int32_t>(src - init);
}

int32_t FromCharParseInt(int32_t* dest, const char*& src,
                         std::span<const DchNode> nodes, size_t index) {
  return FromCharParseIntLength(
    dest, src, static_cast<int32_t>(nodes[index].key->name.size()), nodes,
    index);
}

int32_t FromCharSeqSearch(const char*& src,
                          std::span<const std::string_view> array,
                          const DchNode& node) {
  for (size_t i = 0; *src != '\0' && i < array.size(); ++i) {
    const auto entry = array[i];
    size_t k = 0;
    while (k < entry.size() && src[k] != '\0' &&
           absl::ascii_tolower(entry[k]) == absl::ascii_tolower(src[k])) {
      ++k;
    }
    if (k == entry.size()) {
      src += k;
      return static_cast<int32_t>(i);
    }
  }
  const std::string_view rest{src};
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_DATETIME_FORMAT),
    ERR_MSG("invalid value \"",
            rest.substr(0, rest.find_first_of(" \t\n\r\f\v")), "\" for \"",
            node.key->name, "\""),
    ERR_DETAIL("The given value did not match any of the allowed values "
               "for this field."));
}

void SkipOrdinal(const char*& src, uint8_t suffix) {
  if (!HasOrdinal(suffix)) {
    return;
  }
  for (int32_t i = 0; i < 2; ++i) {
    if (*src != '\0') {
      src += CharLength(src);
    }
  }
}

size_t ZeroOffsetAbbreviationLength(const char* str) {
  std::string token;
  while (token.size() < kZoneTokenMaxLength &&
         absl::ascii_isalpha(str[token.size()])) {
    token.push_back(absl::ascii_tolower(str[token.size()]));
  }
  while (!token.empty() &&
         !absl::c_linear_search(kZeroOffsetAbbreviations, token)) {
    token.pop_back();
  }
  return token.size();
}

void ParseZoneSign(const char*& src, TmFromChar& out, int32_t extra_skip) {
  if (*src == '+' || *src == '-' || *src == ' ') {
    out.tzsign = *src == '-' ? -1 : 1;
    ++src;
  } else if (extra_skip > 0 && *(src - 1) == '-') {
    out.tzsign = -1;
  } else {
    out.tzsign = 1;
  }
}

void ParseZoneOffset(const char*& src, TmFromChar& out, int32_t extra_skip,
                     std::span<const DchNode> nodes, size_t index) {
  ParseZoneSign(src, out, extra_skip);
  FromCharParseIntLength(&out.tzh, src, 2, nodes, index);
  if (*src == ':') {
    ++src;
    FromCharParseIntLength(&out.tzm, src, 2, nodes, index);
  }
}

bool ParseZoneAbbreviation(const char*& src, TmFromChar& out,
                           const DchNode& node) {
  if (const auto length = ZeroOffsetAbbreviationLength(src)) {
    out.has_tz = true;
    out.tzsign = 0;
    src += length;
    return true;
  }
  if (absl::ascii_isalpha(*src)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_DATETIME_FORMAT),
      ERR_MSG("invalid value \"", src, "\" for \"", node.key->name, "\""),
      ERR_DETAIL("Time zone abbreviation is not recognized."));
  }
  return false;
}

void ParseCommaYear(const char*& src, TmFromChar& out, const DchNode& node) {
  int32_t millennia = 0;
  int32_t years = 0;
  int32_t consumed = 0;
  if (std::sscanf(src, "%d,%03d%n", &millennia, &years, &consumed) < 2) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_DATETIME_FORMAT),
                    ERR_MSG("invalid value \"", src, "\" for \"Y,YYY\""));
  }
  if (__builtin_mul_overflow(millennia, 1000, &millennia) ||
      __builtin_add_overflow(years, millennia, &years)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATETIME_FIELD_OVERFLOW),
      ERR_MSG("value for \"Y,YYY\" in source string is out of range"));
  }
  FromCharSetInt(out.year, years, node);
  out.yysz = 4;
  src += consumed;
}

void ParseShortYear(const char*& src, TmFromChar& out,
                    std::span<const DchNode> nodes, size_t index,
                    int32_t size) {
  const auto length = FromCharParseInt(&out.year, src, nodes, index);
  if (length < 4) {
    out.year = AdjustPartialYearTo2020(out.year);
  }
  out.yysz = size;
}

void DchFromChar(std::span<const DchNode> nodes, const char* in,
                 TmFromChar& out) {
  bool fx_mode = false;
  int32_t extra_skip = 0;
  const char* s = in;
  for (size_t i = 0; i < nodes.size() && *s != '\0'; ++i) {
    const auto& node = nodes[i];
    if (!fx_mode &&
        (node.type != NodeType::kAction || node.key->id != DchKey::kFx) &&
        (node.type == NodeType::kAction || i == 0)) {
      while (*s != '\0' && absl::ascii_isspace(*s)) {
        ++s;
        ++extra_skip;
      }
    }
    if (node.type == NodeType::kSpace || node.type == NodeType::kSeparator) {
      if (!fx_mode) {
        --extra_skip;
        if (absl::ascii_isspace(*s) || IsSeparatorChar(*s)) {
          ++s;
          ++extra_skip;
        }
      } else {
        s += CharLength(s);
      }
      continue;
    }
    if (node.type != NodeType::kAction) {
      if (fx_mode || extra_skip <= 0) {
        s += CharLength(s);
      } else {
        --extra_skip;
      }
      continue;
    }
    FromCharSetMode(out, node.key->date_mode);
    switch (node.key->id) {
      case DchKey::kFx:
        fx_mode = true;
        break;
      case DchKey::kAmPeriods:
      case DchKey::kPmPeriods:
      case DchKey::kAmPeriodsLower:
      case DchKey::kPmPeriodsLower:
        FromCharSetInt(out.pm, FromCharSeqSearch(s, kMeridiemLong, node) % 2,
                       node);
        out.clock = Clock::k12Hour;
        break;
      case DchKey::kAm:
      case DchKey::kPm:
      case DchKey::kAmLower:
      case DchKey::kPmLower:
        FromCharSetInt(out.pm, FromCharSeqSearch(s, kMeridiemShort, node) % 2,
                       node);
        out.clock = Clock::k12Hour;
        break;
      case DchKey::kHh:
      case DchKey::kHh12:
        FromCharParseIntLength(&out.hh, s, 2, nodes, i);
        out.clock = Clock::k12Hour;
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kHh24:
        FromCharParseIntLength(&out.hh, s, 2, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kMi:
        FromCharParseInt(&out.mi, s, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kSs:
        FromCharParseInt(&out.ss, s, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kMs: {
        const auto length = FromCharParseIntLength(&out.ms, s, 3, nodes, i);
        out.ms *= length == 1 ? 100 : length == 2 ? 10 : 1;
        SkipOrdinal(s, node.suffix);
        break;
      }
      case DchKey::kFf1:
      case DchKey::kFf2:
      case DchKey::kFf3:
      case DchKey::kFf4:
      case DchKey::kFf5:
      case DchKey::kFf6:
      case DchKey::kUs: {
        if (node.key->id != DchKey::kUs) {
          out.ff = static_cast<int32_t>(node.key->id) -
                   static_cast<int32_t>(DchKey::kFf1) + 1;
        }
        const auto length = FromCharParseIntLength(
          &out.us, s, node.key->id == DchKey::kUs ? 6 : out.ff, nodes, i);
        out.us *= length == 1   ? 100000
                  : length == 2 ? 10000
                  : length == 3 ? 1000
                  : length == 4 ? 100
                  : length == 5 ? 10
                                : 1;
        SkipOrdinal(s, node.suffix);
        break;
      }
      case DchKey::kSsss:
        FromCharParseInt(&out.ssss, s, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kTzLower:
      case DchKey::kTzUpper:
        if (!ParseZoneAbbreviation(s, out, node)) {
          ParseZoneOffset(s, out, extra_skip, nodes, i);
        }
        break;
      case DchKey::kOf:
        ParseZoneOffset(s, out, extra_skip, nodes, i);
        break;
      case DchKey::kTzh:
        ParseZoneSign(s, out, extra_skip);
        FromCharParseIntLength(&out.tzh, s, 2, nodes, i);
        break;
      case DchKey::kTzm:
        if (!out.tzsign) {
          out.tzsign = 1;
        }
        FromCharParseIntLength(&out.tzm, s, 2, nodes, i);
        break;
      case DchKey::kAdPeriods:
      case DchKey::kBcPeriods:
      case DchKey::kAdPeriodsLower:
      case DchKey::kBcPeriodsLower:
        FromCharSetInt(out.bc, FromCharSeqSearch(s, kEraLong, node) % 2, node);
        break;
      case DchKey::kAd:
      case DchKey::kBc:
      case DchKey::kAdLower:
      case DchKey::kBcLower:
        FromCharSetInt(out.bc, FromCharSeqSearch(s, kEraShort, node) % 2, node);
        break;
      case DchKey::kMonthUpper:
      case DchKey::kMonthCapitalized:
      case DchKey::kMonthLower:
        FromCharSetInt(out.mm, FromCharSeqSearch(s, kMonthsFull, node) + 1,
                       node);
        break;
      case DchKey::kMonUpper:
      case DchKey::kMonCapitalized:
      case DchKey::kMonLower:
        FromCharSetInt(out.mm, FromCharSeqSearch(s, kMonthsShort, node) + 1,
                       node);
        break;
      case DchKey::kMm:
        FromCharParseInt(&out.mm, s, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kDayUpper:
      case DchKey::kDayCapitalized:
      case DchKey::kDayLower:
        FromCharSetInt(out.d, FromCharSeqSearch(s, kDaysFull, node), node);
        ++out.d;
        break;
      case DchKey::kDyUpper:
      case DchKey::kDyCapitalized:
      case DchKey::kDyLower:
        FromCharSetInt(out.d, FromCharSeqSearch(s, kDaysShort, node), node);
        ++out.d;
        break;
      case DchKey::kDdd:
        FromCharParseInt(&out.ddd, s, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kIddd:
        FromCharParseIntLength(&out.ddd, s, 3, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kDd:
        FromCharParseInt(&out.dd, s, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kD:
        FromCharParseInt(&out.d, s, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kId:
        FromCharParseIntLength(&out.d, s, 1, nodes, i);
        if (++out.d > 7) {
          out.d = 1;
        }
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kWw:
      case DchKey::kIw:
        FromCharParseInt(&out.ww, s, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kQ:
        FromCharParseInt(nullptr, s, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kCc:
        FromCharParseInt(&out.cc, s, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kYCommaYyy:
        ParseCommaYear(s, out, node);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kYyyy:
      case DchKey::kIyyy:
        FromCharParseInt(&out.year, s, nodes, i);
        out.yysz = 4;
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kYyy:
      case DchKey::kIyy:
        ParseShortYear(s, out, nodes, i, 3);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kYy:
      case DchKey::kIy:
        ParseShortYear(s, out, nodes, i, 2);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kY:
      case DchKey::kI:
        ParseShortYear(s, out, nodes, i, 1);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kRmUpper:
      case DchKey::kRmLower:
        FromCharSetInt(
          out.mm,
          kMonthsPerYear - FromCharSeqSearch(s, kRomanMonthsLower, node), node);
        break;
      case DchKey::kW:
        FromCharParseInt(&out.w, s, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
      case DchKey::kJ:
        FromCharParseInt(&out.j, s, nodes, i);
        SkipOrdinal(s, node.suffix);
        break;
    }
    if (!fx_mode) {
      extra_skip = 0;
      while (*s != '\0' && absl::ascii_isspace(*s)) {
        ++s;
        ++extra_skip;
      }
    }
  }
}

struct PgTm {
  int32_t tm_sec = 0;
  int32_t tm_min = 0;
  int32_t tm_hour = 0;
  int32_t tm_mday = 1;
  int32_t tm_mon = 1;
  int32_t tm_year = 0;
};

struct ParsedDateTime {
  PgTm tm;
  int32_t fsec = 0;
  bool has_tz = false;
  int32_t gmtoffset = 0;
  int32_t fprec = 0;
};

constexpr int32_t kFieldYear = 1 << 0;
constexpr int32_t kFieldMonth = 1 << 1;
constexpr int32_t kFieldDay = 1 << 2;
constexpr int32_t kFieldDate = kFieldYear | kFieldMonth | kFieldDay;

[[noreturn]] void ThrowFieldOverflow(const char* text) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_DATETIME_FIELD_OVERFLOW),
    ERR_MSG("date/time field value out of range: \"", text, "\""));
}

bool IsValidDate(int32_t mask, const PgTm& tm) {
  if ((mask & kFieldMonth) && (tm.tm_mon < 1 || tm.tm_mon > kMonthsPerYear)) {
    return false;
  }
  if ((mask & kFieldDay) && (tm.tm_mday < 1 || tm.tm_mday > 31)) {
    return false;
  }
  return (mask & kFieldDate) != kFieldDate ||
         tm.tm_mday <= (duckdb::Date::IsLeapYear(tm.tm_year)
                          ? duckdb::Date::LEAP_DAYS
                          : duckdb::Date::NORMAL_DAYS)[tm.tm_mon];
}

void ApplyCentury(TmFromChar& tmfc, PgTm& tm, const char* text) {
  if (tmfc.bc) {
    tmfc.cc = WrappingNegate(tmfc.cc);
  }
  tm.tm_year = tmfc.year % 100;
  if (!tm.tm_year) {
    tm.tm_year = static_cast<int32_t>(static_cast<uint32_t>(tmfc.cc) * 100U +
                                      (tmfc.cc >= 0 ? 0U : 1U));
    return;
  }
  int32_t base = 0;
  if (tmfc.cc >= 0) {
    if (__builtin_mul_overflow(tmfc.cc - 1, 100, &base) ||
        __builtin_add_overflow(tm.tm_year, base, &tm.tm_year)) {
      ThrowFieldOverflow(text);
    }
    return;
  }
  if (__builtin_mul_overflow(tmfc.cc + 1, 100, &base) ||
      __builtin_sub_overflow(base, tm.tm_year, &base) ||
      __builtin_add_overflow(base, 1, &tm.tm_year)) {
    ThrowFieldOverflow(text);
  }
}

void ApplyDayOfYear(const TmFromChar& tmfc, PgTm& tm, int32_t& mask) {
  if (!tm.tm_year && !tmfc.bc) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_DATETIME_FORMAT),
      ERR_MSG("cannot calculate day of year without year information"));
  }
  if (tmfc.mode == DateMode::kIsoWeek) {
    const int64_t julian_day =
      int64_t{IsoWeekToJulian(tm.tm_year, 1)} - 1 + tmfc.ddd;
    JulianToDate(static_cast<int32_t>(julian_day), tm.tm_year, tm.tm_mon,
                 tm.tm_mday);
    mask |= kFieldDate;
    return;
  }
  const auto* days = duckdb::Date::IsLeapYear(tm.tm_year)
                       ? duckdb::Date::CUMULATIVE_LEAP_DAYS
                       : duckdb::Date::CUMULATIVE_DAYS;
  int32_t month = 1;
  while (month <= kMonthsPerYear && tmfc.ddd > days[month]) {
    ++month;
  }
  if (tm.tm_mon <= 1) {
    tm.tm_mon = month;
  }
  if (tm.tm_mday <= 1) {
    tm.tm_mday = tmfc.ddd - days[month - 1];
  }
  mask |= kFieldMonth | kFieldDay;
}

ParsedDateTime DoToTimestamp(std::span<const DchNode> nodes, const char* text) {
  TmFromChar tmfc;
  DchFromChar(nodes, text, tmfc);
  ParsedDateTime result;
  auto& tm = result.tm;
  int32_t mask = 0;
  if (tmfc.ssss) {
    int32_t seconds = tmfc.ssss;
    tm.tm_hour = seconds / kSecsPerHour;
    seconds %= kSecsPerHour;
    tm.tm_min = seconds / kSecsPerMinute;
    tm.tm_sec = seconds % kSecsPerMinute;
  }
  if (tmfc.ss) {
    tm.tm_sec = tmfc.ss;
  }
  if (tmfc.mi) {
    tm.tm_min = tmfc.mi;
  }
  if (tmfc.hh) {
    tm.tm_hour = tmfc.hh;
  }
  if (tmfc.clock == Clock::k12Hour) {
    if (tm.tm_hour < 1 || tm.tm_hour > kHoursPerDay / 2) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_DATETIME_FORMAT),
        ERR_MSG("hour \"", tm.tm_hour, "\" is invalid for the 12-hour clock"),
        ERR_HINT("Use the 24-hour clock, or give an hour between 1 and 12."));
    }
    if (tmfc.pm && tm.tm_hour < kHoursPerDay / 2) {
      tm.tm_hour += kHoursPerDay / 2;
    } else if (!tmfc.pm && tm.tm_hour == kHoursPerDay / 2) {
      tm.tm_hour = 0;
    }
  }
  if (tmfc.year) {
    if (tmfc.cc && tmfc.yysz <= 2) {
      ApplyCentury(tmfc, tm, text);
    } else {
      tm.tm_year = tmfc.bc ? WrappingNegate(tmfc.year) : tmfc.year;
      if (tm.tm_year < 0) {
        ++tm.tm_year;
      }
    }
    mask |= kFieldYear;
  } else if (tmfc.cc) {
    if (tmfc.bc) {
      tmfc.cc = WrappingNegate(tmfc.cc);
    }
    if (__builtin_mul_overflow(tmfc.cc >= 0 ? tmfc.cc - 1 : tmfc.cc, 100,
                               &tm.tm_year) ||
        __builtin_add_overflow(tm.tm_year, 1, &tm.tm_year)) {
      ThrowFieldOverflow(text);
    }
    mask |= kFieldYear;
  }
  if (tmfc.j) {
    JulianToDate(tmfc.j, tm.tm_year, tm.tm_mon, tm.tm_mday);
    mask |= kFieldDate;
  }
  if (tmfc.ww) {
    if (tmfc.mode == DateMode::kIsoWeek) {
      if (tmfc.d) {
        IsoWeekDateToDate(tmfc.ww, tmfc.d, tm.tm_year, tm.tm_mon, tm.tm_mday);
      } else {
        JulianToDate(IsoWeekToJulian(tm.tm_year, tmfc.ww), tm.tm_year,
                     tm.tm_mon, tm.tm_mday);
      }
      mask |= kFieldDate;
    } else if (__builtin_sub_overflow(tmfc.ww, 1, &tmfc.ddd) ||
               __builtin_mul_overflow(tmfc.ddd, 7, &tmfc.ddd) ||
               __builtin_add_overflow(tmfc.ddd, 1, &tmfc.ddd)) {
      ThrowFieldOverflow(text);
    }
  }
  if (tmfc.w && (__builtin_sub_overflow(tmfc.w, 1, &tmfc.dd) ||
                 __builtin_mul_overflow(tmfc.dd, 7, &tmfc.dd) ||
                 __builtin_add_overflow(tmfc.dd, 1, &tmfc.dd))) {
    ThrowFieldOverflow(text);
  }
  if (tmfc.dd) {
    tm.tm_mday = tmfc.dd;
    mask |= kFieldDay;
  }
  if (tmfc.mm) {
    tm.tm_mon = tmfc.mm;
    mask |= kFieldMonth;
  }
  if (tmfc.ddd && (tm.tm_mon <= 1 || tm.tm_mday <= 1)) {
    ApplyDayOfYear(tmfc, tm, mask);
  }
  if (tmfc.ms) {
    int32_t millis = 0;
    if (__builtin_mul_overflow(tmfc.ms, 1000, &millis) ||
        __builtin_add_overflow(result.fsec, millis, &result.fsec)) {
      ThrowFieldOverflow(text);
    }
  }
  if (__builtin_add_overflow(result.fsec, tmfc.us, &result.fsec)) {
    ThrowFieldOverflow(text);
  }
  result.fprec = tmfc.ff;
  if (mask != 0 && !IsValidDate(mask, tm)) {
    ThrowFieldOverflow(text);
  }
  if (tm.tm_hour < 0 || tm.tm_hour >= kHoursPerDay || tm.tm_min < 0 ||
      tm.tm_min >= kMinsPerHour || tm.tm_sec < 0 ||
      tm.tm_sec >= kSecsPerMinute || result.fsec < 0 ||
      result.fsec >= kUsecsPerSec) {
    ThrowFieldOverflow(text);
  }
  if (tmfc.tzsign) {
    if (tmfc.tzh < 0 || tmfc.tzh > kMaxTzDispHour || tmfc.tzm < 0 ||
        tmfc.tzm >= kMinsPerHour) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE),
        ERR_MSG("time zone displacement out of range: \"", text, "\""));
    }
    result.has_tz = true;
    result.gmtoffset = (tmfc.tzh * kMinsPerHour + tmfc.tzm) * kSecsPerMinute;
    if (tmfc.tzsign > 0) {
      result.gmtoffset = -result.gmtoffset;
    }
  } else if (tmfc.has_tz) {
    result.has_tz = true;
  }
  return result;
}

struct DecimalNumber {
  bool negative = false;
  std::string digits;
  int32_t scale = 0;
};

void StripLeadingZeros(DecimalNumber& number) {
  const auto first = number.digits.find_first_not_of('0');
  number.digits.erase(
    0, first == std::string::npos ? number.digits.size() : first);
  if (number.digits.empty()) {
    number.negative = false;
  }
}

DecimalNumber DecimalFromText(std::string_view text) {
  DecimalNumber number;
  if (text.starts_with('-')) {
    number.negative = true;
    text.remove_prefix(1);
  }
  const auto point = text.find('.');
  number.digits.assign(text.substr(0, point));
  if (point != std::string_view::npos) {
    const auto fraction = text.substr(point + 1);
    number.digits.append(fraction);
    number.scale = static_cast<int32_t>(fraction.size());
  }
  StripLeadingZeros(number);
  return number;
}

void ShiftDecimal(DecimalNumber& number, int32_t places) {
  number.scale -= places;
  if (number.scale < 0) {
    if (!number.digits.empty()) {
      number.digits.append(-number.scale, '0');
    }
    number.scale = 0;
  }
}

void RoundDecimal(DecimalNumber& number, int32_t scale) {
  if (scale >= number.scale) {
    if (!number.digits.empty()) {
      number.digits.append(scale - number.scale, '0');
    }
    number.scale = scale;
    return;
  }
  const auto keep =
    static_cast<int32_t>(number.digits.size()) - (number.scale - scale);
  const bool round_up = keep >= 0 && number.digits[keep] >= '5';
  number.digits.resize(std::max(keep, 0));
  number.scale = scale;
  if (round_up) {
    auto position = number.digits.size();
    while (position > 0 && number.digits[position - 1] == '9') {
      number.digits[--position] = '0';
    }
    if (position == 0) {
      number.digits.insert(number.digits.begin(), '1');
    } else {
      ++number.digits[position - 1];
    }
  }
  StripLeadingZeros(number);
}

void AppendUnsignedDecimal(std::string& out, const DecimalNumber& number) {
  const auto size = static_cast<int32_t>(number.digits.size());
  const int32_t integer_digits = size - number.scale;
  if (integer_digits > 0) {
    out.append(number.digits, 0, integer_digits);
  } else {
    out.push_back('0');
  }
  if (number.scale > 0) {
    out.push_back('.');
    if (integer_digits < 0) {
      out.append(-integer_digits, '0');
    }
    out.append(number.digits, std::max(integer_digits, 0));
  }
}

std::string ScientificDecimal(DecimalNumber number, int32_t scale) {
  int32_t exponent = 0;
  if (!number.digits.empty()) {
    const auto last = static_cast<int32_t>(number.digits.size()) - 1;
    exponent = last - number.scale;
    number.scale = last;
  }
  RoundDecimal(number, std::max(scale, 0));
  std::string out = number.negative ? "-" : "";
  AppendUnsignedDecimal(out, number);
  out.push_back('e');
  out.push_back(exponent < 0 ? '-' : '+');
  if (std::abs(exponent) < 10) {
    out.push_back('0');
  }
  absl::StrAppend(&out, std::abs(exponent));
  return out;
}

int32_t RoundToInt32(DecimalNumber number) {
  RoundDecimal(number, 0);
  if (number.digits.size() > 10) {
    return std::numeric_limits<int32_t>::max();
  }
  int64_t magnitude = 0;
  for (const char digit : number.digits) {
    magnitude = magnitude * 10 + (digit - '0');
  }
  const int64_t value = number.negative ? -magnitude : magnitude;
  if (value < std::numeric_limits<int32_t>::min() ||
      value > std::numeric_limits<int32_t>::max()) {
    return std::numeric_limits<int32_t>::max();
  }
  return static_cast<int32_t>(value);
}

[[noreturn]] void ThrowNumericOverflow(int32_t precision, int32_t scale) {
  const int32_t max_digits = precision - scale;
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    ERR_MSG("numeric field overflow"),
    ERR_DETAIL("A field with precision ", precision, ", scale ", scale,
               " must round to an absolute value less than ",
               max_digits ? "10^" : "", max_digits ? max_digits : 1, "."));
}

DecimalNumber NumericInput(std::string_view text) {
  DecimalNumber number;
  size_t i = 0;
  while (i < text.size() && absl::ascii_isspace(text[i])) {
    ++i;
  }
  if (i < text.size() && (text[i] == '+' || text[i] == '-')) {
    number.negative = text[i] == '-';
    ++i;
  }
  bool has_digits = false;
  while (i < text.size() && absl::ascii_isdigit(text[i])) {
    number.digits.push_back(text[i++]);
    has_digits = true;
  }
  if (i < text.size() && text[i] == '.') {
    ++i;
    while (i < text.size() && absl::ascii_isdigit(text[i])) {
      number.digits.push_back(text[i++]);
      ++number.scale;
      has_digits = true;
    }
  }
  while (i < text.size() && absl::ascii_isspace(text[i])) {
    ++i;
  }
  if (!has_digits || i != text.size()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid input syntax for type numeric: \"", text, "\""));
  }
  StripLeadingZeros(number);
  return number;
}

duckdb::hugeint_t DecimalToStorage(DecimalNumber number, uint8_t width,
                                   uint8_t scale) {
  RoundDecimal(number, scale);
  if (number.digits.size() > width) {
    ThrowNumericOverflow(width, scale);
  }
  duckdb::hugeint_t value = 0;
  for (const char digit : number.digits) {
    value = value * int64_t{10} + int64_t{digit - '0'};
  }
  return number.negative ? -value : value;
}

std::string IntToRoman(int32_t number) {
  if (number > 3999 || number < 1) {
    return std::string(kMaxRomanLength, '#');
  }
  std::string result;
  const auto digits = absl::StrCat(number);
  auto position = digits.size();
  for (const char digit : digits) {
    const int32_t index = digit - '1';
    if (index >= 0) {
      if (position == 4) {
        result.append(index + 1, 'M');
      } else if (position == 3) {
        result.append(kRomanHundreds[index]);
      } else if (position == 2) {
        result.append(kRomanTens[index]);
      } else {
        result.append(kRomanOnes[index]);
      }
    }
    --position;
  }
  return result;
}

int32_t RomanValue(char c) {
  if (c == 'I') {
    return 1;
  }
  if (c == 'V') {
    return 5;
  }
  if (c == 'X') {
    return 10;
  }
  if (c == 'L') {
    return 50;
  }
  if (c == 'C') {
    return 100;
  }
  if (c == 'D') {
    return 500;
  }
  if (c == 'M') {
    return 1000;
  }
  return 0;
}

bool IsValidRomanSubtraction(char current, char next) {
  return (current == 'I' && (next == 'V' || next == 'X')) ||
         (current == 'X' && (next == 'L' || next == 'C')) ||
         (current == 'C' && (next == 'D' || next == 'M'));
}

struct RomanCounts {
  int32_t v = 0;
  int32_t l = 0;
  int32_t d = 0;

  bool Rejects(int32_t value) const {
    return (v && value >= 5) || (l && value >= 50) || (d && value >= 500);
  }

  void Count(char c) {
    if (c == 'V') {
      ++v;
    } else if (c == 'L') {
      ++l;
    } else if (c == 'D') {
      ++d;
    }
  }
};

const char* LastRelevantDecimalDigit(const char* number) {
  const char* p = std::strchr(number, '.');
  if (!p) {
    return nullptr;
  }
  const char* result = p;
  while (*++p != '\0') {
    if (*p != '0') {
      result = p;
    }
  }
  return result;
}

class NumProcessor {
 public:
  NumProcessor(NumDesc& num, bool is_to_char, char* number, char* inout,
               int32_t input_length)
    : _num{num},
      _is_to_char{is_to_char},
      _number{number},
      _number_p{number},
      _inout{inout},
      _inout_p{inout},
      _input_length{input_length} {}

  char* Run(std::span<const NumNode> nodes, int32_t out_pre_spaces,
            int32_t sign) {
    if (_num.zero_start) {
      --_num.zero_start;
    }
    if (_num.Has(kNumEeee)) {
      if (!_is_to_char) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG("\"EEEE\" not supported for input"));
      }
      return std::strcpy(_inout, _number);
    }
    if (_is_to_char) {
      _sign = sign;
      if (_num.Has(kNumPlus) || _num.Has(kNumMinus)) {
        _sign_wrote = !(_num.Has(kNumPlus) && !_num.Has(kNumMinus));
      } else {
        if (_sign != '-' && _num.Has(kNumFillMode)) {
          _num.flag &= ~kNumBracket;
        }
        _sign_wrote =
          _sign == '+' && _num.Has(kNumFillMode) && !_num.Has(kNumLocaleSign);
        if (_num.lsign == LocaleSign::kPre && _num.pre == _num.pre_lsign_num) {
          _num.lsign = LocaleSign::kPost;
        }
      }
    }
    _num_count = _num.post + _num.pre - 1;
    if (_is_to_char) {
      _out_pre_spaces = out_pre_spaces;
      if (_num.Has(kNumFillMode) && _num.Has(kNumDecimal)) {
        _last_relevant = LastRelevantDecimalDigit(_number);
        if (_last_relevant && _num.zero_end > _out_pre_spaces) {
          const auto last_zero_position =
            std::min(static_cast<int32_t>(std::strlen(_number)) - 1,
                     _num.zero_end - _out_pre_spaces);
          const char* last_zero = _number + last_zero_position;
          if (_last_relevant < last_zero) {
            _last_relevant = last_zero;
          }
        }
      }
      if (!_sign_wrote && _out_pre_spaces == 0) {
        ++_num_count;
      }
    } else {
      _number[0] = ' ';
      _number[1] = '\0';
      _number_p = _number + 1;
    }
    for (const auto& node : nodes) {
      if (!_is_to_char && AtEnd()) {
        break;
      }
      if (node.type != NodeType::kAction) {
        if (_is_to_char) {
          const auto character = node.Character();
          std::memcpy(_inout_p, character.data(), character.size());
          _inout_p += character.size();
        } else {
          _inout_p += CharLength(std::string_view{
            _inout_p, static_cast<size_t>(_inout + _input_length - _inout_p)});
        }
        continue;
      }
      if (Apply(node.key->id)) {
        ++_inout_p;
      }
    }
    if (_is_to_char) {
      *_inout_p = '\0';
      return _inout;
    }
    if (*(_number_p - 1) == '.') {
      *(_number_p - 1) = '\0';
    } else {
      *_number_p = '\0';
    }
    _num.post = _read_post;
    return _number;
  }

 private:
  bool AtEnd() const { return _inout_p >= _inout + _input_length; }

  bool IsPredecimalSpace() const {
    return !_num.Has(kNumZero) && _number == _number_p && *_number == '0' &&
           _num.post != 0;
  }

  bool Apply(NumKey id) {
    switch (id) {
      case NumKey::kNine:
      case NumKey::kZero:
      case NumKey::kDecimalPoint:
      case NumKey::kLocaleDecimal:
        if (_is_to_char) {
          WriteNumberPart(id);
          return false;
        }
        ReadNumberPart(id);
        return true;
      case NumKey::kComma:
      case NumKey::kGroupSeparator:
        if (_is_to_char) {
          if (!_num_in) {
            if (_num.Has(kNumFillMode)) {
              return false;
            }
            *_inout_p = ' ';
          } else {
            *_inout_p = ',';
          }
          return true;
        }
        if (!_num_in && _num.Has(kNumFillMode)) {
          return false;
        }
        return *_inout_p == ',';
      case NumKey::kCurrency:
        if (_is_to_char) {
          *_inout_p = ' ';
          return true;
        }
        EatNonDataChars(1);
        return false;
      case NumKey::kRomanUpper:
      case NumKey::kRomanLower:
        if (_is_to_char) {
          WriteRoman(id == NumKey::kRomanLower);
          return false;
        }
        ReadRoman();
        return false;
      case NumKey::kOrdinalUpper:
      case NumKey::kOrdinalLower:
        if (_num.Has(kNumRoman) || *_number == '#' || _sign == '-' ||
            _num.Has(kNumDecimal)) {
          return false;
        }
        if (_is_to_char) {
          const auto suffix =
            OrdinalSuffix(_number, id == NumKey::kOrdinalUpper);
          std::memcpy(_inout_p, suffix.data(), suffix.size());
          _inout_p += suffix.size();
          return false;
        }
        EatNonDataChars(2);
        return false;
      case NumKey::kMinus:
        return ApplySign('-');
      case NumKey::kPlus:
        return ApplySign('+');
      case NumKey::kPlusMinus:
        if (_is_to_char) {
          *_inout_p = static_cast<char>(_sign);
          return true;
        }
        if (*_inout_p == '-' || *_inout_p == '+') {
          *_number = *_inout_p;
          return true;
        }
        EatNonDataChars(1);
        return false;
      case NumKey::kBlank:
      case NumKey::kIsoCurrency:
      case NumKey::kEeee:
      case NumKey::kFillMode:
      case NumKey::kAngleBrackets:
      case NumKey::kSpell:
      case NumKey::kLocaleSign:
      case NumKey::kMultiplier:
        return false;
    }
  }

  bool ApplySign(char symbol) {
    if (_is_to_char) {
      if (_sign == symbol) {
        *_inout_p = symbol;
      } else if (_num.Has(kNumFillMode)) {
        return false;
      } else {
        *_inout_p = ' ';
      }
      return true;
    }
    if (*_inout_p == symbol) {
      *_number = symbol;
      return true;
    }
    EatNonDataChars(1);
    return false;
  }

  void WriteRoman(bool lower) {
    const std::string_view roman{_number_p};
    if (!_num.Has(kNumFillMode) &&
        roman.size() < static_cast<size_t>(kMaxRomanLength)) {
      const auto padding = kMaxRomanLength - roman.size();
      std::memset(_inout_p, ' ', padding);
      _inout_p += padding;
    }
    for (const char c : roman) {
      *_inout_p++ = lower ? absl::ascii_tolower(c) : c;
    }
  }

  void ReadRoman() {
    const auto value = RomanToInt();
    if (value < 0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                      ERR_MSG("invalid Roman numeral"));
    }
    const auto digits = absl::StrCat(value);
    std::memcpy(_number_p, digits.data(), digits.size());
    _number_p += digits.size();
    _num.pre = static_cast<int32_t>(digits.size());
    _num.post = 0;
  }

  void AppendSign() { *_inout_p++ = _sign == '-' ? '-' : '+'; }

  void WriteNumberPart(NumKey id) {
    if (_num.Has(kNumRoman)) {
      return;
    }
    _num_in = false;
    if (!_sign_wrote &&
        (_num_curr >= _out_pre_spaces ||
         (_num.Has(kNumZero) && _num.zero_start == _num_curr)) &&
        (!IsPredecimalSpace() || (_last_relevant && *_last_relevant == '.'))) {
      if (_num.Has(kNumLocaleSign)) {
        if (_num.lsign == LocaleSign::kPre) {
          AppendSign();
          _sign_wrote = true;
        }
      } else if (_num.Has(kNumBracket)) {
        *_inout_p++ = _sign == '+' ? ' ' : '<';
        _sign_wrote = true;
      } else if (_sign == '+') {
        if (!_num.Has(kNumFillMode)) {
          *_inout_p++ = ' ';
        }
        _sign_wrote = true;
      } else if (_sign == '-') {
        *_inout_p++ = '-';
        _sign_wrote = true;
      }
    }
    if (_num_curr < _out_pre_spaces &&
        (_num.zero_start > _num_curr || !_num.Has(kNumZero))) {
      if (!_num.Has(kNumFillMode)) {
        *_inout_p++ = ' ';
      }
    } else if (_num.Has(kNumZero) && _num_curr < _out_pre_spaces &&
               _num.zero_start <= _num_curr) {
      *_inout_p++ = '0';
      _num_in = true;
    } else {
      if (*_number_p == '.') {
        if (!_last_relevant || *_last_relevant != '.' ||
            _num.Has(kNumFillMode)) {
          *_inout_p++ = '.';
        }
      } else if (!_last_relevant || _number_p <= _last_relevant ||
                 id == NumKey::kZero) {
        if (IsPredecimalSpace()) {
          if (!_num.Has(kNumFillMode)) {
            *_inout_p++ = ' ';
          } else if (_last_relevant && *_last_relevant == '.') {
            *_inout_p++ = '0';
          }
        } else {
          *_inout_p++ = *_number_p;
          _num_in = true;
        }
      }
      if (*_number_p != '\0') {
        ++_number_p;
      }
    }
    int32_t end =
      _num_count + (_out_pre_spaces ? 1 : 0) + (_num.Has(kNumDecimal) ? 1 : 0);
    if (_last_relevant && _last_relevant == _number_p) {
      end = _num_curr;
    }
    if (_num_curr + 1 == end) {
      if (_sign_wrote && _num.Has(kNumBracket)) {
        *_inout_p++ = _sign == '+' ? ' ' : '>';
      } else if (_num.Has(kNumLocaleSign) && _num.lsign == LocaleSign::kPost) {
        AppendSign();
      }
    }
    ++_num_curr;
  }

  void ReadNumberPart(NumKey id) {
    bool is_read = false;
    if (AtEnd()) {
      return;
    }
    if (*_inout_p == ' ') {
      ++_inout_p;
    }
    if (AtEnd()) {
      return;
    }
    if (*_number == ' ' && (id == NumKey::kZero || id == NumKey::kNine) &&
        _read_pre + _read_post == 0) {
      if (_num.Has(kNumLocaleSign) && _num.lsign == LocaleSign::kPre) {
        if (*_inout_p == '-' || *_inout_p == '+') {
          *_number = *_inout_p++;
        }
      } else if (*_inout_p == '-' ||
                 (_num.Has(kNumBracket) && *_inout_p == '<')) {
        *_number = '-';
        ++_inout_p;
      } else if (*_inout_p == '+') {
        *_number = '+';
        ++_inout_p;
      }
    }
    if (AtEnd()) {
      return;
    }
    if (absl::ascii_isdigit(*_inout_p)) {
      if (_read_dec && _read_post == _num.post) {
        return;
      }
      *_number_p++ = *_inout_p;
      if (_read_dec) {
        ++_read_post;
      } else {
        ++_read_pre;
      }
      is_read = true;
    } else if (_num.Has(kNumDecimal) && !_read_dec && *_inout_p == '.') {
      *_number_p++ = '.';
      _read_dec = true;
      is_read = true;
    }
    if (AtEnd()) {
      return;
    }
    if (*_number != ' ' || _read_pre + _read_post == 0) {
      return;
    }
    if (_num.Has(kNumLocaleSign) && is_read &&
        _inout_p + 1 < _inout + _input_length &&
        !absl::ascii_isdigit(*(_inout_p + 1))) {
      char* position = _inout_p++;
      if (*_inout_p == '-' || *_inout_p == '+') {
        *_number = *_inout_p;
      } else {
        _inout_p = position;
      }
    } else if (!is_read && !_num.Has(kNumLocaleSign) &&
               (_num.Has(kNumPlus) || _num.Has(kNumMinus)) &&
               (*_inout_p == '-' || *_inout_p == '+')) {
      *_number = *_inout_p;
    }
  }

  void EatNonDataChars(int32_t count) {
    const std::string_view data_chars{"0123456789.,+-"};
    for (; count > 0 && !AtEnd(); --count) {
      if (data_chars.find(*_inout_p) != std::string_view::npos) {
        break;
      }
      _inout_p += CharLength(std::string_view{
        _inout_p, static_cast<size_t>(_inout + _input_length - _inout_p)});
    }
  }

  int32_t RomanToInt() {
    std::array<char, kMaxRomanLength> chars;
    std::array<int32_t, kMaxRomanLength> values;
    while (!AtEnd() && absl::ascii_isspace(*_inout_p)) {
      ++_inout_p;
    }
    int32_t length = 0;
    for (; length < kMaxRomanLength && !AtEnd(); ++length) {
      const char c = absl::ascii_toupper(*_inout_p);
      const auto value = RomanValue(c);
      if (value == 0) {
        break;
      }
      chars[length] = c;
      values[length] = value;
      ++_inout_p;
    }
    if (length == 0) {
      return -1;
    }
    int32_t result = 0;
    int32_t repeat_count = 1;
    RomanCounts counts;
    bool subtraction = false;
    int32_t last_subtracted = 0;
    for (int32_t i = 0; i < length; ++i) {
      const char current = chars[i];
      const int32_t current_value = values[i];
      if ((subtraction && current_value >= last_subtracted) ||
          counts.Rejects(current_value)) {
        return -1;
      }
      counts.Count(current);
      if (i == length - 1) {
        result += current_value;
        continue;
      }
      const char next = chars[i + 1];
      const int32_t next_value = values[i + 1];
      if (current_value < next_value) {
        if (!IsValidRomanSubtraction(current, next) || repeat_count > 1 ||
            counts.Rejects(next_value)) {
          return -1;
        }
        counts.Count(next);
        ++i;
        repeat_count = 1;
        subtraction = true;
        last_subtracted = current_value;
        result += next_value - current_value;
      } else {
        if (current == next) {
          if (++repeat_count > 3) {
            return -1;
          }
        } else {
          repeat_count = 1;
        }
        result += current_value;
      }
    }
    return result;
  }

  NumDesc& _num;
  bool _is_to_char;
  int32_t _sign = 0;
  bool _sign_wrote = false;
  int32_t _num_count = 0;
  bool _num_in = false;
  int32_t _num_curr = 0;
  int32_t _out_pre_spaces = 0;
  bool _read_dec = false;
  int32_t _read_post = 0;
  int32_t _read_pre = 0;
  char* _number;
  char* _number_p;
  char* _inout;
  char* _inout_p;
  const char* _last_relevant = nullptr;
  int32_t _input_length;
};

struct NumberText {
  std::string number;
  int32_t out_pre_spaces = 0;
  int32_t sign = 0;
};

void FitDigits(NumberText& text, const NumDesc& num, size_t pre_length) {
  const auto length = static_cast<int32_t>(pre_length);
  if (length < num.pre) {
    text.out_pre_spaces = num.pre - length;
  } else if (length > num.pre) {
    text.number.assign(num.pre + num.post + 1, '#');
    text.number[num.pre] = '.';
  }
}

void SetSignedDigits(NumberText& text, std::string digits) {
  text.sign = '+';
  if (digits.starts_with('-')) {
    text.sign = '-';
    digits.erase(0, 1);
  }
  text.number = std::move(digits);
}

enum class FloatStyle : uint8_t {
  kFixed,
  kExponent,
};

std::string FormatFloat(double value, int32_t precision, FloatStyle style) {
  if (std::isnan(value)) {
    return "NaN";
  }
  std::string out;
  if (std::signbit(value)) {
    out.push_back('-');
    value = -value;
  } else if (style == FloatStyle::kExponent) {
    out.push_back(' ');
  }
  if (std::isinf(value)) {
    out.append("Infinity");
    return out;
  }
  constexpr int32_t kMaxPrecision = 350;
  const auto digits = std::min(std::max(precision, 0), kMaxPrecision);
  const auto start = out.size();
  switch (style) {
    case FloatStyle::kFixed: {
      const auto size = std::snprintf(nullptr, 0, "%.*f", digits, value);
      out.resize(start + size);
      std::snprintf(out.data() + start, size + 1, "%.*f", digits, value);
      if (precision > digits) {
        out.append(precision - digits, '0');
      }
      break;
    }
    case FloatStyle::kExponent: {
      const auto size = std::snprintf(nullptr, 0, "%.*e", digits, value);
      out.resize(start + size);
      std::snprintf(out.data() + start, size + 1, "%.*e", digits, value);
      if (precision > digits) {
        out.insert(out.rfind('e'), precision - digits, '0');
      }
      break;
    }
  }
  return out;
}

std::string SignedScientific(const DecimalNumber& number, int32_t scale) {
  auto text = ScientificDecimal(number, scale);
  if (!text.starts_with('-')) {
    text.insert(text.begin(), ' ');
  }
  return text;
}

void AppendZeroFraction(NumberText& text, const NumDesc& num) {
  const auto pre_length = text.number.size();
  if (num.post) {
    text.number.push_back('.');
    text.number.append(num.post, '0');
  }
  FitDigits(text, num, pre_length);
}

NumberText PrepareDecimal(DecimalNumber number, NumDesc& num) {
  NumberText text;
  if (num.Has(kNumRoman)) {
    text.number = IntToRoman(RoundToInt32(std::move(number)));
    return text;
  }
  if (num.Has(kNumEeee)) {
    text.number = SignedScientific(number, num.post);
    return text;
  }
  if (num.Has(kNumMulti)) {
    ShiftDecimal(number, num.multi);
    num.pre += num.multi;
  }
  RoundDecimal(number, num.post);
  text.sign = number.negative ? '-' : '+';
  AppendUnsignedDecimal(text.number, number);
  FitDigits(text, num, std::min(text.number.find('.'), text.number.size()));
  return text;
}

NumberText PrepareInt32(int32_t value, NumDesc& num) {
  NumberText text;
  if (num.Has(kNumRoman)) {
    text.number = IntToRoman(value);
    return text;
  }
  if (num.Has(kNumEeee)) {
    text.number =
      FormatFloat(static_cast<double>(value), num.post, FloatStyle::kExponent);
    return text;
  }
  if (num.Has(kNumMulti)) {
    constexpr std::array<int32_t, 10> kPowers = {
      1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
    };
    const auto factor = num.multi < static_cast<int32_t>(kPowers.size())
                          ? kPowers[num.multi]
                          : std::numeric_limits<int32_t>::min();
    value = static_cast<int32_t>(static_cast<uint32_t>(value) *
                                 static_cast<uint32_t>(factor));
    num.pre += num.multi;
  }
  SetSignedDigits(text, absl::StrCat(value));
  AppendZeroFraction(text, num);
  return text;
}

[[noreturn]] void ThrowBigintOutOfRange() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                  ERR_MSG("bigint out of range"));
}

NumberText PrepareInt64(int64_t value, NumDesc& num) {
  NumberText text;
  if (num.Has(kNumRoman)) {
    const bool fits = value >= std::numeric_limits<int32_t>::min() &&
                      value <= std::numeric_limits<int32_t>::max();
    text.number = IntToRoman(fits ? static_cast<int32_t>(value)
                                  : std::numeric_limits<int32_t>::max());
    return text;
  }
  if (num.Has(kNumEeee)) {
    text.number =
      SignedScientific(DecimalFromText(absl::StrCat(value)), num.post);
    return text;
  }
  if (num.Has(kNumMulti)) {
    const double multiplier = std::rint(std::pow(10.0, num.multi));
    const auto limit = static_cast<double>(std::numeric_limits<int64_t>::min());
    if (!(multiplier >= limit && multiplier < -limit)) {
      ThrowBigintOutOfRange();
    }
    if (__builtin_mul_overflow(value, static_cast<int64_t>(multiplier),
                               &value)) {
      ThrowBigintOutOfRange();
    }
    num.pre += num.multi;
  }
  SetSignedDigits(text, absl::StrCat(value));
  AppendZeroFraction(text, num);
  return text;
}

template<typename T>
NumberText PrepareFloat(T value, NumDesc& num) {
  constexpr int32_t max_digits = std::numeric_limits<T>::digits10;
  NumberText text;
  if (num.Has(kNumRoman)) {
    const T rounded = static_cast<T>(std::rint(value));
    const auto lower = static_cast<T>(std::numeric_limits<int32_t>::min());
    const bool fits =
      !std::isnan(rounded) && rounded >= lower && rounded < -lower;
    text.number = IntToRoman(fits ? static_cast<int32_t>(rounded)
                                  : std::numeric_limits<int32_t>::max());
    return text;
  }
  if (num.Has(kNumEeee)) {
    if (std::isnan(value) || std::isinf(value)) {
      text.number.assign(num.pre + num.post + 6, '#');
      text.number[0] = ' ';
      text.number[num.pre + 1] = '.';
    } else {
      text.number = FormatFloat(value, num.post, FloatStyle::kExponent);
    }
    return text;
  }
  T scaled = value;
  if (num.Has(kNumMulti)) {
    scaled = value * static_cast<T>(std::pow(10.0, num.multi));
    num.pre += num.multi;
  }
  const auto pre_digits = static_cast<int32_t>(
    FormatFloat(std::fabs(scaled), 0, FloatStyle::kFixed).size());
  if (pre_digits >= max_digits) {
    num.post = 0;
  } else if (pre_digits + num.post > max_digits) {
    num.post = max_digits - pre_digits;
  }
  SetSignedDigits(text, FormatFloat(scaled, num.post, FloatStyle::kFixed));
  FitDigits(text, num, std::min(text.number.find('.'), text.number.size()));
  return text;
}

template<typename Keyword>
class FormatCache {
 public:
  const ParsedFormat<Keyword>& Get(std::string_view text) {
    if (!_parsed || text != _text) {
      _parsed = ParseFormat<Keyword>(text);
      _text.assign(text);
    }
    return *_parsed;
  }

 private:
  std::string _text;
  std::optional<ParsedFormat<Keyword>> _parsed;
};

enum class ZoneAbbreviation : uint8_t {
  kUtc,
  kGmt,
  kOffset,
};

ZoneAbbreviation ClassifyZone(std::string_view id) {
  if (absl::c_linear_search(kUtcZoneIds, id)) {
    return ZoneAbbreviation::kUtc;
  }
  if (absl::c_linear_search(kGmtZoneIds, id)) {
    return ZoneAbbreviation::kGmt;
  }
  return ZoneAbbreviation::kOffset;
}

class SessionZone {
 public:
  explicit SessionZone(const duckdb::ICUDateFunc::BindData& info)
    : _calendar{info.calendar->Copy()},
      _lut{info.lut},
      _abbreviation{ClassifyZone(info.calendar->GetTimeZone().GetId())} {}

  int32_t OffsetSeconds(int64_t instant) const {
    int64_t offset = 0;
    if (_lut && _lut->TryOffset(instant, offset)) {
      return static_cast<int32_t>(offset / kUsecsPerSec);
    }
    int32_t raw = 0;
    int32_t dst = 0;
    _calendar->GetTimeZone().GetOffset(FloorDivide(instant, 1000), raw, dst);
    return (raw + dst) / 1000;
  }

  int64_t Resolve(int64_t wall) {
    int64_t instant = 0;
    if (_lut && _lut->TryResolve(wall, instant)) {
      return instant;
    }
    return duckdb::ICUDateFunc::FromNaive(_calendar.get(),
                                          duckdb::timestamp_t{wall})
      .value;
  }

  void AppendAbbreviation(std::string& out, int32_t offset) const {
    switch (_abbreviation) {
      case ZoneAbbreviation::kUtc:
        out.append("UTC");
        return;
      case ZoneAbbreviation::kGmt:
        out.append("GMT");
        return;
      case ZoneAbbreviation::kOffset:
        break;
    }
    const auto magnitude = std::abs(offset);
    out.push_back(offset >= 0 ? '+' : '-');
    AppendNumber(out, magnitude / kSecsPerHour, 2);
    if (magnitude % kSecsPerMinute != 0) {
      AppendNumber(out, magnitude % kSecsPerHour / kSecsPerMinute, 2);
      AppendNumber(out, magnitude % kSecsPerMinute, 2);
    } else if (magnitude % kSecsPerHour != 0) {
      AppendNumber(out, magnitude % kSecsPerHour / kSecsPerMinute, 2);
    }
  }

 private:
  duckdb::unique_ptr<duckdb::Calendar> _calendar;
  duckdb::shared_ptr<const duckdb::ZoneLUT> _lut;
  ZoneAbbreviation _abbreviation;
};

struct FormattingState final : duckdb::FunctionLocalState {
  FormatCache<DchKeyword> datetime_formats;
  FormatCache<NumKeyword> number_formats;
  std::string output;
  std::string input;
  std::string number;
  std::string zone_abbreviation;
  std::optional<SessionZone> zone;
};

FormattingState& GetFormattingState(duckdb::ExpressionState& state) {
  return duckdb::ExecuteFunctionState::GetFunctionState(state)
    ->Cast<FormattingState>();
}

duckdb::unique_ptr<duckdb::FunctionLocalState> InitFormattingState(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<FormattingState>();
}

duckdb::unique_ptr<duckdb::FunctionLocalState> InitZonedFormattingState(
  duckdb::ExpressionState&, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData* bind_data) {
  auto state = duckdb::make_uniq<FormattingState>();
  state->zone.emplace(bind_data->Cast<duckdb::ICUDateFunc::BindData>());
  return state;
}

TmToChar TimestampToTm(int64_t micros) {
  duckdb::date_t date;
  duckdb::dtime_t time;
  duckdb::Timestamp::Convert(duckdb::timestamp_t{micros}, date, time);
  TmToChar tm;
  duckdb::Date::Convert(date, tm.tm_year, tm.tm_mon, tm.tm_mday);
  int32_t hour = 0;
  int32_t minute = 0;
  duckdb::Time::Convert(time, hour, minute, tm.tm_sec, tm.fsec);
  tm.tm_hour = hour;
  tm.tm_min = minute;
  tm.tm_wday = duckdb::Date::ExtractISODayOfTheWeek(date) % 7;
  tm.tm_yday = duckdb::Date::ExtractDayOfTheYear(date);
  return tm;
}

TmToChar IntervalToTm(duckdb::interval_t span) {
  TmToChar tm;
  tm.tm_year = span.months / kMonthsPerYear;
  tm.tm_mon = span.months % kMonthsPerYear;
  tm.tm_mday = span.days;
  int64_t time = span.micros;
  tm.tm_hour = time / kUsecsPerHour;
  time -= tm.tm_hour * kUsecsPerHour;
  tm.tm_min = static_cast<int32_t>(time / kUsecsPerMinute);
  time -= tm.tm_min * kUsecsPerMinute;
  tm.tm_sec = static_cast<int32_t>(time / kUsecsPerSec);
  tm.fsec = static_cast<int32_t>(time - tm.tm_sec * kUsecsPerSec);
  tm.tm_yday = static_cast<int32_t>(
    (int64_t{tm.tm_year} * kMonthsPerYear + tm.tm_mon) * kDaysPerMonth +
    tm.tm_mday);
  return tm;
}

duckdb::string_t FormatDateTime(FormattingState& local, duckdb::Vector& result,
                                duckdb::string_t format, bool is_interval,
                                const TmToChar& tm) {
  const auto& parsed = local.datetime_formats.Get(AsView(format));
  local.output.clear();
  DchToChar(parsed.nodes, is_interval, tm, local.output);
  return duckdb::StringVector::AddString(result, local.output);
}

void TimestampToCharFunction(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  auto& local = GetFormattingState(state);
  duckdb::BinaryExecutor::Execute<duckdb::timestamp_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::timestamp_t value,
        duckdb::string_t format) -> duckdb::optional<duckdb::string_t> {
      if (format.GetSize() == 0 || !value.IsFinite()) {
        return duckdb::nullopt;
      }
      return FormatDateTime(local, result, format, false,
                            TimestampToTm(value.value));
    });
}

[[noreturn]] void ThrowTimestampOutOfRange() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                  ERR_MSG("timestamp out of range"));
}

void TimestampTzToCharFunction(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  auto& local = GetFormattingState(state);
  duckdb::BinaryExecutor::Execute<duckdb::timestamp_tz_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::timestamp_tz_t value,
        duckdb::string_t format) -> duckdb::optional<duckdb::string_t> {
      if (format.GetSize() == 0 || !value.IsFinite()) {
        return duckdb::nullopt;
      }
      const auto offset = local.zone->OffsetSeconds(value.value);
      int64_t wall = 0;
      if (!duckdb::TryAddOperator::Operation(
            value.value, int64_t{offset} * kUsecsPerSec, wall) ||
          !duckdb::timestamp_t{wall}.IsFinite()) {
        ThrowTimestampOutOfRange();
      }
      auto tm = TimestampToTm(wall);
      tm.tm_gmtoff = offset;
      local.zone_abbreviation.clear();
      local.zone->AppendAbbreviation(local.zone_abbreviation, offset);
      tm.tzn = local.zone_abbreviation;
      return FormatDateTime(local, result, format, false, tm);
    });
}

void IntervalToCharFunction(duckdb::DataChunk& args,
                            duckdb::ExpressionState& state,
                            duckdb::Vector& result) {
  auto& local = GetFormattingState(state);
  duckdb::BinaryExecutor::Execute<duckdb::interval_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::interval_t value,
        duckdb::string_t format) -> duckdb::optional<duckdb::string_t> {
      if (format.GetSize() == 0) {
        return duckdb::nullopt;
      }
      return FormatDateTime(local, result, format, true, IntervalToTm(value));
    });
}

void TimeToCharFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  auto& local = GetFormattingState(state);
  duckdb::BinaryExecutor::Execute<duckdb::dtime_t, duckdb::string_t,
                                  duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::dtime_t value,
        duckdb::string_t format) -> duckdb::optional<duckdb::string_t> {
      if (format.GetSize() == 0) {
        return duckdb::nullopt;
      }
      return FormatDateTime(
        local, result, format, true,
        IntervalToTm({.months = 0, .days = 0, .micros = value.value}));
    });
}

template<typename T, typename Prepare>
void NumberToChar(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                  duckdb::Vector& result, Prepare prepare) {
  auto& local = GetFormattingState(state);
  duckdb::BinaryExecutor::Execute<T, duckdb::string_t, duckdb::string_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](T value, duckdb::string_t format) {
      if (format.GetSize() == 0) {
        return duckdb::string_t{""};
      }
      const auto& parsed = local.number_formats.Get(AsView(format));
      auto num = parsed.num;
      auto text = prepare(value, num);
      local.output.assign(
        format.GetSize() * kNumMaxItemSize + text.number.size() + 2, '\0');
      NumProcessor processor{num, true, text.number.data(), local.output.data(),
                             0};
      return duckdb::StringVector::AddString(
        result, processor.Run(parsed.nodes, text.out_pre_spaces, text.sign));
    });
}

template<typename T, auto Prepare>
void NumberToCharFunction(duckdb::DataChunk& args,
                          duckdb::ExpressionState& state,
                          duckdb::Vector& result) {
  NumberToChar<T>(args, state, result, Prepare);
}

template<typename T>
void DecimalToCharFunction(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  const auto& type = args.data[0].GetType();
  const auto width = duckdb::DecimalType::GetWidth(type);
  const auto scale = duckdb::DecimalType::GetScale(type);
  NumberToChar<T>(args, state, result, [&](T value, NumDesc& num) {
    return PrepareDecimal(
      DecimalFromText(duckdb::Decimal::ToString(value, width, scale)), num);
  });
}

duckdb::unique_ptr<duckdb::FunctionData> BindDecimalToChar(
  duckdb::BindScalarFunctionInput& input) {
  uint8_t width = 0;
  uint8_t scale = 0;
  input.GetArguments()[0]->GetReturnType().GetDecimalProperties(width, scale);
  const auto type = duckdb::LogicalType::DECIMAL(width, scale);
  auto& function = input.GetBoundFunction();
  function.GetArguments()[0] = type;
  const auto physical = type.InternalType();
  if (physical == duckdb::PhysicalType::INT16) {
    function.SetFunctionCallback(DecimalToCharFunction<int16_t>);
  } else if (physical == duckdb::PhysicalType::INT32) {
    function.SetFunctionCallback(DecimalToCharFunction<int32_t>);
  } else if (physical == duckdb::PhysicalType::INT64) {
    function.SetFunctionCallback(DecimalToCharFunction<int64_t>);
  } else {
    function.SetFunctionCallback(DecimalToCharFunction<duckdb::hugeint_t>);
  }
  return nullptr;
}

DecimalNumber ReadNumber(FormattingState& local,
                         const ParsedFormat<NumKeyword>& parsed, NumDesc& num,
                         std::string_view value, size_t format_length) {
  local.input.assign(value);
  local.number.assign(format_length * kNumMaxItemSize + 2, '\0');
  NumProcessor processor{num, false, local.number.data(), local.input.data(),
                         static_cast<int32_t>(value.size())};
  const std::string_view digits{processor.Run(parsed.nodes, 0, 0)};
  const int32_t scale = num.post;
  const int32_t precision = num.pre + num.multi + scale;
  auto number = NumericInput(digits);
  RoundDecimal(number, scale);
  if (static_cast<int32_t>(number.digits.size()) - scale > precision - scale) {
    ThrowNumericOverflow(precision, scale);
  }
  if (num.Has(kNumMulti)) {
    number.scale += num.multi;
  }
  return number;
}

void ToNumberFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                      duckdb::Vector& result) {
  auto& local = GetFormattingState(state);
  const auto width = duckdb::DecimalType::GetWidth(result.GetType());
  const auto scale = duckdb::DecimalType::GetScale(result.GetType());
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::hugeint_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t value,
        duckdb::string_t format) -> duckdb::optional<duckdb::hugeint_t> {
      if (format.GetSize() == 0) {
        return duckdb::nullopt;
      }
      const auto& parsed = local.number_formats.Get(AsView(format));
      auto num = parsed.num;
      return DecimalToStorage(
        ReadNumber(local, parsed, num, AsView(value), format.GetSize()), width,
        scale);
    });
}

duckdb::optional<duckdb::Value> BindTimeConstant(
  duckdb::BindScalarFunctionInput& input, duckdb::idx_t index) {
  const auto& argument = *input.GetArguments()[index];
  if (!argument.IsFoldable() && !argument.HasParameter()) {
    return duckdb::nullopt;
  }
  return input.GetConstant(index);
}

uint8_t ToNumberScale(duckdb::BindScalarFunctionInput& input) {
  const auto format = BindTimeConstant(input, 1);
  if (!format || format->IsNull()) {
    return kNumericDefaultScale;
  }
  const auto format_text = format->ToString();
  const auto parsed = ParseFormat<NumKeyword>(format_text);
  auto num = parsed.num;
  const auto value = BindTimeConstant(input, 0);
  if (value && !value->IsNull() && !num.Has(kNumEeee) && !num.Has(kNumRoman)) {
    auto input_text = value->ToString();
    std::string number(format_text.size() * kNumMaxItemSize + 2, '\0');
    NumProcessor processor{num, false, number.data(), input_text.data(),
                           static_cast<int32_t>(input_text.size())};
    processor.Run(parsed.nodes, 0, 0);
  }
  const auto scale = num.Has(kNumMulti)
                       ? num.post + kNumericMinSignificantDigits + num.multi
                       : num.post;
  return static_cast<uint8_t>(std::clamp<int32_t>(scale, 0, kNumericWidth));
}

duckdb::unique_ptr<duckdb::FunctionData> BindToNumber(
  duckdb::BindScalarFunctionInput& input) {
  input.GetBoundFunction().SetReturnType(
    duckdb::LogicalType::DECIMAL(kNumericWidth, ToNumberScale(input)));
  return nullptr;
}

ParsedDateTime ParseDateTime(FormattingState& local, duckdb::string_t text,
                             duckdb::string_t format) {
  local.input.assign(text.GetData(), text.GetSize());
  const auto& parsed = local.datetime_formats.Get(AsView(format));
  return DoToTimestamp(parsed.nodes, local.input.c_str());
}

void ToDateFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                    duckdb::Vector& result) {
  auto& local = GetFormattingState(state);
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::date_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t text, duckdb::string_t format) {
      const auto parsed = ParseDateTime(local, text, format);
      const auto& tm = parsed.tm;
      const auto julian_day =
        IsValidJulian(tm.tm_year, tm.tm_mon)
          ? DateToJulian(tm.tm_year, tm.tm_mon, tm.tm_mday)
          : -1;
      if (julian_day < 0 || julian_day >= kDateEndJulianDay) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
          ERR_MSG("date out of range: \"", local.input.c_str(), "\""));
      }
      return duckdb::date_t{julian_day - kUnixEpochJulianDay};
    });
}

int64_t AdjustTimestampPrecision(int64_t instant, int32_t precision) {
  if (precision == kMaxTimestampPrecision) {
    return instant;
  }
  const auto scale = kTimestampScales[precision];
  const auto offset = kTimestampOffsets[precision];
  const auto relative = instant - kPostgresEpochUsecs;
  const auto rounded = relative >= 0 ? (relative + offset) / scale * scale
                                     : -((-relative + offset) / scale * scale);
  return rounded + kPostgresEpochUsecs;
}

void ToTimestampFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  auto& local = GetFormattingState(state);
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                  duckdb::timestamp_tz_t>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t text, duckdb::string_t format) {
      const auto parsed = ParseDateTime(local, text, format);
      const auto& tm = parsed.tm;
      if (!IsValidJulian(tm.tm_year, tm.tm_mon)) {
        ThrowTimestampOutOfRange();
      }
      const int64_t days =
        DateToJulian(tm.tm_year, tm.tm_mon, tm.tm_mday) - kUnixEpochJulianDay;
      const int64_t time =
        ((int64_t{tm.tm_hour} * kMinsPerHour + tm.tm_min) * kSecsPerMinute +
         tm.tm_sec) *
          kUsecsPerSec +
        parsed.fsec;
      int64_t wall = 0;
      if (!duckdb::TryMultiplyOperator::Operation(days, kUsecsPerDay, wall) ||
          !duckdb::TryAddOperator::Operation(wall, time, wall)) {
        ThrowTimestampOutOfRange();
      }
      int64_t instant = 0;
      if (parsed.has_tz) {
        if (!duckdb::TryAddOperator::Operation(
              wall, int64_t{parsed.gmtoffset} * kUsecsPerSec, instant)) {
          ThrowTimestampOutOfRange();
        }
      } else {
        instant = local.zone->Resolve(wall);
      }
      if (instant < kMinTimestampUsecs ||
          !duckdb::timestamp_t{instant}.IsFinite()) {
        ThrowTimestampOutOfRange();
      }
      if (parsed.fprec) {
        instant = AdjustTimestampPrecision(instant, parsed.fprec);
      }
      return duckdb::timestamp_tz_t{instant};
    });
}

}  // namespace

void RegisterPgFormattingFunctions(duckdb::DatabaseInstance& db) {
  using duckdb::LogicalType;
  struct Overload {
    LogicalType input;
    LogicalType result;
    duckdb::scalar_function_t function;
    duckdb::bind_scalar_function_t bind;
    duckdb::init_local_state_t init;
  };
  duckdb::ExtensionLoader loader{db, "serenedb"};
  const auto add = [&](std::string_view name,
                       std::initializer_list<Overload> overloads) {
    const duckdb::Identifier identifier{name};
    duckdb::ScalarFunctionSet set{identifier};
    for (const auto& overload : overloads) {
      duckdb::ScalarFunction function{
        identifier,      {overload.input, LogicalType::VARCHAR},
        overload.result, overload.function,
        overload.bind,   nullptr,
        overload.init};
      function.SetStability(duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY);
      set.AddFunction(std::move(function));
    }
    set.SetFallible();
    loader.RegisterFunction(std::move(set));
  };
  add("to_char",
      {
        {LogicalType::TIMESTAMP, LogicalType::VARCHAR, TimestampToCharFunction,
         nullptr, InitFormattingState},
        {LogicalType::TIMESTAMP_TZ, LogicalType::VARCHAR,
         TimestampTzToCharFunction, duckdb::ICUDateFunc::Bind,
         InitZonedFormattingState},
        {LogicalType::INTERVAL, LogicalType::VARCHAR, IntervalToCharFunction,
         nullptr, InitFormattingState},
        {LogicalType::TIME, LogicalType::VARCHAR, TimeToCharFunction, nullptr,
         InitFormattingState},
        {LogicalType::INTEGER, LogicalType::VARCHAR,
         NumberToCharFunction<int32_t, PrepareInt32>, nullptr,
         InitFormattingState},
        {LogicalType::BIGINT, LogicalType::VARCHAR,
         NumberToCharFunction<int64_t, PrepareInt64>, nullptr,
         InitFormattingState},
        {LogicalType::FLOAT, LogicalType::VARCHAR,
         NumberToCharFunction<float, PrepareFloat<float>>, nullptr,
         InitFormattingState},
        {LogicalType::DOUBLE, LogicalType::VARCHAR,
         NumberToCharFunction<double, PrepareFloat<double>>, nullptr,
         InitFormattingState},
        {LogicalType{duckdb::LogicalTypeId::DECIMAL}, LogicalType::VARCHAR,
         nullptr, BindDecimalToChar, InitFormattingState},
      });
  add("to_number", {{LogicalType::VARCHAR,
                     LogicalType::DECIMAL(kNumericWidth, kNumericDefaultScale),
                     ToNumberFunction, BindToNumber, InitFormattingState}});
  add("to_date", {{LogicalType::VARCHAR, LogicalType::DATE, ToDateFunction,
                   nullptr, InitFormattingState}});
  add("to_timestamp",
      {{LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ, ToTimestampFunction,
        duckdb::ICUDateFunc::Bind, InitZonedFormattingState}});
}

}  // namespace sdb::connector
