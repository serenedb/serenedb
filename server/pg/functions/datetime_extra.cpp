////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "pg/functions/datetime_extra.h"

#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/lib/DateTimeFormatter.h>
#include <velox/functions/lib/TimeUtils.h>
#include <velox/functions/prestosql/DateTimeImpl.h>
#include <velox/type/SimpleFunctionApi.h>

#include <chrono>

#include "basics/fwd.h"
#include "pg/functions/interval.h"
#include "query/types.h"

namespace sdb::pg::functions {
namespace {

using namespace velox;
using namespace velox::functions;
using pg::Interval;

// make_date(year, month, day) -> date
template<typename T>
struct PgMakeDate {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Date>& result, int32_t year,
                                int32_t month, int32_t day) {
    auto expected = util::daysSinceEpochFromDate(year, month, day);
    VELOX_USER_CHECK(expected.hasValue(), "date field value out of range");
    result = expected.value();
  }
};

// make_timestamp(year, month, day, hour, min, sec) -> timestamp
template<typename T>
struct PgMakeTimestamp {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Timestamp>& result, int32_t year,
                                int32_t month, int32_t day, int32_t hour,
                                int32_t min, double sec) {
    auto expected = util::daysSinceEpochFromDate(year, month, day);
    VELOX_USER_CHECK(expected.hasValue(), "timestamp field value out of range");
    int64_t days = expected.value();
    int32_t whole_sec = static_cast<int32_t>(sec);
    double frac = sec - whole_sec;
    int64_t total_seconds =
      days * 86400LL + hour * 3600LL + min * 60LL + whole_sec;
    int64_t nanos = static_cast<int64_t>(frac * 1'000'000'000);
    result = Timestamp(total_seconds, static_cast<uint64_t>(nanos));
  }
};

// to_timestamp(double) -> timestamptz
// Converts Unix epoch (seconds since 1970-01-01 00:00:00 UTC) to timestamp.
template<typename T>
struct PgToTimestampEpoch {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Timestamp>& result,
                                double epoch_seconds) {
    int64_t secs = static_cast<int64_t>(epoch_seconds);
    double frac = epoch_seconds - secs;
    int64_t nanos = static_cast<int64_t>(frac * 1'000'000'000);
    result = Timestamp(secs, nanos);
  }
};

// clock_timestamp() -> timestamptz
// Returns real-time clock (not statement time).
template<typename T>
struct PgClockTimestamp {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Timestamp>& result) {
    auto now = std::chrono::system_clock::now();
    auto epoch = now.time_since_epoch();
    auto secs = std::chrono::duration_cast<std::chrono::seconds>(epoch).count();
    auto nanos =
      std::chrono::duration_cast<std::chrono::nanoseconds>(epoch).count() -
      secs * 1'000'000'000LL;
    result = Timestamp(secs, nanos);
  }
};

// timeofday() -> text
// Returns wall clock time as formatted string.
template<typename T>
struct PgTimeofday {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Varchar>& result) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&time_t, &tm);
    char buf[128];
    auto len = strftime(buf, sizeof(buf), "%a %b %d %H:%M:%S %Y UTC", &tm);
    result.resize(len);
    std::memcpy(result.data(), buf, len);
  }
};

// isfinite(date) -> boolean
// isfinite(timestamp) -> boolean
// Dates and timestamps are always finite in our system (no infinity support),
// so these always return true.
template<typename T>
struct PgIsFiniteDate {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(bool& result, const arg_type<Date>&) {
    result = true;
  }
};

template<typename T>
struct PgIsFiniteTimestamp {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(bool& result, const arg_type<Timestamp>&) {
    result = true;
  }
};

// age(timestamp, timestamp) -> interval
// Subtracts two timestamps and returns the difference as an interval
// in years, months, days format (like PG).
template<typename T>
struct PgAge {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Interval>& result,
                                const arg_type<Timestamp>& ts1,
                                const arg_type<Timestamp>& ts2) {
    // Convert timestamps to tm structs.
    auto tm1 = functions::getDateTime(ts1, nullptr);
    auto tm2 = functions::getDateTime(ts2, nullptr);

    int32_t years = (1900 + tm1.tm_year) - (1900 + tm2.tm_year);
    int32_t months = tm1.tm_mon - tm2.tm_mon;
    int32_t days = tm1.tm_mday - tm2.tm_mday;

    // Normalize: borrow from months if days < 0.
    if (days < 0) {
      months -= 1;
      // Add days in the month of the earlier timestamp (ts2).
      int mon2 = tm2.tm_mon;
      int year2 = 1900 + tm2.tm_year;
      static constexpr int kDaysInMonth[] = {31, 28, 31, 30, 31, 30,
                                             31, 31, 30, 31, 30, 31};
      int dim = kDaysInMonth[mon2];
      if (mon2 == 1 &&
          (year2 % 4 == 0 && (year2 % 100 != 0 || year2 % 400 == 0))) {
        dim = 29;
      }
      days += dim;
    }

    // Normalize: borrow from years if months < 0.
    if (months < 0) {
      years -= 1;
      months += 12;
    }

    // Calculate time difference (within-day portion).
    int64_t time_us1 = static_cast<int64_t>(tm1.tm_hour) * 3600'000'000LL +
                       static_cast<int64_t>(tm1.tm_min) * 60'000'000LL +
                       static_cast<int64_t>(tm1.tm_sec) * 1'000'000LL;
    int64_t time_us2 = static_cast<int64_t>(tm2.tm_hour) * 3600'000'000LL +
                       static_cast<int64_t>(tm2.tm_min) * 60'000'000LL +
                       static_cast<int64_t>(tm2.tm_sec) * 1'000'000LL;
    int64_t time_diff = time_us1 - time_us2;

    if (time_diff < 0 && days > 0) {
      days -= 1;
      time_diff += 86400'000'000LL;
    } else if (time_diff > 0 && days < 0) {
      days += 1;
      time_diff -= 86400'000'000LL;
    }

    pg::UnpackedInterval iv{};
    iv.month = years * 12 + months;
    iv.day = days;
    iv.time = time_diff;
    result = pg::PackInterval(iv);
  }
};

// date_bin(stride interval, source timestamp, origin timestamp) -> timestamp
// Bins a timestamp into the nearest bin aligned with origin.
template<typename T>
struct PgDateBin {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Timestamp>& result,
                                const arg_type<Interval>& stride_packed,
                                const arg_type<Timestamp>& source,
                                const arg_type<Timestamp>& origin) {
    auto stride = pg::UnpackInterval(stride_packed);

    VELOX_USER_CHECK(stride.time > 0 || stride.day > 0 || stride.month > 0,
                     "stride must be greater than zero");
    VELOX_USER_CHECK(
      stride.month == 0,
      "timestamps cannot be binned into intervals containing months or years");

    // Total stride in microseconds.
    int64_t stride_us =
      stride.time + static_cast<int64_t>(stride.day) * 86400'000'000LL;
    VELOX_USER_CHECK(stride_us > 0, "stride must be greater than zero");

    // Source and origin as microseconds since epoch.
    int64_t src_us =
      source.getSeconds() * 1'000'000LL + source.getNanos() / 1000LL;
    int64_t orig_us =
      origin.getSeconds() * 1'000'000LL + origin.getNanos() / 1000LL;

    int64_t diff = src_us - orig_us;
    // Floor division.
    int64_t bin = diff >= 0 ? (diff / stride_us) * stride_us
                            : ((diff - stride_us + 1) / stride_us) * stride_us;
    int64_t result_us = orig_us + bin;

    result =
      Timestamp(result_us / 1'000'000LL, (result_us % 1'000'000LL) * 1000LL);
  }
};

}  // namespace

void registerDatetimeExtraFunctions(const std::string& prefix) {
  registerFunction<PgMakeDate, Date, int32_t, int32_t, int32_t>(
    {prefix + "make_date"});
  registerFunction<PgMakeTimestamp, Timestamp, int32_t, int32_t, int32_t,
                   int32_t, int32_t, double>({prefix + "make_timestamp"});
  registerFunction<PgToTimestampEpoch, Timestamp, double>(
    {prefix + "to_timestamp"});
  registerFunction<PgClockTimestamp, Timestamp>({prefix + "clock_timestamp"});
  registerFunction<PgTimeofday, Varchar>({prefix + "timeofday"});
  registerFunction<PgIsFiniteDate, bool, Date>({prefix + "isfinite"});
  registerFunction<PgIsFiniteTimestamp, bool, Timestamp>({prefix + "isfinite"});
  registerFunction<PgAge, Interval, Timestamp, Timestamp>({prefix + "age"});
  registerFunction<PgDateBin, Timestamp, Interval, Timestamp, Timestamp>(
    {prefix + "date_bin"});
}

}  // namespace sdb::pg::functions
