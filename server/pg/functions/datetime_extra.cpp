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

#include <absl/time/clock.h>
#include <absl/time/time.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/lib/DateTimeFormatter.h>
#include <velox/functions/lib/TimeUtils.h>
#include <velox/functions/prestosql/DateTimeImpl.h>
#include <velox/type/SimpleFunctionApi.h>

#include <chrono>

#include "basics/fwd.h"
#include "pg/functions/interval.h"
#include "pg/sql_exception_macro.h"
#include "query/types.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {
namespace {

// make_date(year, month, day) -> date
template<typename T>
struct PgMakeDate {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Date>& result, int32_t year,
                                int32_t month, int32_t day) {
    auto expected = velox::util::daysSinceEpochFromDate(year, month, day);
    if (!expected.hasValue()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                      ERR_MSG("date field value out of range: ", year, "-",
                              absl::Dec(month, absl::kZeroPad2), "-",
                              absl::Dec(day, absl::kZeroPad2)));
    }
    result = expected.value();
  }
};

// make_timestamp(year, month, day, hour, min, sec) -> timestamp
template<typename T>
struct PgMakeTimestamp {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>& result,
                                int32_t year, int32_t month, int32_t day,
                                int32_t hour, int32_t min, double sec) {
    auto expected = velox::util::daysSinceEpochFromDate(year, month, day);
    if (!expected.hasValue()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                      ERR_MSG("date field value out of range: ", year, "-",
                              absl::Dec(month, absl::kZeroPad2), "-",
                              absl::Dec(day, absl::kZeroPad2)));
    }
    if (hour < 0 || hour > 23 || min < 0 || min > 59 || sec < 0.0 ||
        sec > 60.0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                      ERR_MSG("date/time field value out of range"));
    }
    int64_t days = expected.value();
    int32_t whole_sec = static_cast<int32_t>(sec);
    double frac = sec - whole_sec;
    int64_t total_seconds =
      days * 86400LL + hour * 3600LL + min * 60LL + whole_sec;
    int64_t nanos = static_cast<int64_t>(frac * 1'000'000'000);
    if (nanos < 0) {
      --total_seconds;
      nanos += 1'000'000'000;
    }
    result = velox::Timestamp(total_seconds, static_cast<uint64_t>(nanos));
  }
};

// to_timestamp(double) -> timestamptz
// Converts Unix epoch (seconds since 1970-01-01 00:00:00 UTC) to timestamp.
template<typename T>
struct PgToTimestampEpoch {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>& result,
                                double epoch_seconds) {
    int64_t secs = static_cast<int64_t>(epoch_seconds);
    int64_t nanos =
      static_cast<int64_t>((epoch_seconds - secs) * 1'000'000'000);
    if (nanos < 0) {
      --secs;
      nanos += 1'000'000'000;
    }
    result = velox::Timestamp(secs, static_cast<uint64_t>(nanos));
  }
};

// clock_timestamp() -> timestamptz
// Returns real-time clock (not statement time).
template<typename T>
struct PgClockTimestamp {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>& result) {
    auto now = std::chrono::system_clock::now();
    auto epoch = now.time_since_epoch();
    auto secs = std::chrono::duration_cast<std::chrono::seconds>(epoch).count();
    auto nanos =
      std::chrono::duration_cast<std::chrono::nanoseconds>(epoch).count() -
      secs * 1'000'000'000LL;
    result = velox::Timestamp(secs, nanos);
  }
};

// timeofday() -> text
// Returns wall clock time as formatted string.
template<typename T>
struct PgTimeofday {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result) {
    auto formatted = absl::FormatTime("%a %b %d %H:%M:%S %Y %Z", absl::Now(),
                                      absl::UTCTimeZone());
    result.resize(formatted.size());
    std::memcpy(result.data(), formatted.data(), formatted.size());
  }
};

// isfinite(date) -> boolean
// isfinite(timestamp) -> boolean
// Dates and timestamps are always finite in our system (no infinity support),
// so these always return true.
template<typename T>
struct PgIsFiniteDate {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(bool& result, const arg_type<velox::Date>&) {
    result = true;
  }
};

template<typename T>
struct PgIsFiniteTimestamp {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(bool& result,
                                const arg_type<velox::Timestamp>&) {
    result = true;
  }
};

// age(timestamp, timestamp) -> interval
// Subtracts two timestamps and returns the difference as an interval
// in years, months, days format (like PG).
template<typename T>
struct PgAge {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void compute_age(out_type<Interval>& result,
                                       const std::tm& tm1, const std::tm& tm2,
                                       int64_t time_us1, int64_t time_us2) {
    int32_t years = (1900 + tm1.tm_year) - (1900 + tm2.tm_year);
    int32_t months = tm1.tm_mon - tm2.tm_mon;
    int32_t days = tm1.tm_mday - tm2.tm_mday;

    int64_t time_diff = time_us1 - time_us2;

    // Normalize time: borrow from days if time is negative.
    if (time_diff < 0) {
      days -= 1;
      time_diff += 86400'000'000LL;
    }

    // Normalize: borrow from months if days < 0.
    if (days < 0) {
      months -= 1;
      int mon2 = tm2.tm_mon;
      int year2 = 1900 + tm2.tm_year;
      static constexpr int kDaysInMonth[] = {
        31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
      };
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

    pg::UnpackedInterval iv{};
    iv.month = years * 12 + months;
    iv.day = days;
    iv.time = time_diff;
    result = pg::PackInterval(iv);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<Interval>& result,
                                const arg_type<velox::Timestamp>& ts1,
                                const arg_type<velox::Timestamp>& ts2) {
    // PostgreSQL computes age for the positive direction and negates for
    // negative intervals to avoid mixed-sign normalization issues.
    bool negate = (ts1.getSeconds() < ts2.getSeconds() ||
                   (ts1.getSeconds() == ts2.getSeconds() &&
                    ts1.getNanos() < ts2.getNanos()));

    const auto& a = negate ? ts2 : ts1;
    const auto& b = negate ? ts1 : ts2;

    auto tm_a = velox::functions::getDateTime(a, nullptr);
    auto tm_b = velox::functions::getDateTime(b, nullptr);

    int64_t time_us_a = static_cast<int64_t>(tm_a.tm_hour) * 3600'000'000LL +
                        static_cast<int64_t>(tm_a.tm_min) * 60'000'000LL +
                        static_cast<int64_t>(tm_a.tm_sec) * 1'000'000LL +
                        a.getNanos() / 1000LL;
    int64_t time_us_b = static_cast<int64_t>(tm_b.tm_hour) * 3600'000'000LL +
                        static_cast<int64_t>(tm_b.tm_min) * 60'000'000LL +
                        static_cast<int64_t>(tm_b.tm_sec) * 1'000'000LL +
                        b.getNanos() / 1000LL;

    compute_age(result, tm_a, tm_b, time_us_a, time_us_b);

    if (negate) {
      auto iv = pg::UnpackInterval(result);
      iv.month = -iv.month;
      iv.day = -iv.day;
      iv.time = -iv.time;
      result = pg::PackInterval(iv);
    }
  }
};

// date_bin(stride interval, source timestamp, origin timestamp) -> timestamp
// Bins a timestamp into the nearest bin aligned with origin.
template<typename T>
struct PgDateBin {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>& result,
                                const arg_type<Interval>& stride_packed,
                                const arg_type<velox::Timestamp>& source,
                                const arg_type<velox::Timestamp>& origin) {
    auto stride = pg::UnpackInterval(stride_packed);

    if (stride.time <= 0 && stride.day <= 0 && stride.month <= 0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("stride must be greater than zero"));
    }
    if (stride.month != 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("timestamps cannot be binned into intervals containing "
                "months or years"));
    }

    // Total stride in microseconds.
    int64_t stride_us =
      stride.time + static_cast<int64_t>(stride.day) * 86400'000'000LL;
    if (stride_us <= 0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("stride must be greater than zero"));
    }

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

    int64_t result_s = result_us / 1'000'000LL;
    int64_t result_us_rem = result_us % 1'000'000LL;
    if (result_us_rem < 0) {
      --result_s;
      result_us_rem += 1'000'000LL;
    }
    result =
      velox::Timestamp(result_s, static_cast<uint64_t>(result_us_rem * 1000LL));
  }
};

velox::Timestamp AddIntervalToTimestamp(const velox::Timestamp& timestamp,
                                        const UnpackedInterval& interval) {
  auto result = velox::functions::addToTimestamp(
    timestamp, velox::functions::DateTimeUnit::kMonth, interval.month);
  result = velox::functions::addToTimestamp(
    result, velox::functions::DateTimeUnit::kDay, interval.day);
  result = velox::functions::addToTimestamp(
    result, velox::functions::DateTimeUnit::kMicrosecond, interval.time);
  return result;
}

template<typename T>
struct TimestampPlusIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>& result,
                                const arg_type<velox::Timestamp>& timestamp,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval);
    result = AddIntervalToTimestamp(timestamp, interval);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>& result,
                                const arg_type<Interval>& interval,
                                const arg_type<velox::Timestamp>& timestamp) {
    call(result, timestamp, interval);
  }
};

template<typename T>
struct TimestampMinusIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>& result,
                                const arg_type<velox::Timestamp>& timestamp,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval).Negate();
    result = AddIntervalToTimestamp(timestamp, interval);
  }
};

}  // namespace

void registerDatetimeExtraFunctions(const std::string& prefix) {
  velox::registerFunction<PgMakeDate, velox::Date, int32_t, int32_t, int32_t>(
    {prefix + "make_date"});
  velox::registerFunction<PgMakeTimestamp, velox::Timestamp, int32_t, int32_t,
                          int32_t, int32_t, int32_t, double>(
    {prefix + "make_timestamp"});
  velox::registerFunction<PgToTimestampEpoch, velox::Timestamp, double>(
    {prefix + "to_timestamp"});
  velox::registerFunction<PgClockTimestamp, velox::Timestamp>(
    {prefix + "clock_timestamp"});
  velox::registerFunction<PgTimeofday, velox::Varchar>({prefix + "timeofday"});
  velox::registerFunction<PgIsFiniteDate, bool, velox::Date>(
    {prefix + "isfinite"});
  velox::registerFunction<PgIsFiniteTimestamp, bool, velox::Timestamp>(
    {prefix + "isfinite"});
  velox::registerFunction<PgAge, Interval, velox::Timestamp, velox::Timestamp>(
    {prefix + "age"});
  velox::registerFunction<PgDateBin, velox::Timestamp, Interval,
                          velox::Timestamp, velox::Timestamp>(
    {prefix + "date_bin"});

  velox::registerFunction<TimestampPlusIntervalFunction, velox::Timestamp,
                          velox::Timestamp, Interval>({prefix + "time_plus"});
  velox::registerFunction<TimestampPlusIntervalFunction, velox::Timestamp,
                          Interval, velox::Timestamp>({prefix + "time_plus"});
  velox::registerFunction<TimestampMinusIntervalFunction, velox::Timestamp,
                          velox::Timestamp, Interval>({prefix + "time_minus"});
}

}  // namespace sdb::pg::functions
