#include "pg/functions/extract.h"

#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/lib/TimeUtils.h>
#include <velox/functions/prestosql/DateTimeFunctions.h>
#include <velox/functions/prestosql/DateTimeImpl.h>
#include <velox/functions/prestosql/types/TimestampWithTimeZoneType.h>
#include <velox/type/SimpleFunctionApi.h>

#include "pg/functions/interval.h"
#include "query/types.h"

namespace sdb::pg::functions {
namespace {

using namespace velox;
using namespace velox::functions;

template<typename T>
struct ExtractMillenniumFunction : public InitSessionTimezone<T>,
                                   public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t GetMillennium(const std::tm& time) {
    int64_t year = 1900 + time.tm_year;
    if (year > 0) {
      return (year + 999) / 1000;
    } else {
      return year / 1000;
    }
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result,
                                const arg_type<Timestamp>& timestamp) {
    result = GetMillennium(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = GetMillennium(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
    int64_t& result, const arg_type<TimestampWithTimezone>& timestamptz) {
    auto timestamp = this->toTimestamp(timestamptz);
    result = GetMillennium(getDateTime(timestamp, nullptr));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval);
    result = interval.month / 12000;
  }
};

template<typename T>
struct ExtractCenturyFunction : public InitSessionTimezone<T>,
                                public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t GetCentury(const std::tm& time) {
    int64_t year = 1900 + time.tm_year;
    if (year > 0) {
      return (year + 99) / 100;
    } else {
      return year / 100;
    }
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result,
                                const arg_type<Timestamp>& timestamp) {
    result = GetCentury(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = GetCentury(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
    int64_t& result, const arg_type<TimestampWithTimezone>& timestamptz) {
    auto timestamp = this->toTimestamp(timestamptz);
    result = GetCentury(getDateTime(timestamp, nullptr));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval);
    result = interval.month / 1200;
  }
};

template<typename T>
struct ExtractDecadeFunction : public InitSessionTimezone<T>,
                               public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t GetDecade(const std::tm& time) {
    int64_t year = 1900 + time.tm_year;
    return year / 10;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result,
                                const arg_type<Timestamp>& timestamp) {
    result = GetDecade(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = GetDecade(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
    int64_t& result, const arg_type<TimestampWithTimezone>& timestamptz) {
    auto timestamp = this->toTimestamp(timestamptz);
    result = GetDecade(getDateTime(timestamp, nullptr));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval);
    result = interval.month / 120;
  }
};

template<typename T>
struct ExtractYearFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int64_t& result,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval);
    result = interval.month / 12;
  }
};

template<typename T>
struct ExtractMonthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int64_t& result,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval);
    result = interval.month % 12;
  }
};

template<typename T>
struct ExtractDayFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int64_t& result,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval);
    result = interval.day;
  }
};

template<typename T>
struct ExtractHourFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int64_t& result,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval);
    result = interval.time / 3600000000LL;
  }
};

template<typename T>
struct ExtractMinuteFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int64_t& result,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval);
    result = (interval.time / 60000000LL) % 60;
  }
};

template<typename T>
struct ExtractSecondFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result,
                                const arg_type<Timestamp>& timestamp) {
    auto date_time = getDateTime(timestamp, nullptr);
    double seconds = date_time.tm_sec;
    double microseconds = timestamp.getNanos() / 1000.0;
    microseconds = std::fmod(microseconds, 1000000.0);
    result = seconds + (microseconds / 1000000.0);
  }

  FOLLY_ALWAYS_INLINE void call(double& result, const arg_type<Date>& date) {
    auto date_time = getDateTime(date);
    result = date_time.tm_sec;
  }

  FOLLY_ALWAYS_INLINE void call(
    double& result, const arg_type<TimestampWithTimezone>& timestamptz) {
    auto timestamp = this->toTimestamp(timestamptz);
    auto date_time = getDateTime(timestamp, nullptr);
    double seconds = date_time.tm_sec;
    double microseconds = timestamp.getNanos() / 1000.0;
    microseconds = std::fmod(microseconds, 1000000.0);
    result = seconds + (microseconds / 1000000.0);
  }

  FOLLY_ALWAYS_INLINE void call(double& result,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval);
    double total_seconds = interval.time / 1000000.0;
    result = std::fmod(total_seconds, 60.0);
  }
};

template<typename T>
struct ExtractMillisecondsFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result,
                                const arg_type<Timestamp>& timestamp) {
    auto date_time = getDateTime(timestamp, nullptr);
    double seconds = date_time.tm_sec;
    double microseconds = timestamp.getNanos() / 1000.0;
    microseconds = std::fmod(microseconds, 1000000.0);
    result = seconds * 1000.0 + (microseconds / 1000.0);
  }

  FOLLY_ALWAYS_INLINE void call(
    double& result, const arg_type<TimestampWithTimezone>& timestamptz) {
    auto timestamp = this->toTimestamp(timestamptz);
    auto date_time = getDateTime(timestamp, nullptr);
    double seconds = date_time.tm_sec;
    double microseconds = timestamp.getNanos() / 1000.0;
    microseconds = std::fmod(microseconds, 1000000.0);
    result = seconds * 1000.0 + (microseconds / 1000.0);
  }

  FOLLY_ALWAYS_INLINE void call(double& result,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval);
    double total_milliseconds = interval.time / 1000.0;
    result = std::fmod(total_milliseconds, 60000.0);
  }
};

template<typename T>
struct ExtractMicrosecondsFunction : public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(double& result,
                                const arg_type<Timestamp>& timestamp) {
    auto date_time = getDateTime(timestamp, nullptr);
    double seconds = date_time.tm_sec;
    double microseconds = timestamp.getNanos() / 1000.0;
    microseconds = std::fmod(microseconds, 1000000.0);
    result = seconds * 1000000.0 + microseconds;
  }

  FOLLY_ALWAYS_INLINE void call(
    double& result, const arg_type<TimestampWithTimezone>& timestamptz) {
    auto timestamp = this->toTimestamp(timestamptz);
    auto date_time = getDateTime(timestamp, nullptr);
    double seconds = date_time.tm_sec;
    double microseconds = timestamp.getNanos() / 1000.0;
    microseconds = std::fmod(microseconds, 1000000.0);
    result = seconds * 1000000.0 + microseconds;
  }

  FOLLY_ALWAYS_INLINE void call(double& result,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval);
    result = std::fmod(static_cast<double>(interval.time), 60000000.0);
  }
};

template<typename T>
struct ExtractDowFunction : public InitSessionTimezone<T>,
                            public TimestampWithTimezoneSupport<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int64_t GetDow(const std::tm& time) {
    return time.tm_wday;
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result,
                                const arg_type<Timestamp>& timestamp) {
    result = GetDow(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int64_t& result, const arg_type<Date>& date) {
    result = GetDow(getDateTime(date));
  }

  FOLLY_ALWAYS_INLINE void call(
    int64_t& result, const arg_type<TimestampWithTimezone>& timestamptz) {
    auto timestamp = this->toTimestamp(timestamptz);
    result = GetDow(getDateTime(timestamp, nullptr));
  }
};

template<typename T>
struct ExtractTimezoneFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
    int64_t& result, const arg_type<TimestampWithTimezone>& timestamptz) {
    const auto timezone_id = unpackZoneKeyId(*timestamptz);
    result = timezone_id * 60;
  }
};

template<typename T>
struct ExtractTimezoneHourFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
    int64_t& result, const arg_type<TimestampWithTimezone>& timestamptz) {
    const auto timezone_id = unpackZoneKeyId(*timestamptz);
    result = timezone_id / 60;
  }
};

template<typename T>
struct ExtractTimezoneMinuteFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
    int64_t& result, const arg_type<TimestampWithTimezone>& timestamptz) {
    const auto timezone_id = unpackZoneKeyId(*timestamptz);
    result = timezone_id % 60;
  }
};

}  // namespace

// Some of them are taken from presto but registered with convinient
// name to make mappings easier.
void registerExtractFunctions(const std::string& prefix) {
  velox::registerFunction<ExtractMillenniumFunction, int64_t, Date>(
    {prefix + "extract_millennium"});
  velox::registerFunction<ExtractMillenniumFunction, int64_t, Timestamp>(
    {prefix + "extract_millennium"});
  velox::registerFunction<ExtractMillenniumFunction, int64_t,
                          TimestampWithTimezone>(
    {prefix + "extract_millennium"});
  velox::registerFunction<ExtractMillenniumFunction, int64_t, Interval>(
    {prefix + "extract_millennium"});

  velox::registerFunction<ExtractCenturyFunction, int64_t, Date>(
    {prefix + "extract_century"});
  velox::registerFunction<ExtractCenturyFunction, int64_t, Timestamp>(
    {prefix + "extract_century"});
  velox::registerFunction<ExtractCenturyFunction, int64_t,
                          TimestampWithTimezone>({prefix + "extract_century"});
  velox::registerFunction<ExtractCenturyFunction, int64_t, Interval>(
    {prefix + "extract_century"});

  velox::registerFunction<ExtractDecadeFunction, int64_t, Date>(
    {prefix + "extract_decade"});
  velox::registerFunction<ExtractDecadeFunction, int64_t, Timestamp>(
    {prefix + "extract_decade"});
  velox::registerFunction<ExtractDecadeFunction, int64_t,
                          TimestampWithTimezone>({prefix + "extract_decade"});
  velox::registerFunction<ExtractDecadeFunction, int64_t, Interval>(
    {prefix + "extract_decade"});

  velox::registerFunction<YearFunction, int64_t, Date>(
    {prefix + "extract_year"});
  velox::registerFunction<YearFunction, int64_t, Timestamp>(
    {prefix + "extract_year"});
  velox::registerFunction<YearFunction, int64_t, TimestampWithTimezone>(
    {prefix + "extract_year"});
  velox::registerFunction<ExtractYearFunction, int64_t, Interval>(
    {prefix + "extract_year"});

  velox::registerFunction<QuarterFunction, int64_t, Date>(
    {prefix + "extract_quarter"});
  velox::registerFunction<QuarterFunction, int64_t, Timestamp>(
    {prefix + "extract_quarter"});
  velox::registerFunction<QuarterFunction, int64_t, TimestampWithTimezone>(
    {prefix + "extract_quarter"});

  velox::registerFunction<MonthFunction, int64_t, Date>(
    {prefix + "extract_month"});
  velox::registerFunction<MonthFunction, int64_t, Timestamp>(
    {prefix + "extract_month"});
  velox::registerFunction<MonthFunction, int64_t, TimestampWithTimezone>(
    {prefix + "extract_month"});
  velox::registerFunction<ExtractMonthFunction, int64_t, Interval>(
    {prefix + "extract_month"});

  velox::registerFunction<WeekFunction, int64_t, Date>(
    {prefix + "extract_week"});
  velox::registerFunction<WeekFunction, int64_t, Timestamp>(
    {prefix + "extract_week"});
  velox::registerFunction<WeekFunction, int64_t, TimestampWithTimezone>(
    {prefix + "extract_week"});

  velox::registerFunction<DayFunction, int64_t, Date>({prefix + "extract_day"});
  velox::registerFunction<DayFunction, int64_t, Timestamp>(
    {prefix + "extract_day"});
  velox::registerFunction<DayFunction, int64_t, TimestampWithTimezone>(
    {prefix + "extract_day"});
  velox::registerFunction<ExtractDayFunction, int64_t, Interval>(
    {prefix + "extract_day"});

  velox::registerFunction<HourFunction, int64_t, Timestamp>(
    {prefix + "extract_hour"});
  velox::registerFunction<HourFunction, int64_t, TimestampWithTimezone>(
    {prefix + "extract_hour"});
  velox::registerFunction<ExtractHourFunction, int64_t, Interval>(
    {prefix + "extract_hour"});

  velox::registerFunction<MinuteFunction, int64_t, Timestamp>(
    {prefix + "extract_minute"});
  velox::registerFunction<MinuteFunction, int64_t, TimestampWithTimezone>(
    {prefix + "extract_minute"});
  velox::registerFunction<ExtractMinuteFunction, int64_t, Interval>(
    {prefix + "extract_minute"});

  velox::registerFunction<ExtractSecondFunction, double, Timestamp>(
    {prefix + "extract_second"});
  velox::registerFunction<ExtractSecondFunction, double, Date>(
    {prefix + "extract_second"});
  velox::registerFunction<ExtractSecondFunction, double, TimestampWithTimezone>(
    {prefix + "extract_second"});
  velox::registerFunction<ExtractSecondFunction, double, Interval>(
    {prefix + "extract_second"});

  velox::registerFunction<ExtractMillisecondsFunction, double, Timestamp>(
    {prefix + "extract_millisecond"});
  velox::registerFunction<ExtractMillisecondsFunction, double,
                          TimestampWithTimezone>(
    {prefix + "extract_millisecond"});
  velox::registerFunction<ExtractMillisecondsFunction, double, Interval>(
    {prefix + "extract_millisecond"});

  velox::registerFunction<ExtractMicrosecondsFunction, double, Timestamp>(
    {prefix + "extract_microsecond"});
  velox::registerFunction<ExtractMicrosecondsFunction, double,
                          TimestampWithTimezone>(
    {prefix + "extract_microsecond"});
  velox::registerFunction<ExtractMicrosecondsFunction, double, Interval>(
    {prefix + "extract_microsecond"});

  velox::registerFunction<ToUnixtimeFunction, double, Timestamp>(
    {prefix + "extract_epoch"});
  velox::registerFunction<ToUnixtimeFunction, double, TimestampWithTimezone>(
    {prefix + "extract_epoch"});

  velox::registerFunction<ExtractDowFunction, int64_t, Date>(
    {prefix + "extract_dow"});
  velox::registerFunction<ExtractDowFunction, int64_t, Timestamp>(
    {prefix + "extract_dow"});
  velox::registerFunction<ExtractDowFunction, int64_t, TimestampWithTimezone>(
    {prefix + "extract_dow"});

  velox::registerFunction<DayOfWeekFunction, int64_t, Date>(
    {prefix + "extract_isodow"});
  velox::registerFunction<DayOfWeekFunction, int64_t, Timestamp>(
    {prefix + "extract_isodow"});
  velox::registerFunction<DayOfWeekFunction, int64_t, TimestampWithTimezone>(
    {prefix + "extract_isodow"});

  velox::registerFunction<DayOfYearFunction, int64_t, Date>(
    {prefix + "extract_doy"});
  velox::registerFunction<DayOfYearFunction, int64_t, Timestamp>(
    {prefix + "extract_doy"});
  velox::registerFunction<DayOfYearFunction, int64_t, TimestampWithTimezone>(
    {prefix + "extract_doy"});

  velox::registerFunction<YearOfWeekFunction, int64_t, Date>(
    {prefix + "extract_isoyear"});
  velox::registerFunction<YearOfWeekFunction, int64_t, Timestamp>(
    {prefix + "extract_isoyear"});
  velox::registerFunction<YearOfWeekFunction, int64_t, TimestampWithTimezone>(
    {prefix + "extract_isoyear"});

  velox::registerFunction<ExtractTimezoneFunction, int64_t,
                          TimestampWithTimezone>({prefix + "extract_timezone"});

  velox::registerFunction<ExtractTimezoneHourFunction, int64_t,
                          TimestampWithTimezone>(
    {prefix + "extract_timezone_hour"});

  velox::registerFunction<ExtractTimezoneMinuteFunction, int64_t,
                          TimestampWithTimezone>(
    {prefix + "extract_timezone_minute"});
}

}  // namespace sdb::pg::functions
