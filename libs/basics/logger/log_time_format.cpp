////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "log_time_format.h"

#include <chrono>
#include <string>

#include "basics/datetime.h"
#include "basics/debugging.h"
#include "basics/exceptions.h"
#include "basics/string_utils.h"
#include "basics/system-functions.h"

namespace {

const sdb::containers::FlatHashMap<std::string_view,
                                   sdb::log_time_formats::TimeFormat>
  kFormatMap{
    {"uptime", sdb::log_time_formats::TimeFormat::Uptime},
    {"uptime-millis", sdb::log_time_formats::TimeFormat::UptimeMillis},
    {"uptime-micros", sdb::log_time_formats::TimeFormat::UptimeMicros},
    {"timestamp", sdb::log_time_formats::TimeFormat::UnixTimestamp},
    {"timestamp-millis",
     sdb::log_time_formats::TimeFormat::UnixTimestampMillis},
    {"timestamp-micros",
     sdb::log_time_formats::TimeFormat::UnixTimestampMicros},
    {"utc-datestring", sdb::log_time_formats::TimeFormat::UTCDateString},
    {"utc-datestring-millis",
     sdb::log_time_formats::TimeFormat::UTCDateStringMillis},
    {"utc-datestring-micros",
     sdb::log_time_formats::TimeFormat::UTCDateStringMicros},
    {"local-datestring", sdb::log_time_formats::TimeFormat::LocalDateString},
  };

const std::chrono::time_point<std::chrono::system_clock> kStartTime =
  std::chrono::system_clock::now();

void AppendNumber(uint64_t value, std::string& out, size_t size) {
  char buffer[22];
  size_t len = sdb::basics::string_utils::Itoa(value, &buffer[0]);
  while (len < size) {
    // zero-padding at the beginning of the output buffer, because
    // we haven't yet written our number into it
    out.push_back('0');
    --size;
  }
  // now, after zero-padding, write our number into the output
  out.append(&buffer[0], len);
}

}  // namespace
namespace sdb {
namespace log_time_formats {

/// whether or not the specified format is a local one
bool IsLocalFormat(TimeFormat format) {
  return format == TimeFormat::LocalDateString;
}

/// whether or not the specified format produces string outputs
/// (in contrast to numeric outputs)
bool IsStringFormat(TimeFormat format) {
  return format == TimeFormat::UTCDateString ||
         format == TimeFormat::UTCDateStringMillis ||
         format == TimeFormat::UTCDateStringMicros ||
         format == TimeFormat::LocalDateString;
}

/// return the name of the default log time format
std::string DefaultFormatName() { return "utc-datestring-micros"; }

/// return the names of all log time formats
containers::FlatHashSet<std::string> GetAvailableFormatNames() {
  containers::FlatHashSet<std::string> formats;
  for (const auto& format : kFormatMap | std::views::keys) {
    formats.emplace(format);
  }
  return formats;
}

/// derive the time format from the name
TimeFormat FormatFromName(const std::string& name) {
  auto it = ::kFormatMap.find(name);
  // if this assertion does not hold true, some SereneDB developer xxxed up
  SDB_ASSERT(it != ::kFormatMap.end());

  if (it == ::kFormatMap.end()) {
    SDB_THROW(ERROR_INTERNAL, "invalid time format");
  }

  return (*it).second;
}

void WriteTime(std::string& out, TimeFormat format,
               std::chrono::system_clock::time_point tp,
               std::chrono::system_clock::time_point start_tp) {
  using namespace std::chrono;

  if (format == TimeFormat::Uptime || format == TimeFormat::UptimeMillis ||
      format == TimeFormat::UptimeMicros) {
    if (start_tp == system_clock::time_point()) {
      // if startTp is not set by caller, we will use the recorded start time.
      // this way it can be overriden easily from tests
      start_tp = ::kStartTime;
    }
    // integral uptime value
    sdb::basics::string_utils::Itoa(
      uint64_t(duration_cast<seconds>(tp - start_tp).count()), out);
    if (format == TimeFormat::UptimeMillis) {
      // uptime with millisecond precision
      out.push_back('.');
      AppendNumber(
        uint64_t(duration_cast<milliseconds>(tp - start_tp).count() % 1000),
        out, 3);
    } else if (format == TimeFormat::UptimeMicros) {
      // uptime with microsecond precision
      out.push_back('.');
      AppendNumber(
        uint64_t(duration_cast<microseconds>(tp - start_tp).count() % 1000000),
        out, 6);
    }
  } else if (format == TimeFormat::UnixTimestamp) {
    // integral unix timestamp
    sdb::basics::string_utils::Itoa(
      uint64_t(duration_cast<seconds>(tp.time_since_epoch()).count()), out);
  } else if (format == TimeFormat::UnixTimestampMillis) {
    // unix timestamp with millisecond precision
    system_clock::time_point tp2(duration_cast<seconds>(tp.time_since_epoch()));
    sdb::basics::string_utils::Itoa(
      uint64_t(duration_cast<seconds>(tp2.time_since_epoch()).count()), out);
    out.push_back('.');
    AppendNumber(uint64_t(duration_cast<milliseconds>(tp - tp2).count()), out,
                 3);
  } else if (format == TimeFormat::UnixTimestampMicros) {
    // unix timestamp with microsecond precision
    system_clock::time_point tp2(duration_cast<seconds>(tp.time_since_epoch()));
    sdb::basics::string_utils::Itoa(
      uint64_t(duration_cast<seconds>(tp2.time_since_epoch()).count()), out);
    out.push_back('.');
    AppendNumber(uint64_t(duration_cast<microseconds>(tp - tp2).count()), out,
                 6);
  } else {
    // all date-string variants handled here
    if (format == TimeFormat::UTCDateString ||
        format == TimeFormat::UTCDateStringMillis ||
        format == TimeFormat::UTCDateStringMicros) {
      // TODO(mbkkt) shouldn't we convert local to UTC here?
      // TODO(mbkkt) btw it's shit
      // UTC datestring
      // UTC datestring with milliseconds
      auto secs = time_point_cast<microseconds>(tp);
      auto days = floor<std::chrono::days>(secs);
      auto ymd = year_month_day(days);
      AppendNumber(uint64_t(static_cast<int>(ymd.year())), out, 4);
      out.push_back('-');
      AppendNumber(uint64_t(static_cast<unsigned>(ymd.month())), out, 2);
      out.push_back('-');
      AppendNumber(uint64_t(static_cast<unsigned>(ymd.day())), out, 2);
      out.push_back('T');
      auto day_time = hh_mm_ss(secs - days);
      AppendNumber(uint64_t(day_time.hours().count()), out, 2);
      out.push_back(':');
      AppendNumber(uint64_t(day_time.minutes().count()), out, 2);
      out.push_back(':');
      AppendNumber(uint64_t(day_time.seconds().count()), out, 2);

      if (format == TimeFormat::UTCDateStringMillis) {
        out.push_back('.');
        AppendNumber(uint64_t(day_time.subseconds().count()) / 1000, out, 3);
      } else if (format == TimeFormat::UTCDateStringMicros) {
        out.push_back('.');
        AppendNumber(uint64_t(day_time.subseconds().count()), out, 6);
      }

      out.push_back('Z');
    } else if (format == TimeFormat::LocalDateString) {
      // local datestring
      time_t tt = time(nullptr);
      struct tm tb;

      utilities::GetLocalTime(tt, &tb);
      char buffer[32];
      strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S", &tb);
      out.append(&buffer[0]);
    }
  }
}

}  // namespace log_time_formats
}  // namespace sdb
