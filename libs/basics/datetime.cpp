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

#include "basics/datetime.h"

#include <date/iso_week.h>
#include <stdlib.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iterator>
#include <map>
#include <ratio>
#include <regex>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/debugging.h"
#include "basics/logger/logger.h"
#include "basics/number_utils.h"

namespace sdb::basics {
namespace {

using namespace std::chrono;

std::string Tail(const std::string& source, const size_t length) {
  if (length >= source.size()) {
    return source;
  }
  return source.substr(source.size() - length);
}

typedef void (*FormatFuncT)(std::string& wrk, const sdb::tp_sys_clock_ms&);
sdb::containers::FlatHashMap<std::string, FormatFuncT> gDateMap;
const auto kUnixEpoch = sys_seconds{seconds{0}};

const std::vector<std::string> kMonthNames = {
  "January", "February", "March",     "April",   "May",      "June",
  "July",    "August",   "September", "October", "November", "December"};

const std::vector<std::string> kMonthNamesShort = {"Jan", "Feb", "Mar", "Apr",
                                                   "May", "Jun", "Jul", "Aug",
                                                   "Sep", "Oct", "Nov", "Dec"};

const std::vector<std::string> kWeekDayNames = {
  "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};

const std::vector<std::string> kWeekDayNamesShort = {"Sun", "Mon", "Tue", "Wed",
                                                     "Thu", "Fri", "Sat"};

const std::vector<std::pair<std::string, FormatFuncT>> kSortedDateMap = {
  {"%&", [](std::string& wrk,
            const sdb::tp_sys_clock_ms& tp) {}},  // Allow for literal "m" after
                                                  // "%m" ("%mm" -> %m%&m)
  // zero-pad 4 digit years to length of 6 and add "+"
  // prefix, keep negative as-is
  {"%yyyyyy",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto ymd = year_month_day(floor<days>(tp));
     auto yearnum = static_cast<int>(ymd.year());
     if (yearnum < 0) {
       if (yearnum > -10) {
         wrk.append("-00000");
       } else if (yearnum > -100) {
         wrk.append("-0000");
       } else if (yearnum > -1000) {
         wrk.append("-000");
       } else if (yearnum > -10000) {
         wrk.append("-00");
       } else if (yearnum > -100000) {
         wrk.append("-0");
       } else {
         wrk.append("-");
       }
       wrk.append(std::to_string(abs(yearnum)));
       return;
     }

     SDB_ASSERT(yearnum >= 0);

     if (yearnum > 99999) {
       // intentionally nothing
     } else if (yearnum > 9999) {
       wrk.append("+0");
     } else if (yearnum > 999) {
       wrk.append("+00");
     } else if (yearnum > 99) {
       wrk.append("+000");
     } else if (yearnum > 9) {
       wrk.append("+0000");
     } else {
       wrk.append("+00000");
     }
     wrk.append(std::to_string(yearnum));
   }},
  {"%mmmm",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto ymd = year_month_day(floor<days>(tp));
     wrk.append(kMonthNames[static_cast<unsigned>(ymd.month()) - 1]);
   }},
  {"%yyyy",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto ymd = year_month_day(floor<days>(tp));
     auto yearnum = static_cast<int>(ymd.year());
     if (yearnum < 0) {
       if (yearnum > -10) {
         wrk.append("-000");
       } else if (yearnum > -100) {
         wrk.append("-00");
       } else if (yearnum > -1000) {
         wrk.append("-0");
       } else {
         wrk.append("-");
       }
       wrk.append(std::to_string(abs(yearnum)));
     } else {
       SDB_ASSERT(yearnum >= 0);
       if (yearnum < 9) {
         wrk.append("000");
         wrk.append(std::to_string(yearnum));
       } else if (yearnum < 99) {
         wrk.append("00");
         wrk.append(std::to_string(yearnum));
       } else if (yearnum < 999) {
         wrk.append("0");
         wrk.append(std::to_string(yearnum));
       } else {
         std::string yearstr(std::to_string(yearnum));
         wrk.append(Tail(yearstr, 4));
       }
     }
   }},

  {"%wwww",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     weekday wd{floor<days>(tp)};
     wrk.append(kWeekDayNames[wd.c_encoding()]);
   }},

  {"%mmm",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto ymd = year_month_day(floor<days>(tp));
     wrk.append(kMonthNamesShort[static_cast<unsigned>(ymd.month()) - 1]);
   }},
  {"%www",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     weekday wd{floor<days>(tp)};
     wrk.append(kWeekDayNamesShort[wd.c_encoding()]);
   }},
  {"%fff",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto day_time = hh_mm_ss(tp - floor<days>(tp));
     uint64_t millis = day_time.subseconds().count();
     if (millis < 10) {
       wrk.append("00");
     } else if (millis < 100) {
       wrk.append("0");
     }
     wrk.append(std::to_string(millis));
   }},
  {"%xxx",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto ymd = year_month_day(floor<days>(tp));
     auto yyyy = year{ymd.year()};
     // we construct the date with the first day in the
     // year:
     auto first_day_in_year = yyyy / January / day{0};
     uint64_t days_since_first =
       duration_cast<days>(tp - sys_days(first_day_in_year)).count();
     if (days_since_first < 10) {
       wrk.append("00");
     } else if (days_since_first < 100) {
       wrk.append("0");
     }
     wrk.append(std::to_string(days_since_first));
   }},

  // there"s no really sensible way to handle negative
  // years, but better not drop the sign
  {"%yy",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto ymd = year_month_day(floor<days>(tp));
     auto yearnum = static_cast<int>(ymd.year());
     if (yearnum < 10 && yearnum > -10) {
       wrk.append("0");
       wrk.append(std::to_string(abs(yearnum)));
     } else {
       std::string yearstr(std::to_string(abs(yearnum)));
       wrk.append(Tail(yearstr, 2));
     }
   }},
  {"%mm",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto ymd = year_month_day(floor<days>(tp));
     auto month = static_cast<unsigned>(ymd.month());
     if (month < 10) {
       wrk.append("0");
     }
     wrk.append(std::to_string(month));
   }},
  {"%dd",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto ymd = year_month_day(floor<days>(tp));
     auto day = static_cast<unsigned>(ymd.day());
     if (day < 10) {
       wrk.append("0");
     }
     wrk.append(std::to_string(day));
   }},
  {"%hh",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto day_time = hh_mm_ss(tp - floor<days>(tp));
     uint64_t hours = day_time.hours().count();
     if (hours < 10) {
       wrk.append("0");
     }
     wrk.append(std::to_string(hours));
   }},
  {"%ii",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto day_time = hh_mm_ss(tp - floor<days>(tp));
     uint64_t minutes = day_time.minutes().count();
     if (minutes < 10) {
       wrk.append("0");
     }
     wrk.append(std::to_string(minutes));
   }},
  {"%ss",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto day_time = hh_mm_ss(tp - floor<days>(tp));
     uint64_t seconds = day_time.seconds().count();
     if (seconds < 10) {
       wrk.append("0");
     }
     wrk.append(std::to_string(seconds));
   }},
  {"%kk",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     iso_week::year_weeknum_weekday yww{floor<days>(tp)};
     uint64_t iso_week = static_cast<unsigned>(yww.weeknum());
     if (iso_week < 10) {
       wrk.append("0");
     }
     wrk.append(std::to_string(iso_week));
   }},

  {"%t",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto diff_duration = tp - kUnixEpoch;
     auto diff =
       duration_cast<duration<double, std::milli>>(diff_duration).count();
     wrk.append(std::to_string(static_cast<int64_t>(std::round(diff))));
   }},
  {"%z",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto formatted = absl::FormatTime("%FT%H:%M:%E3SZ", absl::FromChrono(tp),
                                       absl::UTCTimeZone());
     wrk.append(formatted);
   }},
  {"%w",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     weekday wd{floor<days>(tp)};
     wrk.append(std::to_string(wd.c_encoding()));
   }},
  {"%y",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto ymd = year_month_day(floor<days>(tp));
     wrk.append(std::to_string(static_cast<int>(ymd.year())));
   }},
  {"%m",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto ymd = year_month_day(floor<days>(tp));
     wrk.append(std::to_string(static_cast<unsigned>(ymd.month())));
   }},
  {"%d",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto ymd = year_month_day(floor<days>(tp));
     wrk.append(std::to_string(static_cast<unsigned>(ymd.day())));
   }},
  {"%h",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto day_time = hh_mm_ss(tp - floor<days>(tp));
     uint64_t hours = day_time.hours().count();
     wrk.append(std::to_string(hours));
   }},
  {"%i",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto day_time = hh_mm_ss(tp - floor<days>(tp));
     uint64_t minutes = day_time.minutes().count();
     wrk.append(std::to_string(minutes));
   }},
  {"%s",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto day_time = hh_mm_ss(tp - floor<days>(tp));
     uint64_t seconds = day_time.seconds().count();
     wrk.append(std::to_string(seconds));
   }},
  {"%f",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto day_time = hh_mm_ss(tp - floor<days>(tp));
     uint64_t millis = day_time.subseconds().count();
     wrk.append(std::to_string(millis));
   }},
  {"%x",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto ymd = year_month_day(floor<days>(tp));
     auto yyyy = year{ymd.year()};
     // We construct the date with the first day in the
     // year:
     auto first_day_in_year = yyyy / January / day{0};
     uint64_t days_since_first =
       duration_cast<days>(tp - sys_days(first_day_in_year)).count();
     wrk.append(std::to_string(days_since_first));
   }},
  {"%k",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     iso_week::year_weeknum_weekday yww{floor<days>(tp)};
     uint64_t iso_week = static_cast<unsigned>(yww.weeknum());
     wrk.append(std::to_string(iso_week));
   }},
  {"%l",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     year_month_day ymd{floor<days>(tp)};
     if (ymd.year().is_leap()) {
       wrk.append("1");
     } else {
       wrk.append("0");
     }
   }},
  {"%q",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     year_month_day ymd{floor<days>(tp)};
     month m = ymd.month();
     uint64_t part = static_cast<uint64_t>(ceil(unsigned(m) / 3.0f));
     SDB_ASSERT(part <= 4);
     wrk.append(std::to_string(part));
   }},
  {"%a",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {
     auto ymd = year_month_day{floor<days>(tp)};
     auto last_month_day = ymd.year() / ymd.month() / last;
     wrk.append(std::to_string(static_cast<unsigned>(last_month_day.day())));
   }},
  {"%%",
   [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) { wrk.append("%"); }},
  {"%", [](std::string& wrk, const sdb::tp_sys_clock_ms& tp) {}},
};

// will be populated by DateRegexInitializer
std::regex gDateFormatRegex;

const std::regex kDurationRegex(
  "^P((\\d+)Y)?((\\d+)M)?((\\d+)W)?((\\d+)D)?(T((\\d+)H)?((\\d+)M)?((\\d+)(("
  "\\.|,)"
  "(\\d{1,3}))?S)?)?",
  std::regex::optimize);

struct DateRegexInitializer {
  DateRegexInitializer() {
    std::string myregex;

    gDateMap.reserve(kSortedDateMap.size());
    std::for_each(
      kSortedDateMap.begin(), kSortedDateMap.end(),
      [&myregex](const std::pair<const std::string&, FormatFuncT>& p) {
        (myregex.length() > 0) ? myregex += "|" + p.first : myregex = p.first;
        gDateMap.insert(std::make_pair(p.first, p.second));
      });
    gDateFormatRegex = std::regex(myregex);
  }
};

std::string ExecuteDateFormatRegex(std::string_view search,
                                   const sdb::tp_sys_clock_ms& tp) {
  std::string s;

  auto first = search.begin();
  auto last = search.end();
  typename std::smatch::difference_type position_of_last_match = 0;
  auto end_of_last_match = first;

  auto callback = [&tp, &end_of_last_match, &position_of_last_match,
                   &s](const auto& match) {
    auto position_of_this_match = match.position(0);
    auto diff = position_of_this_match - position_of_last_match;

    auto start_of_this_match = end_of_last_match;
    std::advance(start_of_this_match, diff);

    s.append(end_of_last_match, start_of_this_match);
    auto got = gDateMap.find(match.str(0));
    if (got != gDateMap.end()) {
      got->second(s, tp);
    }
    auto length_of_match = match.length(0);

    position_of_last_match = position_of_this_match + length_of_match;

    end_of_last_match = start_of_this_match;
    std::advance(end_of_last_match, length_of_match);
  };

  std::regex_iterator<std::string_view::const_iterator> end;
  std::regex_iterator<std::string_view::const_iterator> begin(first, last,
                                                              gDateFormatRegex);
  std::for_each(begin, end, callback);

  s.append(end_of_last_match, last);

  return s;
}

// populates dateFormatRegex
static const DateRegexInitializer kInitializer;

struct ParsedDateTime {
  int year = 0;
  int month = 1;
  int day = 1;
  int hour = 0;
  int minute = 0;
  int second = 0;
  int millisecond = 0;
  int tz_offset_hour = 0;
  int tz_offset_minute = 0;
};

/// parses a number value, and returns its length
int ParseNumber(std::string_view date_time, int& result) {
  const char* p = date_time.data();
  const char* e = p + date_time.size();

  while (p != e) {
    char c = *p;
    if (c < '0' || c > '9') {
      break;
    }
    ++p;
  }
  bool valid;
  result = sdb::number_utils::AtoiPositive<int>(date_time.data(), p, valid);
  return static_cast<int>(p - date_time.data());
}

bool ParseDateTime(std::string_view date_time, ParsedDateTime& result) {
  // trim input string
  while (!date_time.empty()) {
    char c = date_time.front();
    if (c != ' ' && c != '\t' && c != '\r' && c != '\n') {
      break;
    }
    date_time = date_time.substr(1);
  }

  if (!date_time.empty()) {
    if (date_time.front() == '+') {
      // skip over initial +
      date_time = date_time.substr(1);
    } else if (date_time.front() == '-') {
      // we can't handle negative date values at all
      return false;
    }
  }

  while (!date_time.empty()) {
    char c = date_time.back();
    if (c != ' ' && c != '\t' && c != '\r' && c != '\n') {
      break;
    }
    date_time.remove_suffix(1);
  }

  // year
  int length = ParseNumber(date_time, result.year);
  if (length == 0 || result.year > 9999) {
    return false;
  }
  if (length > 4) [[unlikely]] {
    // we must have at least 4 digits for the year, however, we
    // allow any amount of leading zeroes
    size_t i = 0;
    while (date_time[i] == '0') {
      ++i;
    }
    if (length - i > 4) {
      return false;
    }
  }
  date_time = date_time.substr(length);

  if (!date_time.empty() && date_time.front() == '-') {
    // month
    date_time = date_time.substr(1);

    length = ParseNumber(date_time, result.month);
    if (length == 0 || length > 2 || result.month < 1 || result.month > 12) {
      return false;
    }
    date_time = date_time.substr(length);

    if (!date_time.empty() && date_time.front() == '-') {
      // day
      date_time = date_time.substr(1);

      length = ParseNumber(date_time, result.day);
      if (length == 0 || length > 2 || result.day < 1 || result.day > 31) {
        return false;
      }
      date_time = date_time.substr(length);
    }
  }

  if (!date_time.empty() &&
      (date_time.front() == ' ' || date_time.front() == 'T')) {
    // time part following
    date_time = date_time.substr(1);

    // hour
    length = ParseNumber(date_time, result.hour);
    if (length == 0 || length > 2 || result.hour > 23) {
      return false;
    }
    date_time = date_time.substr(length);

    if (date_time.empty() || date_time.front() != ':') {
      return false;
    }

    date_time = date_time.substr(1);

    // minute
    length = ParseNumber(date_time, result.minute);
    if (length == 0 || length > 2 || result.minute > 59) {
      return false;
    }
    date_time = date_time.substr(length);

    if (!date_time.empty() && date_time.front() == ':') {
      date_time = date_time.substr(1);

      // second
      length = ParseNumber(date_time, result.second);
      if (length == 0 || length > 2 || result.second > 59) {
        return false;
      }
      date_time = date_time.substr(length);

      if (!date_time.empty() && date_time.front() == '.') {
        date_time = date_time.substr(1);

        // millisecond
        length = ParseNumber(date_time, result.millisecond);
        if (length == 0) {
          return false;
        }
        if (length >= 3) {
          // restrict milliseconds length to 3 digits
          ParseNumber(date_time.substr(0, 3), result.millisecond);
        } else if (length == 2) {
          result.millisecond *= 10;
        } else if (length == 1) {
          result.millisecond *= 100;
        }
        date_time = date_time.substr(length);
      }
    }
  }

  if (!date_time.empty()) {
    if (date_time.front() == 'z' || date_time.front() == 'Z') {
      // z|Z timezone
      date_time = date_time.substr(1);
    } else if (date_time.front() == '+' || date_time.front() == '-') {
      // +|- timezone adjustment
      int factor = date_time.front() == '+' ? 1 : -1;

      date_time = date_time.substr(1);

      // tz adjustment hours
      length = ParseNumber(date_time, result.tz_offset_hour);
      if (length == 0 || length > 2 || result.tz_offset_hour > 23) {
        return false;
      }
      result.tz_offset_hour *= factor;
      date_time = date_time.substr(length);

      if (date_time.empty() || date_time.front() != ':') {
        return false;
      }
      date_time = date_time.substr(1);

      // tz adjustment minutes
      length = ParseNumber(date_time, result.tz_offset_minute);
      if (length == 0 || length > 2 || result.tz_offset_minute > 59) {
        return false;
      }
      date_time = date_time.substr(length);
    }
  }

  return date_time.empty();
}

bool RegexIsoDuration(
  std::string_view iso_duration,
  std::match_results<std::string_view::iterator>& duration_parts) {
  if (iso_duration.length() <= 1) {
    return false;
  }

  return std::regex_match(iso_duration.begin(), iso_duration.end(),
                          duration_parts, kDurationRegex);
}

}  // namespace

bool ParseDateTime(std::string_view date_time, sdb::tp_sys_clock_ms& date_tp) {
  ParsedDateTime result;
  if (!ParseDateTime(date_time, result)) {
    return false;
  }

  date_tp = sys_days(year{result.year} / result.month / result.day);
  date_tp += hours{result.hour};
  date_tp += minutes{result.minute};
  date_tp += seconds{result.second};
  date_tp += milliseconds{result.millisecond};

  if (result.tz_offset_hour != 0 || result.tz_offset_minute != 0) {
    minutes offset = hours{result.tz_offset_hour};
    offset += minutes{result.tz_offset_minute};

    if (offset.count() != 0) {
      // apply timezone adjustment
      date_tp -= offset;

      // revalidate date after timezone adjustment
      auto ymd = year_month_day(floor<days>(date_tp));
      int year = static_cast<int>(ymd.year());
      if (year < 0 || year > 9999) {
        return false;
      }
    }
  }

  return true;
}

std::string FormatDate(std::string_view format_string,
                       const sdb::tp_sys_clock_ms& date_value) {
  return ExecuteDateFormatRegex(format_string, date_value);
}

bool ParseIsoDuration(std::string_view duration,
                      sdb::basics::ParsedDuration& ret) {
  using namespace sdb;

  std::match_results<std::string_view::iterator> duration_parts;
  if (!RegexIsoDuration(duration, duration_parts)) {
    return false;
  }

  const char* begin;

  begin = duration.data() + duration_parts.position(2);
  ret.years =
    number_utils::AtoiUnchecked<int>(begin, begin + duration_parts.length(2));

  begin = duration.data() + duration_parts.position(4);
  ret.months =
    number_utils::AtoiUnchecked<int>(begin, begin + duration_parts.length(4));

  begin = duration.data() + duration_parts.position(6);
  ret.weeks =
    number_utils::AtoiUnchecked<int>(begin, begin + duration_parts.length(6));

  begin = duration.data() + duration_parts.position(8);
  ret.days =
    number_utils::AtoiUnchecked<int>(begin, begin + duration_parts.length(8));

  begin = duration.data() + duration_parts.position(11);
  ret.hours =
    number_utils::AtoiUnchecked<int>(begin, begin + duration_parts.length(11));

  begin = duration.data() + duration_parts.position(13);
  ret.minutes =
    number_utils::AtoiUnchecked<int>(begin, begin + duration_parts.length(13));

  begin = duration.data() + duration_parts.position(15);
  ret.seconds =
    number_utils::AtoiUnchecked<int>(begin, begin + duration_parts.length(15));

  // The Milli seconds can be shortened:
  // .1 => 100ms
  // so we append 00 but only take the first 3 digits
  auto match_length = duration_parts.length(18);
  int number = 0;
  if (match_length > 0) {
    if (match_length > 3) {
      match_length = 3;
    }
    begin = duration.data() + duration_parts.position(18);
    number = number_utils::AtoiUnchecked<int>(begin, begin + match_length);
    if (match_length == 2) {
      number *= 10;
    } else if (match_length == 1) {
      number *= 100;
    }
  }
  ret.milliseconds = number;

  return true;
}

}  // namespace sdb::basics
