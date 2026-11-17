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

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/time/civil_time.h>
#include <gtest/gtest.h>

#include <duckdb/common/types/date.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <random>
#include <string>
#include <string_view>

#include "pg/datetime_text.h"

namespace {

TEST(PgDatetimeText, DaysToCivilMatchesAbsl) {
  for (int32_t days = sdb::pg::kMinFastDays; days <= sdb::pg::kMaxFastDays;
       ++days) {
    const auto expected = absl::CivilDay{1970, 1, 1} + days;
    const auto date = sdb::pg::DaysToCivil(days);
    ASSERT_EQ(date.year, expected.year()) << days;
    ASSERT_EQ(date.month, static_cast<uint32_t>(expected.month())) << days;
    ASSERT_EQ(date.day, static_cast<uint32_t>(expected.day())) << days;
  }
  EXPECT_EQ(sdb::pg::DaysToCivil(sdb::pg::kMinFastDays).year, 1);
  EXPECT_EQ(sdb::pg::DaysToCivil(sdb::pg::kMaxFastDays).year, 9999);
  EXPECT_EQ(sdb::pg::DaysToCivil(sdb::pg::kMaxFastDays + 1).year, 10000);
}

TEST(PgDatetimeText, CivilToDaysInvertsDaysToCivil) {
  for (int32_t days = sdb::pg::kMinFastDays; days <= sdb::pg::kMaxFastDays;
       ++days) {
    const auto date = sdb::pg::DaysToCivil(days);
    ASSERT_EQ(sdb::pg::CivilToDays(static_cast<uint32_t>(date.year), date.month,
                                   date.day),
              days);
  }
}

TEST(PgDatetimeText, ParseIsoDateMatchesDuckDB) {
  std::mt19937_64 rng{13};
  const char digits[] = "0123456789";
  for (int i = 0; i < 2'000'000; ++i) {
    std::string text = "0000-00-00";
    for (const size_t at : {0, 1, 2, 3, 5, 6, 8, 9}) {
      text[at] = digits[rng() % 10];
    }
    if (i % 3 == 0) {
      text[5] = '0' + static_cast<char>(rng() % 2);
      text[8] = '0' + static_cast<char>(rng() % 4);
    }
    if (i % 101 == 0) {
      text[rng() % text.size()] = "x- /"[rng() % 4];
    }
    int32_t days = 0;
    const bool fast = sdb::pg::ParseIsoDate(text, days);
    duckdb::date_t expected;
    duckdb::idx_t pos = 0;
    bool special = false;
    const bool slow = duckdb::Date::TryConvertDate(
                        text.data(), text.size(), pos, expected, special,
                        /*strict=*/true) == duckdb::DateCastResult::SUCCESS;
    const bool canonical =
      text[4] == '-' && text[7] == '-' &&
      absl::c_all_of(std::string_view{text}.substr(0, 4),
                     absl::ascii_isdigit) &&
      absl::ascii_isdigit(text[5]) && absl::ascii_isdigit(text[6]) &&
      absl::ascii_isdigit(text[8]) && absl::ascii_isdigit(text[9]);
    if (fast) {
      ASSERT_TRUE(canonical) << text;
      ASSERT_TRUE(slow) << text;
      ASSERT_EQ(days, expected.days) << text;
    } else if (canonical && !text.starts_with("0000")) {
      ASSERT_FALSE(slow) << text;
    }
  }
}

TEST(PgDatetimeText, FormatTimestampMatchesDuckDB) {
  std::mt19937_64 rng{11};
  const int64_t lo = int64_t{sdb::pg::kMinFastDays} * sdb::pg::kMicrosPerDay;
  const int64_t hi =
    (int64_t{sdb::pg::kMaxFastDays} + 1) * sdb::pg::kMicrosPerDay - 1;
  std::uniform_int_distribution<int64_t> any{lo, hi};
  char buf[sdb::pg::kMaxTimestampText];
  for (int i = 0; i < 1'000'000; ++i) {
    auto micros = any(rng);
    switch (i % 4) {
      case 1:
        micros -= micros % 1'000'000;
        break;
      case 2:
        micros -= micros % 1000;
        break;
      default:
        break;
    }
    const auto size = sdb::pg::FormatTimestamp(buf, micros);
    ASSERT_NE(size, 0) << micros;
    ASSERT_EQ(std::string_view(buf, size),
              duckdb::Timestamp::ToString(duckdb::timestamp_t{micros}))
      << micros;
  }
  EXPECT_EQ(sdb::pg::FormatTimestamp(buf, lo - 1), 0);
  EXPECT_EQ(sdb::pg::FormatTimestamp(buf, hi + 1), 0);
}

}  // namespace
