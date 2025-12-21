////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
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

#include "gtest/gtest.h"

using namespace sdb;
using namespace sdb::basics;

TEST(DateTimeTest, testing) {
  using namespace std::chrono;

  tp_sys_clock_ms tp;

  std::vector<std::string> dates{
    "2017",
    "2017-11",
    "2017-11-12",
  };
  std::vector<std::string> times{
    "",
    "T12:34",
    "T12:34+10:22",
    "T12:34-10:22",

    "T12:34:56",
    "T12:34:56+10:22",
    "T12:34:56-10:22",

    "T12:34:56.789",
    "T12:34:56.789+10:22",
    "T12:34:56.789-10:22",
  };

  std::vector<std::string> dates_to_test;

  for (const auto& d : dates) {
    for (const auto& t : times) {
      dates_to_test.push_back(d + t);
    }
  }

  std::vector<std::string> dates_to_fail{
    "2017-01-01-12",
    "2017-01-01:12:34",
    "2017-01-01:12:34Z+10:20",
    "2017-01-01:12:34Z-10:20",
  };

  for (const auto& date_time : dates_to_test) {
    bool ret = ParseDateTime(date_time, tp);
    ASSERT_TRUE(ret);
  }

  for (const auto& date_time : dates_to_fail) {
    bool ret = ParseDateTime(date_time, tp);
    ASSERT_FALSE(ret);
  }
}
