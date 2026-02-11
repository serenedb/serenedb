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

#include <vpack/builder.h>
#include <vpack/value.h>

#include <cstdint>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "basics/common.h"
#include "basics/hybrid_logical_clock.h"
#include "gtest/gtest.h"

using namespace sdb;

namespace {

class HybridLogicalClockWithFixedTime final
  : public basics::HybridLogicalClock {
 public:
  explicit HybridLogicalClockWithFixedTime(uint64_t t) : _fixed(t) {}

  void GoBack() noexcept {
    // make time go backwards 🚀
    --_fixed;
  }

 protected:
  uint64_t getPhysicalTime() override {
    // arbitrary timestamp from Sep 30, 2022.
    return _fixed;
  }

 private:
  uint64_t _fixed;
};

}  // namespace

TEST(HybridLogicalClockTest, test_encode_decode_timestamp) {
  std::vector<std::pair<uint64_t, std::string_view>> values = {
    {0ULL, ""},
    {1ULL, "_"},
    {2ULL, "A"},
    {10ULL, "I"},
    {100ULL, "_i"},
    {100000ULL, "WYe"},
    {1000000ULL, "ByH-"},
    {10000000ULL, "kHY-"},
    {100000000ULL, "D7cC-"},
    {1000000000ULL, "5kqm-"},
    {10000000000ULL, "HSA8O-"},
    {100000000000ULL, "_bGbse-"},
    {1000000000000ULL, "MhSnP--"},
    {10000000000000ULL, "APfMao--"},
    {100000000000000ULL, "UtKOci--"},
    {1000000000000000ULL, "BhV4ivm--"},
    {10000000000000000ULL, "hftHtuO--"},
    {100000000000000000ULL, "DhPVfbge--"},
    {1000000000000000000ULL, "1erpMlX---"},
    {10000000000000000000ULL, "GpFGuQH4---"},
    {18446744073709551614ULL, "N9999999998"},
    {18446744073709551615ULL, "N9999999999"},
  };

  vpack::Builder b;

  for (const auto& value : values) {
    // encode
    std::string encoded =
      basics::HybridLogicalClock::encodeTimeStamp(value.first);
    ASSERT_EQ(value.second, encoded);

    // encode into a buffer using a std::string_view
    char buffer[11];
    b.clear();
    b.add(basics::HybridLogicalClock::encodeTimeStamp(value.first, buffer));
    ASSERT_EQ(value.first, basics::HybridLogicalClock::decodeTimeStamp(
                             b.slice().stringView()));

    // decode
    ASSERT_EQ(value.first,
              basics::HybridLogicalClock::decodeTimeStamp(encoded));

    // decode from vpack string
    b.clear();
    b.add(value.second);
    ASSERT_EQ(value.first, basics::HybridLogicalClock::decodeTimeStamp(
                             b.slice().stringView()));
  }
}

TEST(HybridLogicalClockTest, test_decode_invalid) {
  std::vector<std::pair<uint64_t, std::string_view>> values = {
    {0ULL, ""},
    {UINT64_MAX, " "},
    {51ULL, "x"},
    {869219571ULL, "xxxxx"},
    {UINT64_MAX, "xxxxxxxxxxxxxxxxxxxxxxxxxxxx"},
    {UINT64_MAX, "N9999999999"},
    {17813666640376327606ULL, "Na000000000"},
    {988218432520154550ULL, "O0000000000"},
  };

  for (const auto& value : values) {
    uint64_t decoded =
      basics::HybridLogicalClock::decodeTimeStamp(std::string(value.second));
    ASSERT_EQ(value.first, decoded);
  }
}

TEST(HybridLogicalClockTest, test_extract_time_and_count) {
  std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> values = {
    {0ULL, 0ULL, 0ULL},
    {1ULL, 0ULL, 1ULL},
    {2ULL, 0ULL, 2ULL},
    {10ULL, 0ULL, 10ULL},
    {100ULL, 0ULL, 100ULL},
    {100000ULL, 97ULL, 672ULL},
    {1000000ULL, 976ULL, 576ULL},
    {10000000ULL, 9765ULL, 640ULL},
    {100000000ULL, 97656ULL, 256ULL},
    {1000000000ULL, 976562ULL, 512ULL},
    {10000000000ULL, 9765625ULL, 0ULL},
    {100000000000ULL, 97656250ULL, 0ULL},
    {1000000000000ULL, 976562500ULL, 0ULL},
    {10000000000000ULL, 9765625000ULL, 0ULL},
    {100000000000000ULL, 97656250000ULL, 0ULL},
    {1000000000000000ULL, 976562500000ULL, 0ULL},
    {10000000000000000ULL, 9765625000000ULL, 0ULL},
    {100000000000000000ULL, 97656250000000ULL, 0ULL},
    {1000000000000000000ULL, 976562500000000ULL, 0ULL},
    {10000000000000000000ULL, 9765625000000000ULL, 0ULL},
    {18446744073709551614ULL, 18014398509481983ULL, 1022ULL},
    {18446744073709551615ULL, 18014398509481983ULL, 1023ULL},
  };

  for (auto [expected, expectedTime, expectedCount] : values) {
    auto actual_time = basics::HybridLogicalClock::extractTime(expected);
    auto actual_count = basics::HybridLogicalClock::extractCount(expected);
    auto actual =
      basics::HybridLogicalClock::assembleTimeStamp(actual_time, actual_count);
    // if (expectedTime != actualTime || expectedCount != actualCount) {
    //   std::cout << "{" << expected << "ULL, " << actualTime << "ULL, "
    //             << actualCount << "ULL}," << std::endl;
    //   continue;
    // }
    EXPECT_EQ(expected, actual);
    EXPECT_EQ(expectedTime, actual_time);
    EXPECT_EQ(expectedCount, actual_count);
  }
}

TEST(HybridLogicalClockTest, test_get_timestamp) {
  // arbitrary timestamp from Sep 30, 2022, that is supposed to
  // be in the past whenever this test runs.
  constexpr uint64_t kDateInThePast = 1664561862434ULL;

  basics::HybridLogicalClock hlc;

  uint64_t initial = hlc.getTimeStamp();

  for (size_t i = 0; i < 4'000'000; ++i) {
    uint64_t stamp = hlc.getTimeStamp();
    // every stamp generated must be >= than current time
    ASSERT_GT(stamp, kDateInThePast);
    // stamps must be increasing
    ASSERT_GT(stamp, initial);
    initial = stamp;
  }
}

TEST(HybridLogicalClockTest, test_values_increase_for_same_physical_time) {
  // arbitrary timestamp from Sep 30, 2022.
  // this timestamp is returned as the clock's physical time.
  // it is intentionally kept constant, so when the clock is
  // asked for its physical time multiple times, we intentionally
  // return the same timestamp. we still expect the HLC values
  // to be ever-increasing.
  ::HybridLogicalClockWithFixedTime hlc(1664561862434ULL);

  uint64_t initial = hlc.getTimeStamp();
  ASSERT_EQ(1664561862434ULL << 10ULL, initial);

  for (size_t i = 0; i < 10'000'000; ++i) {
    uint64_t stamp = hlc.getTimeStamp();
    // stamps must be ever-increasing
    ASSERT_GT(stamp, initial);
    initial = stamp;
  }
}

TEST(HybridLogicalClockTest,
     test_values_increase_when_two_clocks_play_ping_pong) {
  // arbitrary timestamp from Sep 30, 2022.
  // this timestamp is returned as the clock's physical time.
  // it is intentionally kept constant, so when the clock is
  // asked for its physical time multiple times, we intentionally
  // return the same timestamp. we still expect the HLC values
  // to be ever-increasing.
  // here we use two clocks, and we ask one clock for an HLC
  // value using the HLC value of the other, and vice versa.
  // the expected behavior is that we never get the same HLC
  // value ever, and the HLC values are ever-increasing.
  ::HybridLogicalClockWithFixedTime ping(1664561862434ULL);
  ::HybridLogicalClockWithFixedTime pong(1664561862434ULL);

  uint64_t initial_ping = ping.getTimeStamp();
  uint64_t initial_pong = pong.getTimeStamp();
  ASSERT_EQ(1664561862434ULL << 10ULL, initial_ping);
  ASSERT_EQ(1664561862434ULL << 10ULL, initial_pong);

  for (size_t i = 0; i < 10'000'000; ++i) {
    uint64_t stamp = ping.getTimeStamp(initial_pong);
    // stamps must be ever-increasing
    ASSERT_GT(stamp, initial_ping);
    ASSERT_GT(stamp, initial_pong);
    initial_ping = stamp;

    stamp = pong.getTimeStamp(initial_ping);
    ASSERT_GT(stamp, initial_ping);
    ASSERT_GT(stamp, initial_pong);
    initial_pong = stamp;
  }
}

TEST(
  HybridLogicalClockTest,
  test_values_increase_when_two_clocks_play_ping_pong_and_one_clock_is_far_behind) {
  // arbitrary timestamp values from 2022 and 2021.
  // these timestamps are returned as the clocks physical times.
  // one clock is significantly behind the other.
  // we still expect ever-increasing HLC values to come out of
  // both, even if one clock is severely behind.
  ::HybridLogicalClockWithFixedTime ping(1664561862434ULL);
  // pong has a lag of more than 1 year...
  ::HybridLogicalClockWithFixedTime pong(1640482451649ULL);

  uint64_t initial_ping = ping.getTimeStamp();
  uint64_t initial_pong = pong.getTimeStamp();
  ASSERT_EQ(1664561862434ULL << 10ULL, initial_ping);
  ASSERT_EQ(1640482451649ULL << 10ULL, initial_pong);

  for (size_t i = 0; i < 10'000'000; ++i) {
    uint64_t stamp = ping.getTimeStamp(initial_pong);
    // stamps must be ever-increasing
    ASSERT_GT(stamp, initial_ping);
    ASSERT_GT(stamp, initial_pong);
    initial_ping = stamp;

    stamp = pong.getTimeStamp(initial_ping);
    ASSERT_GT(stamp, initial_ping);
    ASSERT_GT(stamp, initial_pong);
    initial_pong = stamp;
  }
}

TEST(HybridLogicalClockTest,
     test_values_increase_even_if_physical_time_goes_backwards) {
  // arbitrary timestamp value from 2022.
  // these timestamps is returned as the clock's initial physical
  // time.
  // the clock is also faked in a way that whenever we ask it for
  // the physical time again, it returns a timestamp that is more
  // in the past than the previous one. in other words: this clock
  // intentionally goes backwards.
  // we still expect ever-increasing HLC values to come out of it.
  ::HybridLogicalClockWithFixedTime hlc(1664561862434ULL);

  uint64_t initial = hlc.getTimeStamp();
  ASSERT_EQ(1664561862434ULL << 10ULL, initial);

  for (size_t i = 0; i < 10'000'000; ++i) {
    uint64_t stamp = hlc.getTimeStamp();
    // stamps must be ever-increasing
    ASSERT_GT(stamp, initial);
    initial = stamp;

    hlc.GoBack();
  }
}
