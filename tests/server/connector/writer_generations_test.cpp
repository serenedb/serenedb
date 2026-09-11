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

#include <gtest/gtest.h>

#include <absl/synchronization/notification.h>
#include <absl/time/time.h>

#include <chrono>
#include <future>

#include "search/writer_generations.h"

namespace sdb::search {
namespace {

constexpr absl::Duration kPoll = absl::Milliseconds(10);
constexpr auto kJoin = std::chrono::seconds(10);

TEST(WriterGenerationsTest, DrainWaitsForPriorWritersOnly) {
  WriterGenerations gens;
  const auto prior = gens.Register();

  absl::Notification polled;
  auto drain = std::async(std::launch::async, [&] {
    return gens.Drain(
      [&] {
        if (!polled.HasBeenNotified()) {
          polled.Notify();
        }
        return false;
      },
      kPoll);
  });
  polled.WaitForNotification();

  const auto later = gens.Register();
  gens.Deregister(prior);

  const bool returned = drain.wait_for(kJoin) == std::future_status::ready;
  EXPECT_TRUE(returned);
  gens.Deregister(later);
  EXPECT_TRUE(drain.get());
}

TEST(WriterGenerationsTest, TheNextDrainWaitsForWritersLeftByACancelledOne) {
  WriterGenerations gens;
  const auto before = gens.Register();

  absl::Notification polled;
  auto first = std::async(std::launch::async, [&] {
    int polls = 0;
    return gens.Drain(
      [&] {
        if (++polls == 1) {
          polled.Notify();
          return false;
        }
        return true;
      },
      kPoll);
  });
  polled.WaitForNotification();
  const auto straddler = gens.Register();
  EXPECT_FALSE(first.get());
  gens.Deregister(before);

  int polls = 0;
  EXPECT_FALSE(gens.Drain(
    [&] {
      ++polls;
      return true;
    },
    kPoll));
  EXPECT_EQ(polls, 1);

  gens.Deregister(straddler);
  EXPECT_TRUE(gens.Drain([] { return true; }, kPoll));
}

TEST(WriterGenerationsTest, DrainWithNoWritersReturnsWithoutPolling) {
  WriterGenerations gens;
  int polls = 0;
  EXPECT_TRUE(gens.Drain(
    [&] {
      ++polls;
      return true;
    },
    kPoll));
  EXPECT_EQ(polls, 0);
}

}  // namespace
}  // namespace sdb::search
