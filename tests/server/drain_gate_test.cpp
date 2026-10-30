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

#include <absl/synchronization/notification.h>
#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <utility>

#include "utils/drain_gate.h"

namespace sdb::utils {
namespace {

constexpr auto kJoin = std::chrono::seconds(10);

TEST(DrainGateTest, CloseWaitsForPassesInFlight) {
  DrainGate gate;
  auto pass = gate.Enter();
  ASSERT_TRUE(pass);

  absl::Notification polled;
  auto close = std::async(std::launch::async, [&] {
    return gate.Close([&] {
      if (!polled.HasBeenNotified()) {
        polled.Notify();
      }
      return false;
    });
  });
  polled.WaitForNotification();
  EXPECT_TRUE(gate.IsClosed());
  EXPECT_FALSE(gate.TryEnter());

  pass.Release();
  ASSERT_EQ(close.wait_for(kJoin), std::future_status::ready);
  EXPECT_TRUE(close.get());
  gate.Open();
}

TEST(DrainGateTest, EnterWaitsWhileClosed) {
  DrainGate gate;
  ASSERT_TRUE(gate.Close([] { return true; }));

  auto enter = std::async(std::launch::async, [&] { return gate.Enter(); });
  EXPECT_NE(enter.wait_for(std::chrono::milliseconds(50)),
            std::future_status::ready);

  gate.Open();
  ASSERT_EQ(enter.wait_for(kJoin), std::future_status::ready);
  EXPECT_TRUE(enter.get());
}

TEST(DrainGateTest, CancelledCloseReopens) {
  DrainGate gate;
  auto pass = gate.TryEnter();
  ASSERT_TRUE(pass);

  int polls = 0;
  EXPECT_FALSE(gate.Close([&] { return ++polls == 2; }));
  EXPECT_EQ(polls, 2);
  EXPECT_FALSE(gate.IsClosed());
  EXPECT_TRUE(gate.TryEnter());
}

TEST(DrainGateTest, WaitOpenReturnsOnOpenAndStopsOnCancel) {
  DrainGate gate;
  int polls = 0;
  EXPECT_TRUE(gate.WaitOpen([&] {
    ++polls;
    return true;
  }));
  EXPECT_EQ(polls, 0);

  ASSERT_TRUE(gate.Close([] { return true; }));
  EXPECT_FALSE(gate.WaitOpen([] { return true; }));

  auto wait = std::async(std::launch::async,
                         [&] { return gate.WaitOpen([] { return false; }); });
  gate.Open();
  ASSERT_EQ(wait.wait_for(kJoin), std::future_status::ready);
  EXPECT_TRUE(wait.get());
}

TEST(DrainGateTest, MovedPassLeavesOnce) {
  DrainGate gate;
  auto first = gate.Enter();
  auto second = std::move(first);
  EXPECT_FALSE(first);
  ASSERT_TRUE(second);
  first.Release();
  second.Release();
  second.Release();

  int polls = 0;
  EXPECT_TRUE(gate.Close([&] {
    ++polls;
    return true;
  }));
  EXPECT_EQ(polls, 0);
  gate.Open();
}

}  // namespace
}  // namespace sdb::utils
