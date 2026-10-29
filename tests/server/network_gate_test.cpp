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

#include <atomic>
#include <thread>
#include <yaclib/coro/task.hpp>
#include <yaclib/exe/inline.hpp>

#include "network/gate.h"

using namespace sdb;

namespace {

yaclib::Task<> Pass(network::Gate& gate, std::atomic<int>& passed) {
  co_await gate.Wait(yaclib::MakeInline());
  passed.fetch_add(1, std::memory_order_relaxed);
  co_return {};
}

}  // namespace

TEST(NetworkGate, AKickRacingAPendingKickFreesTheWaiterSafely) {
  constexpr int kRounds = 100000;
  network::Gate gate;
  std::atomic<int> started{0};
  std::atomic<int> passed{0};
  std::thread kicker{[&] {
    for (int round = 1; round <= kRounds; ++round) {
      while (started.load(std::memory_order_acquire) < round) {
      }
      gate.Kick();
    }
  }};
  for (int round = 1; round <= kRounds; ++round) {
    gate.Kick();
    started.store(round, std::memory_order_release);
    Pass(gate, passed).Detach();
  }
  kicker.join();
  EXPECT_EQ(kRounds, passed.load(std::memory_order_relaxed));
}
