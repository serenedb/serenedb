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

#include <cstddef>

#include "network/server.h"
#include "scheduler/background_scheduler.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {
namespace {

size_t HoldEveryBigSlot(MergeRunner& merges) {
  size_t held = 0;
  while (merges.TryHoldBig()) {
    ++held;
  }
  return held;
}

void ReleaseBigSlots(MergeRunner& merges, size_t held) {
  for (; held != 0; --held) {
    merges.ReleaseBig();
  }
}

TEST(SearchEngineTest, AManualCompactionHoldsABigSlotOnlyWhenOneIsFree) {
  Server server;
  BackgroundScheduler scheduler;
  scheduler.start();
  SearchEngine engine;
  engine.start();
  auto& merges = engine.Merges();
  const auto big = HoldEveryBigSlot(merges);
  EXPECT_LT(0, big);
  {
    const ManualCompactionSlot slot;
    EXPECT_TRUE(ManualCompactionSlot::AnnEnv());
  }
  EXPECT_FALSE(merges.TryHoldBig());
  ReleaseBigSlots(merges, big);
  {
    const ManualCompactionSlot slot;
    const auto rest = HoldEveryBigSlot(merges);
    EXPECT_EQ(big - 1, rest);
    ReleaseBigSlots(merges, rest);
  }
  EXPECT_EQ(big, HoldEveryBigSlot(merges));
  ReleaseBigSlots(merges, big);
  engine.stop();
  scheduler.stop();
}

}  // namespace
}  // namespace sdb::search
