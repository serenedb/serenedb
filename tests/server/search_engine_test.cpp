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

#include <iresearch/formats/ann_build_env.hpp>
#include <thread>

#include "runtime/runtime.h"
#include "search/engine.h"

namespace sdb::search {
namespace {

TEST(SearchEngineTest, ManualCompactionKeepsAnnWorkersWhenEverySlotIsBusy) {
  Runtime runtime{1};
  SearchEngine engine{runtime};
  int held = 0;
  while (engine.TryAcquireCompaction()) {
    ++held;
  }
  ASSERT_EQ(held, SearchEngine::MaxConcurrentCompactions());
  {
    const ManualCompactionSlot slot;
    EXPECT_TRUE(slot.AnnEnv());
  }
  for (; held != 0; --held) {
    engine.ReleaseCompaction();
  }
}

TEST(SearchEngineTest, SnapshotWorkersTakeOnlyFreeCompactionSlots) {
  Runtime runtime{1};
  SearchEngine engine{runtime};
  const auto& env = engine.SnapshotEnv();
  const auto slots =
    static_cast<uint32_t>(SearchEngine::MaxConcurrentCompactions());
  ASSERT_TRUE(engine.TryAcquireCompaction());

  const auto held = env.acquire(slots + 8, 0);
  EXPECT_EQ(held, slots);
  EXPECT_EQ(engine.FreeCompactionSlots(), 0);
  EXPECT_EQ(env.acquire(4, held), 0U);

  engine.ReleaseCompaction();
  EXPECT_EQ(env.acquire(4, held), 1U);
  EXPECT_EQ(engine.FreeCompactionSlots(), 0);

  env.release(held + 1);
  EXPECT_EQ(engine.FreeCompactionSlots(), static_cast<int>(slots));
}

TEST(SearchEngineTest, SnapshotRunsOnItsOwnThreadWhenEverySlotIsBusy) {
  Runtime runtime{1};
  SearchEngine engine{runtime};
  int busy = 0;
  while (engine.TryAcquireCompaction()) {
    ++busy;
  }
  const auto caller = std::this_thread::get_id();
  size_t tasks = 0;
  irs::FanOut(&engine.SnapshotEnv(), 16, [&](size_t) {
    EXPECT_EQ(std::this_thread::get_id(), caller);
    ++tasks;
  });
  EXPECT_EQ(tasks, 16U);
  EXPECT_EQ(engine.FreeCompactionSlots(), 0);
  for (; busy != 0; --busy) {
    engine.ReleaseCompaction();
  }
}

}  // namespace
}  // namespace sdb::search
