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

#include "network/server.h"
#include "scheduler/background_scheduler.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {
namespace {

TEST(SearchEngineTest, ManualCompactionKeepsAnnWorkersWhenEverySlotIsBusy) {
  Server server;
  BackgroundScheduler scheduler;
  scheduler.start();
  SearchEngine engine;
  engine.start();
  size_t held = 0;
  while (engine.TryAcquireCompactions(1) == 1) {
    ++held;
  }
  EXPECT_LT(size_t{0}, held);
  {
    const ManualCompactionSlot slot;
    EXPECT_TRUE(slot.AnnEnv());
  }
  engine.ReleaseCompactions(held);
  engine.stop();
  scheduler.stop();
}

}  // namespace
}  // namespace sdb::search
