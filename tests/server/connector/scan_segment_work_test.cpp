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

#include <algorithm>
#include <cstdint>
#include <vector>

#include "connector/scan/scan_state.h"
#include "gtest/gtest.h"

namespace sdb::connector {
namespace {

void CheckLive(uint32_t rg_count, const std::vector<uint32_t>& live) {
  SegmentWork work;
  work.rg_count = rg_count;
  work.SetLive(live);
  EXPECT_EQ(work.all_live, live.size() == rg_count);
  EXPECT_EQ(work.LiveCount(), live.size());
  for (uint32_t rg = 0; rg <= rg_count; ++rg) {
    const auto before = std::count_if(live.begin(), live.end(),
                                      [&](uint32_t l) { return l < rg; });
    EXPECT_EQ(work.LiveBefore(rg), before) << "row group " << rg;
  }
  for (uint32_t k = 0; k < live.size(); ++k) {
    EXPECT_EQ(work.LiveAt(k), live[k]) << "live row group " << k;
  }
}

TEST(ScanSegmentWork, AllLiveKeepsNothing) {
  std::vector<uint32_t> live(130);
  for (uint32_t rg = 0; rg < live.size(); ++rg) {
    live[rg] = rg;
  }
  CheckLive(130, live);
  SegmentWork work;
  work.rg_count = 130;
  work.SetLive(live);
  EXPECT_TRUE(work.all_live);
  EXPECT_TRUE(work.live_rgs.empty());
}

TEST(ScanSegmentWork, NoneLive) { CheckLive(130, {}); }

TEST(ScanSegmentWork, Sparse) {
  CheckLive(150, {0, 1, 63, 64, 100, 127, 128, 149});
}

TEST(ScanSegmentWork, LongRun) {
  std::vector<uint32_t> live;
  for (uint32_t rg = 50; rg < 250; ++rg) {
    live.push_back(rg);
  }
  CheckLive(300, live);
}

}  // namespace
}  // namespace sdb::connector
