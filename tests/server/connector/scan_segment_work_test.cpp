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
#include <iresearch/utils/containers/bitset.hpp>
#include <vector>

#include "connector/scan/scan_state.h"
#include "gtest/gtest.h"

namespace sdb::connector {
namespace {

std::vector<LiveClaim> CheckClaims(uint32_t rg_count,
                                   const std::vector<uint32_t>& live,
                                   uint32_t threads) {
  irs::bitset bits{rg_count};
  for (const auto rg : live) {
    bits.set(rg);
  }
  const auto count = static_cast<uint32_t>(live.size());
  auto claims = CutClaims(bits, threads);
  EXPECT_GE(claims.size(), 2u);
  EXPECT_EQ(claims.front().rg, 0u);
  EXPECT_EQ(claims.front().live, 0u);
  EXPECT_EQ(claims.back().rg, rg_count);
  EXPECT_EQ(claims.back().live, count);
  uint32_t claimed = 0;
  for (size_t i = 1; i < claims.size(); ++i) {
    claimed += std::max<uint32_t>(1, (count - claimed) / threads);
    EXPECT_EQ(claims[i].live, std::min(claimed, count))
      << "claim " << i << " threads " << threads;
    EXPECT_LT(claims[i - 1].rg, claims[i].rg)
      << "claim " << i << " threads " << threads;
    if (i + 1 < claims.size() && claims[i].live < count) {
      EXPECT_EQ(claims[i].rg, live[claims[i].live])
        << "claim " << i << " threads " << threads;
    }
  }
  return claims;
}

TEST(ScanSegmentWork, AllLive) {
  std::vector<uint32_t> live(130);
  for (uint32_t rg = 0; rg < live.size(); ++rg) {
    live[rg] = rg;
  }
  for (const uint32_t threads : {1, 4, 16}) {
    const auto claims = CheckClaims(130, live, threads);
    for (const auto& claim : claims) {
      EXPECT_EQ(claim.rg, claim.live);
    }
  }
}

TEST(ScanSegmentWork, NoneLive) {
  for (const uint32_t threads : {1, 16}) {
    const auto claims = CheckClaims(130, {}, threads);
    EXPECT_EQ(claims.size(), 2u);
  }
}

TEST(ScanSegmentWork, Sparse) {
  for (const uint32_t threads : {1, 2, 16}) {
    CheckClaims(150, {0, 1, 63, 64, 100, 127, 128, 149}, threads);
  }
}

TEST(ScanSegmentWork, LongRun) {
  std::vector<uint32_t> live;
  for (uint32_t rg = 50; rg < 250; ++rg) {
    live.push_back(rg);
  }
  for (const uint32_t threads : {1, 3, 16}) {
    CheckClaims(300, live, threads);
  }
}

}  // namespace
}  // namespace sdb::connector
