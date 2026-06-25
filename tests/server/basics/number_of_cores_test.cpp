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

#include <cstdint>
#include <thread>
#include <utility>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/number_of_cores.h"

using sdb::CountLogicalCores;
using sdb::CountPhysicalCores;

namespace {

TEST(NumberOfCores, NoLimits) { EXPECT_EQ(CountLogicalCores(8, 0, 0), 8); }

TEST(NumberOfCores, CfsQuotaClamps) {
  EXPECT_EQ(CountLogicalCores(8, 2, 0), 2);
  EXPECT_EQ(CountLogicalCores(8, 1, 0), 1);
}

TEST(NumberOfCores, CpusetClamps) { EXPECT_EQ(CountLogicalCores(8, 0, 4), 4); }

TEST(NumberOfCores, MostRestrictiveWins) {
  EXPECT_EQ(CountLogicalCores(8, 2, 4), 2);
  EXPECT_EQ(CountLogicalCores(8, 4, 2), 2);
}

TEST(NumberOfCores, LimitAboveOnlineIgnored) {
  EXPECT_EQ(CountLogicalCores(8, 16, 0), 8);  // quota bigger than the host
  EXPECT_EQ(CountLogicalCores(2, 0, 8), 2);   // cpuset bigger than the host
}

TEST(NumberOfCores, NeverZero) {
  EXPECT_EQ(CountLogicalCores(0, 0, 0), 1);
  EXPECT_EQ(CountLogicalCores(0, 0, 1), 1);
}

namespace {

// 4 physical cores (package 0, core 0-3), each with a 2nd HT thread: logical
// 0-3 are the first threads, 16-19 the siblings -- like a real /proc/cpuinfo.
const sdb::containers::FlatHashMap<int64_t, std::pair<int64_t, int64_t>>
  kHt4Cores = {
    {0, {0, 0}},  {1, {0, 1}},  {2, {0, 2}},  {3, {0, 3}},
    {16, {0, 0}}, {17, {0, 1}}, {18, {0, 2}}, {19, {0, 3}},
};

}  // namespace

TEST(PhysicalCoresTest, NoCpusetCountsAllPhysical) {
  EXPECT_EQ(CountPhysicalCores(kHt4Cores, {}, 0),
            4);  // 8 logical -> 4 physical
}

TEST(PhysicalCoresTest, CpusetSiblingPairIsOnePhysical) {
  EXPECT_EQ(CountPhysicalCores(kHt4Cores, {0, 16}, 0), 1);  // both -> (0,0)
  EXPECT_EQ(CountPhysicalCores(kHt4Cores, {0, 1, 16, 17}, 0),
            2);  // k8s full cores
  EXPECT_EQ(CountPhysicalCores(kHt4Cores, {0, 1, 2, 3}, 0),
            4);  // first threads
}

TEST(PhysicalCoresTest, QuotaIsAFractionOfPhysical) {
  // 8 logical / 4 physical: the quota is a fraction of the machine, applied to
  // the physical cores -- not "quota-cores == physical-cores".
  EXPECT_EQ(CountPhysicalCores(kHt4Cores, {}, 2),
            1);  // 2/8 = 25% -> ceil(4*.25)=1
  EXPECT_EQ(CountPhysicalCores(kHt4Cores, {}, 4), 2);  // 50% -> 2
  EXPECT_EQ(CountPhysicalCores(kHt4Cores, {}, 9),
            4);  // looser than the machine
}

TEST(PhysicalCoresTest, NoTopologyFallsBackToCount) {
  EXPECT_EQ(CountPhysicalCores({}, {0, 1, 2, 3}, 0),
            4);  // no map -> allowed count
  EXPECT_EQ(CountPhysicalCores(kHt4Cores, {99}, 0),
            1);  // cpu not in map -> >=1
}

TEST(PhysicalCoresTest, NeverZero) {
  EXPECT_EQ(CountPhysicalCores({}, {}, 0), 1);
}

TEST(PhysicalCoresTest, LiveIsSane) {
  const int64_t physical = sdb::CountPhysicalCores();
  EXPECT_GE(physical, 1);
  EXPECT_LE(physical, CountLogicalCores());  // physical never exceeds logical
  std::cerr << "[ INFO ] CountPhysicalCores() = " << physical
            << ", CountLogicalCores() = " << CountLogicalCores() << "\n";
}

TEST(NumberOfCores, GetValueIsSane) {
  const int64_t cores = CountLogicalCores();
  EXPECT_GE(cores, 1);
  // Must never exceed what the OS reports as available to the process.
  EXPECT_LE(cores,
            std::thread::hardware_concurrency() == 0
              ? cores
              : static_cast<int64_t>(std::thread::hardware_concurrency()));
  std::cerr << "[ INFO ] CountLogicalCores() = " << cores << "\n";
}

}  // namespace
