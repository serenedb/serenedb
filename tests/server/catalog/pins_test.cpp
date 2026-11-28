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

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <duckdb/common/file_system.hpp>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <vector>

#include "catalog/log.h"
#include "catalog/pins.h"

namespace sdb::catalog {
namespace {

using Segments = std::vector<uint64_t>;
using Files = std::vector<std::string>;

std::span<uint8_t> Whole(std::span<uint8_t> payload) { return payload; }

std::vector<std::vector<uint8_t>> Keep(
  std::span<const std::span<uint8_t>> parts) {
  std::vector<std::vector<uint8_t>> kept;
  for (const auto part : parts) {
    kept.emplace_back(part.begin(), part.end());
  }
  return kept;
}

TEST(PinsTest, ASegmentIsHeldWhileAPinListsIt) {
  Pins pins;
  pins.Add("a", Segments{1, 2});
  pins.Add("b", Segments{2, 3});
  EXPECT_TRUE(pins.Holds(1));
  EXPECT_TRUE(pins.Holds(2));
  EXPECT_TRUE(pins.Holds(3));
  EXPECT_FALSE(pins.Holds(4));
  EXPECT_TRUE(pins.Contains("a"));
  EXPECT_EQ(pins.Names(), (Files{"a", "b"}));

  EXPECT_TRUE(pins.Remove("a").empty());
  EXPECT_FALSE(pins.Holds(1));
  EXPECT_TRUE(pins.Holds(2));
  EXPECT_FALSE(pins.Contains("a"));
  EXPECT_EQ(pins.Names(), (Files{"b"}));
}

TEST(PinsTest, AddingAPinAgainReplacesItsSegments) {
  Pins pins;
  pins.Add("a", Segments{1});
  pins.Add("a", Segments{2});
  EXPECT_FALSE(pins.Holds(1));
  EXPECT_TRUE(pins.Holds(2));
}

TEST(PinsTest, ParkedFilesAreFreedWithTheLastPinThatHoldsThem) {
  Pins pins;
  pins.Add("a", Segments{1, 2});
  pins.Add("b", Segments{2});
  const Files one{"1.doc", "1.idx"};
  const Files two{"2.doc"};
  EXPECT_TRUE(pins.ParkIfHeld("/root", 1, one));
  EXPECT_TRUE(pins.ParkIfHeld("/root", 2, two));
  EXPECT_FALSE(pins.ParkIfHeld("/root", 3, Files{"3.doc"}));

  auto freed = pins.Remove("a");
  ASSERT_EQ(freed.size(), 1);
  EXPECT_EQ(freed[0].root, "/root");
  EXPECT_EQ(freed[0].files, one);

  freed = pins.Remove("b");
  ASSERT_EQ(freed.size(), 1);
  EXPECT_EQ(freed[0].files, two);
  EXPECT_TRUE(pins.Remove("b").empty());
}

TEST(PinsTest, LoadReadsEveryPinAndDropsTheUnfinishedOnes) {
  const auto root = std::filesystem::temp_directory_path() /
                    absl::StrCat("pins_test_", ::getpid());
  std::filesystem::remove_all(root);
  const auto backups = root / "backups";
  std::filesystem::create_directories(backups);
  const auto fs = duckdb::FileSystem::CreateLocal();
  {
    Log log{*fs, (root / "wal").string(), (root / "snapshot").string()};
    log.Replay([](std::span<uint8_t>) {}, &Whole);
    const Segments segments{5, 6};
    log.WriteSnapshot(0, segments, {}, &Whole, &Keep, false);
    log.Pin((backups / "nightly").string());
  }
  std::ofstream{backups / "weekly.tmp"} << "torn";

  Pins pins;
  pins.Load(*fs, (root / "missing").string());
  EXPECT_TRUE(pins.Names().empty());
  pins.Load(*fs, backups.string());
  EXPECT_EQ(pins.Names(), (Files{"nightly"}));
  EXPECT_TRUE(pins.Holds(5));
  EXPECT_TRUE(pins.Holds(6));
  EXPECT_FALSE(std::filesystem::exists(backups / "weekly.tmp"));
  std::filesystem::remove_all(root);
}

}  // namespace
}  // namespace sdb::catalog
