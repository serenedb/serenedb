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
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "catalog/store/wal.h"

namespace {

using sdb::catalog::CatalogWal;

std::vector<uint8_t> Frame(std::string_view text) {
  return {text.begin(), text.end()};
}

std::vector<std::string> Replay(CatalogWal& wal, const std::string& dir) {
  std::vector<std::string> frames;
  wal.Open(dir, [&](std::span<const uint8_t> payload) {
    frames.emplace_back(reinterpret_cast<const char*>(payload.data()),
                        payload.size());
  });
  return frames;
}

class CatalogWalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _dir = std::filesystem::path{::testing::TempDir()} /
           ("catalog_wal_" +
            std::string{
              ::testing::UnitTest::GetInstance()->current_test_info()->name()});
    std::filesystem::remove_all(_dir);
  }

  void TearDown() override { std::filesystem::remove_all(_dir); }

  std::string Dir() const { return _dir.string(); }
  std::filesystem::path WalPath() const { return _dir / "catalog.wal"; }

  std::filesystem::path _dir;
};

TEST_F(CatalogWalTest, round_trip) {
  {
    CatalogWal wal;
    ASSERT_TRUE(Replay(wal, Dir()).empty());
    wal.Append(Frame("one"));
    wal.Append(Frame("two"));
    wal.Append(Frame(""));
    wal.Append(Frame("four"));
    const auto stats = wal.GetStats();
    EXPECT_EQ(stats.frames, 4);
    EXPECT_EQ(stats.appended_bytes, 3 + 3 + 0 + 4);
    wal.Close();
  }
  CatalogWal wal;
  const auto frames = Replay(wal, Dir());
  ASSERT_EQ(frames.size(), 4);
  EXPECT_EQ(frames[0], "one");
  EXPECT_EQ(frames[1], "two");
  EXPECT_EQ(frames[2], "");
  EXPECT_EQ(frames[3], "four");
  wal.Close();
}

TEST_F(CatalogWalTest, torn_tail_truncated) {
  {
    CatalogWal wal;
    Replay(wal, Dir());
    wal.Append(Frame("keep1"));
    wal.Append(Frame("keep2"));
    wal.Close();
  }
  const auto full_size = std::filesystem::file_size(WalPath());
  {
    std::ofstream out{WalPath(), std::ios::binary | std::ios::app};
    const uint64_t size = 100;
    out.write(reinterpret_cast<const char*>(&size), sizeof(size));
    out.write("torn", 4);
  }
  ASSERT_GT(std::filesystem::file_size(WalPath()), full_size);
  {
    CatalogWal wal;
    const auto frames = Replay(wal, Dir());
    ASSERT_EQ(frames.size(), 2);
    EXPECT_EQ(frames[0], "keep1");
    EXPECT_EQ(frames[1], "keep2");
    EXPECT_EQ(std::filesystem::file_size(WalPath()), full_size);
    wal.Append(Frame("after"));
    wal.Close();
  }
  CatalogWal wal;
  const auto frames = Replay(wal, Dir());
  ASSERT_EQ(frames.size(), 3);
  EXPECT_EQ(frames[2], "after");
  wal.Close();
}

TEST_F(CatalogWalTest, corrupt_payload_truncated) {
  {
    CatalogWal wal;
    Replay(wal, Dir());
    wal.Append(Frame("keep"));
    wal.Append(Frame("corrupt-me"));
    wal.Close();
  }
  {
    std::fstream f{WalPath(),
                   std::ios::binary | std::ios::in | std::ios::out};
    f.seekp(-1, std::ios::end);
    f.put('X');
  }
  CatalogWal wal;
  const auto frames = Replay(wal, Dir());
  ASSERT_EQ(frames.size(), 1);
  EXPECT_EQ(frames[0], "keep");
  wal.Close();
}

TEST_F(CatalogWalTest, compaction_replaces_content) {
  {
    CatalogWal wal;
    Replay(wal, Dir());
    for (int i = 0; i < 100; ++i) {
      wal.Append(Frame("dead"));
    }
    wal.Compact([](CatalogWal::FrameSink sink) {
      const auto a = Frame("live1");
      const auto b = Frame("live2");
      sink({a.data(), a.size()});
      sink({b.data(), b.size()});
    });
    wal.Append(Frame("tail"));
    wal.Close();
  }
  EXPECT_FALSE(
    std::filesystem::exists(std::filesystem::path{Dir()} / "catalog.wal.tmp"));
  CatalogWal wal;
  const auto frames = Replay(wal, Dir());
  ASSERT_EQ(frames.size(), 3);
  EXPECT_EQ(frames[0], "live1");
  EXPECT_EQ(frames[1], "live2");
  EXPECT_EQ(frames[2], "tail");
  wal.Close();
}

TEST_F(CatalogWalTest, stale_tmp_removed_on_open) {
  {
    CatalogWal wal;
    Replay(wal, Dir());
    wal.Append(Frame("live"));
    wal.Close();
  }
  {
    std::ofstream out{std::filesystem::path{Dir()} / "catalog.wal.tmp",
                      std::ios::binary};
    out << "aborted compaction";
  }
  CatalogWal wal;
  const auto frames = Replay(wal, Dir());
  ASSERT_EQ(frames.size(), 1);
  EXPECT_EQ(frames[0], "live");
  EXPECT_FALSE(
    std::filesystem::exists(std::filesystem::path{Dir()} / "catalog.wal.tmp"));
  wal.Close();
}

TEST_F(CatalogWalTest, concurrent_appends_group_commit) {
  constexpr int kThreads = 8;
  constexpr int kPerThread = 200;
  {
    CatalogWal wal;
    Replay(wal, Dir());
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
      threads.emplace_back([&wal, t] {
        for (int i = 0; i < kPerThread; ++i) {
          const auto frame =
            Frame(std::to_string(t) + ":" + std::to_string(i));
          wal.Append({frame.data(), frame.size()});
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    const auto stats = wal.GetStats();
    EXPECT_EQ(stats.frames, kThreads * kPerThread);
    EXPECT_LE(stats.sync_batches, stats.frames);
    wal.Close();
  }
  CatalogWal wal;
  const auto frames = Replay(wal, Dir());
  EXPECT_EQ(frames.size(), kThreads * kPerThread);
  wal.Close();
}

}  // namespace
