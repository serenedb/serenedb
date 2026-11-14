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
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "catalog/log.h"

namespace sdb::catalog {
namespace {

constexpr size_t kPayload = 100;

std::string Payload(char fill) { return std::string(kPayload, fill); }

std::string Half(char fill) { return std::string(Log::kChunkBytes / 2, fill); }

std::span<const uint8_t> Bytes(const std::string& payload) {
  return {reinterpret_cast<const uint8_t*>(payload.data()), payload.size()};
}

std::vector<std::vector<uint8_t>> Keep(
  std::span<const std::span<uint8_t>> parts) {
  std::vector<std::vector<uint8_t>> kept;
  for (const auto part : parts) {
    kept.emplace_back(part.begin(), part.end());
  }
  return kept;
}

class LogTest : public ::testing::Test {
 protected:
  void SetUp() final {
    _dir = std::filesystem::temp_directory_path() /
           absl::StrCat("log_test_", ::getpid());
    std::filesystem::remove_all(_dir);
  }

  void TearDown() final { std::filesystem::remove_all(_dir); }

  std::unique_ptr<Log> Open() {
    _records.clear();
    auto log = std::make_unique<Log>(*_fs, _dir);
    log->Replay(
      [&](std::span<uint8_t> payload) {
        _records.emplace_back(payload.begin(), payload.end());
      },
      [](std::span<uint8_t> payload) { return payload; });
    return log;
  }

  static void Snapshot(Log& log, Lsn covered) {
    log.WriteSnapshot(
      covered, {}, [](std::span<uint8_t> payload) { return payload; }, &Keep);
  }

  struct Written {
    Lsn begin = 0;
    Lsn end = 0;
  };

  static Written WriteUnsynced(Log& log, const std::string& payload) {
    const auto range = log.Reserve(payload.size());
    log.Write(range, Bytes(payload));
    return {.begin = range.begin, .end = range.end};
  }

  std::filesystem::path Chunk(uint64_t number) const {
    return _dir / Log::ChunkName(number);
  }

  void Overwrite(uint64_t number, uint64_t at, const std::string& bytes) const {
    std::fstream file{Chunk(number),
                      std::ios::in | std::ios::out | std::ios::binary};
    file.seekp(static_cast<std::streamoff>(at));
    file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  }

  uint64_t Find(uint64_t number, const std::string& bytes) const {
    std::ifstream file{Chunk(number), std::ios::binary};
    const std::string content{std::istreambuf_iterator<char>{file}, {}};
    return content.find(bytes);
  }

  std::string ReadBytes(uint64_t number, uint64_t at, uint64_t size) const {
    std::ifstream file{Chunk(number), std::ios::binary};
    file.seekg(static_cast<std::streamoff>(at));
    std::string bytes(size, '\0');
    file.read(bytes.data(), static_cast<std::streamsize>(size));
    return bytes;
  }

  std::unique_ptr<duckdb::FileSystem> _fs = duckdb::FileSystem::CreateLocal();
  std::filesystem::path _dir;
  std::vector<std::string> _records;
};

TEST_F(LogTest, ReplayFollowsTheChunks) {
  {
    auto log = Open();
    log->Append(Bytes(Half('A')));
    log->Append(Bytes(Half('B')));
    log->Append(Bytes(Half('C')));
  }
  EXPECT_TRUE(std::filesystem::exists(Chunk(2)));
  {
    auto log = Open();
    EXPECT_EQ(_records, (std::vector{Half('A'), Half('B'), Half('C')}));
    log->Append(Bytes(Payload('D')));
  }
  Open();
  EXPECT_EQ(_records,
            (std::vector{Half('A'), Half('B'), Half('C'), Payload('D')}));
  EXPECT_FALSE(std::filesystem::exists(Chunk(3)));
}

TEST_F(LogTest, ChunksAfterALostTailAreRemoved) {
  Written lost;
  {
    auto log = Open();
    log->Append(Bytes(Payload('A')));
    lost = WriteUnsynced(*log, Half('B'));
    WriteUnsynced(*log, Half('C'));
    WriteUnsynced(*log, Half('E'));
  }
  EXPECT_TRUE(std::filesystem::exists(Chunk(2)));
  Overwrite(0, lost.begin, std::string(lost.end - lost.begin, '\0'));
  {
    auto log = Open();
    EXPECT_EQ(_records, std::vector{Payload('A')});
    EXPECT_FALSE(std::filesystem::exists(Chunk(1)));
    EXPECT_FALSE(std::filesystem::exists(Chunk(2)));
    log->Append(Bytes(Payload('D')));
  }
  Open();
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('D')}));
}

TEST_F(LogTest, ARecordAfterATornOneIsNotRevived) {
  {
    auto log = Open();
    log->Append(Bytes(Payload('A')));
    WriteUnsynced(*log, Payload('B'));
    WriteUnsynced(*log, Payload('C'));
  }
  Overwrite(0, Find(0, Payload('B')), std::string(1, 'X'));
  {
    auto log = Open();
    EXPECT_EQ(_records, std::vector{Payload('A')});
    log->Append(Bytes(Payload('D')));
  }
  Open();
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('D')}));
}

TEST_F(LogTest, ChunksTheSnapshotCoversAreRemovedOnRestart) {
  {
    auto log = Open();
    const auto a = log->Append(Bytes(Half('A')));
    log->Append(Bytes(Half('B')));
    Snapshot(*log, a);
  }
  EXPECT_TRUE(std::filesystem::exists(Chunk(0)));
  Open();
  EXPECT_EQ(_records, (std::vector{Half('A'), Half('B')}));
  EXPECT_FALSE(std::filesystem::exists(Chunk(0)));
  EXPECT_TRUE(std::filesystem::exists(Chunk(1)));
}

TEST_F(LogTest, ASnapshotOfTheWholeLogKeepsTheCurrentChunk) {
  {
    auto log = Open();
    log->Append(Bytes(Half('A')));
    const auto b = log->Append(Bytes(Half('B')));
    Snapshot(*log, b);
    log->DropCovered();
  }
  EXPECT_FALSE(std::filesystem::exists(Chunk(0)));
  EXPECT_TRUE(std::filesystem::exists(Chunk(1)));
  {
    auto log = Open();
    EXPECT_EQ(_records, (std::vector{Half('A'), Half('B')}));
    log->Append(Bytes(Payload('C')));
  }
  Open();
  EXPECT_EQ(_records, (std::vector{Half('A'), Half('B'), Payload('C')}));
  EXPECT_FALSE(std::filesystem::exists(Chunk(2)));
}

TEST_F(LogTest, ARecordOverTheSizeLimitIsRefused) {
  {
    auto log = Open();
    log->Append(Bytes(Payload('A')));
    EXPECT_THROW(log->Reserve(size_t{1} << 32), irs::SqlException);
    log->Append(Bytes(Payload('B')));
  }
  Open();
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('B')}));
}

TEST_F(LogTest, ARecordCopiedToAnotherLsnIsNotReplayed) {
  Lsn a = 0;
  Lsn b = 0;
  {
    auto log = Open();
    a = log->Append(Bytes(Payload('A')));
    b = log->Append(Bytes(Payload('B')));
  }
  Overwrite(0, b, ReadBytes(0, a, b - a));
  Open();
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('B')}));
}

TEST_F(LogTest, ANewLogStartsWithoutFillingAChunk) {
  Lsn a = 0;
  {
    auto log = Open();
    EXPECT_EQ(std::filesystem::file_size(Chunk(0)), 0U);
    EXPECT_TRUE(log->ShouldPrepareNext(log->End()));
    a = log->Append(Bytes(Payload('A')));
  }
  EXPECT_EQ(std::filesystem::file_size(Chunk(0)), a);
  auto log = Open();
  EXPECT_EQ(_records, std::vector{Payload('A')});
  EXPECT_EQ(std::filesystem::file_size(Chunk(0)), a);
  EXPECT_TRUE(log->ShouldPrepareNext(a));
}

TEST_F(LogTest, APreparedChunkTakesOverFromOneThatIsNot) {
  {
    auto log = Open();
    const auto a = log->Append(Bytes(Payload('A')));
    log->PrepareNext();
    EXPECT_EQ(std::filesystem::file_size(Chunk(1)), Log::kChunkBytes);
    EXPECT_FALSE(log->ShouldPrepareNext(a));
    const auto b = log->Append(Bytes(Payload('B')));
    EXPECT_EQ(std::filesystem::file_size(Chunk(0)), a);
    EXPECT_NE(Find(1, Payload('B')), std::string::npos);
    EXPECT_FALSE(log->ShouldPrepareNext(b));
    const auto d = log->Append(Bytes(Half('D')));
    EXPECT_TRUE(log->ShouldPrepareNext(d));
    log->PrepareNext();
    EXPECT_FALSE(log->ShouldPrepareNext(d));
    log->Append(Bytes(Payload('C')));
  }
  EXPECT_NE(Find(1, Payload('C')), std::string::npos);
  Open();
  EXPECT_EQ(_records,
            (std::vector{Payload('A'), Payload('B'), Half('D'), Payload('C')}));
  EXPECT_FALSE(std::filesystem::exists(Chunk(2)));
}

}  // namespace
}  // namespace sdb::catalog
