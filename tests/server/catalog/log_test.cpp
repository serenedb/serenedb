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
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "catalog/log.h"

namespace sdb::catalog {
namespace {

constexpr size_t kPayload = 100;

std::string Payload(char fill) { return std::string(kPayload, fill); }

std::span<const uint8_t> Bytes(const std::string& payload) {
  return {reinterpret_cast<const uint8_t*>(payload.data()), payload.size()};
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
    const auto append = [&](std::span<uint8_t> payload) {
      _records.emplace_back(payload.begin(), payload.end());
    };
    auto log = std::make_unique<Log>(*_fs, _dir);
    log->Replay(
      [&](Lsn, std::span<uint8_t> payload) { append(payload); },
      [&](const Log::Range&, std::span<uint8_t> payload) { append(payload); });
    return log;
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

  uint64_t ChunkHeader(uint64_t number, Lsn begin, Lsn end) const {
    return std::filesystem::file_size(Chunk(number)) - (end - begin);
  }

  void Overwrite(uint64_t number, uint64_t at, const std::string& bytes) const {
    std::fstream file{Chunk(number),
                      std::ios::in | std::ios::out | std::ios::binary};
    file.seekp(static_cast<std::streamoff>(at));
    file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
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

TEST_F(LogTest, ReplayFollowsTheChunkChain) {
  {
    auto log = Open();
    log->Append(Bytes(Payload('A')));
    log->Switch();
    log->Append(Bytes(Payload('B')));
    log->Switch();
    log->Append(Bytes(Payload('C')));
  }
  {
    auto log = Open();
    EXPECT_EQ(_records,
              (std::vector{Payload('A'), Payload('B'), Payload('C')}));
    log->Append(Bytes(Payload('D')));
  }
  Open();
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('B'), Payload('C'),
                                   Payload('D')}));
  EXPECT_TRUE(std::filesystem::exists(Chunk(2)));
  EXPECT_FALSE(std::filesystem::exists(Chunk(3)));
}

TEST_F(LogTest, AChunkAfterALostTailIsRemoved) {
  Written lost;
  {
    auto log = Open();
    log->Append(Bytes(Payload('A')));
    lost = WriteUnsynced(*log, Payload('B'));
    log->Switch();
    WriteUnsynced(*log, Payload('C'));
  }
  const auto header =
    ChunkHeader(1, lost.end, lost.end + (lost.end - lost.begin));
  Overwrite(0, header + lost.begin, std::string(lost.end - lost.begin, '\0'));
  {
    auto log = Open();
    EXPECT_EQ(_records, std::vector{Payload('A')});
    EXPECT_FALSE(std::filesystem::exists(Chunk(1)));
    log->Append(Bytes(Payload('D')));
  }
  Open();
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('D')}));
}

TEST_F(LogTest, ADamagedChunkBeforeASyncedOneIsRefused) {
  Written damaged;
  Lsn end = 0;
  {
    auto log = Open();
    log->Append(Bytes(Payload('A')));
    damaged = WriteUnsynced(*log, Payload('B'));
    log->Switch();
    end = log->Append(Bytes(Payload('C')));
  }
  const auto header = ChunkHeader(1, damaged.end, end);
  Overwrite(0, header + damaged.begin, std::string(1, 'X'));
  try {
    Open();
    FAIL() << "a log with a damaged synced chunk was replayed";
  } catch (const irs::SqlException& e) {
    EXPECT_EQ(
      e.message(),
      absl::StrCat("log '", _dir.string(), "' is damaged: chunk 0 ends at LSN ",
                   damaged.begin, ", but chunk 1 starts at the synced LSN ",
                   damaged.end));
  }
}

TEST_F(LogTest, ChunksTheSnapshotCoversAreRemovedOnRestart) {
  {
    auto log = Open();
    const auto a = log->Append(Bytes(Payload('A')));
    log->Switch();
    log->Append(Bytes(Payload('B')));
    log->WriteSnapshot(
      a, {}, [](std::span<uint8_t> payload) { return payload; },
      [](std::vector<Log::HistoryEntry> entries) { return entries; });
  }
  EXPECT_TRUE(std::filesystem::exists(Chunk(0)));
  Open();
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('B')}));
  EXPECT_FALSE(std::filesystem::exists(Chunk(0)));
}

TEST_F(LogTest, ARecordCopiedToAnotherLsnIsNotReplayed) {
  Lsn end = 0;
  Lsn next = 0;
  {
    auto log = Open();
    end = log->Append(Bytes(Payload('A')));
    log->Switch();
    next = log->Append(Bytes(Payload('B')));
  }
  const auto header = ChunkHeader(1, end, next);
  {
    auto log = Open();
    EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('B')}));
  }
  Overwrite(1, header + (next - end), ReadBytes(1, header, next - end));
  Open();
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('B')}));
}

}  // namespace
}  // namespace sdb::catalog
