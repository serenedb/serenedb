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

#include <array>
#include <cstdint>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/local_file_system.hpp>
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

RecordBuffer Bytes(const std::string& payload) {
  RecordBuffer record;
  record.WriteData(reinterpret_cast<const uint8_t*>(payload.data()),
                   payload.size());
  record.MarkCatalog(0, payload.size());
  return record;
}

std::vector<std::vector<uint8_t>> Keep(
  std::span<const std::span<uint8_t>> parts) {
  std::vector<std::vector<uint8_t>> kept;
  for (const auto part : parts) {
    kept.emplace_back(part.begin(), part.end());
  }
  return kept;
}

class FailingFileSystem final : public duckdb::LocalFileSystem {
 public:
  bool fail_reads = false;
  bool fail_writes = false;
  bool fail_removal = false;

  void Read(duckdb::FileHandle& handle, void* buffer, int64_t nr_bytes,
            duckdb::idx_t location) final {
    if (fail_reads) {
      throw duckdb::IOException("the disk refused the read");
    }
    LocalFileSystem::Read(handle, buffer, nr_bytes, location);
  }

  void Write(duckdb::FileHandle& handle, void* buffer, int64_t nr_bytes,
             duckdb::idx_t location) final {
    if (fail_writes) {
      throw duckdb::IOException("the disk refused the write");
    }
    LocalFileSystem::Write(handle, buffer, nr_bytes, location);
  }

  bool TryRemoveFile(const std::string& filename,
                     duckdb::optional_ptr<duckdb::FileOpener> opener) final {
    if (fail_removal) {
      throw duckdb::IOException("the disk refused the removal");
    }
    return LocalFileSystem::TryRemoveFile(filename, opener);
  }
};

class LogTest : public ::testing::Test {
 protected:
  void SetUp() final {
    _root = std::filesystem::temp_directory_path() /
            absl::StrCat("log_test_", ::getpid());
    _dir = _root / "wal";
    std::filesystem::remove_all(_root);
    std::filesystem::create_directories(_root);
  }

  void TearDown() final { std::filesystem::remove_all(_root); }

  std::filesystem::path SnapshotPath() const { return _root / "snapshot"; }

  std::unique_ptr<Log> Open() {
    _records.clear();
    auto log =
      std::make_unique<Log>(*_fs, _dir.string(), SnapshotPath().string());
    _replayed = log->Replay(
      [&](std::span<uint8_t> payload) {
        _records.emplace_back(payload.begin(), payload.end());
      },
      [](std::span<uint8_t> payload) { return payload; });
    return log;
  }

  static void Snapshot(Log& log, Lsn covered, bool clean = false) {
    log.WriteSnapshot(
      covered, {}, [](std::span<uint8_t> payload) { return payload; }, &Keep,
      clean);
  }

  struct Written {
    Lsn begin = 0;
    Lsn end = 0;
  };

  static Written WriteUnsynced(Log& log, const std::string& payload) {
    const auto range = log.Reserve(payload.size());
    auto record = Bytes(payload);
    log.Write(range, record);
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

  static void Damage(const std::filesystem::path& file,
                     const std::string& bytes) {
    std::ofstream{file, std::ios::binary | std::ios::trunc} << bytes;
  }

  std::unique_ptr<FailingFileSystem> _fs =
    std::make_unique<FailingFileSystem>();
  std::filesystem::path _root;
  std::filesystem::path _dir;
  std::vector<std::string> _records;
  Log::Replayed _replayed;
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

TEST_F(LogTest, ASnapshotReplacesThePreviousOne) {
  {
    auto log = Open();
    const auto a = log->Append(Bytes(Payload('A')));
    Snapshot(*log, a);
    const auto first = std::filesystem::file_size(SnapshotPath());
    const auto b = log->Append(Bytes(Payload('B')));
    Snapshot(*log, b);
    EXPECT_GT(std::filesystem::file_size(SnapshotPath()), first);
    log->Append(Bytes(Payload('C')));
  }
  EXPECT_EQ(std::distance(std::filesystem::directory_iterator{_root},
                          std::filesystem::directory_iterator{}),
            2);
  Open();
  EXPECT_TRUE(_replayed.snapshot);
  EXPECT_FALSE(_replayed.clean);
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('B'), Payload('C')}));
}

TEST_F(LogTest, AnInterruptedReplaceKeepsThePreviousSnapshot) {
  {
    auto log = Open();
    const auto a = log->Append(Bytes(Payload('A')));
    Snapshot(*log, a);
    log->Append(Bytes(Payload('B')));
  }
  const auto replacing = _root / "snapshot.tmp";
  std::ofstream{replacing} << "cut short by a crash";
  Open();
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('B')}));
  EXPECT_FALSE(std::filesystem::exists(replacing));
  EXPECT_TRUE(std::filesystem::exists(SnapshotPath()));
}

TEST_F(LogTest, ASnapshotWithoutItsLogIsRefused) {
  {
    auto log = Open();
    const auto a = log->Append(Bytes(Payload('A')));
    Snapshot(*log, a);
  }
  std::filesystem::remove_all(_dir);
  EXPECT_THROW(Open(), irs::SqlException);
}

TEST_F(LogTest, ASnapshotAtTheStartOfItsChunkWithoutItsLogIsRefused) {
  {
    auto log = Open();
    Snapshot(*log, 0);
    log->Append(Bytes(Payload('A')));
  }
  std::filesystem::remove_all(_dir);
  EXPECT_THROW(Open(), irs::SqlException);
}

TEST_F(LogTest, ADamagedSnapshotIsRefused) {
  {
    auto log = Open();
    const auto a = log->Append(Bytes(Payload('A')));
    Snapshot(*log, a);
  }
  Damage(SnapshotPath(), "cut short");
  EXPECT_THROW(Open(), irs::SqlException);
}

TEST_F(LogTest, ASnapshotOfAnotherVersionIsRefused) {
  struct Header {
    uint64_t version;
    Lsn covered;
    uint64_t number;
    Lsn begin;
    uint64_t clean;
    uint64_t system_identifier;
    std::array<uint8_t, 32> mock_nonce;
  };
  const Header header{.version = uint64_t{1} << 40};
  const std::string payload{reinterpret_cast<const char*>(&header),
                            sizeof(header)};
  {
    Open()->Append(Bytes(payload));
  }
  Damage(SnapshotPath(),
         ReadBytes(0, 0, RecordBuffer::kFrameBytes + payload.size()));
  try {
    Open();
    ADD_FAILURE() << "the snapshot was read";
  } catch (const irs::SqlException& e) {
    EXPECT_NE(std::string_view{e.what()}.find("storage version"),
              std::string_view::npos)
      << e.what();
  }
}

TEST_F(LogTest, ASnapshotKeepsTheIdentityOfItsServer) {
  const Log::Identity identity{.system_identifier = 42, .mock_nonce = {7}};
  {
    auto log = Open();
    EXPECT_FALSE(_replayed.identity);
    log->SetIdentity(identity);
    Snapshot(*log, log->Append(Bytes(Payload('A'))));
  }
  Open();
  EXPECT_EQ(_replayed.identity, identity);
}

TEST_F(LogTest, ACleanSnapshotStartsWithoutTheLog) {
  {
    auto log = Open();
    log->Append(Bytes(Half('A')));
    const auto b = log->Append(Bytes(Half('B')));
    Snapshot(*log, b, true);
    log->Clear();
  }
  EXPECT_TRUE(std::filesystem::is_empty(_dir));
  {
    auto log = Open();
    EXPECT_TRUE(_replayed.clean);
    EXPECT_EQ(_records, (std::vector{Half('A'), Half('B')}));
    log->Append(Bytes(Payload('C')));
  }
  Open();
  EXPECT_FALSE(_replayed.clean);
  EXPECT_EQ(_records, (std::vector{Half('A'), Half('B'), Payload('C')}));
}

TEST_F(LogTest, ADiscardedLogContinuesAfterTheSnapshot) {
  {
    auto log = Open();
    const auto a = log->Append(Bytes(Half('A')));
    Snapshot(*log, a);
    log->Append(Bytes(Half('B')));
    log->Append(Bytes(Half('C')));
  }
  EXPECT_TRUE(std::filesystem::exists(Chunk(1)));
  Log::Discard(*_fs, _dir.string(), SnapshotPath().string());
  EXPECT_FALSE(std::filesystem::exists(Chunk(1)));
  {
    auto log = Open();
    EXPECT_FALSE(_replayed.clean);
    EXPECT_EQ(_records, std::vector{Half('A')});
    log->Append(Bytes(Payload('D')));
  }
  Open();
  EXPECT_EQ(_records, (std::vector{Half('A'), Payload('D')}));
}

TEST_F(LogTest, NoLogIsDiscardedWithoutASnapshot) {
  {
    Open()->Append(Bytes(Payload('A')));
  }
  EXPECT_THROW(Log::Discard(*_fs, _dir.string(), SnapshotPath().string()),
               irs::SqlException);
  Open();
  EXPECT_EQ(_records, std::vector{Payload('A')});
}

TEST_F(LogTest, ANextChunkThatCannotBeWrittenIsNotUsed) {
  {
    auto log = Open();
    log->Append(Bytes(Payload('A')));
    _fs->fail_writes = true;
    _fs->fail_removal = true;
    EXPECT_THROW(log->PrepareNext(), duckdb::IOException);
    EXPECT_FALSE(log->HasNext());
    _fs->fail_writes = false;
    _fs->fail_removal = false;
    log->Append(Bytes(Payload('B')));
  }
  Open();
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('B')}));
}

TEST_F(LogTest, APinKeepsTheSnapshotItCopied) {
  const auto pin = (_root / "pin").string();
  {
    auto log = Open();
    const auto a = log->Append(Bytes(Payload('A')));
    Snapshot(*log, a);
    log->Pin(pin);
    const auto b = log->Append(Bytes(Payload('B')));
    Snapshot(*log, b);
  }
  Log::Copy(*_fs, pin, SnapshotPath().string());
  Log::Discard(*_fs, _dir.string(), SnapshotPath().string());
  Open();
  EXPECT_EQ(_records, std::vector{Payload('A')});
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

TEST_F(LogTest, ASnapshotReadsNoLogFile) {
  {
    auto log = Open();
    log->Append(Bytes(Payload('A')));
    const auto end = log->Append(Bytes(Payload('B')));
    _fs->fail_reads = true;
    Snapshot(*log, end);
    _fs->fail_reads = false;
    EXPECT_EQ(log->TailBytes(), 0);
  }
  Open();
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('B')}));
}

TEST_F(LogTest, ARestartKeepsTheCatalogPartsPastTheSnapshot) {
  Lsn end = 0;
  {
    auto log = Open();
    log->Append(Bytes(Payload('A')));
    end = log->Append(Bytes(Payload('B')));
  }
  {
    auto log = Open();
    EXPECT_EQ(log->TailBytes(), 2 * kPayload);
    _fs->fail_reads = true;
    Snapshot(*log, end);
    _fs->fail_reads = false;
  }
  Open();
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('B')}));
}

TEST_F(LogTest, AFoldedTailReachesTheNextSnapshot) {
  {
    auto log = Open();
    log->Append(Bytes(Payload('A')));
    const auto middle = log->Append(Bytes(Payload('B')));
    const auto end = log->Append(Bytes(Payload('C')));
    log->FoldTail(
      middle, [](std::span<uint8_t> payload) { return payload; }, &Keep);
    EXPECT_EQ(log->TailBytes(), kPayload);
    EXPECT_EQ(log->HistoryBytes(), 2 * kPayload);
    Snapshot(*log, end);
    EXPECT_EQ(log->TailBytes(), 0);
  }
  Open();
  EXPECT_EQ(_records, (std::vector{Payload('A'), Payload('B'), Payload('C')}));
}

TEST_F(LogTest, AFoldAfterASnapshotTookTheTailCompactsNothing) {
  auto log = Open();
  const auto end = log->Append(Bytes(Payload('A')));
  Snapshot(*log, end);
  size_t compactions = 0;
  const auto compact = [&](std::span<const std::span<uint8_t>> parts) {
    ++compactions;
    return Keep(parts);
  };
  log->FoldTail(
    end, [](std::span<uint8_t> payload) { return payload; }, compact);
  EXPECT_EQ(compactions, 0);
  EXPECT_EQ(log->TailBytes(), 0);
  EXPECT_EQ(log->HistoryBytes(), kPayload);
}

TEST_F(LogTest, ARecordWithoutACatalogPartIsNotKept) {
  auto log = Open();
  const auto payload = Payload('A');
  RecordBuffer record;
  record.WriteData(reinterpret_cast<const uint8_t*>(payload.data()),
                   payload.size());
  log->Append(std::move(record));
  EXPECT_EQ(log->TailBytes(), 0);
}

TEST(RecordBufferTest, CopyCatalogSpansThePiecesOfTheBuffer) {
  std::string payload(size_t{4} << 20, '\0');
  for (size_t i = 0; i != payload.size(); ++i) {
    payload[i] = static_cast<char>(i * 7);
  }
  RecordBuffer record;
  record.WriteData(reinterpret_cast<const uint8_t*>(payload.data()),
                   payload.size());
  const size_t begin = 3;
  const size_t size = payload.size() - 10;
  record.MarkCatalog(begin, size);
  const auto copy = record.CopyCatalog();
  EXPECT_EQ(std::string(copy.begin(), copy.end()), payload.substr(begin, size));
  size_t pieces = 0;
  record.ForEachPayload([&](std::span<const uint8_t>) { ++pieces; });
  EXPECT_GT(pieces, 1);
}

}  // namespace
}  // namespace sdb::catalog
