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

#include <absl/cleanup/cleanup.h>
#include <absl/container/btree_map.h>
#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <cstdlib>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/file_system.hpp>
#include <filesystem>
#include <fstream>
#include <iresearch/utils/system_compiler.hpp>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "backup/backup_label.h"
#include "backup/backup_session.h"
#include "backup/pg_control.h"
#include "backup/restore.h"
#include "catalog/boot.h"
#include "catalog/log.h"

namespace sdb::backup {
namespace {

constexpr uint64_t kSystemIdentifier = 4242;
constexpr uint64_t kStartLsn = 1000;
constexpr std::string_view kSegmentFile = "0000000000000001.doc";
constexpr size_t kPayload = 100;

std::string Payload(char fill) { return std::string(kPayload, fill); }

std::string Half(char fill) {
  return std::string(catalog::Log::kChunkBytes / 2, fill);
}

catalog::RecordBuffer Bytes(const std::string& payload) {
  catalog::RecordBuffer record;
  record.WriteData(reinterpret_cast<const uint8_t*>(payload.data()),
                   payload.size());
  record.MarkCatalog(0, payload.size());
  return record;
}

std::span<uint8_t> Whole(std::span<uint8_t> payload) { return payload; }

std::vector<std::vector<uint8_t>> Keep(
  std::span<const std::span<uint8_t>> parts) {
  std::vector<std::vector<uint8_t>> kept;
  for (const auto part : parts) {
    kept.emplace_back(part.begin(), part.end());
  }
  return kept;
}

catalog::ServerDirectory Layout(const std::filesystem::path& root) {
  return {root.string(), catalog::ServerDirectory::DefaultLog(root.string())};
}

class RestoreTest : public ::testing::Test {
 protected:
  void SetUp() final {
    _root = std::filesystem::temp_directory_path() /
            absl::StrCat("restore_", ::getpid());
    std::filesystem::remove_all(_root);
    std::filesystem::create_directory(_root);
    _layout = std::make_unique<catalog::ServerDirectory>(Layout(_root));
    WritePgControl(_root.string(), {.system_identifier = kSystemIdentifier});
    std::filesystem::create_directories(_layout->BackupsDir());
    _cut.snapshot = absl::StrCat(kSessionPinPrefix, kStartLsn);
    {
      const auto fs = duckdb::FileSystem::CreateLocal();
      catalog::Log log{*fs, _layout->LogDir(), _layout->SnapshotPath()};
      log.Replay([](std::span<uint8_t>) {}, &Whole);
      log.Append(Bytes(Payload('A')));
      const auto b = log.Append(Bytes(Half('B')));
      log.WriteSnapshot(b, {}, &Whole, &Keep, false);
      log.Pin(Pin().string());
      log.Append(Bytes(Half('C')));
      const auto d = log.Append(Bytes(Payload('D')));
      log.WriteSnapshot(d, {}, &Whole, &Keep, false);
      log.Append(Bytes(Payload('E')));
    }
    std::filesystem::create_directories(StoreDir());
    WriteFile(StoreDir() / kSegmentFile, 4, 'S');
    WriteFile(_root / "recovery.signal", 0, ' ');
    WriteFile(_root / "standby.signal", 0, ' ');
  }

  void TearDown() final { std::filesystem::remove_all(_root); }

  std::vector<std::string> Records() const {
    std::vector<std::string> records;
    const auto fs = duckdb::FileSystem::CreateLocal();
    catalog::Log::Read(*fs, _layout->LogDir(), _layout->SnapshotPath(),
                       [&](std::span<uint8_t> payload) {
                         records.emplace_back(payload.begin(), payload.end());
                       });
    return records;
  }

  static void WriteFile(const std::filesystem::path& path, size_t size,
                        char fill) {
    std::ofstream out{path, std::ios::binary | std::ios::trunc};
    out << std::string(size, fill);
  }

  std::filesystem::path Pin() const {
    return std::filesystem::path{_layout->BackupsDir()} / _cut.snapshot;
  }

  std::filesystem::path Chunk(uint64_t number) const {
    return std::filesystem::path{_layout->LogDir()} /
           catalog::Log::ChunkName(number);
  }

  std::filesystem::path StoreDir() const { return _layout->StoreDir(); }

  void WriteLabel(uint64_t system_identifier = kSystemIdentifier) const {
    std::ofstream out{_root / kBackupLabelFile, std::ios::trunc};
    out << BuildBackupLabel("test", kStartLsn, system_identifier, absl::Now(),
                            _cut);
  }

  void Prepare() const { PrepareServerDirectory(*_layout); }

  void ExpectRefused(std::string_view message) const {
    EXPECT_EXIT(
      {
        dup2(STDERR_FILENO, STDOUT_FILENO);
        Prepare();
      },
      ::testing::ExitedWithCode(EXIT_FAILURE), std::string{message});
  }

  bool Exists(const std::filesystem::path& path) const {
    return std::filesystem::exists(path);
  }

  static absl::btree_map<std::string, uintmax_t> Tree(
    const std::filesystem::path& root) {
    absl::btree_map<std::string, uintmax_t> tree;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator{root}) {
      tree[entry.path().lexically_relative(root).string()] =
        entry.is_regular_file() ? entry.file_size() : 0;
    }
    return tree;
  }

  void ExpectACrashedRestoreFinishes(bool (*arm)()) {
    WriteLabel();
    const auto reference =
      std::filesystem::path{absl::StrCat(_root.string(), "_reference")};
    std::filesystem::remove_all(reference);
    std::filesystem::copy(_root, reference,
                          std::filesystem::copy_options::recursive);
    absl::Cleanup drop = [&] { std::filesystem::remove_all(reference); };
    PrepareServerDirectory(Layout(reference));

    if (!arm()) {
      GTEST_SKIP() << "this build has no fault injection";
    }
    EXPECT_EXIT(Prepare(),
                ::testing::ExitedWithCode(irs::kPlannedCrashExitCode), "");
    duckdb::FaultPoints::Clear();
    EXPECT_TRUE(Exists(_root / kBackupLabelFile));

    Prepare();
    EXPECT_EQ(Tree(_root), Tree(reference));
    EXPECT_EQ(Records(), (std::vector{Payload('A'), Half('B')}));
    const auto control = ReadPgControl(_root.string());
    ASSERT_TRUE(control);
    EXPECT_EQ(control->checkpoint, kStartLsn);
  }

  std::filesystem::path _root;
  std::unique_ptr<catalog::ServerDirectory> _layout;
  Cut _cut;
};

TEST_F(RestoreTest, AppliesEveryFixupAndRetiresTheLabel) {
  WriteLabel();
  Prepare();

  EXPECT_EQ(Records(), (std::vector{Payload('A'), Half('B')}));
  EXPECT_FALSE(Exists(Pin()));
  EXPECT_TRUE(Exists(StoreDir() / kSegmentFile));

  EXPECT_FALSE(Exists(_root / "recovery.signal"));
  EXPECT_FALSE(Exists(_root / "standby.signal"));
  EXPECT_TRUE(Exists(_root / "subscriptions.hold"));
  const auto control = ReadPgControl(_root.string());
  ASSERT_TRUE(control);
  EXPECT_EQ(control->system_identifier, kSystemIdentifier);
  EXPECT_EQ(control->checkpoint, kStartLsn);
  EXPECT_FALSE(Exists(_root / kBackupLabelFile));
  EXPECT_TRUE(Exists(_root / "backup_label.old"));
}

TEST_F(RestoreTest, TheCommitsAfterTheBackupStartAreDiscarded) {
  EXPECT_TRUE(Exists(Chunk(1)));
  WriteLabel();
  Prepare();
  EXPECT_FALSE(Exists(Chunk(1)));
  EXPECT_EQ(std::filesystem::file_size(Chunk(0)), catalog::Log::kChunkBytes);
  EXPECT_EQ(Records(), (std::vector{Payload('A'), Half('B')}));
}

TEST_F(RestoreTest, ACrashAfterTheLogIsFinishedByTheNextBoot) {
  ExpectACrashedRestoreFinishes(
    [] { return duckdb::FaultPoints::Arm("crash_in_restore_after_log"); });
}

TEST_F(RestoreTest, ACrashAfterTheCleanupIsFinishedByTheNextBoot) {
  ExpectACrashedRestoreFinishes(
    [] { return duckdb::FaultPoints::Arm("crash_in_restore_after_cleanup"); });
}

TEST_F(RestoreTest, ACrashAfterTheControlFileIsFinishedByTheNextBoot) {
  ExpectACrashedRestoreFinishes(
    [] { return duckdb::FaultPoints::Arm("crash_in_restore_after_control"); });
}

TEST_F(RestoreTest, ACrashBeforeTheLabelIsRetiredIsFinishedByTheNextBoot) {
  ExpectACrashedRestoreFinishes(
    [] { return duckdb::FaultPoints::Arm("crash_in_restore_before_retire"); });
}

TEST_F(RestoreTest, WithoutALabelOnlyTheSessionPinsGo) {
  const auto named = std::filesystem::path{_layout->BackupsDir()} / "named";
  WriteFile(named, 4, 'N');
  std::filesystem::remove(_root / kPgControlFile);
  Prepare();
  EXPECT_FALSE(Exists(Pin()));
  EXPECT_TRUE(Exists(named));
  EXPECT_TRUE(Exists(Chunk(1)));
  EXPECT_EQ(Records(), (std::vector{Payload('A'), Half('B'), Half('C'),
                                    Payload('D'), Payload('E')}));
  EXPECT_TRUE(Exists(StoreDir() / kSegmentFile));
  EXPECT_TRUE(Exists(_root / "recovery.signal"));
  EXPECT_TRUE(ReadPgControl(_root.string()));
}

TEST_F(RestoreTest, RefusesATamperedLabel) {
  WriteLabel();
  {
    std::fstream label{_root / kBackupLabelFile,
                       std::ios::in | std::ios::out | std::ios::binary};
    label.seekp(0);
    label.put('X');
  }
  ExpectRefused("backup_label checksum does not match");
}

TEST_F(RestoreTest, RefusesALabelOfAnotherCluster) {
  WriteLabel(kSystemIdentifier + 1);
  ExpectRefused("the backup belongs to another cluster");
}

TEST_F(RestoreTest, RefusesACopyWithoutAControlFile) {
  WriteLabel();
  std::filesystem::remove(_root / kPgControlFile);
  ExpectRefused("the copy has no control file");
}

TEST_F(RestoreTest, RefusesAMissingStoreDirectory) {
  WriteLabel();
  std::filesystem::remove_all(StoreDir());
  ExpectRefused("the backup names a missing directory");
}

TEST_F(RestoreTest, RefusesACopyWithoutTheSnapshotOfTheBackup) {
  WriteLabel();
  std::filesystem::remove(Pin());
  ExpectRefused("the copy has no snapshot of the backup");
}

}  // namespace
}  // namespace sdb::backup
