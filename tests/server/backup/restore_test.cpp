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
#include <filesystem>
#include <fstream>
#include <iresearch/utils/system_compiler.hpp>
#include <iterator>
#include <memory>
#include <string>

#include "backup/backup_label.h"
#include "backup/pg_control.h"
#include "backup/restore.h"
#include "catalog/boot.h"
#include "catalog/log.h"

namespace sdb::backup {
namespace {

constexpr uint64_t kSystemIdentifier = 4242;
constexpr uint64_t kStartLsn = 1000;
constexpr duckdb::idx_t kDatabase = 5;
constexpr duckdb::idx_t kStore = 77;
constexpr std::string_view kLogLink = "log";

class RestoreTest : public ::testing::Test {
 protected:
  void SetUp() final {
    _root = std::filesystem::temp_directory_path() /
            absl::StrCat("restore_", ::getpid());
    std::filesystem::remove_all(_root);
    std::filesystem::create_directory(_root);
    _layout = std::make_unique<catalog::DataDirectory>(_root.string());
    WritePgControl(_root, {.system_identifier = kSystemIdentifier});
    std::filesystem::create_directories(Link());
    WriteFile(Link() / catalog::Log::ChunkName(0), 200, 'L');
    WriteFile(Link() / catalog::Log::ChunkName(200), 100, 'M');
    std::filesystem::create_directories(_layout->LogDir());
    WriteFile(Chunk(0), 500, 'X');
    std::filesystem::create_directories(StoreDir());
    for (const auto* name : {"segments_1", "segments_2", "segments_3",
                             "pending_segments_4", "write.lock", "_1.data"}) {
      WriteFile(StoreDir() / name, 4, 'S');
    }
    WriteFile(_root / "recovery.signal", 0, ' ');
    WriteFile(_root / "standby.signal", 0, ' ');
    _cut.log_link =
      std::filesystem::path{"engine_v1"} / kBackupDir / std::string{kLogLink};
    _cut.log_size = 250;
    _cut.stores.push_back(
      {.database_oid = kDatabase, .oid = kStore, .generation = 2});
  }

  void TearDown() final { std::filesystem::remove_all(_root); }

  static void WriteFile(const std::filesystem::path& path, size_t size,
                        char fill) {
    std::ofstream out{path, std::ios::binary | std::ios::trunc};
    out << std::string(size, fill);
  }

  static std::string Content(const std::filesystem::path& path) {
    std::ifstream in{path, std::ios::binary};
    return std::string(std::istreambuf_iterator<char>{in}, {});
  }

  std::filesystem::path Link() const {
    return _layout->EngineDir() / kBackupDir / std::string{kLogLink};
  }

  std::filesystem::path Chunk(catalog::Lsn begin) const {
    return _layout->LogDir() / catalog::Log::ChunkName(begin);
  }

  std::filesystem::path StoreDir() const {
    return _layout->StorageDir(kDatabase, kStore);
  }

  void WriteLabel(uint64_t system_identifier = kSystemIdentifier) const {
    std::ofstream out{_root / kBackupLabelFile, std::ios::trunc};
    out << BuildBackupLabel("test", kStartLsn, system_identifier, absl::Now(),
                            _cut);
  }

  void Prepare() const { PrepareDataDirectory(*_layout); }

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
    PrepareDataDirectory(catalog::DataDirectory{reference.string()});

    if (!arm()) {
      GTEST_SKIP() << "this build has no fault injection";
    }
    EXPECT_EXIT(Prepare(),
                ::testing::ExitedWithCode(irs::kPlannedCrashExitCode), "");
    duckdb::FaultPoints::Clear();
    EXPECT_TRUE(Exists(_root / kBackupLabelFile));

    Prepare();
    EXPECT_EQ(Tree(_root), Tree(reference));
    const auto control = ReadPgControl(_root);
    ASSERT_TRUE(control);
    EXPECT_EQ(control->checkpoint, kStartLsn);
  }

  std::filesystem::path _root;
  std::unique_ptr<catalog::DataDirectory> _layout;
  Cut _cut;
};

TEST_F(RestoreTest, AppliesEveryFixupAndRetiresTheLabel) {
  WriteLabel();
  Prepare();

  EXPECT_EQ(Content(Chunk(0)), std::string(200, 'L'));
  EXPECT_EQ(Content(Chunk(200)), std::string(50, 'M'));
  EXPECT_FALSE(Exists(_layout->EngineDir() / kBackupDir));

  EXPECT_TRUE(Exists(StoreDir() / "segments_1"));
  EXPECT_TRUE(Exists(StoreDir() / "segments_2"));
  EXPECT_TRUE(Exists(StoreDir() / "_1.data"));
  EXPECT_FALSE(Exists(StoreDir() / "segments_3"));
  EXPECT_FALSE(Exists(StoreDir() / "pending_segments_4"));
  EXPECT_FALSE(Exists(StoreDir() / "write.lock"));

  EXPECT_FALSE(Exists(_root / "recovery.signal"));
  EXPECT_FALSE(Exists(_root / "standby.signal"));
  EXPECT_TRUE(Exists(_layout->EngineDir() / "subscriptions.hold"));
  const auto control = ReadPgControl(_root);
  ASSERT_TRUE(control);
  EXPECT_EQ(control->system_identifier, kSystemIdentifier);
  EXPECT_EQ(control->checkpoint, kStartLsn);
  EXPECT_FALSE(Exists(_root / kBackupLabelFile));
  EXPECT_TRUE(Exists(_root / "backup_label.old"));
  EXPECT_TRUE(Exists(_root / kPgVersionFile));
}

TEST_F(RestoreTest, AChunkPastTheCutIsRemoved) {
  _cut.log_size = 150;
  WriteLabel();
  Prepare();
  EXPECT_EQ(Content(Chunk(0)), std::string(150, 'L'));
  EXPECT_FALSE(Exists(Chunk(200)));
}

TEST_F(RestoreTest, ARestartedRestoreFinishes) {
  std::filesystem::remove(Chunk(0));
  std::filesystem::rename(Link() / catalog::Log::ChunkName(0), Chunk(0));
  std::filesystem::rename(Link() / catalog::Log::ChunkName(200), Chunk(200));
  std::filesystem::remove(Link());
  WriteLabel();
  Prepare();
  EXPECT_EQ(Content(Chunk(0)), std::string(200, 'L'));
  EXPECT_EQ(Content(Chunk(200)), std::string(50, 'M'));
  EXPECT_TRUE(Exists(_root / "backup_label.old"));
}

TEST_F(RestoreTest, ACrashAfterTheLogIsFinishedByTheNextBoot) {
  ExpectACrashedRestoreFinishes(
    [] { return duckdb::FaultPoints::Arm("crash_in_restore_after_log"); });
}

TEST_F(RestoreTest, ACrashAfterTheStoresIsFinishedByTheNextBoot) {
  ExpectACrashedRestoreFinishes(
    [] { return duckdb::FaultPoints::Arm("crash_in_restore_after_stores"); });
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

TEST_F(RestoreTest, WithoutALabelOnlyTheLeftoverBackupDirectoryGoes) {
  std::filesystem::remove(_root / kPgControlFile);
  Prepare();
  EXPECT_FALSE(Exists(_layout->EngineDir() / kBackupDir));
  EXPECT_EQ(Content(Chunk(0)), std::string(500, 'X'));
  EXPECT_TRUE(Exists(StoreDir() / "pending_segments_4"));
  EXPECT_TRUE(Exists(_root / "recovery.signal"));
  EXPECT_TRUE(ReadPgControl(_root));
  EXPECT_TRUE(Exists(_root / kPgVersionFile));
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

TEST_F(RestoreTest, RefusesALogShorterThanTheCut) {
  WriteLabel();
  std::filesystem::resize_file(Link() / catalog::Log::ChunkName(200), 20);
  ExpectRefused("it has 20 bytes, the backup recorded 50");
}

TEST_F(RestoreTest, RefusesALogWithAShortChunkBeforeTheCut) {
  WriteLabel();
  std::filesystem::resize_file(Link() / catalog::Log::ChunkName(0), 100);
  ExpectRefused("it has 100 bytes, the log needs 200");
}

}  // namespace
}  // namespace sdb::backup
