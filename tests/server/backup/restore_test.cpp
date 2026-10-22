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
#include <filesystem>
#include <fstream>
#include <iresearch/utils/debugging.hpp>
#include <iterator>
#include <memory>
#include <string>

#include "backup/backup_label.h"
#include "backup/pg_control.h"
#include "backup/restore.h"
#include "catalog/boot.h"

namespace sdb::backup {
namespace {

constexpr uint64_t kSystemIdentifier = 4242;
constexpr uint64_t kStartLsn = 1000;
constexpr duckdb::idx_t kDatabase = 5;
constexpr duckdb::idx_t kStore = 77;
constexpr uint64_t kCheckpoint = 3;

class RestoreTest : public ::testing::Test {
 protected:
  void SetUp() final {
    _root = std::filesystem::temp_directory_path() /
            absl::StrCat("restore_", ::getpid());
    std::filesystem::remove_all(_root);
    std::filesystem::create_directory(_root);
    _layout = std::make_unique<catalog::DataDirectory>(_root.string());
    WritePgControl(_root, {.system_identifier = kSystemIdentifier});
    std::filesystem::create_directories(_layout->EngineDir() / kBackupDir);
    WriteFile(_layout->EngineDir() / kBackupDir / kCatalogLink, 300, 'L');
    WriteFile(_layout->CatalogLogFile(), 500, 'X');
    std::filesystem::create_directories(StoreDir());
    WriteFile(Wal(), 200, 'W');
    WriteFile(Wal(".checkpoint"), 80, 'C');
    WriteFile(Wal(".recovery"), 10, 'R');
    WriteFile(Wal(".retained.1"), 10, '1');
    WriteFile(Wal(".retained.2"), 10, '2');
    for (const auto* name : {"segments_1", "segments_2", "segments_3",
                             "pending_segments_4", "write.lock", "_1.data"}) {
      WriteFile(StoreDir() / name, 4, 'S');
    }
    WriteFile(_root / "recovery.signal", 0, ' ');
    WriteFile(_root / "standby.signal", 0, ' ');
    _cut.catalog_link = std::filesystem::path{"engine_v1"} / kBackupDir /
                        std::string{kCatalogLink};
    _cut.catalog_log_size = 250;
    _cut.databases.push_back({.oid = kDatabase,
                              .checkpoint_iteration = kCheckpoint,
                              .wal_size = 120,
                              .retained_from = 1});
    _cut.stores.push_back(
      {.database_oid = kDatabase, .oid = kStore, .generation = 2});
  }

  void TearDown() final { std::filesystem::remove_all(_root); }

  static void WriteFile(const std::filesystem::path& path, size_t size,
                        char fill) {
    std::ofstream out{path, std::ios::binary | std::ios::trunc};
    out << std::string(size, fill);
  }

  std::filesystem::path Wal(std::string_view suffix = "") const {
    return absl::StrCat(_layout->DatabaseFile(kDatabase), ".wal", suffix);
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
    EXPECT_EXIT(Prepare(), ::testing::ExitedWithCode(1), "");
    irs::ClearFailurePointsDebugging();
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

  EXPECT_EQ(std::filesystem::file_size(_layout->CatalogLogFile()), 250);
  {
    std::ifstream in{_layout->CatalogLogFile(), std::ios::binary};
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>{in}, {}),
              std::string(250, 'L'));
  }
  EXPECT_FALSE(Exists(_layout->EngineDir() / kBackupDir));

  EXPECT_EQ(std::filesystem::file_size(Wal()), 120);
  EXPECT_FALSE(Exists(Wal(".checkpoint")));
  EXPECT_FALSE(Exists(Wal(".recovery")));
  EXPECT_TRUE(Exists(Wal(".retained.1")));
  EXPECT_TRUE(Exists(Wal(".retained.2")));

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

TEST_F(RestoreTest, AnEmptyCutWalIsRemoved) {
  _cut.databases[0].wal_size = 0;
  WriteLabel();
  Prepare();
  EXPECT_FALSE(Exists(Wal()));
}

TEST_F(RestoreTest, AFailedCheckpointKeepsTheMainWalAndCutsTheCheckpointWal) {
  _cut.databases[0].with_failed_checkpoint = true;
  _cut.databases[0].wal_size = 50;
  WriteLabel();
  Prepare();
  EXPECT_EQ(std::filesystem::file_size(Wal()), 200);
  EXPECT_EQ(std::filesystem::file_size(Wal(".checkpoint")), 50);
  EXPECT_FALSE(Exists(Wal(".recovery")));
  EXPECT_TRUE(Exists(StoreDir() / "pending_segments_4"));
  EXPECT_FALSE(Exists(StoreDir() / "segments_3"));
  EXPECT_FALSE(Exists(StoreDir() / "write.lock"));
}

TEST_F(RestoreTest, ARestartedRestoreFinishes) {
  std::filesystem::rename(_layout->EngineDir() / kBackupDir / kCatalogLink,
                          _layout->CatalogLogFile());
  std::filesystem::resize_file(Wal(), 120);
  WriteLabel();
  Prepare();
  EXPECT_EQ(std::filesystem::file_size(_layout->CatalogLogFile()), 250);
  EXPECT_EQ(std::filesystem::file_size(Wal()), 120);
  EXPECT_TRUE(Exists(_root / "backup_label.old"));
}

TEST_F(RestoreTest, ACrashAfterTheCatalogLogIsFinishedByTheNextBoot) {
  ExpectACrashedRestoreFinishes([] {
    return irs::AddFailurePointDebugging("crash_in_restore_after_catalog_log");
  });
}

TEST_F(RestoreTest, ACrashAfterTheDatabaseWalsIsFinishedByTheNextBoot) {
  ExpectACrashedRestoreFinishes([] {
    return irs::AddFailurePointDebugging(
      "crash_in_restore_after_database_wals");
  });
}

TEST_F(RestoreTest, ACrashAfterTheStoresIsFinishedByTheNextBoot) {
  ExpectACrashedRestoreFinishes([] {
    return irs::AddFailurePointDebugging("crash_in_restore_after_stores");
  });
}

TEST_F(RestoreTest, ACrashAfterTheCleanupIsFinishedByTheNextBoot) {
  ExpectACrashedRestoreFinishes([] {
    return irs::AddFailurePointDebugging("crash_in_restore_after_cleanup");
  });
}

TEST_F(RestoreTest, ACrashAfterTheControlFileIsFinishedByTheNextBoot) {
  ExpectACrashedRestoreFinishes([] {
    return irs::AddFailurePointDebugging("crash_in_restore_after_control");
  });
}

TEST_F(RestoreTest, ACrashBeforeTheLabelIsRetiredIsFinishedByTheNextBoot) {
  ExpectACrashedRestoreFinishes([] {
    return irs::AddFailurePointDebugging("crash_in_restore_before_retire");
  });
}

TEST_F(RestoreTest, WithoutALabelOnlyTheLeftoverBackupDirectoryGoes) {
  std::filesystem::remove(_root / kPgControlFile);
  Prepare();
  EXPECT_FALSE(Exists(_layout->EngineDir() / kBackupDir));
  EXPECT_EQ(std::filesystem::file_size(_layout->CatalogLogFile()), 500);
  EXPECT_EQ(std::filesystem::file_size(Wal()), 200);
  EXPECT_TRUE(Exists(Wal(".checkpoint")));
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

TEST_F(RestoreTest, RefusesAMissingRetainedWal) {
  WriteLabel();
  std::filesystem::remove(Wal(".retained.2"));
  ExpectRefused("the backup needs the retained WALs 1 to 2");
}

TEST_F(RestoreTest, RefusesAWalShorterThanTheCut) {
  WriteLabel();
  std::filesystem::resize_file(Wal(), 100);
  ExpectRefused("it has 100 bytes, the backup recorded 120");
}

}  // namespace
}  // namespace sdb::backup
