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

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <store/simulated_object_store.hpp>
#include <string>
#include <thread>

#include "catalog/database_directory.h"
#include "scheduler/background_scheduler.h"

namespace sdb::catalog {
namespace {

irs::SimulatedObjectStore& ObjectStore() {
  static auto& store = irs::SimulatedObjectStore::Register(
    irs::DuckDBEngine::Instance().instance());
  return store;
}

bool Eventually(const auto& done) {
  const auto deadline =
    std::chrono::steady_clock::now() + std::chrono::seconds{30};
  while (!done()) {
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{5});
  }
  return true;
}

std::unique_ptr<StorageDirectory> Storage(
  const std::shared_ptr<DatabaseDirectory>& database,
  std::string_view tablespace, duckdb::idx_t oid) {
  return database->StorageOf(tablespace, oid);
}

std::unique_ptr<StorageDirectory> Storage(
  const std::shared_ptr<DatabaseDirectory>& database, duckdb::idx_t oid) {
  return Storage(database, database->Tablespace(), oid);
}

void RemoveStorage(std::unique_ptr<StorageDirectory> storage) {
  storage->MarkDropped();
}

class DatabaseDirectoryTest : public ::testing::Test {
 protected:
  void SetUp() final {
    _root = std::filesystem::temp_directory_path() /
            absl::StrCat("database_directory_", ::getpid());
    std::filesystem::remove_all(_root);
    std::filesystem::create_directory(_root);
    _scheduler.start();
  }

  void TearDown() final {
    _scheduler.stop();
    std::filesystem::remove_all(_root);
  }

  static bool Gone(const std::filesystem::path& path) {
    return Eventually([&] { return !std::filesystem::exists(path); });
  }

  static bool ObjectGone(const std::string& path) {
    return Eventually([&] { return !ObjectStore().Find(path); });
  }

  std::string Path(const std::filesystem::path& relative) const {
    return (_root / relative).string();
  }

  std::shared_ptr<DatabaseDirectory> Database(
    const std::string& tablespace) const {
    return std::make_shared<DatabaseDirectory>(5, _root / "5", tablespace);
  }

  std::filesystem::path _root;
  BackgroundScheduler _scheduler;
};

TEST_F(DatabaseDirectoryTest, CreatesOnceAndOpensOnlyWhatExists) {
  const auto directory = Database(_root.string());
  CreateDirectory(directory->Path());
  EXPECT_TRUE(std::filesystem::is_directory(_root / "5"));
  EXPECT_ANY_THROW(CreateDirectory(directory->Path()));
  EXPECT_EQ(directory->DataFile(), Path("5/data.db"));
  const auto storage = Storage(directory, 7)->Path();
  EXPECT_EQ(storage, Path("5/7"));
  EXPECT_FALSE(StorageFileSystem().DirectoryExists(storage));
  CreateDirectory(storage);
  EXPECT_ANY_THROW(CreateDirectory(storage));
  EXPECT_TRUE(StorageFileSystem().DirectoryExists(storage));
}

TEST_F(DatabaseDirectoryTest, ABareDirectoryHoldsItsStorages) {
  const auto directory = std::make_shared<DatabaseDirectory>(5, _root / "5");
  EXPECT_EQ(Storage(directory, 7)->Path(), Path("5/7"));
}

TEST_F(DatabaseDirectoryTest, CreatesEveryMissingParent) {
  std::ofstream{_root / "file"} << "data";
  EXPECT_ANY_THROW(CreateDirectory(_root / "file" / "sub"));
  const auto storage = Path("ts/engine_v1/1-2/5/7");
  CreateDirectory(storage);
  EXPECT_TRUE(std::filesystem::is_directory(storage));
}

TEST_F(DatabaseDirectoryTest, ARemovedStorageLeavesItsSiblings) {
  auto directory = Database(_root.string());
  CreateDirectory(directory->Path());
  CreateDirectory(Storage(directory, 7)->Path());
  CreateDirectory(Storage(directory, 8)->Path());
  std::ofstream{_root / "5" / "7" / "segment"} << "rows";
  RemoveStorage(Storage(directory, 7));
  EXPECT_TRUE(Gone(_root / "5" / "7"));
  EXPECT_TRUE(std::filesystem::is_directory(_root / "5" / "8"));
  directory.reset();
  EXPECT_TRUE(std::filesystem::is_directory(_root / "5" / "8"));
}

TEST_F(DatabaseDirectoryTest, ADroppedDatabaseGoesAfterItsLastStorage) {
  auto directory = Database(_root.string());
  CreateDirectory(directory->Path());
  auto storage = Storage(directory, 7);
  CreateDirectory(storage->Path());
  std::ofstream{_root / "5" / "data.db"} << "pages";
  std::ofstream{_root / "5" / "7" / "segment"} << "rows";
  directory->MarkDropped();
  directory.reset();
  EXPECT_TRUE(std::filesystem::is_directory(_root / "5" / "7"));
  RemoveStorage(std::move(storage));
  EXPECT_TRUE(Gone(_root / "5"));
}

TEST_F(DatabaseDirectoryTest, StoragesInOtherTablespaces) {
  auto directory = Database(Path("indexes"));
  CreateDirectory(directory->Path());
  const auto other = Path("other");
  EXPECT_EQ(Storage(directory, 7)->Path(), Path("indexes/5/7"));
  EXPECT_EQ(Storage(directory, other, 8)->Path(), Path("other/5/8"));
  CreateDirectory(Storage(directory, 7)->Path());
  CreateDirectory(Storage(directory, other, 8)->Path());
  EXPECT_TRUE(std::filesystem::is_directory(_root / "indexes" / "5" / "7"));
  EXPECT_TRUE(std::filesystem::is_directory(_root / "other" / "5" / "8"));
  EXPECT_FALSE(std::filesystem::exists(_root / "5" / "7"));
  directory->SetTablespaces({Path("other"), Path("indexes")});
  directory->MarkDropped();
  directory.reset();
  EXPECT_TRUE(Gone(_root / "5"));
  EXPECT_TRUE(Gone(_root / "indexes" / "5"));
  EXPECT_TRUE(Gone(_root / "other" / "5"));
  EXPECT_TRUE(std::filesystem::is_directory(_root / "other"));
}

TEST_F(DatabaseDirectoryTest, StoragesOnAnObjectStore) {
  ObjectStore();
  const auto root = absl::StrCat(irs::SimulatedObjectStore::kPrefix,
                                 "bucket/database_directory_", ::getpid());
  auto directory = Database(root);
  CreateDirectory(directory->Path());
  auto storage = Storage(directory, 7);
  const auto path = storage->Path();
  EXPECT_EQ(path, absl::StrCat(root, "/5/7"));
  EXPECT_FALSE(StorageFileSystem().DirectoryExists(path));
  CreateDirectory(path);
  EXPECT_TRUE(ObjectStore().Find(absl::StrCat(root, "/5/7/")));
  EXPECT_TRUE(StorageFileSystem().DirectoryExists(path));
  EXPECT_FALSE(
    StorageFileSystem().DirectoryExists(Storage(directory, 8)->Path()));

  ObjectStore().Publish(absl::StrCat(root, "/5/7/segment"), "rows");
  RemoveStorage(std::move(storage));
  EXPECT_TRUE(ObjectGone(absl::StrCat(root, "/5/7/")));
  EXPECT_TRUE(ObjectGone(absl::StrCat(root, "/5/7/segment")));
  EXPECT_FALSE(StorageFileSystem().DirectoryExists(path));
}

TEST_F(DatabaseDirectoryTest, RemoveTreeSaysWhetherTheTreeIsGone) {
  EXPECT_TRUE(RemoveTree(Path("missing")));
  std::filesystem::create_directories(_root / "tree" / "sub");
  std::ofstream{_root / "tree" / "sub" / "file"} << "data";
  EXPECT_TRUE(RemoveTree(Path("tree")));
  EXPECT_FALSE(std::filesystem::exists(_root / "tree"));
}

TEST_F(DatabaseDirectoryTest, UnownedStoragesGoFromADirectory) {
  auto directory = Database(_root.string());
  CreateDirectory(directory->Path());
  CreateDirectory(Storage(directory, 7)->Path());
  CreateDirectory(Storage(directory, 8)->Path());
  std::ofstream{_root / "5" / "8" / "segment"} << "rows";
  std::ofstream{_root / "5" / "notes"} << "kept";

  RemoveUnowned(ListStorages(Path("5")), {7});
  EXPECT_TRUE(std::filesystem::is_directory(_root / "5" / "7"));
  EXPECT_FALSE(std::filesystem::exists(_root / "5" / "8"));
  EXPECT_TRUE(std::filesystem::exists(_root / "5" / "notes"));
}

TEST_F(DatabaseDirectoryTest, UnownedStoragesGoFromAnObjectStore) {
  ObjectStore();
  const auto tablespace = absl::StrCat(irs::SimulatedObjectStore::kPrefix,
                                       "bucket/unowned_", ::getpid());
  const auto root = absl::StrCat(tablespace, "/5");
  auto directory = Database(tablespace);
  CreateDirectory(directory->Path());
  CreateDirectory(Storage(directory, 7)->Path());
  CreateDirectory(Storage(directory, 8)->Path());
  ObjectStore().Publish(absl::StrCat(root, "/7/segment"), "rows");
  ObjectStore().Publish(absl::StrCat(root, "/8/segment"), "rows");
  ObjectStore().Publish(absl::StrCat(root, "/8/segment.meta"), "rows");
  ObjectStore().Publish(absl::StrCat(root, "/9/orphan"), "rows");

  const auto storages = ListStorages(root);
  EXPECT_EQ(3, storages.size());
  RemoveUnowned(storages, {7});
  EXPECT_TRUE(ObjectStore().Find(absl::StrCat(root, "/7/segment")));
  EXPECT_TRUE(ObjectStore().Find(absl::StrCat(root, "/7/")));
  EXPECT_FALSE(ObjectStore().Find(absl::StrCat(root, "/8/segment")));
  EXPECT_FALSE(ObjectStore().Find(absl::StrCat(root, "/8/segment.meta")));
  EXPECT_FALSE(ObjectStore().Find(absl::StrCat(root, "/8/")));
  EXPECT_FALSE(ObjectStore().Find(absl::StrCat(root, "/9/orphan")));
}

}  // namespace
}  // namespace sdb::catalog
