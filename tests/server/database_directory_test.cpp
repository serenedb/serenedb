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
#include <iresearch/store/simulated_object_store.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <optional>
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
    const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{30};
    while (std::filesystem::exists(path)) {
      if (std::chrono::steady_clock::now() > deadline) {
        return false;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    return true;
  }

  static bool Released(const std::weak_ptr<const std::string>& held) {
    const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{30};
    while (!held.expired()) {
      if (std::chrono::steady_clock::now() > deadline) {
        return false;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    return true;
  }

  static bool ObjectGone(const std::string& path) {
    const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{30};
    while (ObjectStore().Find(path)) {
      if (std::chrono::steady_clock::now() > deadline) {
        return false;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    return true;
  }

  std::string Path(const std::filesystem::path& relative) const {
    return (_root / relative).string();
  }

  std::shared_ptr<DatabaseDirectory> Database(
    const std::string& tablespace) const {
    auto root = std::make_shared<TablespaceRoot>(tablespace);
    auto path =
      root->Remote() ? _root / "5" : std::filesystem::path{tablespace} / "5";
    return std::make_shared<DatabaseDirectory>(5, std::move(root),
                                               std::move(path));
  }

  std::filesystem::path _root;
  BackgroundScheduler _scheduler;
};

TEST_F(DatabaseDirectoryTest, CreatesOnceAndOpensOnlyWhatExists) {
  const auto directory = Database(_root.string());
  directory->Create();
  EXPECT_TRUE(std::filesystem::is_directory(_root / "5"));
  EXPECT_ANY_THROW(directory->Create());
  EXPECT_EQ(directory->DataFile(), Path("5/data.db"));
  const auto area = directory->Area(0);
  EXPECT_FALSE(area->OpenStorage(7));
  EXPECT_EQ(area->CreateStorage(7), Path("5/7"));
  EXPECT_ANY_THROW(area->CreateStorage(7));
  EXPECT_EQ(area->OpenStorage(7), std::optional<std::string>{Path("5/7")});
}

TEST_F(DatabaseDirectoryTest, ARemovedStorageLeavesItsSiblings) {
  auto directory = Database(_root.string());
  directory->Create();
  auto area = directory->Area(0);
  area->CreateStorage(7);
  area->CreateStorage(8);
  std::ofstream{_root / "5" / "7" / "segment"} << "rows";
  StorageArea::RemoveStorage(area, 7);
  EXPECT_TRUE(Gone(_root / "5" / "7"));
  EXPECT_TRUE(std::filesystem::is_directory(_root / "5" / "8"));
  area.reset();
  directory.reset();
  EXPECT_TRUE(std::filesystem::is_directory(_root / "5" / "8"));
}

TEST_F(DatabaseDirectoryTest, ADroppedDatabaseGoesAfterItsLastStorage) {
  auto directory = Database(_root.string());
  directory->Create();
  auto area = directory->Area(0);
  area->CreateStorage(7);
  std::ofstream{_root / "5" / "data.db"} << "pages";
  std::ofstream{_root / "5" / "7" / "segment"} << "rows";
  directory->MarkDropped();
  directory.reset();
  EXPECT_TRUE(std::filesystem::is_directory(_root / "5" / "7"));
  StorageArea::RemoveStorage(std::move(area), 7);
  EXPECT_TRUE(Gone(_root / "5"));
}

TEST_F(DatabaseDirectoryTest, StoragesInAnotherTablespace) {
  std::filesystem::create_directory(_root / "indexes");
  auto directory = std::make_shared<DatabaseDirectory>(
    5, std::make_shared<TablespaceRoot>(Path("indexes")), _root / "5");
  directory->Create();
  auto area = directory->Area(0);
  EXPECT_EQ(area->CreateStorage(7), Path("indexes/5/7"));
  EXPECT_TRUE(std::filesystem::is_directory(_root / "indexes" / "5" / "7"));
  EXPECT_FALSE(std::filesystem::exists(_root / "5" / "7"));
  EXPECT_EQ(area->OpenStorage(7),
            std::optional<std::string>{Path("indexes/5/7")});
  area.reset();
  directory->MarkDropped();
  directory.reset();
  EXPECT_TRUE(Gone(_root / "5"));
  EXPECT_TRUE(Gone(_root / "indexes" / "5"));
}

TEST_F(DatabaseDirectoryTest, StoragesOnAnObjectStore) {
  ObjectStore();
  const auto root = absl::StrCat(irs::SimulatedObjectStore::kPrefix,
                                 "bucket/database_directory_", ::getpid());
  auto directory = Database(root);
  directory->Create();
  auto area = directory->Area(0);
  EXPECT_FALSE(area->OpenStorage(7));
  EXPECT_EQ(area->CreateStorage(7), absl::StrCat(root, "/5/7"));
  EXPECT_TRUE(ObjectStore().Find(absl::StrCat(root, "/5/7.storage")));
  EXPECT_ANY_THROW(area->CreateStorage(7));
  EXPECT_EQ(area->OpenStorage(7),
            std::optional<std::string>{absl::StrCat(root, "/5/7")});
  EXPECT_FALSE(area->OpenStorage(8));

  ObjectStore().Publish(absl::StrCat(root, "/5/7/segment"), "rows");
  StorageArea::RemoveStorage(area, 7);
  EXPECT_TRUE(ObjectGone(absl::StrCat(root, "/5/7.storage")));
  EXPECT_TRUE(ObjectGone(absl::StrCat(root, "/5/7/segment")));
  EXPECT_FALSE(area->OpenStorage(7));
}

TEST_F(DatabaseDirectoryTest, ALocalTablespaceIsClaimedOnce) {
  TablespaceRoot root{Path("ts/engine_v1")};
  root.Claim();
  EXPECT_TRUE(std::filesystem::is_directory(_root / "ts" / "engine_v1"));
  EXPECT_ANY_THROW(TablespaceRoot{Path("ts/engine_v1")}.Claim());
}

TEST_F(DatabaseDirectoryTest, ARemoteTablespaceIsClaimedOnce) {
  ObjectStore();
  const auto path = absl::StrCat(irs::SimulatedObjectStore::kPrefix,
                                 "bucket/claimed_", ::getpid(), "/engine_v1");
  TablespaceRoot root{path};
  root.Claim();
  EXPECT_TRUE(ObjectStore().Find(absl::StrCat(path, "/tablespace")));
  EXPECT_ANY_THROW(TablespaceRoot{path}.Claim());
}

TEST_F(DatabaseDirectoryTest, AFailedClaimLeavesTheClaimedDirectory) {
  TablespaceRoot root{Path("ts/engine_v1")};
  root.Claim();
  std::ofstream{_root / "ts" / "engine_v1" / "data"} << "kept";
  {
    auto again = std::make_shared<TablespaceRoot>(Path("ts/engine_v1"));
    EXPECT_ANY_THROW(again->Claim());
    again->Abandon();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds{100});
  EXPECT_TRUE(std::filesystem::exists(_root / "ts" / "engine_v1" / "data"));
}

TEST_F(DatabaseDirectoryTest, ADroppedTablespaceGoesAfterItsLastStorage) {
  auto root = std::make_shared<TablespaceRoot>(Path("ts/engine_v1"));
  root->Claim();
  const auto held = root->HeldPath();
  auto directory =
    std::make_shared<DatabaseDirectory>(5, root, _root / "default" / "5");
  auto area = directory->Area(0);
  area->CreateStorage(7);
  root->MarkRemoved();
  root.reset();
  directory.reset();
  EXPECT_TRUE(std::filesystem::is_directory(_root / "ts" / "engine_v1" / "5"));
  EXPECT_FALSE(held.expired());
  area.reset();
  EXPECT_TRUE(Released(held));
  EXPECT_FALSE(std::filesystem::exists(_root / "ts" / "engine_v1"));
}

TEST_F(DatabaseDirectoryTest, AKeptTablespaceReleasesItsPathWithItsRoot) {
  auto root = std::make_shared<TablespaceRoot>(Path("ts/engine_v1"));
  root->Claim();
  const auto held = root->HeldPath();
  root.reset();
  EXPECT_TRUE(held.expired());
  EXPECT_TRUE(std::filesystem::is_directory(_root / "ts" / "engine_v1"));
}

TEST_F(DatabaseDirectoryTest, ADropWaitsForNoCreationAndBlocksNewOnes) {
  auto root = std::make_shared<TablespaceRoot>(Path("ts/engine_v1"));
  auto pin = root->Pin();
  ASSERT_TRUE(pin);
  auto drop = std::make_shared<bool>();
  EXPECT_FALSE(root->BeginDrop(drop));
  pin.reset();
  EXPECT_TRUE(root->BeginDrop(drop));
  EXPECT_FALSE(root->Pin());
  drop.reset();
  EXPECT_TRUE(root->Pin());
  root->MarkRemoved();
  EXPECT_FALSE(root->Pin());
}

TEST_F(DatabaseDirectoryTest, UnownedStoragesGoFromADirectory) {
  auto directory = Database(_root.string());
  directory->Create();
  auto area = directory->Area(0);
  area->CreateStorage(7);
  area->CreateStorage(8);
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
  directory->Create();
  auto area = directory->Area(0);
  area->CreateStorage(7);
  area->CreateStorage(8);
  ObjectStore().Publish(absl::StrCat(root, "/7/segment"), "rows");
  ObjectStore().Publish(absl::StrCat(root, "/8/segment"), "rows");
  ObjectStore().Publish(absl::StrCat(root, "/8/segment.meta"), "rows");
  ObjectStore().Publish(absl::StrCat(root, "/9/orphan"), "rows");

  const auto storages = ListStorages(root);
  EXPECT_EQ(5, storages.size());
  RemoveUnowned(storages, {7});
  EXPECT_TRUE(ObjectStore().Find(absl::StrCat(root, "/7/segment")));
  EXPECT_TRUE(ObjectStore().Find(absl::StrCat(root, "/7.storage")));
  EXPECT_FALSE(ObjectStore().Find(absl::StrCat(root, "/8/segment")));
  EXPECT_FALSE(ObjectStore().Find(absl::StrCat(root, "/8/segment.meta")));
  EXPECT_FALSE(ObjectStore().Find(absl::StrCat(root, "/8.storage")));
  EXPECT_FALSE(ObjectStore().Find(absl::StrCat(root, "/9/orphan")));
}

}  // namespace
}  // namespace sdb::catalog
