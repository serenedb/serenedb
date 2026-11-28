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

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <duckdb/common/error_data.hpp>
#include <filesystem>
#include <fstream>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/sql_exception.hpp>
#include <iterator>
#include <store/simulated_object_store.hpp>
#include <string>
#include <thread>
#include <vector>

#include "catalog/database_directory.h"
#include "runtime/runtime.h"

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

class DatabaseDirectoryTest : public ::testing::Test {
 protected:
  void SetUp() final {
    _root = std::filesystem::temp_directory_path() /
            absl::StrCat("database_directory_", ::getpid());
    std::filesystem::remove_all(_root);
    std::filesystem::create_directory(_root);
  }

  void TearDown() final {
    _runtime.Background().Drain();
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

  std::filesystem::path _root;
  Runtime _runtime{1};
};

TEST_F(DatabaseDirectoryTest, StoresSitInTheStoreDirectoryOfATablespace) {
  EXPECT_EQ(StoreDirOf(Path("ts")), Path("ts/segments"));
}

TEST_F(DatabaseDirectoryTest, CreatesOnceWithEveryMissingParent) {
  const auto store = Path("ts/segments");
  EXPECT_FALSE(StorageFileSystem().DirectoryExists(store));
  CreateDirectory(store);
  EXPECT_TRUE(std::filesystem::is_directory(store));
  EXPECT_ANY_THROW(CreateDirectory(store));
  std::ofstream{_root / "file"} << "data";
  EXPECT_ANY_THROW(CreateDirectory(Path("file/sub")));
}

TEST_F(DatabaseDirectoryTest, ARemovedStorageLeavesTheFilesOfOthers) {
  CreateDirectory(StoreDirOf(_root.string()));
  std::ofstream{_root / kStoreDir / "0000000000000007.col"} << "rows";
  std::ofstream{_root / kStoreDir / "0000000000000008.col"} << "rows";
  BeginStorageWork();
  RemoveStorage(StoreDirOf(_root.string()), [this] {
    return std::filesystem::remove(_root / kStoreDir / "0000000000000007.col");
  });
  EXPECT_TRUE(Gone(_root / kStoreDir / "0000000000000007.col"));
  EXPECT_TRUE(
    std::filesystem::exists(_root / kStoreDir / "0000000000000008.col"));
}

TEST_F(DatabaseDirectoryTest, AStorageWhoseFilesStayIsKept) {
  CreateDirectory(StoreDirOf(_root.string()));
  std::ofstream{_root / kStoreDir / "0000000000000007.col"} << "rows";
  std::atomic_bool tried{false};
  BeginStorageWork();
  RemoveStorage(StoreDirOf(_root.string()), [&] {
    tried.store(true);
    return false;
  });
  EXPECT_TRUE(Eventually([&] { return tried.load(); }));
  EXPECT_TRUE(
    std::filesystem::exists(_root / kStoreDir / "0000000000000007.col"));
}

TEST_F(DatabaseDirectoryTest, RemovalsWaitWhileTheyAreDeferred) {
  const auto file = _root / "0000000000000007.col";
  std::ofstream{file} << "rows";
  DeferRemovals();
  BeginStorageWork();
  RemoveStorage(StoreDirOf(_root.string()),
                [&] { return std::filesystem::remove(file); });
  std::this_thread::sleep_for(std::chrono::milliseconds{50});
  EXPECT_TRUE(std::filesystem::exists(file));
  ResumeRemovals();
  EXPECT_TRUE(Gone(file));
}

TEST_F(DatabaseDirectoryTest, AStoreDirectoryOnAnObjectStore) {
  ObjectStore();
  const auto store =
    StoreDirOf(absl::StrCat(irs::SimulatedObjectStore::kPrefix,
                            "bucket/database_directory_", ::getpid()));
  EXPECT_FALSE(StorageFileSystem().DirectoryExists(store));
  CreateDirectory(store);
  EXPECT_TRUE(ObjectStore().Find(absl::StrCat(store, "/")));
  EXPECT_TRUE(StorageFileSystem().DirectoryExists(store));
  ObjectStore().Publish(absl::StrCat(store, "/0000000000000007.col"), "rows");
  ObjectStore().Publish(absl::StrCat(store, "/0000000000000008.doc"), "rows");
  EXPECT_EQ(8, StorageBytes(store));
}

TEST_F(DatabaseDirectoryTest, RemoveTreeSaysWhetherTheTreeIsGone) {
  EXPECT_TRUE(RemoveTree(Path("missing")));
  std::filesystem::create_directories(_root / "tree" / "sub");
  std::ofstream{_root / "tree" / "sub" / "file"} << "data";
  EXPECT_TRUE(RemoveTree(Path("tree")));
  EXPECT_FALSE(std::filesystem::exists(_root / "tree"));
}

TEST_F(DatabaseDirectoryTest, RemoveTreeOrThrowSaysWhyTheTreeStays) {
  if (::geteuid() == 0) {
    GTEST_SKIP() << "root removes a file whatever the mode of its directory";
  }
  const auto sub = _root / "kept" / "sub";
  std::filesystem::create_directories(sub);
  std::ofstream{sub / "file"} << "data";
  std::filesystem::permissions(sub, std::filesystem::perms::owner_read |
                                      std::filesystem::perms::owner_exec);
  absl::Cleanup writable = [&] {
    std::filesystem::permissions(sub, std::filesystem::perms::owner_all);
  };
  try {
    RemoveTreeOrThrow(Path("kept"));
    FAIL() << "the removal reported no failure";
  } catch (const std::exception& e) {
    EXPECT_TRUE(
      absl::StrContains(duckdb::ErrorData{e}.RawMessage(), "Permission denied"))
      << duckdb::ErrorData{e}.RawMessage();
  }
  EXPECT_FALSE(RemoveTree(Path("kept")));
  EXPECT_TRUE(std::filesystem::exists(sub / "file"));
}

TEST_F(DatabaseDirectoryTest, CountsTheBytesOfAStoreDirectory) {
  CreateDirectory(StoreDirOf(_root.string()));
  std::ofstream{_root / kStoreDir / "0000000000000007.col"} << "rows";
  std::ofstream{_root / kStoreDir / "0000000000000008.col"} << "more";
  EXPECT_EQ(8, StorageBytes(StoreDirOf(_root.string())));
}

TEST_F(DatabaseDirectoryTest, ALocationBelongsToTheServerThatClaimedIt) {
  const auto location = Path("owned");
  std::filesystem::create_directories(location);
  ClaimLocation(location);
  const auto owner = _root / "owned" / kOwnerFile;
  std::ifstream in{owner};
  const std::string self{std::istreambuf_iterator<char>{in}, {}};
  EXPECT_FALSE(self.empty());
  ClaimLocation(location);
  std::ofstream{owner, std::ios::trunc} << "another\n";
  EXPECT_THROW(ClaimLocation(location), irs::SqlException);
  ReleaseLocation(location);
  _runtime.Background().Drain();
  EXPECT_TRUE(std::filesystem::exists(owner));
}

TEST_F(DatabaseDirectoryTest, AReleasedLocationKeepsItsOwnerUntilItsFilesGo) {
  const auto location = Path("released");
  const auto store = StoreDirOf(location);
  std::filesystem::create_directories(store);
  ClaimLocation(location);
  const auto owner = _root / "released" / kOwnerFile;
  const auto file = std::filesystem::path{store} / "0000000000000007.col";
  std::ofstream{file} << "rows";
  ReleaseLocation(location);
  _runtime.Background().Drain();
  EXPECT_TRUE(std::filesystem::exists(owner));
  BeginStorageWork();
  RemoveStorage(store, [&] { return std::filesystem::remove(file); });
  EXPECT_TRUE(Gone(owner));
  EXPECT_TRUE(std::filesystem::is_directory(store));
}

TEST_F(DatabaseDirectoryTest, AClaimAfterAReleaseKeepsTheOwner) {
  const auto location = Path("reclaimed");
  std::filesystem::create_directories(StoreDirOf(location));
  ClaimLocation(location);
  DeferRemovals();
  ReleaseLocation(location);
  ClaimLocation(location);
  ResumeRemovals();
  _runtime.Background().Drain();
  EXPECT_TRUE(std::filesystem::exists(_root / "reclaimed" / kOwnerFile));
}

TEST_F(DatabaseDirectoryTest, SegmentIdsAreOneCounterForTheServer) {
  EXPECT_EQ(SegmentIds().get(), SegmentIds().get());
}

}  // namespace
}  // namespace sdb::catalog
