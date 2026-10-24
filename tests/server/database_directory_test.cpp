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
#include <memory>
#include <thread>

#include "catalog/database_directory.h"
#include "scheduler/background_scheduler.h"

namespace sdb::catalog {
namespace {

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

  std::filesystem::path _root;
  BackgroundScheduler _scheduler;
};

TEST_F(DatabaseDirectoryTest, CreatesOnceAndOpensOnlyWhatExists) {
  const DatabaseDirectory directory{_root / "5"};
  directory.Create();
  EXPECT_TRUE(std::filesystem::is_directory(_root / "5"));
  EXPECT_ANY_THROW(directory.Create());
  EXPECT_EQ(directory.DataFile(), (_root / "5" / "data.db").string());
  EXPECT_FALSE(directory.OpenStorage(7).has_value());
  EXPECT_EQ(directory.CreateStorage(7), _root / "5" / "7");
  EXPECT_ANY_THROW(directory.CreateStorage(7));
  EXPECT_EQ(directory.OpenStorage(7), _root / "5" / "7");
}

TEST_F(DatabaseDirectoryTest, ARemovedStorageLeavesItsSiblings) {
  auto directory = std::make_shared<DatabaseDirectory>(_root / "5");
  directory->Create();
  directory->CreateStorage(7);
  directory->CreateStorage(8);
  std::ofstream{_root / "5" / "7" / "segment"} << "rows";
  DatabaseDirectory::RemoveStorage(directory, 7);
  EXPECT_TRUE(Gone(_root / "5" / "7"));
  EXPECT_TRUE(std::filesystem::is_directory(_root / "5" / "8"));
  directory.reset();
  EXPECT_TRUE(std::filesystem::is_directory(_root / "5" / "8"));
}

TEST_F(DatabaseDirectoryTest, ADroppedDatabaseGoesAfterItsLastStorage) {
  auto directory = std::make_shared<DatabaseDirectory>(_root / "5");
  directory->Create();
  directory->CreateStorage(7);
  std::ofstream{_root / "5" / "data.db"} << "pages";
  std::ofstream{_root / "5" / "7" / "segment"} << "rows";
  auto storage = directory;
  directory->MarkDropped();
  directory.reset();
  EXPECT_TRUE(std::filesystem::is_directory(_root / "5" / "7"));
  DatabaseDirectory::RemoveStorage(std::move(storage), 7);
  EXPECT_TRUE(Gone(_root / "5"));
}

}  // namespace
}  // namespace sdb::catalog
