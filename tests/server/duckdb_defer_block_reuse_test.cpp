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

#include <cstdlib>
#include <duckdb.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/storage/single_file_block_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <filesystem>
#include <memory>
#include <string>

namespace {

class DuckDBDeferBlockReuse : public ::testing::Test {
 protected:
  void SetUp() final {
    _root = std::filesystem::temp_directory_path() /
            absl::StrCat("defer_block_reuse_", ::getpid());
    std::filesystem::remove_all(_root);
    std::filesystem::create_directory(_root);
    _db = std::make_unique<duckdb::DuckDB>(nullptr);
    _con = std::make_unique<duckdb::Connection>(*_db);
    Run(
      absl::StrCat("ATTACH '", (_root / "blocks.db").string(), "' AS blocks"));
    Run("CREATE TABLE blocks.t (i INTEGER)");
    Run("INSERT INTO blocks.t VALUES (1)");
    Run("CHECKPOINT blocks");
  }

  void TearDown() final {
    _con.reset();
    _db.reset();
    std::filesystem::remove_all(_root);
  }

  void Run(const std::string& sql) {
    auto result = _con->Query(sql);
    ASSERT_FALSE(result->HasError()) << sql << ": " << result->GetError();
  }

  duckdb::SingleFileBlockManager& Blocks() {
    for (auto& attached :
         duckdb::DatabaseManager::Get(*_con->context).GetDatabases()) {
      if (attached->GetName() == "blocks") {
        return attached->GetStorageManager()
          .GetBlockManager()
          .Cast<duckdb::SingleFileBlockManager>();
      }
    }
    std::abort();
  }

  bool HandsOut(duckdb::block_id_t block) {
    auto& blocks = Blocks();
    const auto free = blocks.FreeBlocks();
    for (duckdb::idx_t i = 0; i < free; ++i) {
      if (blocks.GetFreeBlockId() == block) {
        return true;
      }
    }
    return false;
  }

  std::filesystem::path _root;
  std::unique_ptr<duckdb::DuckDB> _db;
  std::unique_ptr<duckdb::Connection> _con;
};

TEST_F(DuckDBDeferBlockReuse, ANewlyUsedBlockIsReusedAtOnceByDefault) {
  auto& blocks = Blocks();
  const auto block = blocks.GetFreeBlockId();
  blocks.MarkBlockAsModified(block);
  EXPECT_EQ(blocks.PeekFreeBlockId(), block);
  EXPECT_EQ(blocks.GetFreeBlockId(), block);
}

TEST_F(DuckDBDeferBlockReuse, ADeferredBlockWaitsForTheNextCheckpoint) {
  auto& blocks = Blocks();
  blocks.SetDeferBlockReuse(true);
  const auto block = blocks.GetFreeBlockId();
  const auto free_before = blocks.FreeBlocks();
  blocks.MarkBlockAsModified(block);
  EXPECT_EQ(blocks.FreeBlocks(), free_before);
  EXPECT_NE(blocks.GetFreeBlockId(), block);

  blocks.SetDeferBlockReuse(false);
  Run("INSERT INTO blocks.t VALUES (2)");
  Run("CHECKPOINT blocks");
  EXPECT_TRUE(HandsOut(block));
}

}  // namespace
