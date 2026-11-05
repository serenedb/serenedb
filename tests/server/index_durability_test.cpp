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

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/memory_file_system.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <optional>
#include <string>

#include "search/index_durability.h"

namespace sdb::search {
namespace {

size_t CountFiles(duckdb::FileSystem& fs, const std::string& root) {
  size_t count = 0;
  fs.ListFiles(root, [&](const std::string&, bool) { ++count; });
  return count;
}

TEST(IndexDurabilityTest, InMemoryIndexFilesGoWithTheIndex) {
  duckdb::DuckDB db{nullptr};
  duckdb::Connection con{db};
  auto attached =
    duckdb::DatabaseManager::Get(*con.context).GetDatabase("memory");
  ASSERT_TRUE(attached);
  ASSERT_TRUE(attached->GetStorageManager().InMemory());
  auto& engine = irs::DuckDBEngine::Instance().instance();
  auto& fs = duckdb::FileSystem::GetFileSystem(engine);
  constexpr duckdb::idx_t kOid = 42;
  const auto root =
    absl::StrCat(duckdb::MemoryFileSystem::PREFIX, attached->oid, "/", kOid);
  {
    IndexDurability durability{*attached, "index", std::nullopt, kOid, true};
    irs::IndexWriterOptions options;
    options.db = &engine;
    auto [writer, stamp] = durability.OpenWriter(true, std::move(options));
    ASSERT_TRUE(writer);
    EXPECT_LT(0, CountFiles(fs, root));
  }
  EXPECT_EQ(0, CountFiles(fs, root));
}

}  // namespace
}  // namespace sdb::search
