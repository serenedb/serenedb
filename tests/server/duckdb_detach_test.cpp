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

#include <gtest/gtest.h>

#include <atomic>
#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <thread>

namespace {

TEST(DuckDBDetach, ClosesAfterAReaderDropsItsReference) {
  duckdb::DuckDB db{nullptr};
  duckdb::Connection con{db};
  auto attach = con.Query("ATTACH ':memory:' AS dx");
  ASSERT_FALSE(attach->HasError()) << attach->GetError();
  duckdb::shared_ptr<duckdb::AttachedDatabase> owner;
  for (auto& attached :
       duckdb::DatabaseManager::Get(*con.context).GetDatabases()) {
    if (attached->GetName() == "dx") {
      owner = attached;
    }
  }
  ASSERT_TRUE(owner);
  auto detach = con.Query("DETACH dx");
  ASSERT_FALSE(detach->HasError()) << detach->GetError();
  std::atomic_bool released = false;
  std::thread reader{[&released, reference = owner]() mutable {
    EXPECT_EQ(reference->GetCatalog().GetCatalogType(), "duckdb");
    reference.reset();
    released.store(true, std::memory_order_relaxed);
  }};
  while (!released.load(std::memory_order_relaxed)) {
    std::this_thread::yield();
  }
  duckdb::AttachedDatabase::InvokeCloseIfLastReference(owner, *con.context);
  EXPECT_FALSE(owner);
  reader.join();
}

}  // namespace
