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

#include <duckdb/common/error_data.hpp>
#include <duckdb/main/prepared_statement.hpp>

#include "network/http/prepared_source.h"

using namespace sdb;
using network::PreparedCache;
using network::PreparedEntry;

namespace {

void Prepare(PreparedEntry& entry) {
  entry.statement =
    duckdb::make_uniq<duckdb::PreparedStatement>(duckdb::ErrorData{entry.sql});
}

TEST(PreparedCacheTest, HitKeepsTheStatement) {
  PreparedCache cache;
  auto& a = cache.Get("a", 8);
  EXPECT_EQ(a.sql, "a");
  EXPECT_EQ(a.statement, nullptr);
  Prepare(a);
  auto* statement = a.statement.get();
  Prepare(cache.Get("b", 8));

  auto& again = cache.Get("a", 8);
  EXPECT_EQ(&again, &a);
  EXPECT_EQ(again.statement.get(), statement);
  EXPECT_EQ(cache.Size(), 2);
}

TEST(PreparedCacheTest, EvictsTheLeastRecentlyUsed) {
  PreparedCache cache;
  Prepare(cache.Get("a", 2));
  Prepare(cache.Get("b", 2));
  cache.Get("a", 2);

  auto& c = cache.Get("c", 2);
  EXPECT_EQ(c.sql, "c");
  EXPECT_EQ(c.statement, nullptr);
  EXPECT_EQ(cache.Size(), 2);
  Prepare(c);

  EXPECT_NE(cache.Get("a", 2).statement, nullptr);
  EXPECT_NE(cache.Get("c", 2).statement, nullptr);
  EXPECT_EQ(cache.Get("b", 2).statement, nullptr);
  EXPECT_EQ(cache.Size(), 2);
}

TEST(PreparedCacheTest, CapacityOneReplaces) {
  PreparedCache cache;
  auto& a = cache.Get("a", 1);
  Prepare(a);

  auto& b = cache.Get("b", 1);
  EXPECT_EQ(&b, &a);
  EXPECT_EQ(b.sql, "b");
  EXPECT_EQ(b.statement, nullptr);
  EXPECT_EQ(cache.Size(), 1);
}

TEST(PreparedCacheTest, UnpreparedEntryIsReusedForTheSameSql) {
  PreparedCache cache;
  auto& a = cache.Get("a", 2);
  auto& again = cache.Get("a", 2);
  EXPECT_EQ(&again, &a);
  EXPECT_EQ(again.statement, nullptr);
  EXPECT_EQ(cache.Size(), 1);
}

}  // namespace
