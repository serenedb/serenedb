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
#include <string>
#include <string_view>

#include "query/prepared_cache.h"

using namespace sdb;

namespace {

using Cache = query::PreparedCache<std::string>;

duckdb::unique_ptr<duckdb::PreparedStatement> Statement(std::string_view sql) {
  return duckdb::make_uniq<duckdb::PreparedStatement>(
    duckdb::ErrorData{std::string{sql}});
}

TEST(QueryPreparedCache, HitKeepsTheStatement) {
  Cache cache;
  EXPECT_EQ(cache.Find(std::string_view{"a"}), nullptr);
  auto& a = cache.Insert("a", Statement("a"), 8);
  cache.Insert("b", Statement("b"), 8);
  EXPECT_EQ(cache.Find(std::string_view{"a"}), &a);
  EXPECT_EQ(cache.Size(), 2);
}

TEST(QueryPreparedCache, EvictsTheLeastRecentlyUsed) {
  Cache cache;
  auto& a = cache.Insert("a", Statement("a"), 2);
  cache.Insert("b", Statement("b"), 2);
  EXPECT_EQ(cache.Find(std::string_view{"a"}), &a);
  auto& c = cache.Insert("c", Statement("c"), 2);
  EXPECT_EQ(cache.Size(), 2);
  EXPECT_EQ(cache.Find(std::string_view{"a"}), &a);
  EXPECT_EQ(cache.Find(std::string_view{"c"}), &c);
  EXPECT_EQ(cache.Find(std::string_view{"b"}), nullptr);
}

TEST(QueryPreparedCache, CapacityOneReplaces) {
  Cache cache;
  cache.Insert("a", Statement("a"), 1);
  auto& b = cache.Insert("b", Statement("b"), 1);
  EXPECT_EQ(cache.Size(), 1);
  EXPECT_EQ(cache.Find(std::string_view{"a"}), nullptr);
  EXPECT_EQ(cache.Find(std::string_view{"b"}), &b);
}

TEST(QueryPreparedCache, EraseIfDropsMatchingKeys) {
  Cache cache;
  cache.Insert("t1:insert", Statement("1"), 8);
  cache.Insert("t1:update", Statement("2"), 8);
  auto& kept = cache.Insert("t2:insert", Statement("3"), 8);
  cache.EraseIf([](const std::string& key) { return key.starts_with("t1:"); });
  EXPECT_EQ(cache.Size(), 1);
  EXPECT_EQ(cache.Find(std::string_view{"t1:insert"}), nullptr);
  EXPECT_EQ(cache.Find(std::string_view{"t2:insert"}), &kept);
}

}  // namespace
