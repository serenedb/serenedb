////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "connector/key_utils.hpp"
#include "gtest/gtest.h"

namespace {

using namespace sdb;
using namespace connector::key_utils;

TEST(KeyUtilsTest, PrepareTableKey) {
  ObjectId id{42};
  std::string key = PrepareTableKey(id);
  ASSERT_EQ(key, std::string_view("\x00\x00\x00\x00\x00\x00\x00\x2A", 8));
}

TEST(KeyUtilsTest, PrepareColumnKey) {
  ObjectId id{1};
  catalog::Column::Id col = 7;
  std::string key = PrepareColumnKey(id, col);
  ASSERT_EQ(key, std::string_view(
                   "\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x07", 12));
}

TEST(KeyUtilsTest, AppendColumnKey) {
  ObjectId id{42};
  std::string key = PrepareTableKey(id);
  AppendColumnKey(key, 5);
  ASSERT_EQ(key, std::string_view(
                   "\x00\x00\x00\x00\x00\x00\x00\x2A\x00\x00\x00\x05", 12));
}

TEST(KeyUtilsTest, CreateTableRange) {
  ObjectId id{10};
  auto range = CreateTableRange(id);
  ASSERT_EQ(range.first,
            std::string_view("\x00\x00\x00\x00\x00\x00\x00\x0A", 8));
  ASSERT_EQ(range.second,
            std::string_view("\x00\x00\x00\x00\x00\x00\x00\x0B", 8));
}

TEST(KeyUtilsTest, CreateTableRangeMax) {
  ObjectId id{std::numeric_limits<decltype(id.id())>::max()};
  auto range = CreateTableRange(id);
  ASSERT_EQ(range.first,
            std::string_view("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 8));
  ASSERT_EQ(
    range.second,
    std::string_view("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 12));
}

TEST(KeyUtilsTest, CreateTableColumnRange) {
  ObjectId id{11};
  catalog::Column::Id col = 2;
  auto range = CreateTableColumnRange(id, col);
  ASSERT_EQ(
    range.first,
    std::string_view("\x00\x00\x00\x00\x00\x00\x00\x0B\x00\x00\x00\x02", 12));
  ASSERT_EQ(
    range.second,
    std::string_view("\x00\x00\x00\x00\x00\x00\x00\x0B\x00\x00\x00\x03", 12));
}

TEST(KeyUtilsTest, CreateTableColumnRangeMax) {
  ObjectId id{std::numeric_limits<decltype(id.id())>::max()};
  catalog::Column::Id col = std::numeric_limits<catalog::Column::Id>::max() - 1;
  auto range = CreateTableColumnRange(id, col);
  ASSERT_EQ(
    range.first,
    std::string_view("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE", 12));
  ASSERT_EQ(
    range.second,
    std::string_view("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 12));
}

}  // namespace
