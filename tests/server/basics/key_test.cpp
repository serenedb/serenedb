////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "basics/common.h"
#include "basics/exceptions.h"
#include "catalog/identifiers/object_id.h"
#include "gtest/gtest.h"
#include "rocksdb_engine_catalog/rocksdb_comparator.h"
#include "rocksdb_engine_catalog/rocksdb_format.h"
#include "rocksdb_engine_catalog/rocksdb_key.h"
#include "rocksdb_engine_catalog/rocksdb_key_bounds.h"
#include "rocksdb_engine_catalog/rocksdb_prefix_extractor.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "vpack/vpack_helper.h"

using namespace sdb;

class RocksDBKeyTestBigEndian : public ::testing::Test {
 protected:
  RocksDBKeyTestBigEndian() {}
};

TEST_F(RocksDBKeyTestBigEndian, test_database) {
  static_assert(static_cast<char>(RocksDBEntryType::Database) == '0', "");

  RocksDBKeyWithBuffer key;
  key.constructDatabase(ObjectId{1});
  const auto& s2 = key.string();

  EXPECT_EQ(s2.size(), sizeof(char) + sizeof(uint64_t));
  EXPECT_EQ(s2, std::string("0\0\0\0\0\0\0\0\x01", 9));

  key.constructDatabase(ObjectId{255});
  const auto& s3 = key.string();

  EXPECT_EQ(s3.size(), sizeof(char) + sizeof(uint64_t));
  EXPECT_EQ(s3, std::string("0\0\0\0\0\0\0\0\xff", 9));

  key.constructDatabase(ObjectId{256});
  const auto& s4 = key.string();

  EXPECT_EQ(s4.size(), sizeof(char) + sizeof(uint64_t));
  EXPECT_EQ(s4, std::string("0\0\0\0\0\0\0\x01\0", 9));

  key.constructDatabase(ObjectId{49152});
  const auto& s5 = key.string();

  EXPECT_EQ(s5.size(), sizeof(char) + sizeof(uint64_t));
  EXPECT_EQ(s5, std::string("0\0\0\0\0\0\0\xc0\0", 9));

  key.constructDatabase(ObjectId{12345678901});
  const auto& s6 = key.string();

  EXPECT_EQ(s6.size(), sizeof(char) + sizeof(uint64_t));
  EXPECT_EQ(s6, std::string("0\0\0\0\x02\xdf\xdc\x1c\x35", 9));

  key.constructDatabase(ObjectId{0xf0f1f2f3f4f5f6f7ULL});
  const auto& s7 = key.string();

  EXPECT_EQ(s7.size(), sizeof(char) + sizeof(uint64_t));
  EXPECT_EQ(s7, std::string("0\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7", 9));
}

TEST_F(RocksDBKeyTestBigEndian, test_schema) {
  static_assert(static_cast<char>(RocksDBEntryType::Schema) == '+');

  RocksDBKeyWithBuffer key;
  key.constructDatabaseObject(RocksDBEntryType::Schema, ObjectId{23},
                              ObjectId{42});
  const auto& s2 = key.string();

  EXPECT_EQ(s2.size(), sizeof(char) + sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_EQ(s2, std::string("+\0\0\0\0\0\0\0\x17\0\0\0\0\0\0\0\x2a", 17));

  key.constructDatabaseObject(RocksDBEntryType::Schema, ObjectId{255},
                              ObjectId{255});
  const auto& s3 = key.string();

  EXPECT_EQ(s3.size(), sizeof(char) + sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_EQ(s3, std::string("+\0\0\0\0\0\0\0\xff\0\0\0\0\0\0\0\xff", 17));

  key.constructDatabaseObject(RocksDBEntryType::Schema, ObjectId{256},
                              ObjectId{257});
  const auto& s4 = key.string();

  EXPECT_EQ(s4.size(), sizeof(char) + sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_EQ(s4, std::string("+\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x01\x01", 17));

  key.constructDatabaseObject(RocksDBEntryType::Schema, ObjectId{49152},
                              ObjectId{16384});
  const auto& s5 = key.string();

  EXPECT_EQ(s5.size(), sizeof(char) + sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_EQ(s5, std::string("+\0\0\0\0\0\0\xc0\0\0\0\0\0\0\0\x40\0", 17));

  key.constructDatabaseObject(RocksDBEntryType::Schema, ObjectId{12345678901},
                              ObjectId{987654321});
  const auto& s6 = key.string();

  EXPECT_EQ(s6.size(), sizeof(char) + sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_TRUE(
    s6 ==
    std::string("+\0\0\0\x02\xdf\xdc\x1c\x35\0\0\0\0\x3a\xde\x68\xb1", 17));

  key.constructDatabaseObject(RocksDBEntryType::Schema,
                              ObjectId{0xf0f1f2f3f4f5f6f7ULL},
                              ObjectId{0xf0f1f2f3f4f5f6f7ULL});
  const auto& s7 = key.string();

  EXPECT_EQ(s7.size(), sizeof(char) + sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_TRUE(
    s7 ==
    std::string(
      "+\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7", 17));
}

TEST_F(RocksDBKeyTestBigEndian, test_collection) {
  static_assert(static_cast<char>(RocksDBEntryType::Table) == '1', "");

  RocksDBKeyWithBuffer key;
  key.constructSchemaObject(RocksDBEntryType::Table, ObjectId{23}, ObjectId{23},
                            ObjectId{42});
  EXPECT_EQ(
    key.string(),
    std::string("1\0\0\0\0\0\0\0\x17\0\0\0\0\0\0\0\x17\0\0\0\0\0\0\0\x2a", 25));

  key.constructSchemaObject(RocksDBEntryType::Table, ObjectId{255},
                            ObjectId{255}, ObjectId{255});
  EXPECT_EQ(
    key.string(),
    std::string("1\0\0\0\0\0\0\0\xff\0\0\0\0\0\0\0\xff\0\0\0\0\0\0\0\xff", 25));

  key.constructSchemaObject(RocksDBEntryType::Table, ObjectId{256},
                            ObjectId{256}, ObjectId{257});
  EXPECT_EQ(key.string(),
            std::string(
              "1\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x01\x01", 25));

  key.constructSchemaObject(RocksDBEntryType::Table, ObjectId{49152},
                            ObjectId{49152}, ObjectId{16384});
  EXPECT_EQ(
    key.string(),
    std::string("1\0\0\0\0\0\0\xc0\0\0\0\0\0\0\0\xc0\0\0\0\0\0\0\0\x40\0", 25));

  key.constructSchemaObject(RocksDBEntryType::Table, ObjectId{12345678901},
                            ObjectId{12345678901}, ObjectId{987654321});
  EXPECT_EQ(key.string(),
            std::string("1\0\0\0\x02\xdf\xdc\x1c\x35\0\0\0\x02\xdf\xdc\x1c\x35"
                        "\0\0\0\0\x3a\xde\x68\xb1",
                        25));

  key.constructSchemaObject(
    RocksDBEntryType::Table, ObjectId{0xf0f1f2f3f4f5f6f7ULL},
    ObjectId{0xf0f1f2f3f4f5f6f7ULL}, ObjectId{0xf0f1f2f3f4f5f6f7ULL});
  EXPECT_EQ(key.string(),
            std::string("1\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf0\xf1\xf2\xf3\xf4"
                        "\xf5\xf6\xf7\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7",
                        25));
}
