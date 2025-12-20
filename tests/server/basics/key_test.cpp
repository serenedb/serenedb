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
  key.constructObject(RocksDBEntryType::Schema, ObjectId{23}, ObjectId{42});
  const auto& s2 = key.string();

  EXPECT_EQ(s2.size(), sizeof(char) + sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_EQ(s2, std::string("+\0\0\0\0\0\0\0\x17\0\0\0\0\0\0\0\x2a", 17));

  key.constructObject(RocksDBEntryType::Schema, ObjectId{255}, ObjectId{255});
  const auto& s3 = key.string();

  EXPECT_EQ(s3.size(), sizeof(char) + sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_EQ(s3, std::string("+\0\0\0\0\0\0\0\xff\0\0\0\0\0\0\0\xff", 17));

  key.constructObject(RocksDBEntryType::Schema, ObjectId{256}, ObjectId{257});
  const auto& s4 = key.string();

  EXPECT_EQ(s4.size(), sizeof(char) + sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_EQ(s4, std::string("+\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x01\x01", 17));

  key.constructObject(RocksDBEntryType::Schema, ObjectId{49152},
                      ObjectId{16384});
  const auto& s5 = key.string();

  EXPECT_EQ(s5.size(), sizeof(char) + sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_EQ(s5, std::string("+\0\0\0\0\0\0\xc0\0\0\0\0\0\0\0\x40\0", 17));

  key.constructObject(RocksDBEntryType::Schema, ObjectId{12345678901},
                      ObjectId{987654321});
  const auto& s6 = key.string();

  EXPECT_EQ(s6.size(), sizeof(char) + sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_TRUE(
    s6 ==
    std::string("+\0\0\0\x02\xdf\xdc\x1c\x35\0\0\0\0\x3a\xde\x68\xb1", 17));

  key.constructObject(RocksDBEntryType::Schema, ObjectId{0xf0f1f2f3f4f5f6f7ULL},
                      ObjectId{0xf0f1f2f3f4f5f6f7ULL});
  const auto& s7 = key.string();

  EXPECT_EQ(s7.size(), sizeof(char) + sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_TRUE(
    s7 ==
    std::string(
      "+\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7", 17));
}

TEST_F(RocksDBKeyTestBigEndian, test_collection) {
  static_assert(static_cast<char>(RocksDBEntryType::Collection) == '1', "");

  RocksDBKeyWithBuffer key;
  key.constructSchemaObject(RocksDBEntryType::Collection, ObjectId{23},
                            ObjectId{23}, ObjectId{42});
  EXPECT_EQ(
    key.string(),
    std::string("1\0\0\0\0\0\0\0\x17\0\0\0\0\0\0\0\x17\0\0\0\0\0\0\0\x2a", 25));

  key.constructSchemaObject(RocksDBEntryType::Collection, ObjectId{255},
                            ObjectId{255}, ObjectId{255});
  EXPECT_EQ(
    key.string(),
    std::string("1\0\0\0\0\0\0\0\xff\0\0\0\0\0\0\0\xff\0\0\0\0\0\0\0\xff", 25));

  key.constructSchemaObject(RocksDBEntryType::Collection, ObjectId{256},
                            ObjectId{256}, ObjectId{257});
  EXPECT_EQ(key.string(),
            std::string(
              "1\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x01\x01", 25));

  key.constructSchemaObject(RocksDBEntryType::Collection, ObjectId{49152},
                            ObjectId{49152}, ObjectId{16384});
  EXPECT_EQ(
    key.string(),
    std::string("1\0\0\0\0\0\0\xc0\0\0\0\0\0\0\0\xc0\0\0\0\0\0\0\0\x40\0", 25));

  key.constructSchemaObject(RocksDBEntryType::Collection, ObjectId{12345678901},
                            ObjectId{12345678901}, ObjectId{987654321});
  EXPECT_EQ(key.string(),
            std::string("1\0\0\0\x02\xdf\xdc\x1c\x35\0\0\0\x02\xdf\xdc\x1c\x35"
                        "\0\0\0\0\x3a\xde\x68\xb1",
                        25));

  key.constructSchemaObject(
    RocksDBEntryType::Collection, ObjectId{0xf0f1f2f3f4f5f6f7ULL},
    ObjectId{0xf0f1f2f3f4f5f6f7ULL}, ObjectId{0xf0f1f2f3f4f5f6f7ULL});
  EXPECT_EQ(key.string(),
            std::string("1\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf0\xf1\xf2\xf3\xf4"
                        "\xf5\xf6\xf7\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7",
                        25));
}

TEST_F(RocksDBKeyTestBigEndian, test_document) {
  RocksDBKeyWithBuffer key;
  key.constructDocument(1, RevisionId(0));
  const auto& s1 = key.string();

  EXPECT_EQ(s1.size(), +sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_EQ(s1, std::string("\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\0\0", 16));

  key.constructDocument(23, RevisionId(42));
  const auto& s2 = key.string();

  EXPECT_EQ(s2.size(), +sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_EQ(s2, std::string("\0\0\0\0\0\0\0\x17\0\0\0\0\0\0\0\x2a", 16));

  key.constructDocument(255, RevisionId(255));
  const auto& s3 = key.string();

  EXPECT_EQ(s3.size(), sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_EQ(s3, std::string("\0\0\0\0\0\0\0\xff\0\0\0\0\0\0\0\xff", 16));

  key.constructDocument(256, RevisionId(257));
  const auto& s4 = key.string();

  EXPECT_EQ(s4.size(), sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_EQ(s4, std::string("\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x01\x01", 16));

  key.constructDocument(49152, RevisionId(16384));
  const auto& s5 = key.string();

  EXPECT_EQ(s5.size(), sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_EQ(s5, std::string("\0\0\0\0\0\0\xc0\0\0\0\0\0\0\0\x40\0", 16));

  key.constructDocument(12345678901, RevisionId(987654321));
  const auto& s6 = key.string();

  EXPECT_EQ(s6.size(), sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_TRUE(
    s6 ==
    std::string("\0\0\0\x02\xdf\xdc\x1c\x35\0\0\0\0\x3a\xde\x68\xb1", 16));

  key.constructDocument(0xf0f1f2f3f4f5f6f7ULL,
                        RevisionId(0xf0f1f2f3f4f5f6f7ULL));
  const auto& s7 = key.string();

  EXPECT_EQ(s7.size(), sizeof(uint64_t) + sizeof(uint64_t));
  EXPECT_TRUE(
    s7 ==
    std::string(
      "\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf0\xf1\xf2\xf3\xf4\xf5\xf6\xf7", 16));
}

TEST_F(RocksDBKeyTestBigEndian, test_primary_index) {
  RocksDBKeyWithBuffer key;
  key.constructPrimaryIndexValue(1, std::string_view("abc"));
  const auto& s2 = key.string();

  EXPECT_EQ(s2.size(), sizeof(uint64_t) + strlen("abc"));
  EXPECT_EQ(s2, std::string("\0\0\0\0\0\0\0\1abc", 11));

  key.constructPrimaryIndexValue(1, std::string_view(" "));
  const auto& s3 = key.string();

  EXPECT_EQ(s3.size(), sizeof(uint64_t) + strlen(" "));
  EXPECT_EQ(s3, std::string("\0\0\0\0\0\0\0\1 ", 9));

  key.constructPrimaryIndexValue(1, std::string_view("this is a key"));
  const auto& s4 = key.string();

  EXPECT_EQ(s4.size(), sizeof(uint64_t) + strlen("this is a key"));
  EXPECT_EQ(s4, std::string("\0\0\0\0\0\0\0\1this is a key", 21));

  // 254 bytes
  const char* long_key =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  key.constructPrimaryIndexValue(1, std::string_view(long_key));
  const auto& s5 = key.string();

  EXPECT_EQ(s5.size(), sizeof(uint64_t) + strlen(long_key));
  EXPECT_EQ(s5, std::string("\0\0\0\0\0\0\0\1", 8) + long_key);

  key.constructPrimaryIndexValue(123456789, std::string_view("this is a key"));
  const auto& s6 = key.string();

  EXPECT_EQ(s6.size(), sizeof(uint64_t) + strlen("this is a key"));
  EXPECT_EQ(s6, std::string("\0\0\0\0\x07\x5b\xcd\x15this is a key", 21));
}

TEST_F(RocksDBKeyTestBigEndian, test_edge_index) {
  RocksDBKeyWithBuffer key1;
  key1.constructEdgeIndexValue(1, std::string_view("a/1"), RevisionId(33));
  RocksDBKeyWithBuffer key2;
  key2.constructEdgeIndexValue(1, std::string_view("b/1"), RevisionId(33));
  const auto& s1 = key1.string();

  EXPECT_TRUE(s1.size() == sizeof(uint64_t) + strlen("a/1") + sizeof(char) +
                             sizeof(uint64_t) + sizeof(char));
  EXPECT_EQ(s1, std::string("\0\0\0\0\0\0\0\1a/1\0\0\0\0\0\0\0\0!\xff", 21));
  EXPECT_TRUE(key2.string().size() == sizeof(uint64_t) + strlen("b/1") +
                                        sizeof(char) + sizeof(uint64_t) +
                                        sizeof(char));
  EXPECT_TRUE(key2.string() ==
              std::string("\0\0\0\0\0\0\0\1b/1\0\0\0\0\0\0\0\0!\xff", 21));

  EXPECT_EQ(RocksDBKey::vertexId(key1.string()).compare("a/1"), 0);
  EXPECT_EQ(RocksDBKey::vertexId(key2.string()).compare("b/1"), 0);

  // check the variable length edge prefix
  auto pe = std::make_unique<RocksDBPrefixExtractor>();
  EXPECT_TRUE(pe->InDomain(key1.string()));

  rocksdb::Slice prefix = pe->Transform(key1.string());
  EXPECT_EQ(prefix.size(), sizeof(uint64_t) + strlen("a/1") + sizeof(char));
  EXPECT_EQ(memcmp(s1.data(), prefix.data(), prefix.size()), 0);

  const rocksdb::Comparator* cmp = rocksdb::BytewiseComparator();
  EXPECT_LT(cmp->Compare(key1.string(), key2.string()), 0);
}

class RocksDBKeyBoundsTestBigEndian : public ::testing::Test {
 protected:
  RocksDBKeyBoundsTestBigEndian() {}
};

/// test edge index with dynamic prefix extractor
TEST_F(RocksDBKeyBoundsTestBigEndian, test_edge_index) {
  RocksDBKeyWithBuffer key1;
  key1.constructEdgeIndexValue(1, std::string_view("a/1"), RevisionId(33));
  // check the variable length edge prefix
  auto pe = std::make_unique<RocksDBPrefixExtractor>();
  ASSERT_TRUE(pe->InDomain(key1.string()));

  // check the correct key bounds comparisons
  RocksDBKeyBounds bounds = RocksDBKeyBounds::EdgeIndex(1);
  ASSERT_FALSE(pe->InDomain(bounds.start()));
  ASSERT_FALSE(pe->InDomain(bounds.end()));
  rocksdb::Slice prefix_begin = pe->Transform(bounds.start());
  rocksdb::Slice prefix_end = pe->Transform(bounds.end());
  ASSERT_FALSE(pe->InDomain(prefix_begin));
  ASSERT_FALSE(pe->InDomain(prefix_end));
  ASSERT_EQ(
    memcmp(bounds.start().data(), prefix_begin.data(), prefix_begin.size()), 0);
  ASSERT_EQ(memcmp(bounds.end().data(), prefix_end.data(), prefix_end.size()),
            0);

  // check our assumptions about bound construction
  const rocksdb::Comparator* cmp = rocksdb::BytewiseComparator();
  EXPECT_LT(cmp->Compare(prefix_begin, prefix_end), 0);
  EXPECT_LT(cmp->Compare(prefix_begin, key1.string()), 0);
  EXPECT_GT(cmp->Compare(prefix_end, key1.string()), 0);

  RocksDBKeyWithBuffer key2;
  key2.constructEdgeIndexValue(1, std::string_view("c/1000"), RevisionId(33));
  EXPECT_LT(cmp->Compare(prefix_begin, key2.string()), 0);
  EXPECT_GT(cmp->Compare(prefix_end, key2.string()), 0);

  // test higher prefix
  RocksDBKeyWithBuffer key3;
  key3.constructEdgeIndexValue(1, std::string_view("c/1000"), RevisionId(33));
  EXPECT_LT(cmp->Compare(prefix_begin, key3.string()), 0);
  EXPECT_GT(cmp->Compare(prefix_end, key3.string()), 0);
}

/// test hash index with prefix over indexed slice
TEST_F(RocksDBKeyBoundsTestBigEndian, test_hash_index) {
  vpack::Builder lower;
  lower.add(vpack::Value(vpack::ValueType::Array));
  lower.add("a");
  lower.close();
  vpack::Builder higher;
  higher.add(vpack::Value(vpack::ValueType::Array));
  higher.add("b");
  higher.close();

  RocksDBKeyWithBuffer key1, key2, key3;
  key1.constructVPackIndexValue(1, lower.slice(), RevisionId(33));
  key2.constructVPackIndexValue(1, higher.slice(), RevisionId(33));
  key3.constructVPackIndexValue(2, lower.slice(), RevisionId(16));

  // check the variable length edge prefix
  std::unique_ptr<const rocksdb::SliceTransform> pe(
    rocksdb::NewFixedPrefixTransform(RocksDBKey::objectIdSize()));

  EXPECT_TRUE(pe->InDomain(key1.string()));

  // check the correct key bounds comparisons
  RocksDBKeyBounds bounds = RocksDBKeyBounds::VPackIndex(1, false);
  EXPECT_TRUE(pe->InDomain(bounds.start()));
  EXPECT_TRUE(pe->InDomain(bounds.end()));
  rocksdb::Slice prefix_begin = pe->Transform(bounds.start());
  rocksdb::Slice prefix_end = pe->Transform(bounds.end());
  EXPECT_TRUE(pe->InDomain(prefix_begin));
  EXPECT_TRUE(pe->InDomain(prefix_end));
  EXPECT_EQ(
    memcmp(bounds.start().data(), prefix_begin.data(), prefix_begin.size()), 0);
  EXPECT_EQ(memcmp(bounds.end().data(), prefix_end.data(), prefix_end.size()),
            0);
  EXPECT_EQ(prefix_begin.data()[0], '\0');
  EXPECT_EQ(prefix_end.data()[0], '\0');
  EXPECT_EQ(prefix_begin.data()[prefix_begin.size() - 2], '\x00');
  EXPECT_EQ(prefix_begin.data()[prefix_begin.size() - 1], '\x01');
  EXPECT_EQ(prefix_end.data()[prefix_end.size() - 2], '\x00');
  EXPECT_EQ(prefix_end.data()[prefix_end.size() - 1], '\x02');

  // prefix is just object id
  auto cmp = std::make_unique<RocksDBVPackComparator>();
  EXPECT_LT(cmp->Compare(prefix_begin, prefix_end), 0);
  EXPECT_LT(cmp->Compare(prefix_begin, key1.string()), 0);
  EXPECT_GT(cmp->Compare(prefix_end, key1.string()), 0);

  EXPECT_LT(cmp->Compare(key1.string(), key2.string()), 0);
  EXPECT_LT(cmp->Compare(key2.string(), key3.string()), 0);
  EXPECT_LT(cmp->Compare(key1.string(), key3.string()), 0);

  EXPECT_LT(cmp->Compare(prefix_end, key3.string()), 0);

  // check again with reverse full iteration bounds
  bounds = RocksDBKeyBounds::VPackIndex(1, true);
  EXPECT_TRUE(pe->InDomain(bounds.start()));
  EXPECT_TRUE(pe->InDomain(bounds.end()));
  prefix_begin = pe->Transform(bounds.start());
  prefix_end = pe->Transform(bounds.end());
  EXPECT_TRUE(pe->InDomain(prefix_begin));
  EXPECT_TRUE(pe->InDomain(prefix_end));
  EXPECT_EQ(
    memcmp(bounds.start().data(), prefix_begin.data(), prefix_begin.size()), 0);
  EXPECT_EQ(memcmp(bounds.end().data(), prefix_end.data(), prefix_end.size()),
            0);
  EXPECT_EQ(prefix_begin.data()[0], '\0');
  EXPECT_EQ(prefix_end.data()[0], '\0');
  EXPECT_EQ(prefix_begin.data()[prefix_begin.size() - 2], '\x00');
  EXPECT_EQ(prefix_begin.data()[prefix_begin.size() - 1], '\x01');
  EXPECT_EQ(prefix_end.data()[prefix_end.size() - 2], '\x00');
  EXPECT_EQ(prefix_end.data()[prefix_end.size() - 1], '\x01');

  EXPECT_EQ(cmp->Compare(prefix_begin, prefix_end), 0);
  EXPECT_LT(cmp->Compare(prefix_begin, key1.string()), 0);
  EXPECT_LT(cmp->Compare(prefix_end, key1.string()), 0);

  EXPECT_LT(cmp->Compare(key1.string(), key2.string()), 0);
  EXPECT_LT(cmp->Compare(key2.string(), key3.string()), 0);
  EXPECT_LT(cmp->Compare(key1.string(), key3.string()), 0);

  EXPECT_LT(cmp->Compare(prefix_end, key3.string()), 0);

  vpack::Builder a;
  a.add(vpack::Value(vpack::ValueType::Array));
  a.add(1);
  a.close();
  vpack::Builder b;
  b.add(vpack::Value(vpack::ValueType::Array));
  b.add(3);
  b.close();
  vpack::Builder c;
  c.add(vpack::Value(vpack::ValueType::Array));
  c.add(5);
  c.close();

  RocksDBKeyWithBuffer key4, key5, key6, key7;
  key4.constructVPackIndexValue(1, a.slice(), RevisionId(18));
  key5.constructVPackIndexValue(1, b.slice(), RevisionId(60));
  key6.constructVPackIndexValue(1, b.slice(), RevisionId(90));
  key7.constructVPackIndexValue(1, c.slice(), RevisionId(12));

  bounds = RocksDBKeyBounds::VPackIndex(1, a.slice(), c.slice());
  EXPECT_LT(cmp->Compare(bounds.start(), key4.string()), 0);
  EXPECT_LT(cmp->Compare(key4.string(), bounds.end()), 0);
  EXPECT_LT(cmp->Compare(bounds.start(), key5.string()), 0);
  EXPECT_LT(cmp->Compare(key5.string(), bounds.end()), 0);
  EXPECT_LT(cmp->Compare(bounds.start(), key6.string()), 0);
  EXPECT_LT(cmp->Compare(key6.string(), bounds.end()), 0);
  EXPECT_LT(cmp->Compare(bounds.start(), key7.string()), 0);
  EXPECT_LT(cmp->Compare(key7.string(), bounds.end()), 0);

  EXPECT_LT(cmp->Compare(key4.string(), key5.string()), 0);
  EXPECT_LT(cmp->Compare(key5.string(), key6.string()), 0);
  EXPECT_LT(cmp->Compare(key4.string(), key6.string()), 0);
  EXPECT_LT(cmp->Compare(key6.string(), key7.string()), 0);
  EXPECT_LT(cmp->Compare(key4.string(), key7.string()), 0);
}
