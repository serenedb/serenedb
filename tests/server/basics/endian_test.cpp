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

#include "basics/endian.h"
#include "gtest/gtest.h"
#include "rocksdb_engine_catalog/rocksdb_format.h"

using namespace sdb;
using namespace sdb::basics;

/// test RocksDBFormat functions class
class RocksDBFormatTest : public ::testing::Test {
 protected:
  std::string _out;
};

TEST_F(RocksDBFormatTest, little_endian) {
  rocksutils::UintToPersistentLittleEndian<uint64_t>(_out, 1);
  EXPECT_EQ(_out.size(), 8);
  EXPECT_EQ(rocksutils::UintFromPersistentLittleEndian<uint64_t>(_out.data()),
            1);
  _out.clear();

  rocksutils::UintToPersistentLittleEndian<uint64_t>(_out, 1337);
  EXPECT_EQ(_out.size(), 8);
  EXPECT_EQ(rocksutils::UintFromPersistentLittleEndian<uint64_t>(_out.data()),
            1337);
  _out.clear();

  rocksutils::UintToPersistentLittleEndian<uint64_t>(_out, 1212321);
  EXPECT_EQ(_out.size(), 8);
  EXPECT_EQ(rocksutils::UintFromPersistentLittleEndian<uint64_t>(_out.data()),
            1212321);
  _out.clear();

  rocksutils::UintToPersistentLittleEndian<uint32_t>(_out, 88888);
  EXPECT_EQ(_out.size(), 4);
  EXPECT_EQ(rocksutils::UintFromPersistentLittleEndian<uint32_t>(_out.data()),
            88888);
  _out.clear();
}

TEST_F(RocksDBFormatTest, big_endian) {
  rocksutils::UintToPersistentBigEndian<uint64_t>(_out, 1);
  EXPECT_EQ(_out.size(), 8);
  EXPECT_EQ(rocksutils::UintFromPersistentBigEndian<uint64_t>(_out.data()), 1);
  _out.clear();

  rocksutils::UintToPersistentBigEndian<uint64_t>(_out, 1337);
  EXPECT_EQ(_out.size(), 8);
  EXPECT_EQ(rocksutils::UintFromPersistentBigEndian<uint64_t>(_out.data()),
            1337);
  _out.clear();

  rocksutils::UintToPersistentBigEndian<uint64_t>(_out, 1212321);
  EXPECT_EQ(_out.size(), 8);
  EXPECT_EQ(rocksutils::UintFromPersistentBigEndian<uint64_t>(_out.data()),
            1212321);
  _out.clear();

  rocksutils::UintToPersistentBigEndian<uint32_t>(_out, 88888);
  EXPECT_EQ(_out.size(), 4);
  EXPECT_EQ(rocksutils::UintFromPersistentBigEndian<uint32_t>(_out.data()),
            88888);
  _out.clear();
}

TEST_F(RocksDBFormatTest, specialized_big_endian) {
  rocksutils::Uint32ToPersistent(_out, 1);
  EXPECT_EQ(_out.size(), 4);
  EXPECT_EQ(rocksutils::Uint32FromPersistent(_out.data()), 1);
  _out.clear();

  rocksutils::Uint16ToPersistent(_out, 1337);
  EXPECT_EQ(_out.size(), 2);
  EXPECT_EQ(rocksutils::Uint16FromPersistent(_out.data()), 1337);
  _out.clear();

  rocksutils::Uint64ToPersistent(_out, 1212321);
  EXPECT_EQ(_out.size(), 8);
  EXPECT_EQ(rocksutils::Uint64FromPersistent(_out.data()), 1212321);
  _out.clear();
}
