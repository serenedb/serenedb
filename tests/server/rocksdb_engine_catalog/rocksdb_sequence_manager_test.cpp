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

#include "rocksdb_engine_catalog/rocksdb_sequence_manager.h"

#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <utilities/merge_operators/uint64add.h>

#include <atomic>
#include <filesystem>
#include <random>
#include <set>
#include <thread>
#include <utility>

namespace sdb {

namespace {

class SequenceFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    _path = std::filesystem::temp_directory_path() /
            ("serenedb_seq_test_" +
             std::to_string(::testing::UnitTest::GetInstance()->random_seed()) +
             "_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
    std::filesystem::remove_all(_path);
    OpenDb();
  }

  void TearDown() override {
    CloseDb();
    std::filesystem::remove_all(_path);
  }

  void OpenDb() {
    rocksdb::DBOptions db_opts;
    db_opts.create_if_missing = true;
    db_opts.create_missing_column_families = true;

    rocksdb::ColumnFamilyOptions default_cf_opts;
    rocksdb::ColumnFamilyOptions seq_cf_opts;
    seq_cf_opts.merge_operator = std::make_shared<rocksdb::UInt64AddOperator>();

    std::vector<rocksdb::ColumnFamilyDescriptor> cfs{
      {rocksdb::kDefaultColumnFamilyName, default_cf_opts},
      {"sequences", seq_cf_opts},
    };
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    auto status = rocksdb::DB::Open(db_opts, _path.string(), cfs, &handles, &_db);
    ASSERT_TRUE(status.ok()) << status.ToString();
    _default_cf = handles[0];
    _seq_cf = handles[1];
  }

  void CloseDb() {
    if (_db != nullptr) {
      _db->DestroyColumnFamilyHandle(_default_cf);
      _db->DestroyColumnFamilyHandle(_seq_cf);
      delete _db;
      _db = nullptr;
    }
  }

  std::filesystem::path _path;
  rocksdb::DB* _db = nullptr;
  rocksdb::ColumnFamilyHandle* _default_cf = nullptr;
  rocksdb::ColumnFamilyHandle* _seq_cf = nullptr;
};

}  // namespace

TEST_F(SequenceFixture, ReserveOneIsContiguousFromOne) {
  TableSequence seq{_db, _seq_cf, ObjectId{42}};
  for (uint64_t expected = 1; expected <= 4096; ++expected) {
    EXPECT_EQ(seq.Reserve(1), expected);
  }
}

TEST_F(SequenceFixture, MixedSizesAreContiguous) {
  TableSequence seq{_db, _seq_cf, ObjectId{7}};
  EXPECT_EQ(seq.Reserve(7), 1u);     // [1..7]
  EXPECT_EQ(seq.Reserve(2048), 8u);  // [8..2055]
  EXPECT_EQ(seq.Reserve(1), 2056u);  // [2056..2056]
  EXPECT_EQ(seq.Reserve(100), 2057u);
}

TEST_F(SequenceFixture, TwoTablesAreIndependent) {
  TableSequence a{_db, _seq_cf, ObjectId{1}};
  TableSequence b{_db, _seq_cf, ObjectId{2}};

  EXPECT_EQ(a.Reserve(10), 1u);
  EXPECT_EQ(b.Reserve(5), 1u);
  EXPECT_EQ(a.Reserve(3), 11u);
  EXPECT_EQ(b.Reserve(7), 6u);
}

TEST_F(SequenceFixture, ConcurrentReservesAreUnique) {
  TableSequence seq{_db, _seq_cf, ObjectId{99}};
  constexpr int kThreads = 16;
  constexpr int kCallsPerThread = 1000;

  std::vector<std::vector<std::pair<uint64_t, uint64_t>>> ranges(kThreads);
  std::atomic<uint64_t> total_reserved{0};

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::mt19937 rng{static_cast<unsigned>(t * 31 + 7)};
      std::uniform_int_distribution<uint64_t> sz_dist{1, 16};
      ranges[t].reserve(kCallsPerThread);
      for (int i = 0; i < kCallsPerThread; ++i) {
        uint64_t n = sz_dist(rng);
        uint64_t base = seq.Reserve(n);
        ranges[t].emplace_back(base, base + n);
        total_reserved.fetch_add(n);
      }
    });
  }
  for (auto& th : threads) th.join();

  // Verify all reserved IDs are unique and contiguous from 1.
  std::set<uint64_t> all_ids;
  uint64_t max_id = 0;
  for (const auto& thread_ranges : ranges) {
    for (auto [lo, hi] : thread_ranges) {
      for (uint64_t id = lo; id < hi; ++id) {
        ASSERT_TRUE(all_ids.insert(id).second) << "duplicate id " << id;
        max_id = std::max(max_id, id);
      }
    }
  }
  EXPECT_EQ(all_ids.size(), total_reserved.load());
  EXPECT_EQ(max_id, total_reserved.load());
  // Every id from 1..max must be present (no gaps).
  EXPECT_EQ(*all_ids.begin(), 1u);
}

TEST_F(SequenceFixture, PersistsAcrossReopen) {
  {
    TableSequence seq{_db, _seq_cf, ObjectId{55}};
    EXPECT_EQ(seq.Reserve(1000), 1u);  // [1..1000]
    EXPECT_EQ(seq.Reserve(1), 1001u);
  }
  // Force flush then reopen.
  ASSERT_TRUE(_db->Flush(rocksdb::FlushOptions{}, _seq_cf).ok());
  CloseDb();
  OpenDb();

  TableSequence seq{_db, _seq_cf, ObjectId{55}};
  EXPECT_EQ(seq.Reserve(1), 1002u);
  EXPECT_EQ(seq.Reserve(10), 1003u);
}

}  // namespace sdb
