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

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "catalog/store/store.h"

namespace {

using sdb::ObjectId;
using sdb::catalog::CatalogStore;
using sdb::catalog::ObjectType;

using DefMap = std::map<std::tuple<uint64_t, uint8_t, uint64_t>, std::string>;

DefMap CollectDefs(CatalogStore& store, ObjectId parent, ObjectType type) {
  DefMap defs;
  store.VisitBoot(parent, type, [&](CatalogStore::Key key, std::string_view v) {
    defs[{key.parent_id.id(), static_cast<uint8_t>(key.type), key.id.id()}] =
      std::string{v};
    return true;
  });
  return defs;
}

class CatalogStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _dir = std::filesystem::path{::testing::TempDir()} /
           ("catalog_store_" +
            std::string{
              ::testing::UnitTest::GetInstance()->current_test_info()->name()});
    std::filesystem::remove_all(_dir);
    std::filesystem::create_directories(_dir);
  }

  void TearDown() override { std::filesystem::remove_all(_dir); }

  std::unique_ptr<CatalogStore> Open() {
    auto store = std::make_unique<CatalogStore>();
    store->Initialize(_dir.string());
    return store;
  }

  static std::unique_ptr<CatalogStore> Reopen(
    std::unique_ptr<CatalogStore> store, const std::filesystem::path& dir) {
    store->Shutdown();
    store.reset();
    auto reopened = std::make_unique<CatalogStore>();
    reopened->Initialize(dir.string());
    return reopened;
  }

  std::filesystem::path _dir;
};

TEST_F(CatalogStoreTest, records_survive_reopen) {
  auto store = Open();
  const ObjectId parent{10};
  store->CreateDefinition(parent, ObjectType::Table, ObjectId{11}, "table-a");
  store->CreateDefinition(parent, ObjectType::Table, ObjectId{12}, "table-b");
  store->CreateDefinition(parent, ObjectType::Sequence, ObjectId{13}, "seq");
  store->PutSequenceValue(ObjectId{13}, 42);
  store->DropDefinition(parent, ObjectType::Table, ObjectId{12});

  store = Reopen(std::move(store), _dir);

  const auto tables = CollectDefs(*store, parent, ObjectType::Table);
  ASSERT_EQ(tables.size(), 1);
  EXPECT_EQ(tables.begin()->second, "table-a");
  EXPECT_EQ(std::get<2>(tables.begin()->first), 11);

  uint64_t value = 0;
  EXPECT_TRUE(store->TryGetBootSequenceValue(ObjectId{13}, value));
  EXPECT_EQ(value, 42);
  store->Shutdown();
}

TEST_F(CatalogStoreTest, drop_entry_sweeps_parent) {
  auto store = Open();
  const ObjectId schema{20};
  store->CreateDefinition(schema, ObjectType::Table, ObjectId{21}, "t");
  store->CreateDefinition(schema, ObjectType::Sequence, ObjectId{22}, "s");
  store->CreateDefinition(schema, ObjectType::PgSqlView, ObjectId{23}, "v");
  store->DropEntry(schema);

  store = Reopen(std::move(store), _dir);

  EXPECT_TRUE(CollectDefs(*store, schema, ObjectType::Table).empty());
  EXPECT_TRUE(CollectDefs(*store, schema, ObjectType::Sequence).empty());
  EXPECT_TRUE(CollectDefs(*store, schema, ObjectType::PgSqlView).empty());
  store->Shutdown();
}

TEST_F(CatalogStoreTest, sequence_updates_keep_latest) {
  auto store = Open();
  store->PutSequenceValue(ObjectId{30}, 1);
  store->PutSequenceValue(ObjectId{30}, 7);
  store->PutSequenceValue(ObjectId{30}, 100);
  store->PutSequenceValue(ObjectId{31}, 5);
  store->DropSequence(ObjectId{31});

  store = Reopen(std::move(store), _dir);

  uint64_t value = 0;
  EXPECT_TRUE(store->TryGetBootSequenceValue(ObjectId{30}, value));
  EXPECT_EQ(value, 100);
  EXPECT_FALSE(store->TryGetBootSequenceValue(ObjectId{31}, value));
  store->Shutdown();
}

TEST_F(CatalogStoreTest, compaction_preserves_state) {
  auto store = Open();
  const ObjectId parent{40};
  const std::string def(512, 'x');
  // Enough dead records to cross the compaction threshold (dead > live and
  // >= 1MB on disk): rewrite the same key many times, keep a small live set.
  for (uint64_t i = 0; i < 4096; ++i) {
    store->CreateDefinition(parent, ObjectType::Table, ObjectId{41},
                            def + std::to_string(i));
  }
  store->CreateDefinition(parent, ObjectType::Table, ObjectId{42}, "live");
  store->PutSequenceValue(ObjectId{43}, 9);

  const auto stats = store->WalStats();
  EXPECT_LT(stats.size_on_disk, 1024 * 1024)
    << "compaction should have rewritten the wal";

  store = Reopen(std::move(store), _dir);

  const auto tables = CollectDefs(*store, parent, ObjectType::Table);
  ASSERT_EQ(tables.size(), 2);
  EXPECT_EQ(tables.at({parent.id(), static_cast<uint8_t>(ObjectType::Table),
                       41}),
            def + "4095");
  EXPECT_EQ(tables.at({parent.id(), static_cast<uint8_t>(ObjectType::Table),
                       42}),
            "live");
  uint64_t value = 0;
  EXPECT_TRUE(store->TryGetBootSequenceValue(ObjectId{43}, value));
  EXPECT_EQ(value, 9);
  store->Shutdown();
}

TEST_F(CatalogStoreTest, parse_frame_round_trips_wal_records) {
  auto store = Open();
  const ObjectId parent{50};
  store->CreateDefinition(parent, ObjectType::Table, ObjectId{51}, "def-bytes");
  store->Shutdown();
  store.reset();

  size_t frames = 0;
  std::optional<CatalogStore::Entry> put;
  sdb::catalog::CatalogWal::Scan(
    (_dir / "engine_catalog").string(), [&](std::span<const uint8_t> frame) {
      for (auto& record : CatalogStore::ParseFrame(frame)) {
        if (record.op == CatalogStore::Op::PutDefinition &&
            record.key.type == ObjectType::Table) {
          put = std::move(record);
        }
      }
      ++frames;
    });

  EXPECT_GT(frames, 0);
  ASSERT_TRUE(put.has_value());
  EXPECT_EQ(put->key.parent_id.id(), 50);
  EXPECT_EQ(put->key.id.id(), 51);
  EXPECT_EQ(put->def, "def-bytes");
}

}  // namespace
