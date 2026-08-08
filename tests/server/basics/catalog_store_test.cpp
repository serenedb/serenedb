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
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "catalog/sequence.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "catalog/table_options.h"

namespace {

using duckdb::CatalogType;
using sdb::ObjectId;
using sdb::catalog::CatalogStore;

using DefMap = std::map<uint64_t, std::string>;

constexpr auto kCreate = sdb::catalog::wal::PutMode::Create;
constexpr auto kReplace = sdb::catalog::wal::PutMode::Replace;

// The definitions a replay hands the catalog. The store keeps none of its
// own -- the catalog is where one lives -- so this fixture, which has no
// catalog, is the applier: a definition record registers its object under a
// key, a removal record retires it. Exactly what Catalog::ReplayRecords does
// with the same records.
class Applier {
 public:
  void operator()(std::span<const sdb::catalog::wal::Entry> entries) {
    namespace wal = sdb::catalog::wal;
    for (const auto& entry : entries) {
      std::visit(
        [&](const auto& e) {
          using T = std::decay_t<decltype(e)>;
          if constexpr (std::is_same_v<T, wal::PutTable>) {
            Put(e.schema_id, duckdb::CatalogType::TABLE_ENTRY, e.id, e.info);
            for (const auto& seq : e.sequences) {
              Put(e.schema_id, duckdb::CatalogType::SEQUENCE_ENTRY, seq.id,
                  seq.info);
            }
          } else if constexpr (std::is_same_v<T, wal::PutEntry>) {
            Put(e.parent_id, e.type, e.id, e.info);
          } else if constexpr (std::is_same_v<T, wal::DropObject>) {
            _defs.erase({e.parent_id.id(), e.type, e.id.id()});
          } else if constexpr (std::is_same_v<T, wal::DropChildren>) {
            DropChildren(e.parent_id);
          } else if constexpr (std::is_same_v<T, wal::DropPrepare>) {
            if (e.subtree) {
              for (const auto& node : *e.subtree) {
                _defs.erase({node.parent_id.id(), node.type, node.id.id()});
              }
            }
            _defs.erase({e.parent_id.id(), e.type, e.id.id()});
          }
        },
        entry);
    }
  }

  DefMap Defs(ObjectId parent, duckdb::CatalogType type) const {
    DefMap result;
    for (const auto& [key, name] : _defs) {
      const auto& [parent_id, kind, id] = key;
      if (parent_id == parent.id() && kind == type) {
        result[id] = name;
      }
    }
    return result;
  }

 private:
  using Key = std::tuple<uint64_t, duckdb::CatalogType, uint64_t>;

  // Every record's definition is a CreateInfo, so the name it round-trips is
  // read off whichever kind it turns out to be.
  void Put(ObjectId parent, duckdb::CatalogType type, ObjectId id,
           const std::shared_ptr<const duckdb::CreateInfo>& info) {
    std::string name;
    if (const auto* schema =
          dynamic_cast<const duckdb::CreateSchemaInfo*>(info.get());
        schema != nullptr) {
      name = std::string{sdb::catalog::SchemaNameOf(*schema)};
    } else if (const auto* sequence =
                 dynamic_cast<const duckdb::CreateSequenceInfo*>(info.get());
               sequence != nullptr) {
      name = std::string{sdb::catalog::SequenceNameOf(*sequence)};
    } else if (const auto* table =
                 dynamic_cast<const duckdb::CreateTableInfo*>(info.get());
               table != nullptr) {
      name = std::string{sdb::catalog::TableNameOf(*table)};
    }
    _defs[{parent.id(), type, id.id()}] = std::move(name);
  }

  void DropChildren(ObjectId parent) {
    for (auto it = _defs.begin(); it != _defs.end();) {
      it =
        std::get<0>(it->first) == parent.id() ? _defs.erase(it) : std::next(it);
    }
  }

  std::map<Key, std::string> _defs;
};

// Entries carry definitions, so the store round-trips one only if it can
// rebuild it: these name each definition after the value the test asserts on.
sdb::catalog::TableInfoRef MakeTable(ObjectId schema, ObjectId id,
                                     std::string_view name) {
  auto info = sdb::catalog::NewTableInfo();
  info->SetTableName(duckdb::Identifier{name});
  sdb::catalog::SetIdentity(*info, id, schema);
  return info;
}

// PutTable takes the definition by reference and the owner beside it.
void PutTable(CatalogStore& store, const duckdb::CreateTableInfo& table,
              sdb::catalog::wal::PutMode mode) {
  store.Write([&](CatalogStore::WriteContext& ctx) {
    ctx.catalog().PutTable(table, mode, sdb::catalog::Permissions{ObjectId{1}});
  });
}

std::shared_ptr<const duckdb::CreateSequenceInfo> MakeSequence(
  ObjectId schema, ObjectId id, std::string_view name) {
  sdb::catalog::SequenceOptions options;
  options.name = std::string{name};
  return sdb::catalog::MakeSequenceInfo(id, schema, options);
}

// A sequence's entry is the object, so its definition goes through PutEntry.
void PutSequence(CatalogStore& store, ObjectId schema, ObjectId id,
                 std::string_view name) {
  store.Write([&](CatalogStore::WriteContext& ctx) {
    ctx.catalog().PutEntry(schema, duckdb::CatalogType::SEQUENCE_ENTRY, id,
                           kCreate, MakeSequence(schema, id, name));
  });
}

// A schema is a catalog entry built from its CreateInfo, so it goes through
// PutEntry rather than PutObject -- there is no schema object to hand over.
void PutSchema(CatalogStore& store, ObjectId db, ObjectId id,
               std::string_view name) {
  store.Write([&](CatalogStore::WriteContext& ctx) {
    ctx.catalog().PutEntry(db, duckdb::CatalogType::SCHEMA_ENTRY, id, kCreate,
                           sdb::catalog::MakeSchemaInfo(id, db, name),
                           sdb::catalog::Permissions{ObjectId{1}});
  });
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
    store->Replay(_replayed);
    return store;
  }

  // Reopening replays the file, so the applier starts from nothing: what comes
  // back has to come out of the log rather than out of the run that wrote it.
  std::unique_ptr<CatalogStore> Reopen(std::unique_ptr<CatalogStore> store) {
    store->Shutdown();
    store.reset();
    _replayed = Applier{};
    return Open();
  }

  DefMap Defs(ObjectId parent, duckdb::CatalogType type) const {
    return _replayed.Defs(parent, type);
  }

  std::filesystem::path _dir;
  Applier _replayed;
};

TEST_F(CatalogStoreTest, records_survive_reopen) {
  auto store = Open();
  const ObjectId parent{10};
  PutTable(*store, *MakeTable(parent, ObjectId{11}, "table-a"), kCreate);
  PutTable(*store, *MakeTable(parent, ObjectId{12}, "table-b"), kCreate);
  PutSequence(*store, parent, ObjectId{13}, "seq");
  store->PutSequenceValue(ObjectId{13}, 42);
  store->DropObject(parent, duckdb::CatalogType::TABLE_ENTRY, ObjectId{12});

  store = Reopen(std::move(store));

  const auto tables = Defs(parent, duckdb::CatalogType::TABLE_ENTRY);
  ASSERT_EQ(tables.size(), 1);
  EXPECT_EQ(tables.begin()->second, "table-a");
  EXPECT_EQ(tables.begin()->first, 11);

  EXPECT_EQ(store->TryGetBootSequenceValue(ObjectId{13}), 42);
  store->Shutdown();
}

// A drop is opened and committed on the root; each level of the cascade erases
// its own children on the way up, so nothing has to rediscover the subtree.
TEST_F(CatalogStoreTest, drop_erases_each_level) {
  auto store = Open();
  const ObjectId db{19};
  const ObjectId schema{20};
  const ObjectId table{21};
  PutSchema(*store, db, schema, "s");
  PutTable(*store, *MakeTable(schema, table, "t"), kCreate);
  PutSequence(*store, schema, ObjectId{22}, "q");
  PutTable(*store, *MakeTable(table, ObjectId{23}, "child_of_table"), kCreate);

  // The record carries the whole subtree it retires, which is what makes a
  // cascade one record and what boot rebuilds the reclamation from.
  auto subtree = std::make_shared<std::vector<sdb::catalog::wal::DropNode>>(
    std::vector<sdb::catalog::wal::DropNode>{
      {.parent_id = db,
       .id = schema,
       .type = duckdb::CatalogType::SCHEMA_ENTRY},
      {.parent_id = schema,
       .id = table,
       .type = duckdb::CatalogType::TABLE_ENTRY},
      {.parent_id = schema,
       .id = ObjectId{22},
       .type = duckdb::CatalogType::SEQUENCE_ENTRY}});
  store->DropPrepare({.parent_id = db,
                      .type = duckdb::CatalogType::SCHEMA_ENTRY,
                      .id = schema,
                      .database_id = db,
                      .schema_id = schema,
                      .subtree = std::move(subtree)});
  // What the drop-task cascade emits: the table level names its own children,
  // the root does not -- its commit takes its definition and its children.
  store->Write([&](CatalogStore::WriteContext& ctx) {
    ctx.catalog().DropChildren(table);
  });
  store->Write([&](CatalogStore::WriteContext& ctx) {
    ctx.catalog().PrepareCommit(schema);
  });

  store = Reopen(std::move(store));

  EXPECT_TRUE(Defs(db, duckdb::CatalogType::SCHEMA_ENTRY).empty());
  EXPECT_TRUE(Defs(schema, duckdb::CatalogType::TABLE_ENTRY).empty());
  EXPECT_TRUE(Defs(schema, duckdb::CatalogType::SEQUENCE_ENTRY).empty());
  EXPECT_TRUE(Defs(table, duckdb::CatalogType::TABLE_ENTRY).empty());
  EXPECT_TRUE(store->AllOpenDrops().empty());
  store->Shutdown();
}

// A drop applies the moment its record lands -- what stays open is the
// reclamation of the artifacts, not the visibility -- so replay retires the
// subtree and keeps the tombstone for boot to redo the sweep from.
TEST_F(CatalogStoreTest, uncommitted_drop_replays) {
  auto store = Open();
  const ObjectId db{29};
  const ObjectId schema{30};
  PutSchema(*store, db, schema, "s");
  PutTable(*store, *MakeTable(schema, ObjectId{31}, "t"), kCreate);
  auto subtree = std::make_shared<std::vector<sdb::catalog::wal::DropNode>>(
    std::vector<sdb::catalog::wal::DropNode>{
      {.parent_id = db,
       .id = schema,
       .type = duckdb::CatalogType::SCHEMA_ENTRY},
      {.parent_id = schema,
       .id = ObjectId{31},
       .type = duckdb::CatalogType::TABLE_ENTRY}});
  store->DropPrepare({.parent_id = db,
                      .type = duckdb::CatalogType::SCHEMA_ENTRY,
                      .id = schema,
                      .database_id = db,
                      .schema_id = schema,
                      .subtree = std::move(subtree)});

  store = Reopen(std::move(store));

  EXPECT_TRUE(Defs(schema, duckdb::CatalogType::TABLE_ENTRY).empty());
  ASSERT_EQ(store->AllOpenDrops().size(), 1);
  EXPECT_EQ(store->AllOpenDrops().front(), schema);
  ASSERT_TRUE(store->OpenDrop(schema).has_value());
  EXPECT_EQ(store->OpenDrop(schema)->database_id, db);
  store->Shutdown();
}

TEST_F(CatalogStoreTest, sequence_updates_keep_latest) {
  auto store = Open();
  store->PutSequenceValue(ObjectId{30}, 1);
  store->PutSequenceValue(ObjectId{30}, 7);
  store->PutSequenceValue(ObjectId{30}, 100);
  store->PutSequenceValue(ObjectId{31}, 5);
  store->DropSequence(ObjectId{31});

  store = Reopen(std::move(store));

  EXPECT_EQ(store->TryGetBootSequenceValue(ObjectId{30}), 100);
  EXPECT_EQ(store->TryGetBootSequenceValue(ObjectId{31}), std::nullopt);
  store->Shutdown();
}

// A checkpoint is the catalog written out, so a store with no catalog behind it
// -- which is what this fixture is -- never folds its log, and the whole log is
// what has to replay to the right state. Compaction itself is covered where a
// catalog exists: tests/sqllogic/recovery/catalog_checkpoint.test.
TEST_F(CatalogStoreTest, an_unfolded_log_replays_to_the_last_write) {
  auto store = Open();
  const ObjectId parent{40};
  const std::string def(512, 'x');
  for (uint64_t i = 0; i < 4096; ++i) {
    PutTable(*store, *MakeTable(parent, ObjectId{41}, def + std::to_string(i)),
             i == 0 ? kCreate : kReplace);
  }
  PutTable(*store, *MakeTable(parent, ObjectId{42}, "live"), kCreate);
  store->PutSequenceValue(ObjectId{43}, 9);

  EXPECT_GT(store->WalStats().size_on_disk, 1024 * 1024)
    << "nothing can write a checkpoint here, so the log must still be whole";

  store = Reopen(std::move(store));

  const auto tables = Defs(parent, duckdb::CatalogType::TABLE_ENTRY);
  ASSERT_EQ(tables.size(), 2);
  EXPECT_EQ(tables.at(41), def + "4095");
  EXPECT_EQ(tables.at(42), "live");
  EXPECT_EQ(store->TryGetBootSequenceValue(ObjectId{43}), 9);
  store->Shutdown();
}

// A create's entries live on the statement's transaction until it commits;
// CommitFrames is what the commit hands them to, together with the install of
// what they describe. Frames land in the order they were produced, and the
// install runs after all of them.
TEST_F(CatalogStoreTest, deferred_frames_land_at_commit_in_order) {
  auto store = Open();
  const ObjectId parent{60};

  std::vector<std::vector<sdb::catalog::wal::Entry>> frames;
  frames.push_back({sdb::catalog::wal::PutTable{
    .schema_id = parent,
    .id = ObjectId{61},
    .mode = kCreate,
    .info = MakeTable(parent, ObjectId{61}, "first")}});
  frames.push_back({sdb::catalog::wal::PutTable{
    .schema_id = parent,
    .id = ObjectId{61},
    .mode = kReplace,
    .info = MakeTable(parent, ObjectId{61}, "second")}});
  const auto before = store->LogPosition();
  bool installed = false;
  const auto position =
    store->CommitFrames(nullptr, frames, [&] { installed = true; });
  EXPECT_TRUE(installed);
  EXPECT_EQ(position, before + 2);

  store = Reopen(std::move(store));

  const auto tables = Defs(parent, duckdb::CatalogType::TABLE_ENTRY);
  ASSERT_EQ(tables.size(), 1);
  EXPECT_EQ(tables.at(61), "second");
  store->Shutdown();
}

// A table's owned sequences are seeded by the entry that creates the table, and
// that entry is appended when the transaction commits -- after any nextval the
// same transaction already issued. The seed is therefore a floor: assigning it
// would rewind the durable horizon and hand the same values out twice.
TEST_F(CatalogStoreTest, owned_sequence_seed_never_rewinds_the_horizon) {
  auto store = Open();
  const ObjectId parent{70};
  const ObjectId sequence{72};
  store->AdvanceSequenceValue(sequence, 5000);

  std::vector<std::vector<sdb::catalog::wal::Entry>> frames;
  frames.push_back({sdb::catalog::wal::PutTable{
    .schema_id = parent,
    .id = ObjectId{71},
    .mode = kCreate,
    .info = MakeTable(parent, ObjectId{71}, "t"),
    .sequences = {{.id = sequence,
                   .info = MakeSequence(parent, sequence, "t_a_seq"),
                   .seed = 1}}}});
  store->CommitFrames(nullptr, frames, [] {});

  EXPECT_EQ(store->TryGetBootSequenceValue(sequence), 5000);

  store = Reopen(std::move(store));

  EXPECT_EQ(store->TryGetBootSequenceValue(sequence), 5000);
  store->Shutdown();
}

TEST_F(CatalogStoreTest, parse_frame_round_trips_wal_records) {
  auto store = Open();
  const ObjectId parent{50};
  PutTable(*store, *MakeTable(parent, ObjectId{51}, "def-bytes"), kCreate);
  store->Shutdown();
  store.reset();

  size_t frames = 0;
  std::optional<sdb::catalog::wal::PutTable> put;
  sdb::catalog::CatalogWal::Scan(
    (_dir / "engine_catalog").string(), [&](std::span<const uint8_t> frame) {
      for (auto& tagged : CatalogStore::ParseFrame(frame).entries) {
        if (auto* e = std::get_if<sdb::catalog::wal::PutTable>(&tagged.entry)) {
          put = *e;
        }
      }
      ++frames;
    });

  EXPECT_GT(frames, 0);
  ASSERT_TRUE(put.has_value());
  EXPECT_EQ(put->schema_id.id(), 50);
  EXPECT_EQ(put->id.id(), 51);
  EXPECT_EQ(sdb::catalog::TableNameOf(*put->info), "def-bytes");
}

// A table's definition is duckdb's duckdb::CreateTableInfo plus what it has no
// room for, so the record has to bring back the ids on the columns and
// constraints, the exact-match keying of the column list and the per-column
// grants.
TEST_F(CatalogStoreTest, table_info_round_trips_through_put_entry) {
  namespace catalog = sdb::catalog;
  const ObjectId schema{60};
  const ObjectId table_id{61};

  auto info = sdb::catalog::NewTableInfo();
  info->SetTableName(duckdb::Identifier{"orders"});
  info->SetSchema(duckdb::Identifier{"public"});
  info->comment = duckdb::Value("a table");
  // "A" and "a" are two columns: the list is keyed exactly, and the flag that
  // says so is not part of duckdb's own payload.
  for (const auto& [name, host] :
       {std::pair<const char*, uint64_t>{"A", 101},
        std::pair<const char*, uint64_t>{"a", 102}}) {
    duckdb::ColumnDefinition column{duckdb::Identifier{std::string{name}},
                                    duckdb::LogicalType::INTEGER};
    column.SetCatalogOid(host);
    info->columns.AddColumn(std::move(column));
  }
  auto not_null =
    duckdb::make_uniq<duckdb::NotNullConstraint>(duckdb::LogicalIndex{0});
  not_null->constraint_name = "orders_A_not_null";
  not_null->oid = 201;
  info->constraints.push_back(std::move(not_null));
  auto pk = duckdb::make_uniq<duckdb::UniqueConstraint>(
    duckdb::vector<duckdb::Identifier>{duckdb::Identifier{"A"}}, true);
  pk->constraint_name = "orders_pkey";
  pk->oid = 202;
  pk->host_index_id = 203;
  info->constraints.push_back(std::move(pk));
  duckdb::ForeignKeyInfo fk_info;
  fk_info.type = duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
  fk_info.schema = duckdb::Identifier{"public"};
  fk_info.table = duckdb::Identifier{"customers"};
  auto fk = duckdb::make_uniq<duckdb::ForeignKeyConstraint>(
    duckdb::vector<duckdb::Identifier>{duckdb::Identifier{"id"}},
    duckdb::vector<duckdb::Identifier>{duckdb::Identifier{"a"}},
    std::move(fk_info));
  fk->constraint_name = "orders_a_fkey";
  fk->oid = 204;
  fk->host_referenced_id = 99;
  info->constraints.push_back(std::move(fk));
  // Column grants ride the permissions beside the definition, not the
  // definition itself.
  catalog::Permissions perm{ObjectId{1}};
  catalog::SetColumnAcl(
    perm.column_acl, ObjectId{102},
    catalog::Acl{catalog::AclItem{.grantee = ObjectId{7},
                                  .grantor = ObjectId{1},
                                  .privs = catalog::AclMode::Select}});

  auto store = Open();
  store->Write([&](CatalogStore::WriteContext& ctx) {
    ctx.catalog().PutEntry(schema, duckdb::CatalogType::TABLE_ENTRY, table_id,
                           kCreate, info, perm);
  });
  store->Shutdown();
  store.reset();

  std::optional<sdb::catalog::wal::PutEntry> put;
  sdb::catalog::CatalogWal::Scan(
    (_dir / "engine_catalog").string(), [&](std::span<const uint8_t> frame) {
      for (auto& tagged : CatalogStore::ParseFrame(frame).entries) {
        if (auto* e = std::get_if<sdb::catalog::wal::PutEntry>(&tagged.entry);
            e != nullptr && e->type == duckdb::CatalogType::TABLE_ENTRY) {
          put = *e;
        }
      }
    });

  ASSERT_TRUE(put.has_value());
  EXPECT_EQ(put->parent_id.id(), schema.id());
  EXPECT_EQ(put->id.id(), table_id.id());
  EXPECT_EQ(put->perm.owner, 1);
  const auto* read =
    dynamic_cast<const duckdb::CreateTableInfo*>(put->info.get());
  ASSERT_NE(read, nullptr);
  EXPECT_EQ(sdb::catalog::TableNameOf(*read), "orders");
  EXPECT_EQ(duckdb::StringValue::Get(read->comment), "a table");
  ASSERT_TRUE(read->columns.IsCaseSensitive());
  ASSERT_EQ(read->columns.LogicalColumnCount(), 2);
  ASSERT_NE(sdb::catalog::ColumnById(*read, ObjectId{101}), nullptr);
  EXPECT_EQ(
    sdb::catalog::ColumnById(*read, ObjectId{101})->Name().GetIdentifierName(),
    "A");
  EXPECT_EQ(
    sdb::catalog::ColumnById(*read, ObjectId{102})->Name().GetIdentifierName(),
    "a");
  ASSERT_EQ(read->constraints.size(), 3);
  EXPECT_EQ(read->constraints[0]->oid, 201);
  EXPECT_EQ(read->constraints[1]->oid, 202);
  EXPECT_EQ(
    read->constraints[1]->Cast<duckdb::UniqueConstraint>().host_index_id, 203);
  EXPECT_EQ(read->constraints[2]
              ->Cast<duckdb::ForeignKeyConstraint>()
              .host_referenced_id,
            99);
  ASSERT_EQ(catalog::ColumnAclOf(put->perm.column_acl, ObjectId{102}).size(),
            1);
  EXPECT_EQ(
    catalog::ColumnAclOf(put->perm.column_acl, ObjectId{102})[0].grantee, 7);
  EXPECT_TRUE(
    catalog::ColumnAclOf(put->perm.column_acl, ObjectId{101}).empty());
}

// Not covered here: a record count past the frame's size. It is rejected by
// SDB_ENSURE, which asserts-and-aborts in dev builds and only throws in
// release, so it is not expressible as EXPECT_ANY_THROW without a death test.
//
// An empty batch must round-trip: header, count 0 and no records.
TEST_F(CatalogStoreTest, parse_frame_accepts_an_empty_batch) {
  std::vector<sdb::catalog::wal::Entry> none;
  duckdb::MemoryStream stream;
  sdb::catalog::wal::SerializeEntries({.position = 9}, none, stream);
  const auto parsed =
    CatalogStore::ParseFrame({stream.GetData(), stream.GetPosition()});

  EXPECT_TRUE(parsed.entries.empty());
  EXPECT_EQ(parsed.header.position, 9);
}

}  // namespace
