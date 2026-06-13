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

// Execution-verifies the duckdb custom-index contract the inverted-index
// BoundIndex relies on (plan doc "B3 verdict"): a non-unique custom index
// receives every insert exactly once AT COMMIT with final row ids, deletes
// arrive with re-fetched values, UPDATE decomposes into delete+insert,
// rolled-back transactions never reach the index, and after reopening the
// database the index instance is recreated through create_instance with the
// WAL/checkpoint state.

#include <gtest/gtest.h>

#include <algorithm>
#include <duckdb.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/index/bound_index.hpp>
#include <duckdb/execution/index/index_type.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/index_storage_info.hpp>
#include <duckdb/storage/table/append_state.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace sdb::connector {
namespace {

struct ProbeEvent {
  enum Kind : uint8_t { Append, Delete } kind;
  std::vector<int64_t> row_ids;
  std::vector<int32_t> values;
};

struct ProbeLog {
  std::mutex mutex;
  std::vector<ProbeEvent> events;
  int create_instance_calls = 0;
  int build_finalize_calls = 0;

  void Record(ProbeEvent::Kind kind, duckdb::DataChunk& chunk,
              duckdb::Vector& row_ids) {
    ProbeEvent ev;
    ev.kind = kind;
    duckdb::UnifiedVectorFormat row_fmt;
    row_ids.ToUnifiedFormat(chunk.size(), row_fmt);
    duckdb::UnifiedVectorFormat val_fmt;
    chunk.data[0].ToUnifiedFormat(chunk.size(), val_fmt);
    for (duckdb::idx_t i = 0; i < chunk.size(); ++i) {
      ev.row_ids.push_back(duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(
        row_fmt)[row_fmt.sel->get_index(i)]);
      ev.values.push_back(duckdb::UnifiedVectorFormat::GetData<int32_t>(
        val_fmt)[val_fmt.sel->get_index(i)]);
    }
    std::lock_guard lock{mutex};
    events.push_back(std::move(ev));
  }
};

ProbeLog& Log() {
  static ProbeLog log;
  return log;
}

class ProbeIndex final : public duckdb::BoundIndex {
 public:
  static constexpr const char* kTypeName = "sdb_probe";

  ProbeIndex(
    const std::string& name, duckdb::TableIOManager& io,
    const duckdb::vector<duckdb::column_t>& column_ids,
    const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& exprs,
    duckdb::AttachedDatabase& db)
    : BoundIndex(name, kTypeName, duckdb::IndexConstraintType::NONE, column_ids,
                 io, exprs, db) {}

  duckdb::ErrorData Append(duckdb::IndexLock&, duckdb::DataChunk& chunk,
                           duckdb::Vector& row_ids) override {
    Log().Record(ProbeEvent::Append, chunk, row_ids);
    return {};
  }
  duckdb::ErrorData Insert(duckdb::IndexLock&, duckdb::DataChunk& chunk,
                           duckdb::Vector& row_ids) override {
    Log().Record(ProbeEvent::Append, chunk, row_ids);
    return {};
  }
  void Delete(duckdb::IndexLock&, duckdb::DataChunk& chunk,
              duckdb::Vector& row_ids) override {
    Log().Record(ProbeEvent::Delete, chunk, row_ids);
  }
  idx_t TryDelete(
    duckdb::IndexLock& l, duckdb::DataChunk& chunk, duckdb::Vector& row_ids,
    duckdb::optional_ptr<duckdb::SelectionVector> deleted_sel,
    duckdb::optional_ptr<duckdb::SelectionVector> non_deleted_sel) override {
    Delete(l, chunk, row_ids);
    if (deleted_sel) {
      for (duckdb::idx_t i = 0; i < chunk.size(); ++i) {
        deleted_sel->set_index(i, i);
      }
    }
    return chunk.size();
  }
  std::string GetConstraintViolationMessage(duckdb::VerifyExistenceType, idx_t,
                                            duckdb::DataChunk&) override {
    return "probe constraint violation";
  }
  void ResetStorage(duckdb::IndexLock&) override {}
  bool MergeIndexes(duckdb::IndexLock&, duckdb::BoundIndex&) override {
    return true;
  }
  void Vacuum(duckdb::IndexLock&) override {}
  idx_t GetInMemorySize(duckdb::IndexLock&) override { return 0; }
  void Verify(duckdb::IndexLock&) override {}
  std::string ToString(duckdb::IndexLock&, bool) override { return "probe"; }
  void VerifyAllocations(duckdb::IndexLock&) override {}
  void VerifyBuffers(duckdb::IndexLock&) override {}
  // The index data lives outside duckdb storage; one empty allocator entry
  // makes the info IsValid() for WAL/checkpoint round-trips.
  duckdb::IndexStorageInfo FabricateStorageInfo() const {
    duckdb::IndexStorageInfo info{name};
    info.allocator_infos.emplace_back();
    return info;
  }
  duckdb::IndexStorageInfo SerializeToDisk(
    duckdb::QueryContext,
    const duckdb::case_insensitive_map_t<duckdb::Value>&) override {
    return FabricateStorageInfo();
  }
  duckdb::IndexStorageInfo SerializeToWAL(
    const duckdb::case_insensitive_map_t<duckdb::Value>&) override {
    return FabricateStorageInfo();
  }
};

struct ProbeBuildGlobalState final : duckdb::IndexBuildGlobalState {
  duckdb::unique_ptr<ProbeIndex> index;
};

struct ProbeBuildLocalState final : duckdb::IndexBuildLocalState {};

void RegisterProbeIndexType(duckdb::DatabaseInstance& db) {
  duckdb::IndexType type;
  type.name = ProbeIndex::kTypeName;
  type.build_bind = [](duckdb::IndexBuildBindInput&)
    -> duckdb::unique_ptr<duckdb::IndexBuildBindData> { return nullptr; };
  type.build_global_init = [](duckdb::IndexBuildInitGlobalStateInput& input)
    -> duckdb::unique_ptr<duckdb::IndexBuildGlobalState> {
    auto state = duckdb::make_uniq<ProbeBuildGlobalState>();
    state->index = duckdb::make_uniq<ProbeIndex>(
      input.info.index_name,
      duckdb::TableIOManager::Get(input.table.GetStorage()), input.storage_ids,
      input.expressions, input.table.GetStorage().db);
    return std::move(state);
  };
  type.build_local_init = [](duckdb::IndexBuildInitLocalStateInput&)
    -> duckdb::unique_ptr<duckdb::IndexBuildLocalState> {
    return duckdb::make_uniq<ProbeBuildLocalState>();
  };
  type.build_sink = [](duckdb::IndexBuildSinkInput& input,
                       duckdb::DataChunk& key_chunk,
                       duckdb::DataChunk& row_chunk) {
    auto& gstate = input.global_state.Cast<ProbeBuildGlobalState>();
    duckdb::IndexLock lock;
    gstate.index->InitializeLock(lock);
    auto err = gstate.index->Append(lock, key_chunk, row_chunk.data[0]);
    if (err.HasError()) {
      err.Throw();
    }
  };
  type.build_combine = [](duckdb::IndexBuildCombineInput&) {};
  type.build_finalize = [](duckdb::IndexBuildFinalizeInput& input)
    -> duckdb::unique_ptr<duckdb::BoundIndex> {
    Log().build_finalize_calls++;
    auto& gstate = input.global_state.Cast<ProbeBuildGlobalState>();
    return std::move(gstate.index);
  };
  type.create_instance = [](duckdb::CreateIndexInput& input)
    -> duckdb::unique_ptr<duckdb::BoundIndex> {
    Log().create_instance_calls++;
    return duckdb::make_uniq<ProbeIndex>(input.name, input.table_io_manager,
                                         input.column_ids,
                                         input.unbound_expressions, input.db);
  };
  db.config.GetIndexTypes().RegisterIndexType(type);
}

class IndexLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _dir = std::filesystem::temp_directory_path() /
           ("sdb_index_lifecycle_" + std::to_string(::getpid()));
    std::filesystem::remove_all(_dir);
    std::filesystem::create_directories(_dir);
    Log().events.clear();
    Log().create_instance_calls = 0;
    Open();
  }
  void TearDown() override {
    _conn.reset();
    _db.reset();
    std::filesystem::remove_all(_dir);
  }
  void Open(bool checkpoint_on_shutdown = true) {
    _conn.reset();
    _db.reset();
    duckdb::DBConfig config;
    config.options.checkpoint_on_shutdown = checkpoint_on_shutdown;
    _db =
      std::make_unique<duckdb::DuckDB>((_dir / "probe.db").string(), &config);
    RegisterProbeIndexType(*_db->instance);
    _conn = std::make_unique<duckdb::Connection>(*_db);
    auto res = _conn->Query("USE probe.main");
    ASSERT_FALSE(res->HasError()) << res->GetError();
  }
  void Exec(const std::string& sql) {
    auto res = _conn->Query(sql);
    ASSERT_FALSE(res->HasError()) << sql << ": " << res->GetError();
  }

  std::filesystem::path _dir;
  std::unique_ptr<duckdb::DuckDB> _db;
  std::unique_ptr<duckdb::Connection> _conn;
};

TEST_F(IndexLifecycleTest, AppendAtCommitWithFinalRowIds) {
  Exec("CREATE TABLE t(v INTEGER)");
  Exec("CREATE INDEX probe_idx ON t USING sdb_probe(v)");
  Log().events.clear();

  Exec("BEGIN");
  Exec("INSERT INTO t VALUES (10), (20)");
  ASSERT_TRUE(Log().events.empty());
  Exec("INSERT INTO t VALUES (30)");
  ASSERT_TRUE(Log().events.empty());
  Exec("COMMIT");

  std::vector<int64_t> rows;
  std::vector<int32_t> vals;
  for (const auto& ev : Log().events) {
    ASSERT_EQ(ev.kind, ProbeEvent::Append);
    rows.insert(rows.end(), ev.row_ids.begin(), ev.row_ids.end());
    vals.insert(vals.end(), ev.values.begin(), ev.values.end());
  }
  ASSERT_EQ(rows, (std::vector<int64_t>{0, 1, 2}));
  ASSERT_EQ(vals, (std::vector<int32_t>{10, 20, 30}));
}

TEST_F(IndexLifecycleTest, RollbackNeverReachesIndex) {
  Exec("CREATE TABLE t(v INTEGER)");
  Exec("CREATE INDEX probe_idx ON t USING sdb_probe(v)");
  Log().events.clear();

  Exec("BEGIN");
  Exec("INSERT INTO t VALUES (10)");
  Exec("ROLLBACK");
  ASSERT_TRUE(Log().events.empty());
}

TEST_F(IndexLifecycleTest, DeleteArrivesWithValues) {
  Exec("CREATE TABLE t(v INTEGER)");
  Exec("CREATE INDEX probe_idx ON t USING sdb_probe(v)");
  Exec("INSERT INTO t VALUES (10), (20), (30)");
  Log().events.clear();

  Exec("DELETE FROM t WHERE v = 20");

  ASSERT_EQ(Log().events.size(), 1u);
  const auto& ev = Log().events[0];
  ASSERT_EQ(ev.kind, ProbeEvent::Delete);
  ASSERT_EQ(ev.row_ids, (std::vector<int64_t>{1}));
  ASSERT_EQ(ev.values, (std::vector<int32_t>{20}));
}

TEST_F(IndexLifecycleTest, UpdateIsDeleteThenInsert) {
  Exec("CREATE TABLE t(v INTEGER)");
  Exec("CREATE INDEX probe_idx ON t USING sdb_probe(v)");
  Exec("INSERT INTO t VALUES (10), (20)");
  Log().events.clear();

  Exec("UPDATE t SET v = 25 WHERE v = 20");

  bool saw_delete = false;
  bool saw_append = false;
  for (const auto& ev : Log().events) {
    if (ev.kind == ProbeEvent::Delete) {
      saw_delete = true;
      ASSERT_EQ(ev.values, (std::vector<int32_t>{20}));
    } else {
      saw_append = true;
      ASSERT_EQ(ev.values, (std::vector<int32_t>{25}));
    }
  }
  ASSERT_TRUE(saw_delete);
  ASSERT_TRUE(saw_append);
}

TEST_F(IndexLifecycleTest, ReopenRecreatesViaCreateInstanceAndReplays) {
  Exec("CREATE TABLE t(v INTEGER)");
  Exec("CREATE INDEX probe_idx ON t USING sdb_probe(v)");
  Exec("INSERT INTO t VALUES (10), (20)");

  Open();
  ASSERT_EQ(Log().create_instance_calls, 0);
  Log().events.clear();

  // First DML after reopen forces the unbound index to bind and replay.
  Exec("INSERT INTO t VALUES (30)");

  ASSERT_GE(Log().create_instance_calls, 1);
  std::vector<int32_t> vals;
  for (const auto& ev : Log().events) {
    if (ev.kind == ProbeEvent::Append) {
      vals.insert(vals.end(), ev.values.begin(), ev.values.end());
    }
  }
  ASSERT_EQ(vals, (std::vector<int32_t>{30}));
}

// P1a: a database closed WITHOUT a checkpoint replays the whole WAL --
// including the CREATE INDEX itself -- and delivers every append to the
// recreated index at first bind.
TEST_F(IndexLifecycleTest, DirtyCloseReplaysWalAppendsIntoIndex) {
  Open(/*checkpoint_on_shutdown=*/false);
  Exec("CREATE TABLE t(v INTEGER)");
  Exec("CREATE INDEX probe_idx ON t USING sdb_probe(v)");
  Exec("INSERT INTO t VALUES (10), (20)");

  Open();  // reopen; WAL of the dirty close replays
  Log().events.clear();
  Exec("INSERT INTO t VALUES (30)");  // first DML binds + applies buffers

  std::vector<int32_t> vals;
  for (const auto& ev : Log().events) {
    if (ev.kind == ProbeEvent::Append) {
      vals.insert(vals.end(), ev.values.begin(), ev.values.end());
    }
  }
  std::sort(vals.begin(), vals.end());
  ASSERT_EQ(vals, (std::vector<int32_t>{10, 20, 30}));
}

// P1b: delta exactness -- rows covered by a checkpoint are NOT redelivered;
// only post-checkpoint appends and deletes replay after a dirty close.
TEST_F(IndexLifecycleTest, DirtyCloseReplaysOnlyPostCheckpointDelta) {
  Open(/*checkpoint_on_shutdown=*/false);
  Exec("CREATE TABLE t(v INTEGER)");
  Exec("CREATE INDEX probe_idx ON t USING sdb_probe(v)");
  Exec("INSERT INTO t VALUES (10), (20)");
  Exec("CHECKPOINT");
  Exec("INSERT INTO t VALUES (30)");
  Exec("DELETE FROM t WHERE v = 10");

  Open();
  Log().events.clear();
  Exec("INSERT INTO t VALUES (40)");

  std::vector<int32_t> appended;
  std::vector<int32_t> deleted;
  for (const auto& ev : Log().events) {
    auto& out = ev.kind == ProbeEvent::Append ? appended : deleted;
    out.insert(out.end(), ev.values.begin(), ev.values.end());
  }
  std::sort(appended.begin(), appended.end());
  ASSERT_EQ(appended, (std::vector<int32_t>{30, 40}));
  ASSERT_EQ(deleted, (std::vector<int32_t>{10}));
}

}  // namespace
}  // namespace sdb::connector
