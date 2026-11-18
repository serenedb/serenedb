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

#include <atomic>
#include <duckdb.hpp>
#include <memory>
#include <thread>

#include "pg/connection_context.h"
#include "pg/progress_registry.h"

namespace sdb::pg {
namespace {

std::optional<ProgressSnapshot> FindByPid(int32_t pid) {
  for (auto& s : ProgressRegistry::Instance().GetSnapshots()) {
    if (s.pid == pid) {
      return s;
    }
  }
  return std::nullopt;
}

std::shared_ptr<ProgressSource> MakeSource(int32_t pid) {
  auto source = std::make_shared<ProgressSource>();
  source->pid = pid;
  source->datid = 7;
  source->user = "postgres";
  source->database = "postgres";
  source->backend_start_us = 123456;
  return source;
}

TEST(ProgressRegistryTest, RegisterSnapshotUnregister) {
  auto source = MakeSource(1001);
  ProgressRegistry::Instance().Register(source);

  auto snap = FindByPid(1001);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->datid, 7);
  EXPECT_EQ(snap->user, "postgres");
  EXPECT_EQ(snap->database, "postgres");
  EXPECT_EQ(snap->backend_start_us, 123456);
  EXPECT_EQ(snap->query_start_us, 0);
  EXPECT_EQ(snap->query, "");
  EXPECT_EQ(snap->percent, -1);

  ProgressRegistry::Instance().Unregister(source.get());
  EXPECT_FALSE(FindByPid(1001).has_value());
}

TEST(ProgressRegistryTest, QueryLifecycle) {
  auto source = MakeSource(1002);
  ProgressRegistry::Instance().Register(source);

  source->BeginQuery("SELECT 1");
  auto snap = FindByPid(1002);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->query, "SELECT 1");
  EXPECT_EQ(snap->state, BackendState::Active);
  EXPECT_GT(snap->query_start_us, 0);
  EXPECT_EQ(snap->xact_start_us, snap->query_start_us);
  EXPECT_EQ(snap->state_change_us, snap->query_start_us);
  const auto started = snap->query_start_us;

  source->Ready(BackendState::Idle);
  snap = FindByPid(1002);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->query, "SELECT 1");
  EXPECT_EQ(snap->state, BackendState::Idle);
  EXPECT_EQ(snap->query_start_us, started);
  EXPECT_EQ(snap->xact_start_us, 0);
  EXPECT_GE(snap->state_change_us, started);

  ProgressRegistry::Instance().Unregister(source.get());
}

TEST(ProgressRegistryTest, TransactionBlockLifecycle) {
  auto source = MakeSource(1007);
  ProgressRegistry::Instance().Register(source);

  source->BeginQuery("BEGIN");
  source->Ready(BackendState::IdleInTransaction);
  auto snap = FindByPid(1007);
  ASSERT_TRUE(snap.has_value());
  const auto began = snap->xact_start_us;
  EXPECT_GT(began, 0);
  EXPECT_EQ(snap->state, BackendState::IdleInTransaction);

  source->BeginQuery("SELECT 1/0");
  snap = FindByPid(1007);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->state, BackendState::Active);
  EXPECT_EQ(snap->xact_start_us, began);
  EXPECT_GE(snap->query_start_us, began);

  source->Ready(BackendState::IdleInTransactionAborted);
  snap = FindByPid(1007);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->state, BackendState::IdleInTransactionAborted);
  EXPECT_EQ(snap->xact_start_us, 0);

  source->BeginQuery("ROLLBACK");
  snap = FindByPid(1007);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->state, BackendState::Active);
  EXPECT_EQ(snap->xact_start_us, 0);
  source->Ready(BackendState::Idle);
  snap = FindByPid(1007);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->state, BackendState::Idle);
  EXPECT_EQ(snap->xact_start_us, 0);
  EXPECT_GE(snap->state_change_us, snap->query_start_us);

  source->BeginQuery("SELECT 2");
  snap = FindByPid(1007);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->xact_start_us, snap->query_start_us);

  ProgressRegistry::Instance().Unregister(source.get());
}

TEST(ProgressRegistryTest, SnapshotsNeverMixTwoStates) {
  auto source = MakeSource(1008);
  ProgressRegistry::Instance().Register(source);
  std::atomic<bool> stop{false};
  std::thread session{[&] {
    while (!stop.load(std::memory_order_relaxed)) {
      source->BeginQuery("BEGIN");
      source->Ready(BackendState::IdleInTransaction);
      source->BeginQuery("COMMIT");
      source->Ready(BackendState::Idle);
    }
  }};
  for (int i = 0; i < 2000; ++i) {
    const auto snap = FindByPid(1008);
    if (!snap) {
      ADD_FAILURE() << "the session is not registered";
      break;
    }
    EXPECT_EQ(snap->state == BackendState::Idle, snap->xact_start_us == 0);
  }
  stop.store(true, std::memory_order_relaxed);
  session.join();
  ProgressRegistry::Instance().Unregister(source.get());
}

TEST(ProgressRegistryTest, BeginQueryResetsMetrics) {
  auto source = MakeSource(1003);
  ProgressRegistry::Instance().Register(source);

  source->BeginQuery("COPY t FROM STDIN");
  auto& m = source->metrics;
  m.SetCommand(ProgressCommand::CopyFrom);
  m.SetIoType(ProgressIoType::Pipe);
  ProgressMetrics::Set(m.relid, 42);
  m.AddTuples(100);
  m.AddProcessed(50, 4096);

  auto snap = FindByPid(1003);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->command, std::to_underlying(ProgressCommand::CopyFrom));
  EXPECT_EQ(snap->io_type, std::to_underlying(ProgressIoType::Pipe));
  EXPECT_EQ(snap->relid, 42);
  EXPECT_EQ(snap->tuples_processed, 150);
  EXPECT_EQ(snap->bytes_processed, 4096);

  source->Ready(BackendState::Idle);
  source->BeginQuery("SELECT 1");
  snap = FindByPid(1003);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->command, 0);
  EXPECT_EQ(snap->io_type, 0);
  EXPECT_EQ(snap->relid, 0);
  EXPECT_EQ(snap->tuples_processed, 0);
  EXPECT_EQ(snap->bytes_processed, 0);

  ProgressRegistry::Instance().Unregister(source.get());
}

TEST(ProgressRegistryTest, StageAndStepCounters) {
  auto source = MakeSource(1004);
  ProgressRegistry::Instance().Register(source);
  source->BeginQuery("VACUUM");

  auto& m = source->metrics;
  m.SetCommand(ProgressCommand::Vacuum);
  m.SetPhase(progress_phase::Vacuum::VacuumingIndexes);
  ProgressMetrics::Set(m.items_total, 3);
  ProgressMetrics::Set(m.stages_total, 4);
  ProgressMetrics::Add(m.stage, 1);
  ProgressMetrics::Set(m.steps_total, 10);
  ProgressMetrics::Set(m.step, 7);
  ProgressMetrics::Add(m.items_processed, 1);

  auto snap = FindByPid(1004);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->items_total, 3);
  EXPECT_EQ(snap->items_processed, 1);
  EXPECT_EQ(snap->stages_total, 4);
  EXPECT_EQ(snap->stage, 1);
  EXPECT_EQ(snap->steps_total, 10);
  EXPECT_EQ(snap->step, 7);

  ProgressRegistry::Instance().Unregister(source.get());
}

TEST(ProgressRegistryTest, DetachedSourceReportsNoDuckDBProgress) {
  auto source = MakeSource(1005);
  ProgressRegistry::Instance().Register(source);
  source->BeginQuery("SELECT 1");
  source->Detach();

  auto snap = FindByPid(1005);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->percent, -1);
  EXPECT_EQ(snap->rows_processed, 0);

  ProgressRegistry::Instance().Unregister(source.get());
}

TEST(ProgressRegistryTest,
     ClosedRegistrationDetachesBeforeTheConnectionIsDestroyed) {
  duckdb::DuckDB db{nullptr};
  auto conn = std::make_unique<duckdb::Connection>(
    duckdb::make_shared_ptr<ConnectionContext>(
      *db.instance, SessionIdentity{.user = "postgres",
                                    .database = "memory",
                                    .backend_pid = 1006,
                                    .client_backend = true}));
  auto& state = ConnectionContext::Get(*conn->context);
  ProgressRegistration registration{state.ProgressSource()};
  state.ProgressSource()->BeginQuery("SELECT 1");

  bool listed = false;
  std::atomic_bool snapshotted;
  std::thread observer{[&] {
    listed = FindByPid(1006).has_value();
    snapshotted.store(true, std::memory_order_relaxed);
  }};
  while (!snapshotted.load(std::memory_order_relaxed)) {
  }
  registration = {};
  conn.reset();
  observer.join();

  EXPECT_TRUE(listed);
  EXPECT_FALSE(FindByPid(1006).has_value());
}

TEST(ProgressNamesTest, CommandIoTypePhase) {
  EXPECT_EQ(ProgressCommandName(ProgressCommand::CopyFrom), "COPY FROM");
  EXPECT_EQ(ProgressCommandName(ProgressCommand::CopyTo), "COPY TO");
  EXPECT_EQ(ProgressCommandName(ProgressCommand::CreateIndex), "CREATE INDEX");
  EXPECT_EQ(ProgressCommandName(ProgressCommand::None), "");
  EXPECT_EQ(ProgressIoTypeName(ProgressIoType::Pipe), "PIPE");
  EXPECT_EQ(ProgressIoTypeName(ProgressIoType::File), "FILE");
  EXPECT_EQ(ProgressPhaseName(
              ProgressCommand::CreateIndex,
              std::to_underlying(progress_phase::CreateIndex::BuildingIndex)),
            "building index");
  EXPECT_EQ(ProgressPhaseName(
              ProgressCommand::Vacuum,
              std::to_underlying(progress_phase::Vacuum::VacuumingIndexes)),
            "vacuuming indexes");
  EXPECT_EQ(ProgressCommandName(ProgressCommand::BaseBackup), "BASEBACKUP");
  EXPECT_EQ(ProgressPhaseName(ProgressCommand::BaseBackup,
                              std::to_underlying(
                                progress_phase::BaseBackup::WaitingForCommits)),
            "waiting for in-flight commits");
  EXPECT_EQ(ProgressPhaseName(ProgressCommand::CopyFrom, 1), "");
}

}  // namespace
}  // namespace sdb::pg
