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

#include <memory>

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
  EXPECT_GT(snap->query_start_us, 0);

  source->EndQuery();
  snap = FindByPid(1002);
  ASSERT_TRUE(snap.has_value());
  // last statement text stays visible, only the start timestamp is cleared
  EXPECT_EQ(snap->query, "SELECT 1");
  EXPECT_EQ(snap->query_start_us, 0);

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
  ProgressMetrics::Add(m.tuples_processed, 100);
  ProgressMetrics::Add(m.tuples_processed, 50);
  ProgressMetrics::Add(m.bytes_processed, 4096);

  auto snap = FindByPid(1003);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(snap->command, std::to_underlying(ProgressCommand::CopyFrom));
  EXPECT_EQ(snap->io_type, std::to_underlying(ProgressIoType::Pipe));
  EXPECT_EQ(snap->relid, 42);
  EXPECT_EQ(snap->tuples_processed, 150);
  EXPECT_EQ(snap->bytes_processed, 4096);

  source->EndQuery();
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
  EXPECT_EQ(ProgressPhaseName(ProgressCommand::CopyFrom, 1), "");
}

}  // namespace
}  // namespace sdb::pg
