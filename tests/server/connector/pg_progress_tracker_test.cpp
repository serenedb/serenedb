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

#include <cstdint>
#include <optional>
#include <utility>

#include "catalog/identifiers/object_id.h"
#include "gtest/gtest.h"
#include "pg/progress_tracker.h"

namespace {

using namespace sdb;
using namespace sdb::pg;

std::optional<ProgressSnapshot> SnapshotForRelid(ObjectId relid,
                                                 ProgressCommand cmd) {
  for (auto& s : ProgressTracker::Instance().GetSnapshots()) {
    if (s.relid == relid && s.command_type == cmd) {
      return s;
    }
  }
  return std::nullopt;
}

int64_t Param(const ProgressSnapshot& s, copy_progress::Param p) {
  return s.params[std::to_underlying(p)];
}

int64_t Param(const ProgressSnapshot& s, create_index_progress::Param p) {
  return s.params[std::to_underlying(p)];
}

TEST(ProgressReporterTest, CopyLifecycleAndInitialParams) {
  const ObjectId datid{42};
  const ObjectId relid{2'000'001};
  ASSERT_FALSE(SnapshotForRelid(relid, ProgressCommand::Copy).has_value())
    << "tracker dirty at start -- another test leaked its relid";

  {
    ProgressReporter reporter{datid, relid, ProgressCommand::Copy};
    reporter.SetCommand(copy_progress::Command::CopyFrom);
    reporter.SetType(copy_progress::Type::File);

    auto snap = SnapshotForRelid(relid, ProgressCommand::Copy);
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->datid, datid);
    EXPECT_EQ(snap->relid, relid);
    EXPECT_EQ(snap->command_type, ProgressCommand::Copy);
    EXPECT_EQ(Param(*snap, copy_progress::Param::Command),
              std::to_underlying(copy_progress::Command::CopyFrom));
    EXPECT_EQ(Param(*snap, copy_progress::Param::Type),
              std::to_underlying(copy_progress::Type::File));
    // Everything not explicitly set starts at zero.
    EXPECT_EQ(Param(*snap, copy_progress::Param::BytesProcessed), 0);
    EXPECT_EQ(Param(*snap, copy_progress::Param::BytesTotal), 0);
    EXPECT_EQ(Param(*snap, copy_progress::Param::TuplesProcessed), 0);
    EXPECT_EQ(Param(*snap, copy_progress::Param::TuplesExcluded), 0);
    EXPECT_EQ(Param(*snap, copy_progress::Param::TuplesSkipped), 0);
  }

  EXPECT_FALSE(SnapshotForRelid(relid, ProgressCommand::Copy).has_value())
    << "destructor must EndCommand and remove the entry";
}

TEST(ProgressReporterTest, SetIsAbsoluteAddAccumulates) {
  const ObjectId datid{42};
  const ObjectId relid{2'000'002};
  ProgressReporter reporter{datid, relid, ProgressCommand::Copy};

  reporter.Set(copy_progress::Param::BytesTotal, 1'000'000);
  reporter.Add(copy_progress::Param::TuplesProcessed, 10);
  reporter.Add(copy_progress::Param::BytesProcessed, 1024);
  reporter.Add(copy_progress::Param::TuplesExcluded, 2);
  reporter.Add(copy_progress::Param::TuplesProcessed, 5);
  reporter.Add(copy_progress::Param::BytesProcessed, 512);
  reporter.Add(copy_progress::Param::TuplesExcluded, 3);

  auto snap = SnapshotForRelid(relid, ProgressCommand::Copy);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(Param(*snap, copy_progress::Param::BytesTotal), 1'000'000);
  EXPECT_EQ(Param(*snap, copy_progress::Param::TuplesProcessed), 15);
  EXPECT_EQ(Param(*snap, copy_progress::Param::BytesProcessed), 1536);
  EXPECT_EQ(Param(*snap, copy_progress::Param::TuplesExcluded), 5);

  // Set replaces; it must not disturb the running counters.
  reporter.Set(copy_progress::Param::BytesTotal, 2'000'000);
  snap = SnapshotForRelid(relid, ProgressCommand::Copy);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(Param(*snap, copy_progress::Param::BytesTotal), 2'000'000);
  EXPECT_EQ(Param(*snap, copy_progress::Param::TuplesProcessed), 15);
}

TEST(ProgressReporterTest, AllCopyCommandAndTypeVariantsRoundTrip) {
  const struct {
    copy_progress::Command cmd;
    copy_progress::Type type;
    ObjectId relid;
  } cases[] = {
    {copy_progress::Command::CopyFrom, copy_progress::Type::File,
     ObjectId{2'000'010}},
    {copy_progress::Command::CopyFrom, copy_progress::Type::Program,
     ObjectId{2'000'011}},
    {copy_progress::Command::CopyFrom, copy_progress::Type::Pipe,
     ObjectId{2'000'012}},
    {copy_progress::Command::CopyFrom, copy_progress::Type::Callback,
     ObjectId{2'000'013}},
    {copy_progress::Command::CopyTo, copy_progress::Type::File,
     ObjectId{2'000'014}},
    {copy_progress::Command::CopyTo, copy_progress::Type::Pipe,
     ObjectId{2'000'015}},
  };
  const ObjectId datid{42};

  for (const auto& c : cases) {
    ProgressReporter reporter{datid, c.relid, ProgressCommand::Copy};
    reporter.SetCommand(c.cmd);
    reporter.SetType(c.type);

    auto snap = SnapshotForRelid(c.relid, ProgressCommand::Copy);
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(Param(*snap, copy_progress::Param::Command),
              std::to_underlying(c.cmd));
    EXPECT_EQ(Param(*snap, copy_progress::Param::Type),
              std::to_underlying(c.type));
  }
}

TEST(ProgressReporterTest, CreateIndexPhasesAndCounters) {
  const ObjectId datid{42};
  const ObjectId relid{2'000'003};
  ProgressReporter reporter{datid, relid, ProgressCommand::CreateIndex};

  reporter.SetCommand(create_index_progress::Command::CreateIndex);
  reporter.SetPhase(create_index_progress::Phase::Initializing);
  reporter.Set(create_index_progress::Param::TuplesTotal, 100);

  auto snap = SnapshotForRelid(relid, ProgressCommand::CreateIndex);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(Param(*snap, create_index_progress::Param::Command),
            std::to_underlying(create_index_progress::Command::CreateIndex));
  EXPECT_EQ(Param(*snap, create_index_progress::Param::Phase),
            std::to_underlying(create_index_progress::Phase::Initializing));
  EXPECT_EQ(Param(*snap, create_index_progress::Param::TuplesTotal), 100);
  EXPECT_EQ(Param(*snap, create_index_progress::Param::TuplesDone), 0);

  reporter.SetPhase(create_index_progress::Phase::BuildingIndex);
  reporter.Add(create_index_progress::Param::TuplesDone, 30);
  reporter.Add(create_index_progress::Param::TuplesDone, 20);
  reporter.SetPhase(create_index_progress::Phase::Committing);

  snap = SnapshotForRelid(relid, ProgressCommand::CreateIndex);
  ASSERT_TRUE(snap.has_value());
  EXPECT_EQ(Param(*snap, create_index_progress::Param::Phase),
            std::to_underlying(create_index_progress::Phase::Committing));
  EXPECT_EQ(Param(*snap, create_index_progress::Param::TuplesDone), 50);
}

}  // namespace
