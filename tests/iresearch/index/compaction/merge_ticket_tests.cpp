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

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <duckdb/common/fault_points.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/directory_utils.hpp>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
#include "index/merge/support/parallel_merge.hpp"
#include "index/merge/support/segment_gen.hpp"
#include "insert_field.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests::merge;
using irs::CompactionError;

using Positions = std::vector<std::vector<size_t>>;

void MergeEverything(irs::Compaction& candidates, const irs::IndexReader& index,
                     const irs::CompactingSegments&) {
  for (const auto& segment : index) {
    candidates.emplace_back(&segment);
  }
}

class MergeTicketTest : public ::testing::Test {
 protected:
  void SetUp() final {
    auto options = irs::tests::DefaultWriterOptions();
    options.column_options = SketchedColumns;
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
  }

  void AddSegments(size_t count, double delete_ratio = 0) {
    for (size_t i = 0; i != count; ++i) {
      SegmentShape shape;
      shape.docs = 50;
      shape.delete_ratio = delete_ratio;
      shape.columns = {0, 1};
      WriteSegment(*_writer, _gen, shape, _next_id);
    }
  }

  void InsertUncommitted() {
    auto trx = _writer->GetBatch();
    auto doc = trx.Insert();
    tests::StringField id{"id", absl::StrCat("doc-", _next_id++)};
    id.id = kIdField;
    tests::InsertField(doc, id);
    trx.Commit();
  }

  std::vector<std::string> Names(const Positions& positions = {}) const {
    const auto snapshot = _writer->GetSnapshot();
    std::vector<std::string> names;
    if (positions.empty()) {
      for (const auto& segment : snapshot) {
        names.emplace_back(segment.Meta().name);
      }
    }
    for (const auto& group : positions) {
      for (const auto i : group) {
        names.emplace_back(snapshot[i].Meta().name);
      }
    }
    std::ranges::sort(names);
    return names;
  }

  std::vector<irs::MergeTicket> Reserve(
    const Positions& positions, std::vector<std::string>* seen = nullptr,
    bool* called = nullptr, bool* busy = nullptr) {
    bool blocked = false;
    auto tickets = _writer->ReserveMerges(
      [&](const irs::IndexReader& index,
          const irs::CompactingSegments& reserved, irs::MergeGroups& groups) {
        if (called) {
          *called = true;
        }
        if (seen) {
          seen->clear();
          for (const auto& segment : index) {
            if (reserved.contains(segment.Meta().name)) {
              seen->emplace_back(segment.Meta().name);
            }
          }
          std::ranges::sort(*seen);
        }
        for (const auto& group : positions) {
          auto& candidates = groups.emplace_back();
          for (const auto i : group) {
            candidates.emplace_back(&index[i]);
          }
        }
      },
      blocked);
    if (busy) {
      *busy = blocked;
    }
    return tickets;
  }

  std::vector<std::string> ReservedNames() {
    std::vector<std::string> seen;
    Reserve({}, &seen);
    return seen;
  }

  irs::CompactionResult Run(irs::MergeTicket ticket,
                            const irs::MergeProgress& progress = {}) {
    const auto executor = MakeExecutor();
    return irs::GetReady(_writer->RunMerge(std::move(ticket), nullptr, progress,
                                           nullptr, executor));
  }

  static std::vector<std::string> NamesOf(const irs::MergeTicket& ticket) {
    std::vector<std::string> names;
    for (const auto* candidate : ticket.Candidates()) {
      names.emplace_back(candidate->Meta().name);
    }
    std::ranges::sort(names);
    return names;
  }

  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
  Generator _gen{7};
  size_t _next_id = 0;
};

TEST_F(MergeTicketTest, ReservesDisjointGroupsAndReleasesThem) {
  AddSegments(4);
  std::vector<std::string> seen{"stale"};
  auto reservation = Reserve({{0, 1}, {2, 3}}, &seen);
  EXPECT_TRUE(seen.empty());
  ASSERT_EQ(2, reservation.size());
  EXPECT_EQ(Names({{0, 1}}), NamesOf(reservation[0]));
  EXPECT_EQ(Names({{2, 3}}), NamesOf(reservation[1]));
  EXPECT_FALSE(reservation[0].Revoked());
  EXPECT_EQ(Names(), ReservedNames());

  const auto second = Names({{2, 3}});
  {
    const auto released = std::move(reservation[0]);
  }
  EXPECT_TRUE(reservation[0].Candidates().empty());
  EXPECT_FALSE(reservation[0].Revoked());
  EXPECT_EQ(second, ReservedNames());

  reservation.clear();
  EXPECT_TRUE(ReservedNames().empty());
}

TEST_F(MergeTicketTest, MovedTicketsKeepOneReservation) {
  AddSegments(2);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.size());
  const auto first = Names({{0, 1}});
  irs::MergeTicket moved{std::move(reservation[0])};
  EXPECT_TRUE(reservation[0].Candidates().empty());
  EXPECT_EQ(first, NamesOf(moved));

  auto& self = moved;
  moved = std::move(self);
  EXPECT_EQ(first, NamesOf(moved));
  EXPECT_EQ(Names(), ReservedNames());

  AddSegments(2);
  auto other = Reserve({{2, 3}});
  ASSERT_EQ(1, other.size());
  moved = std::move(other[0]);
  EXPECT_TRUE(other[0].Candidates().empty());
  EXPECT_EQ(Names({{2, 3}}), ReservedNames());
  {
    const auto released = std::move(moved);
  }
  EXPECT_TRUE(ReservedNames().empty());
}

TEST_F(MergeTicketTest, OverlappingGroupsAreBusy) {
  AddSegments(3);
  auto first = Reserve({{0, 1}, {1, 2}});
  ASSERT_EQ(1, first.size());
  EXPECT_EQ(Names({{0, 1}}), NamesOf(first[0]));

  bool busy = false;
  auto second = Reserve({{1, 2}}, nullptr, nullptr, &busy);
  EXPECT_TRUE(second.empty());
  EXPECT_TRUE(busy);

  EXPECT_EQ(CompactionError::Busy, _writer->Compact(MergeEverything).error);
  EXPECT_EQ(Names({{0, 1}}), ReservedNames());
}

TEST_F(MergeTicketTest, ReservesNothingWithoutWork) {
  bool called = false;
  auto empty = Reserve({{}}, nullptr, &called);
  EXPECT_FALSE(called);
  EXPECT_TRUE(empty.empty());

  AddSegments(1);
  bool busy = true;
  auto none = Reserve({{}, {}}, nullptr, nullptr, &busy);
  EXPECT_TRUE(none.empty());
  EXPECT_FALSE(busy);
  const auto compacted = _writer->Compact(MergeEverything);
  EXPECT_EQ(CompactionError::Ok, compacted.error);
  EXPECT_EQ(0, compacted.size);
  EXPECT_TRUE(ReservedNames().empty());

  auto lone = Reserve({{}, {0}});
  ASSERT_EQ(1, lone.size());
  EXPECT_EQ(Names({{0}}), NamesOf(lone[0]));
}

TEST_F(MergeTicketTest, RunMergePublishesAndHandsTheReservationToTheCommit) {
  AddSegments(4, 0.2);
  const auto live = _writer->GetSnapshot().live_docs_count();
  auto reservation = Reserve({{0, 1}, {2, 3}});
  ASSERT_EQ(2, reservation.size());
  for (auto& ticket : reservation) {
    const auto result = Run(std::move(ticket));
    EXPECT_EQ(CompactionError::Ok, result.error);
    EXPECT_EQ(2, result.size);
    EXPECT_EQ(2, result.stats.inputs.size());
    EXPECT_FALSE(result.stats.output.empty());
  }
  EXPECT_EQ(4, ReservedNames().size());

  ASSERT_TRUE(_writer->RefreshCommit());
  const auto snapshot = _writer->GetSnapshot();
  EXPECT_EQ(2, snapshot.size());
  EXPECT_EQ(live, snapshot.live_docs_count());
  EXPECT_EQ(snapshot.docs_count(), snapshot.live_docs_count());
  EXPECT_TRUE(ReservedNames().empty());
}

TEST_F(MergeTicketTest, AReservedMergeWhoseInputIsDroppedIsBusy) {
  AddSegments(2);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.size());
  std::vector<std::string> first;
  for (size_t i = 0; i != 50; ++i) {
    first.emplace_back(absl::StrCat("doc-", i));
  }
  RemoveTerms(*_writer, kIdField, first);
  ASSERT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(CompactionError::Busy, Run(std::move(reservation[0])).error);
  EXPECT_TRUE(ReservedNames().empty());
}

TEST_F(MergeTicketTest, AWaitingTicketHoldsOnlyItsOwnSegments) {
  AddSegments(4);
  auto reservation = Reserve({{0, 1}, {2, 3}});
  ASSERT_EQ(2, reservation.size());
  const auto replaced = NamesOf(reservation[1]);
  ASSERT_EQ(CompactionError::Ok, Run(std::move(reservation[1])).error);
  ASSERT_TRUE(_writer->RefreshCommit());
  irs::directory_utils::RemoveAllUnreferenced(_dir);
  for (const auto& name : replaced) {
    bool left = false;
    _dir.visit([&](std::string_view file) {
      left |= file.starts_with(absl::StrCat(name, "."));
      return true;
    });
    EXPECT_FALSE(left) << name;
  }
  ASSERT_EQ(CompactionError::Ok, Run(std::move(reservation[0])).error);
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(2, _writer->GetSnapshot().size());
  EXPECT_EQ(200, _writer->GetSnapshot().live_docs_count());
}

TEST_F(MergeTicketTest, RunMergeDuringACommitPublishesWithTheNextOne) {
  AddSegments(3);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.size());
  InsertUncommitted();
  ASSERT_TRUE(_writer->RefreshBegin());
  EXPECT_EQ(CompactionError::Pending, Run(std::move(reservation[0])).error);
  _writer->RefreshFinish();
  EXPECT_EQ(4, _writer->GetSnapshot().size());
  EXPECT_EQ(2, ReservedNames().size());

  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(3, _writer->GetSnapshot().size());
  EXPECT_EQ(151, _writer->GetSnapshot().live_docs_count());
  EXPECT_TRUE(ReservedNames().empty());
}

TEST_F(MergeTicketTest, ArmingAFloorRevokesQueuedTickets) {
  AddSegments(3);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.size());

  auto floor = _writer->ArmCompactionFloor();
  ASSERT_TRUE(floor.Held());
  EXPECT_TRUE(reservation[0].Revoked());

  const auto result = Run(std::move(reservation[0]));
  EXPECT_EQ(CompactionError::Cancelled, result.error);
  EXPECT_EQ(0, result.size);
  EXPECT_FALSE(result);
  EXPECT_EQ(3, _writer->GetSnapshot().size());

  std::vector<std::string> seen;
  auto below = Reserve({{0, 1, 2}}, &seen);
  EXPECT_EQ(Names(), seen);
  EXPECT_TRUE(below.empty());

  AddSegments(2);
  auto above = Reserve({{0, 1, 2, 3, 4}});
  ASSERT_EQ(1, above.size());
  EXPECT_EQ(Names({{3, 4}}), NamesOf(above[0]));
  EXPECT_FALSE(_writer->ArmCompactionFloor().Held());

  floor = {};
  above.clear();
  EXPECT_EQ(2, Reserve({{0, 1}, {2, 3}}).size());
}

TEST_F(MergeTicketTest, APendingFloorBlocksReservationsUntilReleased) {
  AddSegments(3);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.size());
  InsertUncommitted();
  ASSERT_TRUE(_writer->RefreshBegin());
  ASSERT_EQ(CompactionError::Pending, Run(std::move(reservation[0])).error);
  _writer->RefreshFinish();

  auto pending = _writer->ArmCompactionFloor();
  EXPECT_FALSE(pending.Held());
  bool called = false;
  bool busy = false;
  auto blocked = Reserve({{2, 3}}, nullptr, &called, &busy);
  EXPECT_FALSE(called);
  EXPECT_TRUE(blocked.empty());
  EXPECT_TRUE(busy);
  EXPECT_EQ(CompactionError::Busy, _writer->Compact(MergeEverything).error);

  pending = _writer->ArmCompactionFloor();
  EXPECT_FALSE(pending.Held());
  EXPECT_EQ(CompactionError::Busy, _writer->Compact(MergeEverything).error);

  ASSERT_TRUE(_writer->RefreshCommit());
  pending = _writer->ArmCompactionFloor();
  EXPECT_TRUE(pending.Held());
  EXPECT_NE(0, pending.Floor());

  pending = {};
  auto free = Reserve({{0, 1}});
  EXPECT_EQ(1, free.size());
}

TEST_F(MergeTicketTest, ArmingAFloorStopsARunningMerge) {
  AddSegments(2);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.size());
  irs::IndexWriter::CompactionFloorGuard floor;
  bool armed = false;
  const irs::MergeProgress progress = [&] {
    if (!armed) {
      floor = _writer->ArmCompactionFloor();
      armed = true;
    }
    return true;
  };
  const auto result = Run(std::move(reservation[0]), progress);
  EXPECT_EQ(CompactionError::Cancelled, result.error);
  EXPECT_EQ(2, result.size);
  EXPECT_FALSE(floor.Held());
  EXPECT_EQ(CompactionError::Busy, _writer->Compact(MergeEverything).error);

  floor = _writer->ArmCompactionFloor();
  EXPECT_TRUE(floor.Held());
  EXPECT_FALSE(_writer->RefreshCommit());
  EXPECT_EQ(2, _writer->GetSnapshot().size());
}

TEST_F(MergeTicketTest, AStoppedMergeIsCancelledAndReleased) {
  AddSegments(2);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.size());
  const auto result = Run(std::move(reservation[0]), [] { return false; });
  EXPECT_EQ(CompactionError::Cancelled, result.error);
  EXPECT_FALSE(result);
  EXPECT_TRUE(ReservedNames().empty());
  EXPECT_FALSE(_writer->RefreshCommit());
  EXPECT_EQ(2, _writer->GetSnapshot().size());

  EXPECT_EQ(
    CompactionError::Cancelled,
    _writer->Compact(MergeEverything, nullptr, [] { return false; }).error);
  EXPECT_TRUE(ReservedNames().empty());
}

TEST_F(MergeTicketTest, AFailedMergeIsAFailureAndReleased) {
  AddSegments(2);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.size());
  if (!duckdb::FaultPoints::Arm("irs::PipelineMergeExecutor::Run")) {
    GTEST_SKIP() << "built without fault injection";
  }
  const auto result = Run(std::move(reservation[0]));
  EXPECT_TRUE(duckdb::FaultPoints::Disarm("irs::PipelineMergeExecutor::Run"));
  EXPECT_EQ(CompactionError::Fail, result.error);
  EXPECT_EQ(2, result.size);
  EXPECT_TRUE(ReservedNames().empty());
  EXPECT_FALSE(_writer->RefreshCommit());
  EXPECT_EQ(2, _writer->GetSnapshot().size());
}

TEST_F(MergeTicketTest, AThrowingUnitFailsTheMergeAndReleasesItsTicket) {
  if (!duckdb::FaultPoints::Arm("irs::MergeUnit")) {
    GTEST_SKIP() << "built without fault injection";
  }
  AddSegments(2);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.size());
  EXPECT_ANY_THROW(Run(std::move(reservation[0])));
  EXPECT_TRUE(duckdb::FaultPoints::Disarm("irs::MergeUnit"));
  EXPECT_TRUE(ReservedNames().empty());
  EXPECT_EQ(2, _writer->GetSnapshot().size());
  EXPECT_EQ(1, Reserve({{0, 1}}).size());
}

TEST_F(MergeTicketTest, ARevokedTicketStaysReleasedAcrossRevocations) {
  AddSegments(2);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.size());
  {
    auto floor = _writer->ArmCompactionFloor();
    ASSERT_TRUE(floor.Held());
  }
  auto fresh = Reserve({{0, 1}});
  ASSERT_EQ(1, fresh.size());
  {
    auto floor = _writer->ArmCompactionFloor();
    EXPECT_TRUE(floor.Held());
  }
  EXPECT_TRUE(reservation[0].Revoked());
  EXPECT_TRUE(fresh[0].Revoked());
  reservation.clear();
  fresh.clear();
  EXPECT_TRUE(ReservedNames().empty());
}

TEST_F(MergeTicketTest, ClearRevokesEveryTicket) {
  AddSegments(4);
  auto reservation = Reserve({{0, 1}, {2, 3}});
  ASSERT_EQ(2, reservation.size());
  _writer->Clear();
  EXPECT_TRUE(reservation[0].Revoked());
  EXPECT_TRUE(reservation[1].Revoked());
  EXPECT_EQ(0, _writer->GetSnapshot().size());

  AddSegments(2);
  auto fresh = Reserve({{0, 1}});
  ASSERT_EQ(1, fresh.size());
  const auto names = ReservedNames();
  EXPECT_EQ(Names(), names);

  EXPECT_EQ(CompactionError::Cancelled, Run(std::move(reservation[0])).error);
  reservation.clear();
  EXPECT_EQ(names, ReservedNames());

  EXPECT_EQ(CompactionError::Ok, Run(std::move(fresh[0])).error);
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(100, _writer->GetSnapshot().live_docs_count());
}

TEST_F(MergeTicketTest, ClearStopsARunningMerge) {
  AddSegments(2);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.size());
  bool cleared = false;
  const irs::MergeProgress progress = [&] {
    if (!cleared) {
      cleared = true;
      _writer->Clear();
    }
    return true;
  };
  EXPECT_EQ(CompactionError::Cancelled,
            Run(std::move(reservation[0]), progress).error);
  EXPECT_TRUE(cleared);
  EXPECT_EQ(0, _writer->GetSnapshot().size());
  EXPECT_TRUE(ReservedNames().empty());

  AddSegments(2);
  EXPECT_EQ(1, Reserve({{0, 1}}).size());
}

TEST_F(MergeTicketTest, AReservationAfterTheReaderSwapSeesOnlyTheNewSegments) {
  AddSegments(3);
  auto merge = Reserve({{0, 1}});
  ASSERT_EQ(1, merge.size());
  ASSERT_EQ(CompactionError::Ok, Run(std::move(merge[0])).error);
  if (!duckdb::FaultPoints::Arm("pause_irs_commit_after_reader_swap")) {
    GTEST_SKIP() << "built without fault injection";
  }
  std::thread committer{[&] { EXPECT_TRUE(_writer->RefreshCommit()); }};
  absl::Cleanup release = [&] {
    duckdb::FaultPoints::Arm("pause_irs_commit_after_reader_swap:release");
    committer.join();
    duckdb::FaultPoints::Clear();
  };
  while (
    !duckdb::FaultPoints::Disarm("pause_irs_commit_after_reader_swap:parked")) {
    std::this_thread::yield();
  }

  ASSERT_EQ(2, _writer->GetSnapshot().size());
  std::vector<std::string> seen{"stale"};
  auto reservation = Reserve({{0, 1}}, &seen);
  EXPECT_TRUE(seen.empty());
  ASSERT_EQ(1, reservation.size());
  EXPECT_EQ(Names(), NamesOf(reservation[0]));

  std::move(release).Invoke();
  EXPECT_EQ(Names(), ReservedNames());
}

TEST_F(MergeTicketTest, AReservationNeverHoldsSegmentsACommitReplaced) {
  AddSegments(2);
  std::atomic_bool done{false};
  std::atomic_size_t stale{0};
  std::atomic_size_t reserved{0};
  std::thread planner{[&] {
    while (!done.load(std::memory_order_relaxed)) {
      bool blocked = false;
      auto reservation = _writer->ReserveMerges(
        [](const irs::IndexReader& index, const irs::CompactingSegments& busy,
           irs::MergeGroups& groups) {
          auto& group = groups.emplace_back();
          for (const auto& segment : index) {
            if (!busy.contains(segment.Meta().name)) {
              group.emplace_back(&segment);
            }
          }
        },
        blocked);
      if (reservation.empty()) {
        continue;
      }
      ++reserved;
      const auto snapshot = _writer->GetSnapshot();
      for (const auto* candidate : reservation.front().Candidates()) {
        const auto& name = candidate->Meta().name;
        stale += std::none_of(snapshot.begin(), snapshot.end(),
                              [&](const irs::SubReader& segment) {
                                return segment.Meta().name == name;
                              });
      }
    }
  }};
  size_t merged = 0;
  for (size_t round = 0; round != 300; ++round) {
    AddSegments(1);
    auto reservation = Reserve({{0, 1}});
    if (reservation.empty()) {
      continue;
    }
    merged += Run(std::move(reservation[0])).error == CompactionError::Ok;
    _writer->RefreshCommit();
  }
  done = true;
  planner.join();
  EXPECT_EQ(0, stale.load());
  EXPECT_LT(0, merged);
  EXPECT_LT(0, reserved.load());
}

TEST_F(MergeTicketTest, DisjointTicketsMergeConcurrentlyWithACommit) {
  AddSegments(6, 0.1);
  const auto live = _writer->GetSnapshot().live_docs_count();
  auto reservation = Reserve({{0, 1}, {2, 3}, {4, 5}});
  ASSERT_EQ(3, reservation.size());

  std::vector<irs::CompactionResult> results(reservation.size());
  std::vector<std::thread> threads;
  for (size_t i = 0; i != reservation.size(); ++i) {
    threads.emplace_back([this, &results, &reservation, i] {
      results[i] = Run(std::move(reservation[i]));
    });
  }
  InsertUncommitted();
  _writer->RefreshCommit();
  for (auto& thread : threads) {
    thread.join();
  }
  for (const auto& result : results) {
    EXPECT_TRUE(result.error == CompactionError::Ok ||
                result.error == CompactionError::Pending)
      << static_cast<int>(result.error);
  }

  _writer->RefreshCommit();
  _writer->RefreshCommit();
  const auto snapshot = _writer->GetSnapshot();
  EXPECT_EQ(4, snapshot.size());
  EXPECT_EQ(live + 1, snapshot.live_docs_count());
  EXPECT_TRUE(ReservedNames().empty());
}

}  // namespace
