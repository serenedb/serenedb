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

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
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

class RefusingExecutor final : public irs::MergeExecutor {
 public:
  bool Run(irs::MergePlan&) final { return false; }
};

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

  irs::MergeReservation Reserve(const Positions& positions,
                                std::vector<std::string>* seen = nullptr,
                                bool* called = nullptr) {
    return _writer->ReserveMerges([&](const irs::IndexReader& index,
                                      const irs::CompactingSegments& reserved,
                                      irs::MergeGroups& groups) {
      if (called) {
        *called = true;
      }
      if (seen) {
        seen->assign(reserved.begin(), reserved.end());
        std::ranges::sort(*seen);
      }
      for (const auto& group : positions) {
        auto& candidates = groups.emplace_back();
        for (const auto i : group) {
          candidates.emplace_back(&index[i]);
        }
      }
    });
  }

  std::vector<std::string> ReservedNames() {
    std::vector<std::string> seen;
    Reserve({}, &seen);
    return seen;
  }

  irs::CompactionResult Run(
    irs::MergeTicket ticket,
    const irs::MergeWriter::FlushProgress& progress = {},
    irs::MergeExecutor* executor = nullptr) {
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
  EXPECT_FALSE(reservation.busy);
  ASSERT_EQ(2, reservation.tickets.size());
  EXPECT_EQ(Names({{0, 1}}), NamesOf(reservation.tickets[0]));
  EXPECT_EQ(Names({{2, 3}}), NamesOf(reservation.tickets[1]));
  EXPECT_FALSE(reservation.tickets[0].Revoked());
  EXPECT_EQ(Names(), ReservedNames());

  const auto second = Names({{2, 3}});
  reservation.tickets[0].Reset();
  EXPECT_FALSE(reservation.tickets[0]);
  EXPECT_TRUE(reservation.tickets[0].Candidates().empty());
  EXPECT_FALSE(reservation.tickets[0].Revoked());
  EXPECT_EQ(second, ReservedNames());

  reservation.tickets.clear();
  EXPECT_TRUE(ReservedNames().empty());
}

TEST_F(MergeTicketTest, MovedTicketsKeepOneReservation) {
  AddSegments(2);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.tickets.size());
  irs::MergeTicket moved{std::move(reservation.tickets[0])};
  EXPECT_FALSE(reservation.tickets[0]);
  ASSERT_TRUE(moved);

  irs::MergeTicket assigned;
  assigned = std::move(moved);
  EXPECT_FALSE(moved);
  ASSERT_TRUE(assigned);
  auto& self = assigned;
  assigned = std::move(self);
  ASSERT_TRUE(assigned);
  EXPECT_EQ(Names(), ReservedNames());

  AddSegments(2);
  auto other = Reserve({{2, 3}});
  ASSERT_EQ(1, other.tickets.size());
  assigned = std::move(other.tickets[0]);
  EXPECT_EQ(Names({{2, 3}}), ReservedNames());
  assigned = {};
  EXPECT_TRUE(ReservedNames().empty());
}

TEST_F(MergeTicketTest, OverlappingGroupsAreBusy) {
  AddSegments(3);
  auto first = Reserve({{0, 1}, {1, 2}});
  EXPECT_TRUE(first.busy);
  ASSERT_EQ(1, first.tickets.size());
  EXPECT_EQ(Names({{0, 1}}), NamesOf(first.tickets[0]));

  auto second = Reserve({{1, 2}});
  EXPECT_TRUE(second.busy);
  EXPECT_TRUE(second.tickets.empty());

  EXPECT_EQ(CompactionError::Busy, _writer->Compact(MergeEverything).error);
  EXPECT_EQ(Names({{0, 1}}), ReservedNames());
}

TEST_F(MergeTicketTest, ReservesNothingWithoutWork) {
  bool called = false;
  auto empty = Reserve({{}}, nullptr, &called);
  EXPECT_FALSE(called);
  EXPECT_FALSE(empty.busy);
  EXPECT_TRUE(empty.tickets.empty());

  AddSegments(1);
  auto none = Reserve({{}, {0}});
  EXPECT_FALSE(none.busy);
  EXPECT_TRUE(none.tickets.empty());
  EXPECT_TRUE(ReservedNames().empty());

  AddSegments(1, 0.5);
  ASSERT_LT(_writer->GetSnapshot()[1].live_docs_count(),
            _writer->GetSnapshot()[1].docs_count());
  auto purge = Reserve({{1}});
  ASSERT_EQ(1, purge.tickets.size());
  EXPECT_EQ(Names({{1}}), NamesOf(purge.tickets[0]));
}

TEST_F(MergeTicketTest, RunMergePublishesAndHandsTheReservationToTheCommit) {
  AddSegments(4, 0.2);
  const auto live = _writer->GetSnapshot().live_docs_count();
  auto reservation = Reserve({{0, 1}, {2, 3}});
  ASSERT_EQ(2, reservation.tickets.size());
  for (auto& ticket : reservation.tickets) {
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

TEST_F(MergeTicketTest, RunMergeDuringACommitPublishesWithTheNextOne) {
  AddSegments(3);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.tickets.size());
  InsertUncommitted();
  ASSERT_TRUE(_writer->RefreshBegin());
  EXPECT_EQ(CompactionError::Pending,
            Run(std::move(reservation.tickets[0])).error);
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
  ASSERT_EQ(1, reservation.tickets.size());

  auto floor = _writer->ArmCompactionFloor();
  ASSERT_TRUE(floor.Held());
  EXPECT_FALSE(floor.Pending());
  EXPECT_TRUE(reservation.tickets[0].Revoked());

  const auto result = Run(std::move(reservation.tickets[0]));
  EXPECT_EQ(CompactionError::Cancelled, result.error);
  EXPECT_EQ(0, result.size);
  EXPECT_FALSE(result);
  EXPECT_EQ(3, _writer->GetSnapshot().size());

  std::vector<std::string> seen;
  auto below = Reserve({{0, 1, 2}}, &seen);
  EXPECT_EQ(Names(), seen);
  EXPECT_FALSE(below.busy);
  EXPECT_TRUE(below.tickets.empty());

  AddSegments(2);
  auto above = Reserve({{0, 1, 2, 3, 4}});
  ASSERT_EQ(1, above.tickets.size());
  EXPECT_EQ(Names({{3, 4}}), NamesOf(above.tickets[0]));
  EXPECT_FALSE(_writer->ArmCompactionFloor().Held());

  floor = {};
  above.tickets.clear();
  EXPECT_EQ(2, Reserve({{0, 1}, {2, 3}}).tickets.size());
}

TEST_F(MergeTicketTest, APendingFloorBlocksReservationsUntilReleased) {
  AddSegments(3);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.tickets.size());
  InsertUncommitted();
  ASSERT_TRUE(_writer->RefreshBegin());
  ASSERT_EQ(CompactionError::Pending,
            Run(std::move(reservation.tickets[0])).error);
  _writer->RefreshFinish();

  auto pending = _writer->ArmCompactionFloor();
  EXPECT_FALSE(pending.Held());
  EXPECT_TRUE(pending.Pending());
  bool called = false;
  auto blocked = Reserve({{2, 3}}, nullptr, &called);
  EXPECT_TRUE(blocked.busy);
  EXPECT_FALSE(called);
  EXPECT_TRUE(blocked.tickets.empty());
  EXPECT_EQ(CompactionError::Busy, _writer->Compact(MergeEverything).error);

  pending = _writer->ArmCompactionFloor();
  EXPECT_TRUE(pending.Pending());
  EXPECT_TRUE(Reserve({{2, 3}}).busy);

  ASSERT_TRUE(_writer->RefreshCommit());
  pending = _writer->ArmCompactionFloor();
  EXPECT_TRUE(pending.Held());
  EXPECT_NE(0, pending.Floor());

  pending = {};
  auto free = Reserve({{0, 1}});
  EXPECT_FALSE(free.busy);
  EXPECT_EQ(1, free.tickets.size());
}

TEST_F(MergeTicketTest, ArmingAFloorStopsARunningMerge) {
  AddSegments(2);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.tickets.size());
  irs::IndexWriter::CompactionFloorGuard floor;
  const irs::MergeWriter::FlushProgress progress = [&] {
    if (!floor.Pending()) {
      floor = _writer->ArmCompactionFloor();
    }
    return true;
  };
  const auto result = Run(std::move(reservation.tickets[0]), progress);
  EXPECT_EQ(CompactionError::Cancelled, result.error);
  EXPECT_EQ(2, result.size);
  EXPECT_TRUE(floor.Pending());
  EXPECT_TRUE(Reserve({{0, 1}}).busy);

  floor = _writer->ArmCompactionFloor();
  EXPECT_TRUE(floor.Held());
  EXPECT_FALSE(_writer->RefreshCommit());
  EXPECT_EQ(2, _writer->GetSnapshot().size());
}

TEST_F(MergeTicketTest, AStoppedMergeIsCancelledAndReleased) {
  AddSegments(2);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.tickets.size());
  const auto result =
    Run(std::move(reservation.tickets[0]), [] { return false; });
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
  ASSERT_EQ(1, reservation.tickets.size());
  RefusingExecutor refusing;
  const auto result = Run(std::move(reservation.tickets[0]), {}, &refusing);
  EXPECT_EQ(CompactionError::Fail, result.error);
  EXPECT_EQ(2, result.size);
  EXPECT_TRUE(ReservedNames().empty());
  EXPECT_FALSE(_writer->RefreshCommit());
  EXPECT_EQ(2, _writer->GetSnapshot().size());
}

TEST_F(MergeTicketTest, ARevokedTicketStaysReleasedAcrossRevocations) {
  AddSegments(2);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.tickets.size());
  {
    auto floor = _writer->ArmCompactionFloor();
    ASSERT_TRUE(floor.Held());
  }
  auto fresh = Reserve({{0, 1}});
  ASSERT_EQ(1, fresh.tickets.size());
  {
    auto floor = _writer->ArmCompactionFloor();
    EXPECT_TRUE(floor.Held());
  }
  EXPECT_TRUE(reservation.tickets[0].Revoked());
  EXPECT_TRUE(fresh.tickets[0].Revoked());
  reservation.tickets.clear();
  fresh.tickets.clear();
  EXPECT_TRUE(ReservedNames().empty());
}

TEST_F(MergeTicketTest, ClearRevokesEveryTicket) {
  AddSegments(4);
  auto reservation = Reserve({{0, 1}, {2, 3}});
  ASSERT_EQ(2, reservation.tickets.size());
  _writer->Clear();
  EXPECT_TRUE(reservation.tickets[0].Revoked());
  EXPECT_TRUE(reservation.tickets[1].Revoked());
  EXPECT_EQ(0, _writer->GetSnapshot().size());

  AddSegments(2);
  auto fresh = Reserve({{0, 1}});
  ASSERT_EQ(1, fresh.tickets.size());
  const auto names = ReservedNames();
  EXPECT_EQ(Names(), names);

  EXPECT_EQ(CompactionError::Cancelled,
            Run(std::move(reservation.tickets[0])).error);
  reservation.tickets.clear();
  EXPECT_EQ(names, ReservedNames());

  EXPECT_EQ(CompactionError::Ok, Run(std::move(fresh.tickets[0])).error);
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(100, _writer->GetSnapshot().live_docs_count());
}

TEST_F(MergeTicketTest, ClearStopsARunningMerge) {
  AddSegments(2);
  auto reservation = Reserve({{0, 1}});
  ASSERT_EQ(1, reservation.tickets.size());
  bool cleared = false;
  const irs::MergeWriter::FlushProgress progress = [&] {
    if (!cleared) {
      cleared = true;
      _writer->Clear();
    }
    return true;
  };
  EXPECT_EQ(CompactionError::Cancelled,
            Run(std::move(reservation.tickets[0]), progress).error);
  EXPECT_TRUE(cleared);
  EXPECT_EQ(0, _writer->GetSnapshot().size());
  EXPECT_TRUE(ReservedNames().empty());

  AddSegments(2);
  EXPECT_EQ(1, Reserve({{0, 1}}).tickets.size());
}

TEST_F(MergeTicketTest, DisjointTicketsMergeConcurrentlyWithACommit) {
  AddSegments(6, 0.1);
  const auto live = _writer->GetSnapshot().live_docs_count();
  auto reservation = Reserve({{0, 1}, {2, 3}, {4, 5}});
  ASSERT_EQ(3, reservation.tickets.size());

  std::vector<irs::CompactionResult> results(reservation.tickets.size());
  std::vector<std::thread> threads;
  for (size_t i = 0; i != reservation.tickets.size(); ++i) {
    threads.emplace_back([this, &results, &reservation, i] {
      results[i] = Run(std::move(reservation.tickets[i]));
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
