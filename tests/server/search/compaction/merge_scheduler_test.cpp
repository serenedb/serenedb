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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iresearch/index/index_reader.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <yaclib/async/contract.hpp>
#include <yaclib/exe/inline.hpp>

#include "search/compaction/merge_scheduler.h"

namespace {

using sdb::search::EstimateMerge;
using sdb::search::MakeMergeRelation;
using sdb::search::MergeJob;
using sdb::search::MergeLane;
using sdb::search::MergeOutcome;
using sdb::search::MergeScheduler;

constexpr uint64_t kMiB = uint64_t{1} << 20;

class SegmentStub final : public irs::SubReader {
 public:
  SegmentStub(std::string name, uint64_t bytes, uint32_t docs, uint32_t live)
    : _meta{.name = std::move(name),
            .docs_count = docs,
            .live_docs_count = live,
            .byte_size = bytes} {}

  uint64_t CountMappedMemory() const final { return 0; }
  const irs::SegmentInfo& Meta() const final { return _meta; }
  const irs::DocumentMask* docs_mask() const final { return nullptr; }
  irs::lead::Node::ptr docs_iterator() const final { return nullptr; }
  std::span<const irs::field_id> field_ids() const final { return {}; }
  const irs::TermReader* field(irs::field_id) const final { return nullptr; }
  irs::NormReader::ptr norms(irs::field_id) const final { return {}; }

 private:
  irs::SegmentInfo _meta;
};

struct Board {
  std::vector<std::string> started;
  std::map<std::string, yaclib::Promise<MergeOutcome>> promises;
  std::map<std::string, const std::atomic_bool*> stops;
  std::map<std::string, size_t> helpers;
  std::mutex destroyed_mutex;
  std::vector<std::string> destroyed;
  bool throw_on_run = false;
  bool throw_int_on_run = false;
};

class FakeJob final : public MergeJob {
 public:
  FakeJob(Board& board, std::string name, uint64_t bytes, size_t segments)
    : _board{board}, _name{std::move(name)} {
    for (size_t i = 0; i != segments; ++i) {
      _segments.emplace_back(std::make_unique<SegmentStub>(
        _name + "." + std::to_string(i), bytes / segments, 100, 100));
      _candidates.emplace_back(_segments.back().get());
    }
  }

  ~FakeJob() final {
    std::lock_guard lock{_board.destroyed_mutex};
    _board.destroyed.emplace_back(_name);
  }

  irs::CompactionView Candidates() const noexcept final { return _candidates; }

  yaclib::Future<MergeOutcome> Run(const std::atomic_bool& stop,
                                   size_t helpers) final {
    if (_board.throw_on_run) {
      throw std::runtime_error{"boom"};
    }
    if (_board.throw_int_on_run) {
      throw 42;
    }
    _board.started.emplace_back(_name);
    _board.stops[_name] = &stop;
    _board.helpers[_name] = helpers;
    auto [future, promise] = yaclib::MakeContract<MergeOutcome>();
    _board.promises.emplace(_name, std::move(promise));
    return std::move(future);
  }

 private:
  Board& _board;
  std::string _name;
  std::vector<std::unique_ptr<SegmentStub>> _segments;
  irs::Compaction _candidates;
};

class MergeSchedulerTest : public ::testing::Test {
 protected:
  std::unique_ptr<MergeScheduler> Make(
    size_t threads, yaclib::IExecutor& executor = yaclib::MakeInline()) {
    MergeScheduler::Options options;
    options.threads = threads;
    options.helpers_per_merge = threads / 2;
    options.now = [this] { return _now; };
    return std::make_unique<MergeScheduler>(executor, std::move(options));
  }

  std::shared_ptr<MergeScheduler::Relation> Relation(uint64_t id) {
    return std::make_shared<MergeScheduler::Relation>(
      id, [this, id] { ++_nudges[id]; });
  }

  void Enqueue(MergeScheduler& scheduler,
               const std::shared_ptr<MergeScheduler::Relation>& relation,
               std::string name, uint64_t bytes, size_t segments = 4) {
    scheduler.Enqueue(relation, std::make_unique<FakeJob>(
                                  _board, std::move(name), bytes, segments));
  }

  void Complete(const std::string& name, MergeOutcome outcome) {
    auto it = _board.promises.find(name);
    ASSERT_NE(it, _board.promises.end()) << name;
    auto promise = std::move(it->second);
    _board.promises.erase(it);
    std::move(promise).Set(outcome);
  }

  bool Started(const std::string& name) const {
    return std::ranges::find(_board.started, name) != _board.started.end();
  }

  bool Destroyed(const std::string& name) {
    std::lock_guard lock{_board.destroyed_mutex};
    return std::ranges::find(_board.destroyed, name) != _board.destroyed.end();
  }

  void CompleteAll(MergeOutcome outcome) {
    while (!_board.promises.empty()) {
      Complete(_board.promises.begin()->first, outcome);
    }
  }

  Board _board;
  absl::Time _now = absl::FromUnixSeconds(1'000'000);
  std::map<uint64_t, size_t> _nudges;
};

TEST(MergeEstimateTest, LanesAndScores) {
  SegmentStub big{"a", 300 * kMiB, 100, 100};
  SegmentStub half{"b", 100 * kMiB, 100, 50};
  SegmentStub empty{"c", kMiB, 0, 0};
  const irs::SubReader* const segments[] = {&big, &half, &empty};

  const auto merge = EstimateMerge(segments, 64 * kMiB);
  EXPECT_EQ(401 * kMiB, merge.bytes);
  EXPECT_EQ(350 * kMiB, merge.rewrite_bytes);
  EXPECT_EQ(3, merge.segments);
  EXPECT_EQ(MergeLane::Big, merge.lane);
  EXPECT_DOUBLE_EQ(static_cast<double>(353 * kMiB) / 2, merge.score);

  const auto purge = EstimateMerge({&segments[1], 1}, 64 * kMiB);
  EXPECT_EQ(50 * kMiB, purge.rewrite_bytes);
  EXPECT_EQ(MergeLane::Small, purge.lane);
  EXPECT_DOUBLE_EQ(static_cast<double>(51 * kMiB), purge.score);
}

TEST_F(MergeSchedulerTest, SmallMergesRunBesideABigOneOnOneThread) {
  auto scheduler = Make(1);
  auto relation = Relation(1);
  Enqueue(*scheduler, relation, "big", 512 * kMiB);
  Enqueue(*scheduler, relation, "small1", 8 * kMiB);
  Enqueue(*scheduler, relation, "small2", 8 * kMiB);
  EXPECT_TRUE(Started("big"));
  EXPECT_TRUE(Started("small1"));
  EXPECT_FALSE(Started("small2"));
  EXPECT_EQ(0, _board.helpers["big"]);
  EXPECT_EQ(0, _board.helpers["small1"]);

  Complete("small1", MergeOutcome::Merged);
  EXPECT_TRUE(Destroyed("small1"));
  EXPECT_TRUE(Started("small2"));
  EXPECT_EQ(1, _nudges[1]);
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(3, _nudges[1]);
}

TEST_F(MergeSchedulerTest, BigMergesLeaveAThreadForSmallOnes) {
  auto scheduler = Make(3);
  std::vector<std::shared_ptr<MergeScheduler::Relation>> relations;
  for (uint64_t id = 1; id <= 3; ++id) {
    relations.emplace_back(Relation(id));
    Enqueue(*scheduler, relations.back(), "big" + std::to_string(id),
            512 * kMiB);
  }
  EXPECT_TRUE(Started("big1"));
  EXPECT_TRUE(Started("big2"));
  EXPECT_FALSE(Started("big3"));
  EXPECT_EQ(1, _board.helpers["big1"]);
  EXPECT_EQ(1, scheduler->FreeThreads());

  Enqueue(*scheduler, relations[0], "small", kMiB);
  EXPECT_TRUE(Started("small"));
  EXPECT_EQ(0, scheduler->FreeThreads());
  EXPECT_FALSE(scheduler->TryAcquireHelper());

  Complete("big1", MergeOutcome::Merged);
  EXPECT_TRUE(Started("big3"));
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(3, scheduler->FreeThreads());
}

TEST_F(MergeSchedulerTest, ARelationTakesAtMostHalfTheThreadsForBigMerges) {
  auto scheduler = Make(4);
  auto first = Relation(1);
  auto second = Relation(2);
  EXPECT_EQ(3, scheduler->Wanted(*first));
  Enqueue(*scheduler, first, "a1", 512 * kMiB);
  Enqueue(*scheduler, first, "a2", 512 * kMiB);
  Enqueue(*scheduler, first, "a3", 512 * kMiB);
  EXPECT_TRUE(Started("a1"));
  EXPECT_TRUE(Started("a2"));
  EXPECT_FALSE(Started("a3"));
  EXPECT_EQ(0, scheduler->Wanted(*first));

  Enqueue(*scheduler, second, "b1", 512 * kMiB);
  EXPECT_TRUE(Started("b1"));
  EXPECT_EQ(2, scheduler->Wanted(*second));

  Complete("a1", MergeOutcome::Merged);
  EXPECT_TRUE(Started("a3"));
  EXPECT_EQ(1, scheduler->Wanted(*first));
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(3, scheduler->Wanted(*first));
}

TEST_F(MergeSchedulerTest, CheapMergesGoFirstAndWaitingOnesAge) {
  auto scheduler = Make(2);
  auto blocker = Relation(1);
  auto first = Relation(2);
  auto second = Relation(3);
  auto third = Relation(4);
  Enqueue(*scheduler, blocker, "blocker", 512 * kMiB);
  Enqueue(*scheduler, first, "huge", 64 * 1024 * kMiB, 2);
  Enqueue(*scheduler, second, "cheap", 256 * kMiB, 8);
  EXPECT_FALSE(Started("huge"));
  EXPECT_FALSE(Started("cheap"));
  const auto queued = scheduler->Snapshot();
  ASSERT_EQ(3, queued.size());
  EXPECT_TRUE(queued[0].running);
  EXPECT_FALSE(queued[1].running);
  EXPECT_EQ(std::vector<std::string>({"huge.0", "huge.1"}), queued[1].inputs);
  EXPECT_EQ(2, queued[1].relation);

  Complete("blocker", MergeOutcome::Merged);
  EXPECT_TRUE(Started("cheap"));
  EXPECT_FALSE(Started("huge"));

  _now += absl::Seconds(30 * 12);
  Enqueue(*scheduler, third, "fresh", 256 * kMiB, 8);
  Complete("cheap", MergeOutcome::Merged);
  EXPECT_TRUE(Started("huge"));
  EXPECT_FALSE(Started("fresh"));
  CompleteAll(MergeOutcome::Merged);
  EXPECT_TRUE(Started("fresh"));
  CompleteAll(MergeOutcome::Merged);
}

TEST_F(MergeSchedulerTest, CancelQueuedDropsOnlyThatRelation) {
  auto scheduler = Make(1);
  auto blocker = Relation(1);
  auto first = Relation(2);
  auto second = Relation(3);
  Enqueue(*scheduler, blocker, "blocker", 512 * kMiB);
  Enqueue(*scheduler, first, "a", 512 * kMiB);
  Enqueue(*scheduler, second, "b", 512 * kMiB);
  EXPECT_EQ(1, scheduler->Wanted(*first));

  scheduler->CancelQueued(*first);
  EXPECT_TRUE(Destroyed("a"));
  EXPECT_FALSE(Destroyed("b"));
  EXPECT_EQ(2, scheduler->Wanted(*first));

  Complete("blocker", MergeOutcome::Merged);
  EXPECT_FALSE(Started("a"));
  EXPECT_TRUE(Started("b"));
  CompleteAll(MergeOutcome::Merged);
}

TEST_F(MergeSchedulerTest, StopRunningRaisesOnlyThatRelationsFlags) {
  auto scheduler = Make(4);
  auto first = Relation(1);
  auto second = Relation(2);
  Enqueue(*scheduler, first, "a", 512 * kMiB);
  Enqueue(*scheduler, second, "b", 512 * kMiB);
  scheduler->StopRunning(*first);
  EXPECT_TRUE(_board.stops["a"]->load());
  EXPECT_FALSE(_board.stops["b"]->load());

  Complete("a", MergeOutcome::Cancelled);
  EXPECT_EQ(0, _nudges[1]);
  EXPECT_TRUE(scheduler->Blocked(*first).empty());
  CompleteAll(MergeOutcome::Merged);
}

TEST_F(MergeSchedulerTest, FailedMergesBlockTheirInputsWithGrowingBackoff) {
  auto scheduler = Make(2);
  auto relation = Relation(1);
  Enqueue(*scheduler, relation, "a", 512 * kMiB, 2);
  Complete("a", MergeOutcome::Failed);
  EXPECT_EQ(1, _nudges[1]);
  auto blocked = scheduler->Blocked(*relation);
  std::ranges::sort(blocked);
  EXPECT_EQ(std::vector<std::string>({"a.0", "a.1"}), blocked);

  _now += absl::Seconds(1);
  EXPECT_TRUE(scheduler->Blocked(*relation).empty());

  Enqueue(*scheduler, relation, "b", 512 * kMiB, 1);
  Complete("b", MergeOutcome::Failed);
  _now += absl::Seconds(1);
  EXPECT_EQ(1, scheduler->Blocked(*relation).size());
  _now += absl::Seconds(1);
  EXPECT_TRUE(scheduler->Blocked(*relation).empty());

  Enqueue(*scheduler, relation, "c", 512 * kMiB, 1);
  Complete("c", MergeOutcome::Merged);
  Enqueue(*scheduler, relation, "d", 512 * kMiB, 1);
  Complete("d", MergeOutcome::Failed);
  _now += absl::Seconds(1);
  EXPECT_TRUE(scheduler->Blocked(*relation).empty());

  Enqueue(*scheduler, relation, "e", 512 * kMiB, 1);
  Complete("e", MergeOutcome::Busy);
  EXPECT_TRUE(scheduler->Blocked(*relation).empty());
  EXPECT_EQ(5, _nudges[1]);
}

TEST_F(MergeSchedulerTest, AThrowingJobFailsAndFreesItsThread) {
  auto scheduler = Make(1);
  auto relation = Relation(1);
  _board.throw_on_run = true;
  Enqueue(*scheduler, relation, "a", 512 * kMiB, 1);
  EXPECT_TRUE(Destroyed("a"));
  EXPECT_EQ(1, scheduler->Blocked(*relation).size());
  EXPECT_EQ(1, scheduler->FreeThreads());
}

TEST_F(MergeSchedulerTest, HelpersComeFromTheBigMergesThreads) {
  auto scheduler = Make(4);
  auto first = Relation(1);
  auto second = Relation(2);
  Enqueue(*scheduler, first, "a", 512 * kMiB);
  EXPECT_TRUE(scheduler->TryAcquireHelper());
  EXPECT_TRUE(scheduler->TryAcquireHelper());
  EXPECT_FALSE(scheduler->TryAcquireHelper());
  Enqueue(*scheduler, second, "b", 512 * kMiB);
  EXPECT_FALSE(Started("b"));
  Enqueue(*scheduler, second, "small", kMiB);
  EXPECT_TRUE(Started("small"));

  scheduler->ReleaseHelper();
  EXPECT_TRUE(Started("b"));
  scheduler->ReleaseHelper();
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(4, scheduler->FreeThreads());
}

TEST_F(MergeSchedulerTest, StopDropsTheQueueAndWaitsForRunningMerges) {
  auto scheduler = Make(1);
  auto relation = Relation(1);
  Enqueue(*scheduler, relation, "running", 512 * kMiB);
  Enqueue(*scheduler, relation, "queued", 512 * kMiB);
  EXPECT_FALSE(Started("queued"));

  std::atomic_bool stopped{false};
  std::thread stopper{[&] {
    scheduler->Stop();
    stopped = true;
  }};
  while (!_board.stops["running"]->load()) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds{50});
  EXPECT_FALSE(stopped.load());
  Complete("running", MergeOutcome::Cancelled);
  stopper.join();
  EXPECT_TRUE(stopped.load());
  EXPECT_TRUE(Destroyed("queued"));

  EXPECT_EQ(0, scheduler->Wanted(*relation));
  EXPECT_FALSE(scheduler->TryAcquireHelper());
  Enqueue(*scheduler, relation, "late", 512 * kMiB);
  EXPECT_TRUE(Destroyed("late"));
  EXPECT_FALSE(Started("late"));
  EXPECT_TRUE(scheduler->Snapshot().empty());
}

TEST_F(MergeSchedulerTest, AnyThrownValueFailsTheMerge) {
  auto scheduler = Make(1);
  auto relation = Relation(1);
  _board.throw_int_on_run = true;
  Enqueue(*scheduler, relation, "a", 512 * kMiB, 1);
  EXPECT_TRUE(Destroyed("a"));
  EXPECT_EQ(1, scheduler->Blocked(*relation).size());
  EXPECT_EQ(1, scheduler->FreeThreads());
}

TEST_F(MergeSchedulerTest, SmallMergesFillingTheThreadsHoldBackBigOnes) {
  auto scheduler = Make(3);
  auto first = Relation(1);
  auto second = Relation(2);
  Enqueue(*scheduler, first, "big1", 512 * kMiB);
  Enqueue(*scheduler, first, "small1", kMiB);
  Enqueue(*scheduler, second, "small2", kMiB);
  EXPECT_TRUE(Started("small2"));
  Enqueue(*scheduler, second, "big2", 512 * kMiB);
  EXPECT_FALSE(Started("big2"));
  EXPECT_FALSE(scheduler->TryAcquireHelper());

  Complete("small1", MergeOutcome::Merged);
  EXPECT_TRUE(Started("big2"));
  CompleteAll(MergeOutcome::Merged);
}

TEST_F(MergeSchedulerTest, ADeferredMergeResetsTheFailureBackoff) {
  auto scheduler = Make(2);
  auto relation = Relation(1);
  Enqueue(*scheduler, relation, "a", 512 * kMiB, 1);
  Complete("a", MergeOutcome::Failed);
  Enqueue(*scheduler, relation, "b", 512 * kMiB, 1);
  Complete("b", MergeOutcome::Deferred);
  Enqueue(*scheduler, relation, "c", 512 * kMiB, 1);
  Complete("c", MergeOutcome::Failed);
  _now += absl::Seconds(1);
  EXPECT_TRUE(scheduler->Blocked(*relation).empty());
}

TEST_F(MergeSchedulerTest, ARelationWithoutANudgeStillFinishes) {
  auto scheduler = Make(1);
  auto relation = std::make_shared<MergeScheduler::Relation>(9, nullptr);
  EXPECT_EQ(2, scheduler->RoundSize());
  Enqueue(*scheduler, relation, "a", 512 * kMiB);
  Complete("a", MergeOutcome::Merged);
  EXPECT_TRUE(Destroyed("a"));
  EXPECT_EQ(2, scheduler->Wanted(*relation));
}

TEST_F(MergeSchedulerTest, TakingARelationOverStopsItsMergesAndPausesIt) {
  auto scheduler = Make(1);
  auto first = Relation(1);
  auto second = Relation(2);
  Enqueue(*scheduler, first, "running", 512 * kMiB);
  Enqueue(*scheduler, first, "queued", 512 * kMiB);
  Enqueue(*scheduler, second, "other", 512 * kMiB);
  EXPECT_FALSE(Started("other"));

  std::atomic_bool taken{false};
  MergeScheduler::Lease lease;
  std::thread taker{[&] {
    lease = scheduler->TakeOver(first);
    taken = true;
  }};
  while (!_board.stops["running"]->load()) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds{50});
  EXPECT_FALSE(taken.load());
  Complete("running", MergeOutcome::Cancelled);
  taker.join();
  EXPECT_TRUE(taken.load());
  EXPECT_TRUE(lease);
  EXPECT_TRUE(Destroyed("queued"));
  EXPECT_TRUE(Started("other"));

  EXPECT_EQ(0, scheduler->Wanted(*first));
  Enqueue(*scheduler, first, "late", 512 * kMiB);
  EXPECT_TRUE(Destroyed("late"));
  EXPECT_EQ(0, _nudges[1]);
  lease = {};
  EXPECT_EQ(1, _nudges[1]);
  EXPECT_EQ(2, scheduler->Wanted(*first));
  CompleteAll(MergeOutcome::Merged);
}

TEST_F(MergeSchedulerTest, ALeaseIsReleasedOnceWhereverItMoves) {
  auto scheduler = Make(2);
  auto relation = Relation(1);
  auto lease = scheduler->TakeOver(relation);
  EXPECT_TRUE(lease);
  MergeScheduler::Lease moved{std::move(lease)};
  EXPECT_FALSE(lease);
  MergeScheduler::Lease assigned;
  assigned = std::move(moved);
  auto& self = assigned;
  assigned = std::move(self);
  EXPECT_TRUE(assigned);

  auto nested = scheduler->TakeOver(relation);
  assigned = {};
  EXPECT_EQ(0, scheduler->Wanted(*relation));
  EXPECT_EQ(1, _nudges[1]);
  nested = {};
  EXPECT_EQ(2, _nudges[1]);
  EXPECT_EQ(scheduler->RoundSize(), scheduler->Wanted(*relation));

  auto quiet = std::make_shared<MergeScheduler::Relation>(5, nullptr);
  {
    auto silent = scheduler->TakeOver(quiet);
    EXPECT_EQ(0, scheduler->Wanted(*quiet));
  }
  EXPECT_EQ(scheduler->RoundSize(), scheduler->Wanted(*quiet));
}

TEST_F(MergeSchedulerTest, ADroppedSubmissionCountsAsCancelled) {
  auto scheduler = Make(1, yaclib::MakeInline(yaclib::StopTag{}));
  auto relation = Relation(1);
  Enqueue(*scheduler, relation, "a", 512 * kMiB);
  EXPECT_FALSE(Started("a"));
  EXPECT_TRUE(Destroyed("a"));
  EXPECT_EQ(0, _nudges[1]);
  EXPECT_EQ(1, scheduler->FreeThreads());
  EXPECT_EQ(2, scheduler->Wanted(*relation));
}

struct NudgedStorage {
  void NudgeCompaction() noexcept { ++nudges; }

  size_t nudges = 0;
};

TEST_F(MergeSchedulerTest, ARelationNudgesItsStorageOnlyWhileItLives) {
  auto scheduler = Make(2);
  auto storage = std::make_shared<NudgedStorage>();
  const std::weak_ptr<NudgedStorage> weak = storage;
  const auto relation = MakeMergeRelation(3, weak);
  EXPECT_EQ(3, relation->Id());
  scheduler->TakeOver(relation);
  EXPECT_EQ(1, storage->nudges);
  storage.reset();
  scheduler->TakeOver(relation);
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(scheduler->RoundSize(), scheduler->Wanted(*relation));
}

}  // namespace
