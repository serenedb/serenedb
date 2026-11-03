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

#include <absl/synchronization/mutex.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iresearch/index/index_reader.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <yaclib/async/contract.hpp>
#include <yaclib/exe/inline.hpp>

#include "search/compaction/merge_scheduler.h"
#include "search/store_stats.h"
#include "server/utils/metrics.h"

namespace {

using sdb::search::DescribeInputs;
using sdb::search::EstimateMerge;
using sdb::search::MaintainedStore;
using sdb::search::MergeJob;
using sdb::search::MergeLane;
using sdb::search::MergeOrigin;
using sdb::search::MergeOutcome;
using sdb::search::MergeResult;
using sdb::search::MergeScheduler;

constexpr uint64_t kMiB = uint64_t{1} << 20;

MergeResult ResultOf(MergeOutcome outcome) {
  switch (outcome) {
    case MergeOutcome::Merged:
      return {.outcome = {.size = 1, .error = irs::CompactionError::Ok}};
    case MergeOutcome::Deferred:
      return {.outcome = {.size = 1, .error = irs::CompactionError::Pending}};
    case MergeOutcome::Nothing:
      return {.outcome = {.error = irs::CompactionError::Ok}};
    case MergeOutcome::Busy:
      return {.outcome = {.size = 1, .error = irs::CompactionError::Busy}};
    case MergeOutcome::Cancelled:
      return {.outcome = {.error = irs::CompactionError::Cancelled}};
    case MergeOutcome::Running:
    case MergeOutcome::Failed:
      break;
  }
  return {};
}

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
  std::map<std::string, yaclib::Promise<MergeResult>> promises;
  std::map<std::string, const std::atomic_bool*> stops;
  std::map<std::string, size_t> helpers;
  absl::Mutex destroyed_mutex;
  std::vector<std::string> destroyed;
  std::set<std::string> stale;
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

  bool Stale() const noexcept final { return _board.stale.contains(_name); }

  yaclib::Future<MergeResult> Run(const std::atomic_bool& stop,
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
    auto [future, promise] = yaclib::MakeContract<MergeResult>();
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
    _threads = threads;
    return std::make_unique<MergeScheduler>(executor, threads);
  }

  std::shared_ptr<MergeScheduler::Relation> Relation(uint64_t id) {
    return std::make_shared<MergeScheduler::Relation>(id);
  }

  size_t FreeThreads() const {
    const auto busy = static_cast<size_t>(
      sdb::metrics::Get(sdb::metrics::Gauge::CompactionThreads));
    return busy < _threads ? _threads - busy : 0;
  }

  struct Job {
    std::string name;
    uint64_t bytes = 0;
  };

  void Enqueue(MergeScheduler& scheduler,
               const std::shared_ptr<MergeScheduler::Relation>& relation,
               std::string name, uint64_t bytes, size_t segments = 4) {
    std::vector<std::unique_ptr<MergeJob>> jobs;
    jobs.emplace_back(
      std::make_unique<FakeJob>(_board, std::move(name), bytes, segments));
    scheduler.Enqueue(relation, std::move(jobs));
  }

  void EnqueueRound(MergeScheduler& scheduler,
                    const std::shared_ptr<MergeScheduler::Relation>& relation,
                    std::vector<Job> round) {
    std::vector<std::unique_ptr<MergeJob>> jobs;
    jobs.reserve(round.size());
    for (auto& job : round) {
      jobs.emplace_back(
        std::make_unique<FakeJob>(_board, std::move(job.name), job.bytes, 4));
    }
    scheduler.Enqueue(relation, std::move(jobs));
  }

  void Complete(const std::string& name, MergeOutcome outcome) {
    auto it = _board.promises.find(name);
    ASSERT_NE(it, _board.promises.end()) << name;
    auto promise = std::move(it->second);
    _board.promises.erase(it);
    std::move(promise).Set(ResultOf(outcome));
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
  size_t _threads = 0;
};

TEST(MergeEstimateTest, LanesAndScores) {
  SegmentStub big{"a", 300 * kMiB, 100, 100};
  SegmentStub half{"b", 100 * kMiB, 100, 50};
  SegmentStub empty{"c", kMiB, 0, 0};
  const irs::SubReader* const segments[] = {&big, &half, &empty};

  const auto merge = EstimateMerge(segments);
  EXPECT_EQ(350 * kMiB, merge.rewrite_bytes);
  EXPECT_EQ(MergeLane::Big, merge.lane);
  EXPECT_DOUBLE_EQ(static_cast<double>(353 * kMiB) / 2, merge.score);

  const auto purge = EstimateMerge({&segments[1], 1});
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
  EXPECT_EQ(1, relation->CompactionGeneration());
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(3, relation->CompactionGeneration());
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
  EXPECT_FALSE(Started("big2"));
  EXPECT_FALSE(Started("big3"));
  EXPECT_EQ(1, _board.helpers["big1"]);
  EXPECT_EQ(1, FreeThreads());

  Enqueue(*scheduler, relations[0], "small", kMiB);
  EXPECT_TRUE(Started("small"));
  EXPECT_EQ(0, FreeThreads());
  EXPECT_FALSE(scheduler->TryAcquireThread());

  Complete("big1", MergeOutcome::Merged);
  EXPECT_TRUE(Started("big2"));
  EXPECT_TRUE(Started("big3"));
  EXPECT_EQ(0, _board.helpers["big2"]);
  EXPECT_EQ(0, _board.helpers["big3"]);
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(3, FreeThreads());
}

TEST_F(MergeSchedulerTest, ARelationTakesAtMostHalfTheThreadsForBigMerges) {
  auto scheduler = Make(4);
  auto first = Relation(1);
  auto second = Relation(2);
  EXPECT_EQ(3, scheduler->Wanted(*first));
  EnqueueRound(*scheduler, first,
               {{.name = "a1", .bytes = 512 * kMiB},
                {.name = "a2", .bytes = 512 * kMiB},
                {.name = "a3", .bytes = 512 * kMiB}});
  EXPECT_TRUE(Started("a1"));
  EXPECT_TRUE(Started("a2"));
  EXPECT_FALSE(Started("a3"));
  EXPECT_EQ(1, _board.helpers["a1"]);
  EXPECT_EQ(0, _board.helpers["a2"]);
  EXPECT_EQ(1, scheduler->Wanted(*first));

  Enqueue(*scheduler, second, "b1", 512 * kMiB);
  EXPECT_FALSE(Started("b1"));
  EXPECT_EQ(2, scheduler->Wanted(*second));

  Complete("a1", MergeOutcome::Merged);
  EXPECT_TRUE(Started("a3"));
  EXPECT_TRUE(Started("b1"));
  EXPECT_EQ(1, scheduler->Wanted(*first));
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(3, scheduler->Wanted(*first));
}

TEST_F(MergeSchedulerTest, SmallMergesRunBesideBigMergesWaitingForTheirTurn) {
  auto scheduler = Make(4);
  auto relation = Relation(1);
  EnqueueRound(*scheduler, relation,
               {{.name = "big1", .bytes = 512 * kMiB},
                {.name = "big2", .bytes = 512 * kMiB},
                {.name = "big3", .bytes = 512 * kMiB}});
  EXPECT_TRUE(Started("big1"));
  EXPECT_TRUE(Started("big2"));
  EXPECT_FALSE(Started("big3"));
  ASSERT_EQ(1, scheduler->Wanted(*relation));
  Enqueue(*scheduler, relation, "small", 8 * kMiB);
  EXPECT_TRUE(Started("small"));
  EXPECT_EQ(0, scheduler->Wanted(*relation));
  CompleteAll(MergeOutcome::Merged);
}

TEST_F(MergeSchedulerTest, CheapMergesGoFirstAndWaitingOnesAge) {
  auto scheduler = Make(2);
  auto blocker = Relation(1);
  auto first = Relation(2);
  auto second = Relation(3);
  auto third = Relation(4);
  auto small = Relation(5);
  Enqueue(*scheduler, blocker, "blocker", 512 * kMiB);
  Enqueue(*scheduler, first, "huge", 64 * 1024 * kMiB, 2);
  Enqueue(*scheduler, second, "cheap", 256 * kMiB, 8);
  EXPECT_FALSE(Started("huge"));
  EXPECT_FALSE(Started("cheap"));
  const auto blocking = scheduler->Active(1);
  ASSERT_EQ(1, blocking.size());
  EXPECT_FALSE(blocking[0].Queued());
  const auto huge = scheduler->Active(2);
  ASSERT_EQ(1, huge.size());
  EXPECT_TRUE(huge[0].Queued());
  EXPECT_EQ(std::vector<std::string>({"huge.0", "huge.1"}),
            huge[0].stats.inputs);
  EXPECT_EQ(2, huge[0].relation_id);
  EXPECT_EQ(1, scheduler->Active(3).size());
  EXPECT_EQ(2, sdb::metrics::Get(sdb::metrics::Gauge::CompactionPending));
  EXPECT_EQ(1, sdb::metrics::Get(sdb::metrics::Gauge::CompactionThreads));

  Complete("blocker", MergeOutcome::Merged);
  const auto history = scheduler->History(1);
  ASSERT_EQ(1, history.size());
  EXPECT_EQ(blocking[0].id, history[0].id);
  EXPECT_EQ(MergeOutcome::Merged, history[0].outcome);
  EXPECT_EQ(1, sdb::metrics::Get(sdb::metrics::Gauge::CompactionPending));
  EXPECT_TRUE(Started("cheap"));
  EXPECT_FALSE(Started("huge"));

  for (size_t i = 0; i != 8 * 12; ++i) {
    const auto name = "small" + std::to_string(i);
    Enqueue(*scheduler, small, name, kMiB);
    Complete(name, MergeOutcome::Merged);
  }
  EXPECT_FALSE(Started("huge"));
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

TEST_F(MergeSchedulerTest, StaleQueuedMergesLeaveWithoutAThread) {
  auto scheduler = Make(1);
  const auto busy = Relation(1);
  const auto dead = Relation(2);
  Enqueue(*scheduler, busy, "busy", kMiB);
  ASSERT_TRUE(Started("busy"));
  const auto round = scheduler->Wanted(*dead);
  Enqueue(*scheduler, dead, "dead", kMiB);
  ASSERT_FALSE(Started("dead"));
  EXPECT_EQ(round - 1, scheduler->Wanted(*dead));
  _board.stale.insert("dead");
  Enqueue(*scheduler, busy, "next", kMiB);
  EXPECT_TRUE(Destroyed("dead"));
  EXPECT_FALSE(Started("dead"));
  EXPECT_EQ(round, scheduler->Wanted(*dead));
  EXPECT_EQ(1, dead->CompactionGeneration());
  CompleteAll(MergeOutcome::Merged);
}

TEST_F(MergeSchedulerTest, StopRunningRaisesOnlyThatRelationsFlags) {
  auto scheduler = Make(4);
  auto first = Relation(1);
  auto second = Relation(2);
  Enqueue(*scheduler, first, "a", 512 * kMiB);
  Enqueue(*scheduler, second, "b", kMiB);
  scheduler->StopRunning(*first);
  EXPECT_TRUE(_board.stops["a"]->load());
  EXPECT_FALSE(_board.stops["b"]->load());

  Complete("a", MergeOutcome::Cancelled);
  EXPECT_EQ(0, first->CompactionGeneration());
  EXPECT_TRUE(scheduler->StartRound(*first).empty());
  CompleteAll(MergeOutcome::Merged);
}

TEST_F(MergeSchedulerTest, FailedMergesBlockTheirInputsWithGrowingBackoff) {
  auto scheduler = Make(2);
  auto relation = Relation(1);
  Enqueue(*scheduler, relation, "a", 512 * kMiB, 2);
  Complete("a", MergeOutcome::Failed);
  EXPECT_EQ(1, relation->CompactionGeneration());
  auto blocked = scheduler->StartRound(*relation);
  std::ranges::sort(blocked);
  EXPECT_EQ(std::vector<std::string>({"a.0", "a.1"}), blocked);
  EXPECT_TRUE(scheduler->StartRound(*relation).empty());

  Enqueue(*scheduler, relation, "b", 512 * kMiB, 1);
  Complete("b", MergeOutcome::Failed);
  EXPECT_EQ(1, scheduler->StartRound(*relation).size());
  EXPECT_EQ(1, scheduler->StartRound(*relation).size());
  EXPECT_TRUE(scheduler->StartRound(*relation).empty());

  Enqueue(*scheduler, relation, "c", 512 * kMiB, 1);
  Complete("c", MergeOutcome::Merged);
  Enqueue(*scheduler, relation, "d", 512 * kMiB, 1);
  Complete("d", MergeOutcome::Failed);
  EXPECT_EQ(1, scheduler->StartRound(*relation).size());
  EXPECT_TRUE(scheduler->StartRound(*relation).empty());

  Enqueue(*scheduler, relation, "e", 512 * kMiB, 1);
  Complete("e", MergeOutcome::Busy);
  EXPECT_TRUE(scheduler->StartRound(*relation).empty());
  EXPECT_EQ(5, relation->CompactionGeneration());
}

TEST_F(MergeSchedulerTest, AFailedMergeIsRetriedOnlyAfterItsBlockedRounds) {
  auto scheduler = Make(1);
  auto relation = Relation(1);
  std::vector<uint64_t> attempts;
  for (uint64_t round = 1; round != 16; ++round) {
    if (!scheduler->StartRound(*relation).empty()) {
      continue;
    }
    attempts.emplace_back(round);
    Enqueue(*scheduler, relation, "a", 512 * kMiB, 1);
    Complete("a", MergeOutcome::Failed);
  }
  EXPECT_EQ(std::vector<uint64_t>({1, 3, 6, 11}), attempts);
}

TEST_F(MergeSchedulerTest, TheFailureBackoffStopsGrowingAtItsCap) {
  auto scheduler = Make(1);
  auto relation = Relation(1);
  for (size_t failure = 0; failure != 10; ++failure) {
    Enqueue(*scheduler, relation, "a", 512 * kMiB, 1);
    Complete("a", MergeOutcome::Failed);
  }
  size_t rounds = 0;
  while (rounds != 1000 && !scheduler->StartRound(*relation).empty()) {
    ++rounds;
  }
  EXPECT_EQ(300, rounds);
}

TEST_F(MergeSchedulerTest, AThrowingJobFailsAndFreesItsThread) {
  auto scheduler = Make(1);
  auto relation = Relation(1);
  _board.throw_on_run = true;
  Enqueue(*scheduler, relation, "a", 512 * kMiB, 1);
  EXPECT_TRUE(Destroyed("a"));
  EXPECT_EQ(1, scheduler->StartRound(*relation).size());
  EXPECT_EQ(1, FreeThreads());
}

TEST_F(MergeSchedulerTest, HelpersComeFromTheBigMergesThreads) {
  auto scheduler = Make(4);
  auto first = Relation(1);
  auto second = Relation(2);
  Enqueue(*scheduler, first, "a", 512 * kMiB);
  EXPECT_EQ(2, _board.helpers["a"]);
  EXPECT_EQ(1, FreeThreads());
  EXPECT_FALSE(scheduler->TryAcquireThread());
  Enqueue(*scheduler, second, "b", 512 * kMiB);
  EXPECT_FALSE(Started("b"));
  Enqueue(*scheduler, second, "small", kMiB);
  EXPECT_TRUE(Started("small"));
  EXPECT_EQ(0, FreeThreads());

  Complete("a", MergeOutcome::Merged);
  EXPECT_TRUE(Started("b"));
  EXPECT_EQ(2, _board.helpers["b"]);
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(4, FreeThreads());
}

TEST_F(MergeSchedulerTest, AManualCompactionHoldsABigThread) {
  auto scheduler = Make(4);
  auto relation = Relation(1);
  EXPECT_TRUE(scheduler->TryAcquireThread());
  EXPECT_TRUE(scheduler->TryAcquireThread());
  EXPECT_TRUE(scheduler->TryAcquireThread());
  EXPECT_FALSE(scheduler->TryAcquireThread());
  Enqueue(*scheduler, relation, "big", 512 * kMiB);
  EXPECT_FALSE(Started("big"));
  Enqueue(*scheduler, relation, "small", kMiB);
  EXPECT_TRUE(Started("small"));

  scheduler->ReleaseThread();
  EXPECT_TRUE(Started("big"));
  EXPECT_EQ(0, _board.helpers["big"]);
  scheduler->ReleaseThread();
  scheduler->ReleaseThread();
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(4, FreeThreads());
}

TEST_F(MergeSchedulerTest, ARoundStartsAllItsMergesBeforeAnyGetsHelpers) {
  auto scheduler = Make(4);
  auto relation = Relation(1);
  EnqueueRound(*scheduler, relation,
               {{.name = "big1", .bytes = 512 * kMiB},
                {.name = "big2", .bytes = 512 * kMiB},
                {.name = "small", .bytes = kMiB}});
  EXPECT_TRUE(Started("big1"));
  EXPECT_TRUE(Started("big2"));
  EXPECT_TRUE(Started("small"));
  EXPECT_EQ(1, _board.helpers["big1"]);
  EXPECT_EQ(0, _board.helpers["big2"]);
  EXPECT_EQ(0, _board.helpers["small"]);
  EXPECT_EQ(0, FreeThreads());

  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(4, FreeThreads());
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
  EXPECT_FALSE(scheduler->TryAcquireThread());
  Enqueue(*scheduler, relation, "late", 512 * kMiB);
  EXPECT_TRUE(Destroyed("late"));
  EXPECT_FALSE(Started("late"));
  EXPECT_TRUE(scheduler->Active(1).empty());
  EXPECT_EQ(0, sdb::metrics::Get(sdb::metrics::Gauge::CompactionPending));
  EXPECT_EQ(0, sdb::metrics::Get(sdb::metrics::Gauge::CompactionThreads));
}

TEST_F(MergeSchedulerTest, StopReturnsTheHelpersOfRunningMerges) {
  auto scheduler = Make(4);
  auto first = Relation(1);
  auto second = Relation(2);
  Enqueue(*scheduler, first, "running", 512 * kMiB);
  Enqueue(*scheduler, second, "queued", 512 * kMiB);
  EXPECT_EQ(2, _board.helpers["running"]);
  EXPECT_FALSE(Started("queued"));
  EXPECT_EQ(1, FreeThreads());
  EXPECT_EQ(3, sdb::metrics::Get(sdb::metrics::Gauge::CompactionThreads));

  std::thread stopper{[&] { scheduler->Stop(); }};
  while (!_board.stops["running"]->load()) {
    std::this_thread::yield();
  }
  Complete("running", MergeOutcome::Cancelled);
  stopper.join();
  EXPECT_TRUE(Destroyed("queued"));
  EXPECT_EQ(4, FreeThreads());
  EXPECT_EQ(0, sdb::metrics::Get(sdb::metrics::Gauge::CompactionPending));
  EXPECT_EQ(0, sdb::metrics::Get(sdb::metrics::Gauge::CompactionThreads));
}

TEST_F(MergeSchedulerTest, AnyThrownValueFailsTheMerge) {
  auto scheduler = Make(1);
  auto relation = Relation(1);
  _board.throw_int_on_run = true;
  Enqueue(*scheduler, relation, "a", 512 * kMiB, 1);
  EXPECT_TRUE(Destroyed("a"));
  EXPECT_EQ(1, scheduler->StartRound(*relation).size());
  EXPECT_EQ(1, FreeThreads());
}

TEST_F(MergeSchedulerTest, SmallMergesFillingTheThreadsHoldBackBigOnes) {
  auto scheduler = Make(3);
  auto first = Relation(1);
  auto second = Relation(2);
  Enqueue(*scheduler, first, "small1", kMiB);
  Enqueue(*scheduler, second, "small2", kMiB);
  Enqueue(*scheduler, first, "big1", 512 * kMiB);
  EXPECT_TRUE(Started("big1"));
  EXPECT_EQ(0, _board.helpers["big1"]);
  Enqueue(*scheduler, second, "big2", 512 * kMiB);
  EXPECT_FALSE(Started("big2"));
  EXPECT_FALSE(scheduler->TryAcquireThread());

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
  EXPECT_EQ(2, scheduler->StartRound(*relation).size());
  EXPECT_TRUE(scheduler->StartRound(*relation).empty());
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
  EXPECT_EQ(0, first->CompactionGeneration());
  lease = {};
  EXPECT_EQ(1, first->CompactionGeneration());
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
  EXPECT_EQ(1, relation->CompactionGeneration());
  nested = {};
  EXPECT_EQ(2, relation->CompactionGeneration());
  EXPECT_EQ(scheduler->RoundSize(), scheduler->Wanted(*relation));

  auto quiet = Relation(5);
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
  EXPECT_EQ(0, relation->CompactionGeneration());
  EXPECT_EQ(1, FreeThreads());
  EXPECT_EQ(2, scheduler->Wanted(*relation));
}

TEST_F(MergeSchedulerTest, ABigMergeThatNeverRunsReturnsItsHelpers) {
  auto scheduler = Make(4, yaclib::MakeInline(yaclib::StopTag{}));
  auto relation = Relation(1);
  Enqueue(*scheduler, relation, "a", 512 * kMiB);
  EXPECT_FALSE(Started("a"));
  EXPECT_TRUE(Destroyed("a"));
  EXPECT_EQ(4, FreeThreads());
  EXPECT_EQ(0, sdb::metrics::Get(sdb::metrics::Gauge::CompactionThreads));
}

TEST_F(MergeSchedulerTest, AMergeKeepsOneIdFromTheQueueToTheHistory) {
  auto scheduler = Make(1);
  auto relation = Relation(1);
  Enqueue(*scheduler, relation, "running", 512 * kMiB);
  Enqueue(*scheduler, relation, "dropped", 512 * kMiB);
  ASSERT_EQ(2, scheduler->Active(1).size());
  scheduler->CancelQueued(*relation);
  const auto active = scheduler->Active(1);
  ASSERT_EQ(1, active.size());
  EXPECT_FALSE(active[0].Queued());

  Complete("running", MergeOutcome::Merged);
  EXPECT_TRUE(scheduler->Active(1).empty());
  auto history = scheduler->History(1);
  ASSERT_EQ(1, history.size());
  EXPECT_EQ(active[0].id, history[0].id);
  EXPECT_EQ(MergeOutcome::Merged, history[0].outcome);
  EXPECT_EQ(4, history[0].stats.inputs.size());

  Enqueue(*scheduler, relation, "revoked", 512 * kMiB);
  EXPECT_EQ(1, scheduler->Active(1).size());
  Complete("revoked", MergeOutcome::Cancelled);
  EXPECT_TRUE(scheduler->Active(1).empty());
  EXPECT_EQ(1, scheduler->History(1).size());

  _board.throw_on_run = true;
  Enqueue(*scheduler, relation, "thrown", 512 * kMiB, 1);
  history = scheduler->History(1);
  ASSERT_EQ(2, history.size());
  EXPECT_EQ(MergeOutcome::Failed, history[1].outcome);
  EXPECT_EQ("boom", history[1].error);
}

TEST_F(MergeSchedulerTest, ForegroundMergesAreRecordedBesideQueuedOnes) {
  auto scheduler = Make(1);
  auto relation = Relation(1);
  const SegmentStub first{"_1", kMiB, 10, 10};
  const SegmentStub second{"_2", kMiB, 30, 20};
  const irs::SubReader* const segments[] = {&first, &second};
  const auto vacuum = scheduler->StartForeground(1, MergeOrigin::Vacuum,
                                                 DescribeInputs(segments));
  Enqueue(*scheduler, relation, "background", 512 * kMiB);
  const auto busy = scheduler->StartForeground(1, MergeOrigin::Sql, {});
  const auto nothing = scheduler->StartForeground(1, MergeOrigin::Sql, {});
  const auto active = scheduler->Active(1);
  ASSERT_EQ(4, active.size());
  EXPECT_EQ(vacuum, active[0].id);
  EXPECT_EQ(MergeOrigin::Vacuum, active[0].origin);
  EXPECT_FALSE(active[0].Queued());
  EXPECT_EQ(0, active[0].queued_us);
  EXPECT_EQ(40, active[0].stats.input_docs);
  EXPECT_EQ(30, active[0].stats.input_live_docs);
  EXPECT_EQ(MergeOrigin::Background, active[1].origin);
  EXPECT_EQ(busy, active[2].id);
  EXPECT_EQ(nothing, active[3].id);

  scheduler->FinishForeground(vacuum, MergeOutcome::Merged, {}, {});
  scheduler->FinishForeground(busy, MergeOutcome::Busy, {}, {});
  scheduler->FinishForeground(nothing, MergeOutcome::Nothing, {}, {});
  Complete("background", MergeOutcome::Merged);
  EXPECT_TRUE(scheduler->Active(1).empty());
  const auto history = scheduler->History(1);
  ASSERT_EQ(3, history.size());
  EXPECT_EQ(vacuum, history[0].id);
  EXPECT_EQ(MergeOutcome::Merged, history[0].outcome);
  EXPECT_EQ(std::vector<std::string>({"_1", "_2"}), history[0].stats.inputs);
  EXPECT_EQ(busy, history[1].id);
  EXPECT_EQ(MergeOutcome::Busy, history[1].outcome);
  EXPECT_EQ(active[1].id, history[2].id);
  EXPECT_EQ(MergeOutcome::Merged, history[2].outcome);
}

TEST_F(MergeSchedulerTest, AStoreSeesTheNudgesOfItsRelation) {
  auto scheduler = Make(2);
  MaintainedStore store{3, {}, nullptr};
  const auto& relation = store.MergeRelation();
  EXPECT_EQ(3, relation->id);
  scheduler->TakeOver(relation);
  EXPECT_EQ(1, store.CompactionGeneration());
  store.NudgeCompaction();
  EXPECT_EQ(2, relation->CompactionGeneration());
  EXPECT_EQ(scheduler->RoundSize(), scheduler->Wanted(*relation));
}

}  // namespace
