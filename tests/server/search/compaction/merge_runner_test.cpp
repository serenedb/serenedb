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
#include <limits>
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

#include "search/compaction/merge_runner.h"
#include "search/maintenance.h"
#include "server/utils/metrics.h"

namespace {

using sdb::search::DescribeInputs;
using sdb::search::MergeBytes;
using sdb::search::MergeJob;
using sdb::search::MergeOrigin;
using sdb::search::MergeOutcome;
using sdb::search::MergeResult;
using sdb::search::MergeRunner;
using sdb::search::MergeSizeCap;

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
  std::span<const irs::TermReader* const> fields(irs::field_id) const final {
    return {};
  }
  irs::NormReader::ptr norms(irs::field_id) const final { return {}; }

 private:
  irs::SegmentInfo _meta;
};

struct Board {
  std::vector<std::string> started;
  std::map<std::string, yaclib::Promise<MergeResult>> promises;
  std::map<std::string, const std::atomic_bool*> stops;
  absl::Mutex destroyed_mutex;
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

  yaclib::Future<MergeResult> Run(const std::atomic_bool& stop) final {
    if (_board.throw_on_run) {
      throw std::runtime_error{"boom"};
    }
    if (_board.throw_int_on_run) {
      throw 42;
    }
    _board.started.emplace_back(_name);
    _board.stops[_name] = &stop;
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

class MergeRunnerTest : public ::testing::Test {
 protected:
  using Relation = std::shared_ptr<MergeRunner::Relation>;

  struct Job {
    std::string name;
    uint64_t bytes = 0;
  };

  std::unique_ptr<MergeRunner> Make(
    size_t slots, yaclib::IExecutor& executor = yaclib::MakeInline()) {
    return std::make_unique<MergeRunner>(executor, slots);
  }

  Relation MakeRelation(uint64_t id) {
    return std::make_shared<MergeRunner::Relation>(id);
  }

  static int64_t Pending() {
    return sdb::metrics::Get(sdb::metrics::Gauge::CompactionPending);
  }

  static int64_t HeldSlots() {
    return sdb::metrics::Get(sdb::metrics::Gauge::CompactionThreads);
  }

  size_t Start(MergeRunner& runner, const Relation& relation, std::string name,
               uint64_t bytes, size_t segments = 4) {
    std::vector<std::unique_ptr<MergeJob>> jobs;
    jobs.emplace_back(
      std::make_unique<FakeJob>(_board, std::move(name), bytes, segments));
    return runner.Start(relation, std::move(jobs));
  }

  size_t StartAll(MergeRunner& runner, const Relation& relation,
                  std::vector<Job> round) {
    std::vector<std::unique_ptr<MergeJob>> jobs;
    jobs.reserve(round.size());
    for (auto& job : round) {
      jobs.emplace_back(
        std::make_unique<FakeJob>(_board, std::move(job.name), job.bytes, 4));
    }
    return runner.Start(relation, std::move(jobs));
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
};

TEST(MergeSizeTest, AMergeCountsTheLiveBytesOfItsSegments) {
  SegmentStub big{"a", 300 * kMiB, 100, 100};
  SegmentStub half{"b", 100 * kMiB, 100, 50};
  const irs::SubReader* const segments[] = {&big, &half};

  EXPECT_EQ(350 * kMiB, MergeBytes(segments));
  EXPECT_EQ(50 * kMiB, MergeBytes({&segments[1], 1}));
  EXPECT_EQ(0, MergeBytes({}));
}

TEST(MergeSizeTest, TheLastFreeSlotsTakeOnlySmallerMerges) {
  constexpr auto kAny = std::numeric_limits<uint64_t>::max();
  EXPECT_EQ(kAny, MergeSizeCap(6, 0));
  EXPECT_EQ(kAny, MergeSizeCap(6, 2));
  EXPECT_EQ(1024 * kMiB, MergeSizeCap(6, 3));
  EXPECT_EQ(256 * kMiB, MergeSizeCap(6, 4));
  EXPECT_EQ(64 * kMiB, MergeSizeCap(6, 5));
  EXPECT_EQ(kAny, MergeSizeCap(2, 0));
  EXPECT_EQ(64 * kMiB, MergeSizeCap(2, 1));
  for (size_t slots = 2; slots != 40; ++slots) {
    for (size_t merges = 1; merges != slots; ++merges) {
      EXPECT_LE(MergeSizeCap(slots, merges), MergeSizeCap(slots, merges - 1))
        << slots << " " << merges;
    }
  }
}

TEST_F(MergeRunnerTest, RunningMergesNeverExceedTheSlots) {
  auto runner = Make(4);
  auto first = MakeRelation(1);
  auto second = MakeRelation(2);
  EXPECT_EQ(3, StartAll(*runner, first,
                        {{.name = "small1", .bytes = kMiB},
                         {.name = "small2", .bytes = kMiB},
                         {.name = "big1", .bytes = 512 * kMiB}}));
  EXPECT_EQ(1, StartAll(*runner, second,
                        {{.name = "small3", .bytes = kMiB},
                         {.name = "small4", .bytes = kMiB},
                         {.name = "big2", .bytes = 512 * kMiB}}));
  EXPECT_TRUE(Started("small3"));
  EXPECT_FALSE(Started("small4"));
  EXPECT_FALSE(Started("big2"));
  EXPECT_TRUE(Destroyed("small4"));
  EXPECT_TRUE(Destroyed("big2"));
  EXPECT_EQ(4, HeldSlots());
  EXPECT_EQ(1, Pending());
  EXPECT_FALSE(runner->TryHoldBig());

  Complete("small1", MergeOutcome::Merged);
  EXPECT_EQ(1, second->CompactionGeneration());
  EXPECT_EQ(0, Pending());
  EXPECT_EQ(3, HeldSlots());
  EXPECT_EQ(1, Start(*runner, second, "small5", kMiB));
  EXPECT_EQ(0, Start(*runner, second, "small6", kMiB));
  EXPECT_EQ(4, HeldSlots());
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(0, HeldSlots());
}

TEST_F(MergeRunnerTest, BigMergesLeaveASlotForASmallOne) {
  auto runner = Make(3);
  std::vector<Relation> relations;
  for (uint64_t id = 1; id <= 3; ++id) {
    relations.emplace_back(MakeRelation(id));
    Start(*runner, relations.back(), "big" + std::to_string(id), 512 * kMiB);
  }
  EXPECT_TRUE(Started("big1"));
  EXPECT_TRUE(Started("big2"));
  EXPECT_FALSE(Started("big3"));
  EXPECT_TRUE(Destroyed("big3"));
  EXPECT_EQ(1, Pending());
  EXPECT_EQ(2, HeldSlots());
  EXPECT_FALSE(runner->TryHoldBig());

  auto small = MakeRelation(4);
  EXPECT_EQ(1, Start(*runner, small, "small1", kMiB));
  EXPECT_EQ(3, HeldSlots());
  EXPECT_EQ(0, Start(*runner, small, "small2", kMiB));
  EXPECT_EQ(2, Pending());

  Complete("small1", MergeOutcome::Merged);
  EXPECT_EQ(1, relations[2]->CompactionGeneration());
  EXPECT_EQ(2, small->CompactionGeneration());
  EXPECT_EQ(0, Pending());
  EXPECT_EQ(0, Start(*runner, relations[2], "big3", 512 * kMiB));
  EXPECT_EQ(1, Pending());

  Complete("big1", MergeOutcome::Merged);
  EXPECT_EQ(2, relations[2]->CompactionGeneration());
  EXPECT_EQ(1, Start(*runner, relations[2], "big3", 512 * kMiB));
  EXPECT_EQ(0, Pending());
  EXPECT_EQ(2, HeldSlots());
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(0, HeldSlots());
}

TEST_F(MergeRunnerTest, OneSlotStillRunsASmallMergeBesideABigOne) {
  auto runner = Make(1);
  auto relation = MakeRelation(1);
  EXPECT_EQ(1, Start(*runner, relation, "big1", 512 * kMiB));
  EXPECT_EQ(0, Start(*runner, relation, "big2", 512 * kMiB));
  EXPECT_EQ(1, Start(*runner, relation, "small1", 8 * kMiB));
  EXPECT_EQ(0, Start(*runner, relation, "small2", 8 * kMiB));
  EXPECT_TRUE(Started("big1"));
  EXPECT_TRUE(Started("small1"));
  EXPECT_FALSE(Started("small2"));
  EXPECT_TRUE(Destroyed("big2"));
  EXPECT_TRUE(Destroyed("small2"));
  EXPECT_EQ(1, Pending());
  EXPECT_EQ(2, HeldSlots());

  Complete("small1", MergeOutcome::Merged);
  EXPECT_TRUE(Destroyed("small1"));
  EXPECT_EQ(2, relation->CompactionGeneration());
  EXPECT_EQ(0, Pending());
  EXPECT_EQ(1, Start(*runner, relation, "small2", 8 * kMiB));
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(4, relation->CompactionGeneration());
  EXPECT_EQ(0, HeldSlots());
}

TEST_F(MergeRunnerTest, ARelationPlansUpToItsRoundBesideItsRunningMerges) {
  auto runner = Make(4);
  auto relation = MakeRelation(1);
  auto other = MakeRelation(2);
  EXPECT_EQ(3, runner->MergesPerRound());
  EXPECT_EQ(3, runner->Wanted(*relation));
  Start(*runner, relation, "big1", 512 * kMiB);
  Start(*runner, relation, "small", kMiB);
  EXPECT_EQ(1, runner->Wanted(*relation));
  EXPECT_EQ(
    2, StartAll(*runner, other,
                {{.name = "a", .bytes = kMiB}, {.name = "b", .bytes = kMiB}}));
  EXPECT_EQ(0, Start(*runner, relation, "big2", 512 * kMiB));
  EXPECT_EQ(1, runner->Wanted(*relation));

  Complete("big1", MergeOutcome::Merged);
  EXPECT_EQ(2, runner->Wanted(*relation));
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(3, runner->Wanted(*relation));
}

TEST_F(MergeRunnerTest, ARelationWaitsOnceForAFreeSlot) {
  auto runner = Make(2);
  auto blocker = MakeRelation(1);
  auto waiter = MakeRelation(2);
  Start(*runner, blocker, "blocker", 512 * kMiB);
  EXPECT_EQ(0, Start(*runner, waiter, "a", 512 * kMiB));
  EXPECT_EQ(0, Start(*runner, waiter, "b", 512 * kMiB));
  EXPECT_EQ(1, Pending());

  Complete("blocker", MergeOutcome::Merged);
  EXPECT_EQ(1, waiter->CompactionGeneration());
  EXPECT_EQ(0, Pending());
}

TEST_F(MergeRunnerTest, ARoundThatStartsAllItsMergesEndsTheWait) {
  auto runner = Make(2);
  auto blocker = MakeRelation(1);
  auto relation = MakeRelation(2);
  Start(*runner, blocker, "blocker", 512 * kMiB);
  EXPECT_EQ(0, Start(*runner, relation, "big1", 512 * kMiB));
  EXPECT_EQ(1, Pending());
  EXPECT_EQ(1, Start(*runner, relation, "small", kMiB));
  EXPECT_EQ(0, Pending());
  EXPECT_EQ(0, Start(*runner, relation, "big2", 512 * kMiB));
  EXPECT_EQ(1, Pending());
  EXPECT_EQ(0, runner->Start(relation, {}));
  EXPECT_EQ(0, Pending());

  Complete("blocker", MergeOutcome::Merged);
  EXPECT_EQ(0, relation->CompactionGeneration());
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(1, relation->CompactionGeneration());
}

TEST_F(MergeRunnerTest, ForgetDropsOnlyThatRelationsTurn) {
  auto runner = Make(2);
  auto blocker = MakeRelation(1);
  auto first = MakeRelation(2);
  auto second = MakeRelation(3);
  Start(*runner, blocker, "blocker", 512 * kMiB);
  Start(*runner, first, "a", 512 * kMiB);
  Start(*runner, second, "b", 512 * kMiB);
  EXPECT_EQ(2, Pending());

  runner->Forget(*first);
  EXPECT_EQ(1, Pending());
  Complete("blocker", MergeOutcome::Merged);
  EXPECT_EQ(0, first->CompactionGeneration());
  EXPECT_EQ(1, second->CompactionGeneration());
  EXPECT_EQ(0, Pending());
}

TEST_F(MergeRunnerTest, StopRunningRaisesOnlyThatRelationsFlags) {
  auto runner = Make(4);
  auto first = MakeRelation(1);
  auto second = MakeRelation(2);
  Start(*runner, first, "a", 512 * kMiB);
  Start(*runner, second, "b", kMiB);
  runner->StopRunning(*first);
  EXPECT_TRUE(_board.stops["a"]->load());
  EXPECT_FALSE(_board.stops["b"]->load());

  Complete("a", MergeOutcome::Cancelled);
  EXPECT_EQ(0, first->CompactionGeneration());
  EXPECT_TRUE(runner->StartRound(*first).empty());
  CompleteAll(MergeOutcome::Merged);
}

TEST_F(MergeRunnerTest, FailedMergesBlockTheirInputsWithGrowingBackoff) {
  auto runner = Make(2);
  auto relation = MakeRelation(1);
  Start(*runner, relation, "a", 512 * kMiB, 2);
  Complete("a", MergeOutcome::Failed);
  EXPECT_EQ(1, relation->CompactionGeneration());
  auto blocked = runner->StartRound(*relation);
  std::ranges::sort(blocked);
  EXPECT_EQ(std::vector<std::string>({"a.0", "a.1"}), blocked);
  EXPECT_TRUE(runner->StartRound(*relation).empty());

  Start(*runner, relation, "b", 512 * kMiB, 1);
  Complete("b", MergeOutcome::Failed);
  EXPECT_EQ(1, runner->StartRound(*relation).size());
  EXPECT_EQ(1, runner->StartRound(*relation).size());
  EXPECT_TRUE(runner->StartRound(*relation).empty());

  Start(*runner, relation, "c", 512 * kMiB, 1);
  Complete("c", MergeOutcome::Merged);
  Start(*runner, relation, "d", 512 * kMiB, 1);
  Complete("d", MergeOutcome::Failed);
  EXPECT_EQ(1, runner->StartRound(*relation).size());
  EXPECT_TRUE(runner->StartRound(*relation).empty());

  Start(*runner, relation, "e", 512 * kMiB, 1);
  Complete("e", MergeOutcome::Busy);
  EXPECT_TRUE(runner->StartRound(*relation).empty());
  EXPECT_EQ(5, relation->CompactionGeneration());
}

TEST_F(MergeRunnerTest, AFailedMergeIsRetriedOnlyAfterItsBlockedRounds) {
  auto runner = Make(1);
  auto relation = MakeRelation(1);
  std::vector<uint64_t> attempts;
  for (uint64_t round = 1; round != 16; ++round) {
    if (!runner->StartRound(*relation).empty()) {
      continue;
    }
    attempts.emplace_back(round);
    Start(*runner, relation, "a", 512 * kMiB, 1);
    Complete("a", MergeOutcome::Failed);
  }
  EXPECT_EQ(std::vector<uint64_t>({1, 3, 6, 11}), attempts);
}

TEST_F(MergeRunnerTest, TheFailureBackoffStopsGrowingAtItsCap) {
  auto runner = Make(1);
  auto relation = MakeRelation(1);
  for (size_t failure = 0; failure != 10; ++failure) {
    Start(*runner, relation, "a", 512 * kMiB, 1);
    Complete("a", MergeOutcome::Failed);
  }
  size_t rounds = 0;
  while (rounds != 1000 && !runner->StartRound(*relation).empty()) {
    ++rounds;
  }
  EXPECT_EQ(300, rounds);
}

TEST_F(MergeRunnerTest, AThrowingJobFailsAndFreesItsSlot) {
  auto runner = Make(1);
  auto relation = MakeRelation(1);
  _board.throw_on_run = true;
  Start(*runner, relation, "a", 512 * kMiB, 1);
  EXPECT_TRUE(Destroyed("a"));
  EXPECT_EQ(1, runner->StartRound(*relation).size());
  EXPECT_EQ(0, HeldSlots());
  EXPECT_TRUE(runner->TryHoldBig());
  runner->ReleaseBig();
}

TEST_F(MergeRunnerTest, AnyThrownValueFailsTheMerge) {
  auto runner = Make(1);
  auto relation = MakeRelation(1);
  _board.throw_int_on_run = true;
  Start(*runner, relation, "a", 512 * kMiB, 1);
  EXPECT_TRUE(Destroyed("a"));
  EXPECT_EQ(1, runner->StartRound(*relation).size());
  EXPECT_EQ(0, HeldSlots());
}

TEST_F(MergeRunnerTest, AManualCompactionHoldsABigSlot) {
  auto runner = Make(3);
  auto relation = MakeRelation(1);
  auto other = MakeRelation(2);
  EXPECT_TRUE(runner->TryHoldBig());
  EXPECT_EQ(1, HeldSlots());
  EXPECT_EQ(1, Start(*runner, relation, "big1", 512 * kMiB));
  EXPECT_FALSE(runner->TryHoldBig());
  EXPECT_EQ(0, Start(*runner, relation, "big2", 512 * kMiB));
  EXPECT_EQ(1, Pending());
  EXPECT_EQ(1, Start(*runner, other, "small", kMiB));
  EXPECT_EQ(1, Pending());
  EXPECT_EQ(3, HeldSlots());

  runner->ReleaseBig();
  EXPECT_EQ(1, relation->CompactionGeneration());
  EXPECT_EQ(0, other->CompactionGeneration());
  EXPECT_EQ(0, Pending());
  EXPECT_EQ(2, HeldSlots());
  EXPECT_EQ(0, Start(*runner, relation, "big2", 512 * kMiB));
  EXPECT_EQ(1, Pending());

  Complete("small", MergeOutcome::Merged);
  EXPECT_EQ(2, relation->CompactionGeneration());
  EXPECT_EQ(1, Start(*runner, relation, "big2", 512 * kMiB));
  EXPECT_EQ(0, Pending());
  CompleteAll(MergeOutcome::Merged);
  EXPECT_EQ(0, HeldSlots());
}

TEST_F(MergeRunnerTest, StopWaitsForRunningMerges) {
  auto runner = Make(1);
  auto relation = MakeRelation(1);
  Start(*runner, relation, "running", 512 * kMiB);
  EXPECT_EQ(0, Start(*runner, relation, "refused", 512 * kMiB));

  std::atomic_bool stopped{false};
  std::thread stopper{[&] {
    runner->Stop();
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
  EXPECT_TRUE(Destroyed("refused"));

  EXPECT_EQ(0, runner->Wanted(*relation));
  EXPECT_FALSE(runner->TryHoldBig());
  EXPECT_EQ(0, Start(*runner, relation, "late", 512 * kMiB));
  EXPECT_TRUE(Destroyed("late"));
  EXPECT_FALSE(Started("late"));
  EXPECT_TRUE(runner->Active(1).empty());
  EXPECT_EQ(0, Pending());
  EXPECT_EQ(0, HeldSlots());
}

TEST_F(MergeRunnerTest, StopReleasesTheSlotsOfRunningMerges) {
  auto runner = Make(2);
  auto first = MakeRelation(1);
  auto second = MakeRelation(2);
  Start(*runner, first, "running", 512 * kMiB);
  EXPECT_EQ(0, Start(*runner, second, "refused", 512 * kMiB));
  EXPECT_EQ(1, HeldSlots());
  EXPECT_EQ(1, Pending());

  std::thread stopper{[&] { runner->Stop(); }};
  while (!_board.stops["running"]->load()) {
    std::this_thread::yield();
  }
  Complete("running", MergeOutcome::Cancelled);
  stopper.join();
  EXPECT_EQ(0, second->CompactionGeneration());
  EXPECT_EQ(0, Pending());
  EXPECT_EQ(0, HeldSlots());
}

TEST_F(MergeRunnerTest, ADeferredMergeResetsTheFailureBackoff) {
  auto runner = Make(2);
  auto relation = MakeRelation(1);
  Start(*runner, relation, "a", 512 * kMiB, 1);
  Complete("a", MergeOutcome::Failed);
  Start(*runner, relation, "b", 512 * kMiB, 1);
  Complete("b", MergeOutcome::Deferred);
  Start(*runner, relation, "c", 512 * kMiB, 1);
  Complete("c", MergeOutcome::Failed);
  EXPECT_EQ(2, runner->StartRound(*relation).size());
  EXPECT_TRUE(runner->StartRound(*relation).empty());
}

TEST_F(MergeRunnerTest, TakingARelationOverStopsItsMergesAndPausesIt) {
  auto runner = Make(2);
  auto first = MakeRelation(1);
  auto second = MakeRelation(2);
  Start(*runner, first, "running", 512 * kMiB);
  EXPECT_EQ(0, Start(*runner, first, "refused", 512 * kMiB));
  EXPECT_EQ(0, Start(*runner, second, "other", 512 * kMiB));
  EXPECT_EQ(2, Pending());

  std::atomic_bool taken{false};
  MergeRunner::Lease lease;
  std::thread taker{[&] {
    lease = runner->TakeOver(first);
    taken = true;
  }};
  while (!_board.stops["running"]->load()) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds{50});
  EXPECT_FALSE(taken.load());
  EXPECT_EQ(1, Pending());
  Complete("running", MergeOutcome::Cancelled);
  taker.join();
  EXPECT_TRUE(taken.load());
  EXPECT_TRUE(lease);
  EXPECT_EQ(1, second->CompactionGeneration());

  EXPECT_EQ(0, runner->Wanted(*first));
  EXPECT_EQ(0, Start(*runner, first, "late", 512 * kMiB));
  EXPECT_TRUE(Destroyed("late"));
  EXPECT_EQ(0, Pending());
  EXPECT_EQ(0, first->CompactionGeneration());
  lease = {};
  EXPECT_EQ(1, first->CompactionGeneration());
  EXPECT_EQ(2, runner->Wanted(*first));
}

TEST_F(MergeRunnerTest, ALeaseIsReleasedOnceWhereverItMoves) {
  auto runner = Make(2);
  auto relation = MakeRelation(1);
  auto lease = runner->TakeOver(relation);
  EXPECT_TRUE(lease);
  MergeRunner::Lease moved{std::move(lease)};
  EXPECT_FALSE(lease);
  MergeRunner::Lease assigned;
  assigned = std::move(moved);
  auto& self = assigned;
  assigned = std::move(self);
  EXPECT_TRUE(assigned);

  auto nested = runner->TakeOver(relation);
  assigned = {};
  EXPECT_EQ(0, runner->Wanted(*relation));
  EXPECT_EQ(1, relation->CompactionGeneration());
  nested = {};
  EXPECT_EQ(2, relation->CompactionGeneration());
  EXPECT_EQ(runner->MergesPerRound(), runner->Wanted(*relation));

  auto quiet = MakeRelation(5);
  {
    auto silent = runner->TakeOver(quiet);
    EXPECT_EQ(0, runner->Wanted(*quiet));
  }
  EXPECT_EQ(runner->MergesPerRound(), runner->Wanted(*quiet));
}

TEST_F(MergeRunnerTest, ADroppedSubmissionCountsAsCancelled) {
  auto runner = Make(1, yaclib::MakeInline(yaclib::StopTag{}));
  auto relation = MakeRelation(1);
  EXPECT_EQ(1, Start(*runner, relation, "a", 512 * kMiB));
  EXPECT_FALSE(Started("a"));
  EXPECT_TRUE(Destroyed("a"));
  EXPECT_EQ(0, relation->CompactionGeneration());
  EXPECT_EQ(0, HeldSlots());
  EXPECT_EQ(2, runner->Wanted(*relation));
  EXPECT_TRUE(runner->Active(1).empty());
}

TEST_F(MergeRunnerTest, ABigMergeThatNeverRunsReturnsItsSlot) {
  auto runner = Make(2, yaclib::MakeInline(yaclib::StopTag{}));
  auto relation = MakeRelation(1);
  Start(*runner, relation, "a", 512 * kMiB);
  EXPECT_FALSE(Started("a"));
  EXPECT_TRUE(Destroyed("a"));
  EXPECT_EQ(0, HeldSlots());
  EXPECT_TRUE(runner->TryHoldBig());
  runner->ReleaseBig();
}

TEST_F(MergeRunnerTest, AMergeKeepsOneIdFromItsStartToTheHistory) {
  auto runner = Make(1);
  auto relation = MakeRelation(1);
  Start(*runner, relation, "running", 512 * kMiB);
  EXPECT_EQ(0, Start(*runner, relation, "refused", 512 * kMiB));
  const auto active = runner->Active(1);
  ASSERT_EQ(1, active.size());
  EXPECT_FALSE(active[0].Queued());
  EXPECT_EQ(MergeOrigin::Background, active[0].origin);
  EXPECT_EQ(std::vector<std::string>(
              {"running.0", "running.1", "running.2", "running.3"}),
            active[0].stats.inputs);

  Complete("running", MergeOutcome::Merged);
  EXPECT_TRUE(runner->Active(1).empty());
  auto history = runner->History(1);
  ASSERT_EQ(1, history.size());
  EXPECT_EQ(active[0].id, history[0].id);
  EXPECT_EQ(MergeOutcome::Merged, history[0].outcome);
  EXPECT_EQ(4, history[0].stats.inputs.size());

  Start(*runner, relation, "revoked", 512 * kMiB);
  EXPECT_EQ(1, runner->Active(1).size());
  Complete("revoked", MergeOutcome::Cancelled);
  EXPECT_TRUE(runner->Active(1).empty());
  EXPECT_EQ(1, runner->History(1).size());

  _board.throw_on_run = true;
  Start(*runner, relation, "thrown", 512 * kMiB, 1);
  history = runner->History(1);
  ASSERT_EQ(2, history.size());
  EXPECT_EQ(MergeOutcome::Failed, history[1].outcome);
  EXPECT_EQ("boom", history[1].error);
}

TEST_F(MergeRunnerTest, ForegroundMergesAreRecordedBesideBackgroundOnes) {
  auto runner = Make(1);
  auto relation = MakeRelation(1);
  const SegmentStub first{"_1", kMiB, 10, 10};
  const SegmentStub second{"_2", kMiB, 30, 20};
  const irs::SubReader* const segments[] = {&first, &second};
  const auto vacuum =
    runner->StartForeground(1, MergeOrigin::Vacuum, DescribeInputs(segments));
  Start(*runner, relation, "background", 512 * kMiB);
  const auto busy = runner->StartForeground(1, MergeOrigin::Sql, {});
  const auto nothing = runner->StartForeground(1, MergeOrigin::Sql, {});
  const auto active = runner->Active(1);
  ASSERT_EQ(4, active.size());
  EXPECT_EQ(vacuum, active[0].id);
  EXPECT_EQ(MergeOrigin::Vacuum, active[0].origin);
  EXPECT_FALSE(active[0].Queued());
  EXPECT_EQ(0, active[0].queued_us);
  EXPECT_EQ(40, active[0].stats.input_docs);
  EXPECT_EQ(30, active[0].stats.input_live_docs);
  EXPECT_EQ(MergeOrigin::Background, active[1].origin);
  EXPECT_NE(0, active[1].queued_us);
  EXPECT_EQ(busy, active[2].id);
  EXPECT_EQ(nothing, active[3].id);

  runner->FinishForeground(vacuum, MergeOutcome::Merged, {}, {});
  runner->FinishForeground(busy, MergeOutcome::Busy, {}, {});
  runner->FinishForeground(nothing, MergeOutcome::Nothing, {}, {});
  Complete("background", MergeOutcome::Merged);
  EXPECT_TRUE(runner->Active(1).empty());
  const auto history = runner->History(1);
  ASSERT_EQ(3, history.size());
  EXPECT_EQ(vacuum, history[0].id);
  EXPECT_EQ(MergeOutcome::Merged, history[0].outcome);
  EXPECT_EQ(std::vector<std::string>({"_1", "_2"}), history[0].stats.inputs);
  EXPECT_EQ(busy, history[1].id);
  EXPECT_EQ(MergeOutcome::Busy, history[1].outcome);
  EXPECT_EQ(active[1].id, history[2].id);
  EXPECT_EQ(MergeOutcome::Merged, history[2].outcome);
}

TEST_F(MergeRunnerTest, ARelationSeesItsNudges) {
  auto runner = Make(2);
  const auto relation = MakeRelation(3);
  runner->TakeOver(relation);
  EXPECT_EQ(1, relation->CompactionGeneration());
  relation->NudgeCompaction();
  EXPECT_EQ(2, relation->CompactionGeneration());
  EXPECT_EQ(runner->MergesPerRound(), runner->Wanted(*relation));
}

}  // namespace
