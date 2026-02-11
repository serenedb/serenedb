////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021-2021 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include <basics/unshackled_mutex.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

/**
 * The constructor of WorkerThread starts a thread, which immediately
 * starts waiting on a condition variable. The execute() method takes a
 * callback, which is passed to the waiting thread and is then executed by it,
 * while execute() returns.
 */
struct WorkerThread : std::enable_shared_from_this<WorkerThread> {
  WorkerThread() = default;

  void Run() {
    // This can't be done in the constructor (shared_from_this)
    _thread = std::thread([self = this->shared_from_this()] {
      auto guard = std::unique_lock(self->_mutex);

      auto wait = [&] {
        self->_cv.wait(
          guard, [&] { return self->_callback != nullptr || self->_stopped; });
      };

      for (wait(); !self->_stopped; wait()) {
        self->_callback.operator()();
        self->_callback = {};
      }
    });
  }

  void Execute(std::function<void()> callback) {
    ASSERT_TRUE(_thread.joinable());
    ASSERT_TRUE(callback);
    {
      auto guard = std::unique_lock(_mutex);
      ASSERT_TRUE(!_stopped);
      ASSERT_FALSE(_callback);
      std::swap(_callback, callback);
    }

    _cv.notify_one();
  }

  void Stop() {
    {
      auto guard = std::unique_lock(_mutex);
      _stopped = true;
    }

    _cv.notify_one();
  }

 private:
  std::thread _thread;
  std::mutex _mutex;
  std::condition_variable _cv;
  std::function<void()> _callback;
  bool _stopped = false;
};

auto operator<<(WorkerThread& worker_thread, std::function<void()> callback) {
  return worker_thread.Execute(std::move(callback));
}

// Used as memoizable thread indexes
enum Thread {
  kAlpha = 0,
  kBeta = 1,
  kGamma = 2,
  kDelta = 3,
  kEpsilon = 4,
  kZeta = 5,
  kEta = 6,
  kTheta = 7,
  kIota = 8,
  kKappa = 9,
  kLambda = 10,
  kMu = 11,
  kNu = 12,
  kXi = 13,
  kOmikron = 14,
  kPi = 15,
  kRho = 16,
  kSigma = 17,
  kTau = 18,
  kUpsilon = 19,
  kPhi = 20,
  kChi = 21,
  kPsi = 22,
  kOmega = 23,
};

// Note that this test will probably succeed even for std::mutex or others,
// unless you run it with TSan.
TEST(UnshackledMutexTest, interleavedThreadsTest) {
  using namespace std::chrono_literals;

  constexpr auto kCountUpTo = [](Thread thread) -> size_t {
    return std::to_underlying(thread) + 1;
  };

  constexpr auto kNumThreads = kCountUpTo(Thread::kEpsilon);
  static_assert(kNumThreads == 5);

  auto threads = std::array<std::shared_ptr<WorkerThread>, kNumThreads>();
  std::generate(threads.begin(), threads.end(),
                [] { return std::make_shared<WorkerThread>(); });
  for (auto& it : threads) {
    it->Run();
  }

  auto wait_until_at_most = [](auto predicate, auto sleep_time, auto timeout) {
    for (auto start = std::chrono::steady_clock::now(); !predicate();
         std::this_thread::sleep_for(sleep_time)) {
      ASSERT_LT(std::chrono::steady_clock::now(), start + timeout);
    }
  };

  auto testee = ::sdb::basics::UnshackledMutex();

  // run a code block in the named thread
#define RUN(thr) *threads[thr] << [&]

  constexpr auto kNumCheckpoints = kNumThreads;
  auto checkpoint_reached = std::array<std::atomic<bool>, kNumCheckpoints>();
  std::fill(checkpoint_reached.begin(), checkpoint_reached.end(), false);

  // ALPHA takes the lock at first
  RUN(kAlpha) {
    testee.lock();
    checkpoint_reached[kAlpha] = true;
  };

  wait_until_at_most([&]() -> bool { return checkpoint_reached[kAlpha]; }, 1us,
                     1s);

  // BETA has to wait for the lock as ALPHA still holds it
  RUN(kBeta) {
    testee.lock();
    checkpoint_reached[kBeta] = true;
  };

  std::this_thread::sleep_for(1ms);
  ASSERT_FALSE(checkpoint_reached[kBeta]);

  // ALPHA releases the lock it took, allowing BETA to take it and continue
  RUN(kAlpha) { testee.unlock(); };

  // BETA should now finish its last callback
  wait_until_at_most([&]() -> bool { return checkpoint_reached[kBeta]; }, 1us,
                     1s);

  // BETA holds the lock now
  ASSERT_FALSE(testee.try_lock());

  // GAMMA has to wait for the lock as BETA still holds it
  RUN(kGamma) {
    testee.lock();
    checkpoint_reached[kGamma] = true;
  };

  std::this_thread::sleep_for(1ms);
  ASSERT_FALSE(checkpoint_reached[kGamma]);

  // DELTA now unlocks the lock that BETA is holding
  // That this is allowed sets UnshackledMutex apart from other mutexes.
  RUN(kDelta) {
    testee.unlock();
    checkpoint_reached[kDelta] = true;
  };

  wait_until_at_most([&]() -> bool { return checkpoint_reached[kDelta]; }, 1us,
                     1s);

  // As DELTA has unlocked the mutex, GAMMA is now able to obtain the lock
  wait_until_at_most([&]() -> bool { return checkpoint_reached[kGamma]; }, 1us,
                     1s);

  // GAMMA holds the lock now
  ASSERT_FALSE(testee.try_lock());

  // EPSILON now unlocks the lock that GAMMA is holding
  RUN(kEpsilon) {
    testee.unlock();
    checkpoint_reached[kEpsilon] = true;
  };

  wait_until_at_most([&]() -> bool { return checkpoint_reached[kEpsilon]; },
                     1us, 1s);

  ASSERT_TRUE(testee.try_lock());
  testee.unlock();

#undef RUN
}
