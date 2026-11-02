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
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>
#include <yaclib/coro/task.hpp>
#include <yaclib/exe/inline.hpp>
#include <yaclib/runtime/fair_thread_pool.hpp>

#include "network/gate.h"

using namespace sdb;

namespace {

yaclib::Task<> Pass(network::Gate& gate, std::atomic<int>& passed) {
  co_await gate.Wait(yaclib::MakeInline());
  passed.fetch_add(1, std::memory_order_relaxed);
  co_return {};
}

struct Channel {
  network::Gate gate;
  std::atomic<uint64_t> produced{0};
  std::atomic<uint64_t> consumed{0};
};

yaclib::Task<> Consume(Channel& channel, yaclib::IExecutor& resume,
                       uint64_t last) {
  while (channel.consumed.load(std::memory_order_relaxed) < last) {
    const auto produced = channel.produced.load(std::memory_order_acquire);
    if (channel.consumed.load(std::memory_order_relaxed) < produced) {
      channel.consumed.store(produced, std::memory_order_release);
      continue;
    }
    co_await channel.gate.Wait(resume);
  }
  co_return {};
}

}  // namespace

TEST(NetworkGate, AKickRacingAPendingKickFreesTheWaiterSafely) {
  constexpr int kRounds = 100000;
  network::Gate gate;
  std::atomic<int> started{0};
  std::atomic<int> passed{0};
  std::thread kicker{[&] {
    for (int round = 1; round <= kRounds; ++round) {
      while (started.load(std::memory_order_acquire) < round) {
      }
      gate.Kick();
    }
  }};
  for (int round = 1; round <= kRounds; ++round) {
    gate.Kick();
    started.store(round, std::memory_order_release);
    Pass(gate, passed).Detach();
  }
  kicker.join();
  EXPECT_EQ(kRounds, passed.load(std::memory_order_relaxed));
}

TEST(NetworkGate, NoKickIsLostWhileTheWaiterResumesOnOtherThreads) {
  constexpr uint64_t kItems = 50000;
  constexpr int kChannels = 4;
  auto pool = yaclib::MakeFairThreadPool(2 * kChannels);
  std::vector<std::unique_ptr<Channel>> channels;
  std::atomic<uint64_t> stalls{0};
  std::vector<std::thread> producers;
  for (int i = 0; i < kChannels; ++i) {
    auto& channel = *channels.emplace_back(std::make_unique<Channel>());
    Consume(channel, *pool, kItems).Detach(*pool);
    producers.emplace_back([&channel, &stalls] {
      for (uint64_t item = 1; item <= kItems; ++item) {
        channel.produced.store(item, std::memory_order_release);
        channel.gate.Kick();
        auto deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (channel.consumed.load(std::memory_order_acquire) < item) {
          if (std::chrono::steady_clock::now() > deadline) {
            stalls.fetch_add(1, std::memory_order_relaxed);
            channel.gate.Kick();
            deadline =
              std::chrono::steady_clock::now() + std::chrono::seconds(10);
          }
          std::this_thread::yield();
        }
      }
    });
  }
  for (auto& producer : producers) {
    producer.join();
  }
  pool->Stop();
  pool->Wait();
  EXPECT_EQ(0u, stalls.load(std::memory_order_relaxed));
}
