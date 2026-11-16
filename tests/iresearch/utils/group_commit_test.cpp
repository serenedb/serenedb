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

#include <atomic>
#include <chrono>
#include <iresearch/utils/group_commit.hpp>
#include <span>
#include <stdexcept>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace {

struct Request {
  int runs = 0;
};

void RunOnce(std::span<Request* const> batch) {
  for (auto* request : batch) {
    ++request->runs;
  }
}

TEST(GroupCommitTest, a_lone_caller_runs_its_own_request) {
  irs::GroupCommit<Request> commit;
  Request request;
  size_t runs = 0;
  commit.Submit(request, [&](std::span<Request* const> batch) {
    ++runs;
    ASSERT_EQ(1, batch.size());
    EXPECT_EQ(&request, batch.front());
    RunOnce(batch);
  });
  EXPECT_EQ(1, runs);
  EXPECT_EQ(1, request.runs);
}

TEST(GroupCommitTest, every_request_runs_once_when_callers_race) {
  constexpr size_t kThreads = 16;
  constexpr size_t kSubmits = 200;
  irs::GroupCommit<Request> commit;
  std::atomic<size_t> handled{0};
  std::vector<std::vector<Request>> requests(kThreads,
                                             std::vector<Request>(kSubmits));
  std::vector<std::thread> threads;
  for (auto& own : requests) {
    threads.emplace_back([&] {
      for (auto& request : own) {
        commit.Submit(request, [&](std::span<Request* const> batch) {
          RunOnce(batch);
          handled.fetch_add(batch.size(), std::memory_order_relaxed);
          std::this_thread::sleep_for(std::chrono::microseconds{20});
        });
        EXPECT_EQ(1, request.runs);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(kThreads * kSubmits, handled.load());
  for (const auto& own : requests) {
    for (const auto& request : own) {
      EXPECT_EQ(1, request.runs);
    }
  }
}

TEST(GroupCommitTest, a_failed_run_throws_to_its_callers_and_frees_the_lead) {
  constexpr size_t kThreads = 8;
  irs::GroupCommit<Request> commit;
  std::atomic<size_t> failed{0};
  std::vector<std::thread> threads;
  for (size_t i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      Request request;
      try {
        commit.Submit(request, [](std::span<Request* const>) {
          std::this_thread::sleep_for(std::chrono::microseconds{200});
          throw std::runtime_error{"sync failed"};
        });
      } catch (const std::runtime_error&) {
        failed.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(kThreads, failed.load());

  Request next;
  commit.Submit(next, RunOnce);
  EXPECT_EQ(1, next.runs);
}

}  // namespace
