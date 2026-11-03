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

#include <atomic>
#include <chrono>
#include <duckdb/common/exception.hpp>
#include <duckdb/main/connection.hpp>
#include <future>
#include <iresearch/utils/duckdb_engine.hpp>

namespace sdb {
namespace {

constexpr auto kJoin = std::chrono::seconds(10);
constexpr auto kBlocked = std::chrono::milliseconds(50);

TEST(ClientContextAwaitTest, AnInterruptEndsEveryWaitOfTheQuery) {
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *connection->context;
  const auto wait = [&] {
    absl::MutexLock lock{&duckdb::ClientContext::AwaitMutex()};
    context.Await([] { return false; });
  };
  auto first = std::async(std::launch::async, wait);
  auto second = std::async(std::launch::async, wait);
  EXPECT_NE(first.wait_for(kBlocked), std::future_status::ready);
  EXPECT_NE(second.wait_for(kBlocked), std::future_status::ready);

  context.Interrupt();
  ASSERT_EQ(first.wait_for(kJoin), std::future_status::ready);
  ASSERT_EQ(second.wait_for(kJoin), std::future_status::ready);
  EXPECT_THROW(first.get(), duckdb::InterruptException);
  EXPECT_THROW(second.get(), duckdb::InterruptException);
}

TEST(ClientContextAwaitTest, AnInterruptAfterSuppressInterruptsLeavesTheWait) {
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *connection->context;
  context.SuppressInterrupts();
  std::atomic<bool> done = false;
  auto wait = std::async(std::launch::async, [&] {
    absl::MutexLock lock{&duckdb::ClientContext::AwaitMutex()};
    context.Await([&] { return done.load(); });
  });

  context.Interrupt();
  EXPECT_NE(wait.wait_for(kBlocked), std::future_status::ready);
  done = true;
  duckdb::ClientContext::Notify();
  ASSERT_EQ(wait.wait_for(kJoin), std::future_status::ready);
  EXPECT_NO_THROW(wait.get());
}

}  // namespace
}  // namespace sdb
