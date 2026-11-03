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

#include <chrono>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/optional_idx.hpp>
#include <duckdb/common/time_point.hpp>
#include <duckdb/main/connection.hpp>
#include <future>
#include <iresearch/utils/duckdb_engine.hpp>
#include <thread>
#include <tuple>

#include "utils/drain_gate.h"

namespace sdb::utils {
namespace {

constexpr auto kJoin = std::chrono::seconds(10);
constexpr auto kBlocked = std::chrono::milliseconds(50);

void WaitClosed(const DrainGate& gate) {
  while (!gate.IsClosed()) {
    std::this_thread::yield();
  }
}

TEST(DrainGateTest, CloseWaitsForPassesInFlight) {
  DrainGate gate;
  auto pass = gate.TryEnter();
  ASSERT_TRUE(pass);
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();

  auto close = std::async(std::launch::async,
                          [&] { return gate.Close(*connection->context); });
  WaitClosed(gate);
  EXPECT_FALSE(gate.TryEnter());
  EXPECT_NE(close.wait_for(kBlocked), std::future_status::ready);

  pass.reset();
  ASSERT_EQ(close.wait_for(kJoin), std::future_status::ready);
  auto closed = close.get();
  ASSERT_TRUE(closed);
  EXPECT_FALSE(gate.TryEnter());
  closed.reset();
  EXPECT_TRUE(gate.TryEnter());
}

TEST(DrainGateTest, EnterWaitsWhileClosed) {
  DrainGate gate;
  auto closer = irs::DuckDBEngine::Instance().CreateConnection();
  auto closed = gate.Close(*closer->context);

  auto entrant = irs::DuckDBEngine::Instance().CreateConnection();
  auto enter = std::async(std::launch::async,
                          [&] { return gate.Enter(*entrant->context); });
  EXPECT_NE(enter.wait_for(kBlocked), std::future_status::ready);

  closed.reset();
  ASSERT_EQ(enter.wait_for(kJoin), std::future_status::ready);
  EXPECT_TRUE(enter.get());
}

TEST(DrainGateTest, InterruptEndsEnter) {
  DrainGate gate;
  auto closer = irs::DuckDBEngine::Instance().CreateConnection();
  const auto closed = gate.Close(*closer->context);

  auto entrant = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *entrant->context;
  auto enter =
    std::async(std::launch::async, [&] { return gate.Enter(context); });
  EXPECT_NE(enter.wait_for(kBlocked), std::future_status::ready);

  context.Interrupt();
  ASSERT_EQ(enter.wait_for(kJoin), std::future_status::ready);
  EXPECT_THROW(std::ignore = enter.get(), duckdb::InterruptException);
  EXPECT_TRUE(gate.IsClosed());
}

TEST(DrainGateTest, InterruptEndsCloseAndReopens) {
  DrainGate gate;
  auto pass = gate.TryEnter();
  ASSERT_TRUE(pass);
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *connection->context;

  auto close =
    std::async(std::launch::async, [&] { return gate.Close(context); });
  WaitClosed(gate);
  EXPECT_NE(close.wait_for(kBlocked), std::future_status::ready);

  context.Interrupt();
  ASSERT_EQ(close.wait_for(kJoin), std::future_status::ready);
  EXPECT_THROW(std::ignore = close.get(), duckdb::InterruptException);
  EXPECT_FALSE(gate.IsClosed());
  EXPECT_TRUE(gate.TryEnter());
}

TEST(DrainGateTest, CloseEndsAtTheStatementDeadline) {
  DrainGate gate;
  const auto pass = gate.TryEnter();
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *connection->context;
  context.query_deadline = duckdb::optional_idx{static_cast<duckdb::idx_t>(
    duckdb::TimePoint::Now() + duckdb::TimePoint::FromNanos(50'000'000))};

  EXPECT_THROW(std::ignore = gate.Close(context), duckdb::InterruptException);
  EXPECT_FALSE(gate.IsClosed());
  EXPECT_FALSE(context.IsInterrupted());
}

TEST(DrainGateTest, AnInterruptBeforeAWaitEndsIt) {
  DrainGate gate;
  const auto pass = gate.TryEnter();
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *connection->context;
  context.Interrupt();

  EXPECT_THROW(std::ignore = gate.Close(context), duckdb::InterruptException);
  EXPECT_FALSE(gate.IsClosed());
}

TEST(DrainGateTest, AWaitThatNeedsNoBlockingIgnoresAnInterrupt) {
  DrainGate gate;
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *connection->context;
  context.Interrupt();

  EXPECT_TRUE(gate.Enter(context));
  EXPECT_TRUE(gate.Close(context));
}

}  // namespace
}  // namespace sdb::utils
