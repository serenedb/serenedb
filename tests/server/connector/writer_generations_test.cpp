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
#include <duckdb/main/connection.hpp>
#include <future>
#include <iresearch/utils/duckdb_engine.hpp>
#include <thread>

#include "search/writer_generations.h"

namespace sdb::search {
namespace {

constexpr auto kJoin = std::chrono::seconds(10);
constexpr auto kBlocked = std::chrono::milliseconds(50);

WriterGenerations::Writer RegisterInNextGeneration(WriterGenerations& gens) {
  for (;;) {
    auto writer = gens.Register();
    if (writer.get_deleter().slot == 1) {
      return writer;
    }
    std::this_thread::yield();
  }
}

TEST(WriterGenerationsTest, DrainWaitsForPriorWritersOnly) {
  WriterGenerations gens;
  auto prior = gens.Register();
  ASSERT_TRUE(prior);
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();

  auto drain =
    std::async(std::launch::async, [&] { gens.Drain(*connection->context); });
  const auto later = RegisterInNextGeneration(gens);
  EXPECT_NE(drain.wait_for(kBlocked), std::future_status::ready);

  prior.reset();
  ASSERT_EQ(drain.wait_for(kJoin), std::future_status::ready);
  EXPECT_NO_THROW(drain.get());
}

TEST(WriterGenerationsTest, TheNextDrainWaitsForWritersLeftByAnInterruptedOne) {
  WriterGenerations gens;
  auto before = gens.Register();

  auto first_connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto& first_context = *first_connection->context;
  auto first =
    std::async(std::launch::async, [&] { gens.Drain(first_context); });
  auto straddler = RegisterInNextGeneration(gens);
  EXPECT_NE(first.wait_for(kBlocked), std::future_status::ready);
  first_context.Interrupt();
  ASSERT_EQ(first.wait_for(kJoin), std::future_status::ready);
  EXPECT_THROW(first.get(), duckdb::InterruptException);
  before.reset();

  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *connection->context;
  context.Interrupt();
  EXPECT_THROW(gens.Drain(context), duckdb::InterruptException);

  straddler.reset();
  EXPECT_NO_THROW(gens.Drain(context));
}

TEST(WriterGenerationsTest, WritersLeavingBeforeTheFlipDoNotHoldTheDrain) {
  WriterGenerations gens;
  auto before = gens.Register();
  auto first_connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto& first_context = *first_connection->context;
  auto first =
    std::async(std::launch::async, [&] { gens.Drain(first_context); });
  auto straddler = RegisterInNextGeneration(gens);
  first_context.Interrupt();
  ASSERT_EQ(first.wait_for(kJoin), std::future_status::ready);
  EXPECT_THROW(first.get(), duckdb::InterruptException);

  auto current = gens.Register();
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto drain =
    std::async(std::launch::async, [&] { gens.Drain(*connection->context); });
  EXPECT_NE(drain.wait_for(kBlocked), std::future_status::ready);
  current.reset();
  straddler.reset();
  EXPECT_NE(drain.wait_for(kBlocked), std::future_status::ready);

  before.reset();
  ASSERT_EQ(drain.wait_for(kJoin), std::future_status::ready);
  EXPECT_NO_THROW(drain.get());
}

TEST(WriterGenerationsTest, DrainWithNoWritersIgnoresAnInterrupt) {
  WriterGenerations gens;
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  connection->context->Interrupt();
  EXPECT_NO_THROW(gens.Drain(*connection->context));
}

}  // namespace
}  // namespace sdb::search
