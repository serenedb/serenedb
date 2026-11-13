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

#include <array>
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

  auto drain = std::async(
    std::launch::async, [&] { gens.Drain(*connection->context, nullptr, {}); });
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
  auto first = std::async(std::launch::async,
                          [&] { gens.Drain(first_context, nullptr, {}); });
  auto straddler = RegisterInNextGeneration(gens);
  EXPECT_NE(first.wait_for(kBlocked), std::future_status::ready);
  first_context.Interrupt();
  ASSERT_EQ(first.wait_for(kJoin), std::future_status::ready);
  EXPECT_THROW(first.get(), duckdb::InterruptException);
  before.reset();

  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *connection->context;
  context.Interrupt();
  EXPECT_THROW(gens.Drain(context, nullptr, {}), duckdb::InterruptException);

  straddler.reset();
  EXPECT_NO_THROW(gens.Drain(context, nullptr, {}));
}

TEST(WriterGenerationsTest, WritersLeavingBeforeTheFlipDoNotHoldTheDrain) {
  WriterGenerations gens;
  auto before = gens.Register();
  auto first_connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto& first_context = *first_connection->context;
  auto first = std::async(std::launch::async,
                          [&] { gens.Drain(first_context, nullptr, {}); });
  auto straddler = RegisterInNextGeneration(gens);
  first_context.Interrupt();
  ASSERT_EQ(first.wait_for(kJoin), std::future_status::ready);
  EXPECT_THROW(first.get(), duckdb::InterruptException);

  auto current = gens.Register();
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto drain = std::async(
    std::launch::async, [&] { gens.Drain(*connection->context, nullptr, {}); });
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
  EXPECT_NO_THROW(gens.Drain(*connection->context, nullptr, {}));
}

TEST(WriterGenerationsTest, DrainSkipsTheCallersOwnRegistration) {
  WriterGenerations gens;
  auto own = gens.Register();
  auto prior = gens.Register();
  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto drain = std::async(std::launch::async,
                          [&] { gens.Drain(*connection->context, &own, {}); });
  EXPECT_NE(drain.wait_for(kBlocked), std::future_status::ready);

  prior.reset();
  ASSERT_EQ(drain.wait_for(kJoin), std::future_status::ready);
  EXPECT_NO_THROW(drain.get());

  own.reset();
  const auto later = RegisterInNextGeneration(gens);
  connection->context->Interrupt();
  EXPECT_NO_THROW(gens.Drain(*connection->context, &later, {}));
}

TEST(WriterGenerationsTest, DrainFailsWhenItsWaitClosesACycle) {
  WriterGenerations first;
  WriterGenerations second;
  auto first_writer = first.Register();
  auto second_writer = second.Register();
  const std::array<const WriterGenerations*, 1> wrote_first{&first};
  const std::array<const WriterGenerations*, 1> wrote_second{&second};

  auto waiting_connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto waiting = std::async(std::launch::async, [&] {
    second.Drain(*waiting_connection->context, nullptr, wrote_first);
  });
  EXPECT_NE(waiting.wait_for(kBlocked), std::future_status::ready);

  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  EXPECT_THROW(first.Drain(*connection->context, nullptr, wrote_second),
               std::exception);

  second_writer.reset();
  ASSERT_EQ(waiting.wait_for(kJoin), std::future_status::ready);
  EXPECT_NO_THROW(waiting.get());
}

TEST(WriterGenerationsTest, DrainWaitsWhenTheOtherWaiterDoesNotWaitForIt) {
  WriterGenerations first;
  WriterGenerations second;
  WriterGenerations third;
  auto second_writer = second.Register();
  auto third_writer = third.Register();
  const std::array<const WriterGenerations*, 1> wrote_third{&third};
  const std::array<const WriterGenerations*, 1> wrote_first{&first};

  auto waiting_connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto waiting = std::async(std::launch::async, [&] {
    second.Drain(*waiting_connection->context, nullptr, wrote_first);
  });
  EXPECT_NE(waiting.wait_for(kBlocked), std::future_status::ready);

  auto connection = irs::DuckDBEngine::Instance().CreateConnection();
  auto drain = std::async(std::launch::async, [&] {
    third.Drain(*connection->context, nullptr, wrote_third);
  });
  EXPECT_NE(drain.wait_for(kBlocked), std::future_status::ready);

  third_writer.reset();
  ASSERT_EQ(drain.wait_for(kJoin), std::future_status::ready);
  EXPECT_NO_THROW(drain.get());
  second_writer.reset();
  ASSERT_EQ(waiting.wait_for(kJoin), std::future_status::ready);
  EXPECT_NO_THROW(waiting.get());
}

}  // namespace
}  // namespace sdb::search
