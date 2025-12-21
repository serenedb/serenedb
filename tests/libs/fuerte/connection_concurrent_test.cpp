////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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

#include <absl/cleanup/cleanup.h>
#include <fuerte/fuerte.h>
#include <fuerte/helper.h>
#include <fuerte/loop.h>

#include <thread>
#include <yaclib/algo/wait_group.hpp>

#include "basics/thread_guard.h"
#include "connection_test.h"
#include "gtest/gtest.h"

using namespace sdb;

// Tesuite checks the thread-safety properties of the connection
// implementations. Try to send requests on the same connection object
// concurrently

// need a new class to use test fixture
class ConcurrentConnectionF : public ConnectionTestF {
  virtual void SetUp() override {
    ConnectionTestF::SetUp();
    dropCollection("concurrent");  // ignore response
    ASSERT_EQ(createTable("concurrent"), fu::kStatusOk);
  }

  virtual void TearDown() override {
    // drop the collection
    ASSERT_EQ(dropCollection("concurrent"), fu::kStatusOk);
    ConnectionTestF::TearDown();
  }
};

TEST_P(ConcurrentConnectionF, ApiVersionParallel) {
  yaclib::WaitGroup<> wg;
  std::atomic<size_t> counter(0);
  auto cb = [&](fu::Error error, std::unique_ptr<fu::Request> req,
                std::unique_ptr<fu::Response> res) {
    absl::Cleanup done = [&] { wg.Done(); };
    if (error != fu::Error::NoError) {
      ASSERT_TRUE(false) << fu::ToString(error);
    } else {
      ASSERT_EQ(res->statusCode(), fu::kStatusOk);
      auto slice = res->slices().front();
      auto version = slice.get("version").copyString();
      auto server = slice.get("server").copyString();
      ASSERT_EQ(server, "serenedb");
      ASSERT_EQ(version[0], kMajorSereneVersion);
      counter.fetch_add(1, std::memory_order_relaxed);
    }
  };

  std::vector<std::shared_ptr<fu::Connection>> connections;
  connections.push_back(_connection);
  for (size_t i = 1; i < threads(); i++) {
    connections.push_back(createConnection());
  }

  absl::Cleanup guard = [&]() noexcept {
    for (auto& c : connections) {
      c.reset();
    }
  };

  auto joins = ThreadGuard(threads());

  wg.Add(threads() * repeat());
  for (size_t t = 0; t < threads(); t++) {
    joins.emplace([&] {
      for (size_t i = 0; i < repeat(); i++) {
        auto request = fu::CreateRequest(fu::RestVerb::Get, "/_api/version");
        auto& conn = *connections[i % connections.size()];
        while (conn.requestsLeft() >= 24) {
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        conn.sendRequest(std::move(request), cb);
      }
    });
  }

  // wait for all threads to return
  ASSERT_TRUE(wg.WaitFor(std::chrono::seconds(300)));

  // wait for all threads to end
  joins.joinAll();

  ASSERT_EQ(repeat() * threads(), counter);
}

TEST_P(ConcurrentConnectionF, CreateDocumentsParallel) {
  yaclib::WaitGroup<> wg;
  std::atomic<size_t> counter(0);
  auto cb = [&](fu::Error error, std::unique_ptr<fu::Request> req,
                std::unique_ptr<fu::Response> res) {
    absl::Cleanup done = [&] { wg.Done(); };
    counter.fetch_add(1, std::memory_order_relaxed);
    if (error != fu::Error::NoError) {
      ASSERT_TRUE(false) << fu::ToString(error);
    } else {
      ASSERT_EQ(res->statusCode(), fu::kStatusAccepted);
      auto slice = res->slices().front();
      ASSERT_TRUE(slice.get("_key").isString());
      ASSERT_TRUE(slice.get("_rev").isString());
    }
  };

  vpack::Builder builder;
  builder.openObject();
  builder.add("hello", "world");
  builder.close();

  std::vector<std::shared_ptr<fu::Connection>> connections;
  connections.push_back(_connection);
  for (size_t i = 1; i < threads(); i++) {
    connections.push_back(createConnection());
  }

  absl::Cleanup guard = [&]() noexcept {
    for (auto& c : connections) {
      c.reset();
    }
  };

  auto joins = ThreadGuard(threads());

  wg.Add(threads() * repeat());
  for (size_t t = 0; t < threads(); t++) {
    joins.emplace([=, this] {
      for (size_t i = 0; i < repeat(); i++) {
        auto request =
          fu::CreateRequest(fu::RestVerb::Post, "/_api/document/concurrent");
        request->addVPack(builder.slice());
        auto& conn = *connections[i % connections.size()];
        while (conn.requestsLeft() >= 24) {
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        conn.sendRequest(std::move(request), cb);
      }
    });
  }
  ASSERT_TRUE(
    wg.WaitFor(std::chrono::seconds(600)));  // wait for all threads to return
  // This test is suspected to timeout. I enabled request logging to figure out
  // if the requests reach the server or if fuerte deadlocks internally.

  // wait for all threads to end
  joins.joinAll();

  ASSERT_EQ(repeat() * threads(), counter);
}

static const ConnectionTestParams kParams[] = {
  {/*._protocol=*/fu::ProtocolType::Http, /* ._threads=*/2, /*._repeat=*/500},
  {/*._protocol=*/fu::ProtocolType::Http2, /* ._threads=*/2,
   /*._repeat=*/500},
  {/*._protocol=*/fu::ProtocolType::Http, /* ._threads=*/4,
   /*._repeat=*/5000},
  {/*._protocol=*/fu::ProtocolType::Http2, /* ._threads=*/4,
   /*._repeat=*/5000},
};

INSTANTIATE_TEST_CASE_P(ConcurrentRequestsTests, ConcurrentConnectionF,
                        ::testing::ValuesIn(kParams));
