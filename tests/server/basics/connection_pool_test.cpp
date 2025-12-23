////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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

#include <fuerte/connection.h>
#include <fuerte/requests.h>

#include <atomic>

#include "app/app_server.h"
#include "gtest/gtest.h"
#include "metrics/gauge.h"
#include "metrics/metrics_feature.h"
#include "network/connection_pool.h"
#include "rest_server/serened_single.h"

using namespace sdb;
using namespace sdb::network;

namespace {

void DoNothing(fuerte::Error, std::unique_ptr<fuerte::Request> req,
               std::unique_ptr<fuerte::Response> res) {
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
};
}  // namespace

namespace sdb {
namespace tests {

struct NetworkConnectionPoolTest : public ::testing::Test {
  SerenedServer server{{}, {}};
  metrics::MetricsFeature metrics{server};

 protected:
  metrics::MetricsFeature& Metrics() { return metrics; }

  uint64_t ExtractCurrentMetric() {
    auto m = Metrics().get(kCurrentConnectionsMetric);
    SDB_ASSERT(m != nullptr, "Metric not registered");
    auto gauge = dynamic_cast<metrics::Gauge<uint64_t>*>(m);
    return gauge->load();
  }

  static constexpr metrics::MetricKeyView kCurrentConnectionsMetric{
    "serenedb_connection_pool_connections_current", "pool=\"\""};
};

TEST_F(NetworkConnectionPoolTest, prune_while_in_flight) {
  ConnectionPool::Config config(Metrics());
  config.num_io_threads = 1;
  config.max_open_connections = 3;
  config.idle_connection_milli = 5;  // extra small for testing
  config.verify_hosts = false;
  config.protocol = fuerte::ProtocolType::Http;

  ConnectionPool pool(config);

  std::atomic<bool> done1 = false;
  std::atomic<bool> done2 = false;

  auto waiter = [&done1, &done2](fuerte::Error,
                                 std::unique_ptr<fuerte::Request> req,
                                 std::unique_ptr<fuerte::Response> res) {
    done2.store(true);
    done2.notify_one();

    done1.wait(false);
  };

  {
    bool is_from_pool;
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);
    conn1->sendRequest(
      fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset),
      waiter);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.com:80", is_from_pool);
    ASSERT_NE(conn1.get(), conn2.get());
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);
  }
  ASSERT_EQ(pool.numOpenConnections(), 2);
  EXPECT_EQ(ExtractCurrentMetric(), 2ull);

  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  ASSERT_EQ(pool.numOpenConnections(), 2);
  EXPECT_EQ(ExtractCurrentMetric(), 2ull);

  pool.pruneConnections();

  ASSERT_EQ(pool.numOpenConnections(), 1);
  EXPECT_EQ(ExtractCurrentMetric(), 1ull);

  // wake up blocked connection
  done1.store(true);
  done1.notify_one();

  done2.wait(false);

  // let it wake up and finish
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  pool.pruneConnections();

  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);
}

TEST_F(NetworkConnectionPoolTest, acquire_endpoint) {
  GTEST_SKIP() << "It's flaky";
  ConnectionPool::Config config(Metrics());
  config.num_io_threads = 1;
  config.max_open_connections = 3;
  config.idle_connection_milli = 10;  // extra small for testing
  config.verify_hosts = false;
  config.protocol = fuerte::ProtocolType::Http;
  ConnectionPool pool(config);

  bool is_from_pool;
  auto conn = pool.leaseConnection("tcp://example.org:80", is_from_pool);
  ASSERT_EQ(pool.numOpenConnections(), 1);
  EXPECT_EQ(ExtractCurrentMetric(), 1ull);
  auto req =
    fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset);
  auto res = conn->sendRequest(std::move(req));
  ASSERT_EQ(res->statusCode(), fuerte::kStatusOk);
  ASSERT_TRUE(res->payloadSize() > 0);
}

TEST_F(NetworkConnectionPoolTest, acquire_multiple_endpoint) {
  ConnectionPool::Config config(Metrics());
  config.num_io_threads = 1;
  config.max_open_connections = 3;
  config.idle_connection_milli = 10;  // extra small for testing
  config.verify_hosts = false;
  config.protocol = fuerte::ProtocolType::Http;

  ConnectionPool pool(config);

  bool is_from_pool;
  auto conn1 =
    pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);

  conn1->sendRequest(
    fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset),
    DoNothing);

  auto conn2 =
    pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);

  ASSERT_NE(conn1.get(), conn2.get());
  ASSERT_EQ(pool.numOpenConnections(), 2);
  EXPECT_EQ(ExtractCurrentMetric(), 2ull);

  auto conn3 =
    pool.leaseConnection("tcp://example-invalid-url.com:80", is_from_pool);
  ASSERT_NE(conn1.get(), conn3.get());

  ASSERT_EQ(pool.numOpenConnections(), 3);
  EXPECT_EQ(ExtractCurrentMetric(), 3ull);
}

TEST_F(NetworkConnectionPoolTest, release_multiple_endpoints_one) {
  ConnectionPool::Config config(Metrics());
  config.num_io_threads = 1;
  config.max_open_connections = 3;
  config.idle_connection_milli = 5;  // extra small for testing
  config.verify_hosts = false;
  config.protocol = fuerte::ProtocolType::Http;

  ConnectionPool pool(config);

  {
    bool is_from_pool;
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);
    conn1->sendRequest(
      fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset),
      DoNothing);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.com:80", is_from_pool);
    ASSERT_NE(conn1.get(), conn2.get());
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);
  }
  ASSERT_EQ(pool.numOpenConnections(), 2);
  EXPECT_EQ(ExtractCurrentMetric(), 2ull);

  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  // number of connections should go down quickly, as we are calling
  // pruneConnections() and the TTL for connections is just 5 ms
  int tries = 0;
  while (++tries < 1'000) {
    if (pool.numOpenConnections() == 0) {
      break;
    }
    pool.pruneConnections();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);
}

TEST_F(NetworkConnectionPoolTest, release_multiple_endpoints_two) {
  ConnectionPool::Config config(Metrics());
  config.num_io_threads = 1;
  config.max_open_connections = 3;
  config.idle_connection_milli = 10;  // extra small for testing
  config.verify_hosts = false;
  config.protocol = fuerte::ProtocolType::Http;

  ConnectionPool pool(config);

  bool is_from_pool;
  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);
    conn1->sendRequest(
      fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset),
      DoNothing);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.com:80", is_from_pool);
    ASSERT_NE(conn1.get(), conn2.get());
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);
  }
  ASSERT_EQ(pool.numOpenConnections(), 2);
  EXPECT_EQ(ExtractCurrentMetric(), 2ull);

  std::this_thread::sleep_for(std::chrono::milliseconds(21));
  // this will only expire conn2 (conn1 is still in use)

  // number of connections should go down quickly, as we are calling
  // pruneConnections() and the TTL for connections is just 5 ms
  int tries = 0;
  while (++tries < 1'000) {
    if (pool.numOpenConnections() == 0) {
      break;
    }
    pool.pruneConnections();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  pool.drainConnections();

  // Drain needs to erase all connections
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.com:80", is_from_pool);
    ASSERT_NE(conn1.get(), conn2.get());
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);
  }
  ASSERT_EQ(pool.numOpenConnections(), 2);
  EXPECT_EQ(ExtractCurrentMetric(), 2ull);

  std::this_thread::sleep_for(std::chrono::milliseconds(21));
  pool.pruneConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  pool.drainConnections();
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);
    conn1->sendRequest(
      fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset),
      DoNothing);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.com:80", is_from_pool);
    ASSERT_NE(conn1.get(), conn2.get());
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);

    conn2->sendRequest(
      fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset),
      DoNothing);
  }
  ASSERT_EQ(pool.numOpenConnections(), 2);
  EXPECT_EQ(ExtractCurrentMetric(), 2ull);

  std::this_thread::sleep_for(std::chrono::milliseconds(21));

  tries = 0;
  while (++tries < 10'000) {
    if (pool.numOpenConnections() == 0) {
      break;
    }
    pool.pruneConnections();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  pool.drainConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);
}

TEST_F(NetworkConnectionPoolTest, force_drain) {
  ConnectionPool::Config config(Metrics());
  config.num_io_threads = 1;
  config.max_open_connections = 3;
  config.idle_connection_milli = 10;  // extra small for testing
  config.verify_hosts = false;
  config.protocol = fuerte::ProtocolType::Http;

  ConnectionPool pool(config);

  bool is_from_pool;
  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);
    conn1->sendRequest(
      fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset),
      DoNothing);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.com:80", is_from_pool);
    conn2->sendRequest(
      fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset),
      DoNothing);
    ASSERT_NE(conn1.get(), conn2.get());
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);
  }
  ASSERT_EQ(pool.numOpenConnections(), 2);
  EXPECT_EQ(ExtractCurrentMetric(), 2ull);

  pool.drainConnections();
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);
}

TEST_F(NetworkConnectionPoolTest, checking_min_and_max_connections) {
  ConnectionPool::Config config(Metrics());
  config.num_io_threads = 1;
  config.max_open_connections = 2;
  config.idle_connection_milli = 10;  // extra small for testing
  config.verify_hosts = false;
  config.protocol = fuerte::ProtocolType::Http;

  ConnectionPool pool(config);

  bool is_from_pool;
  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);

    conn1->sendRequest(
      fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset),
      DoNothing);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_NE(conn1.get(), conn2.get());
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);

    conn2->sendRequest(
      fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset),
      DoNothing);

    auto conn3 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_NE(conn1.get(), conn3.get());
    ASSERT_NE(conn1.get(), conn2.get());
    ASSERT_EQ(pool.numOpenConnections(), 3);
    EXPECT_EQ(ExtractCurrentMetric(), 3ull);
  }
  ASSERT_EQ(pool.numOpenConnections(), 3);
  EXPECT_EQ(ExtractCurrentMetric(), 3ull);

  // 21ms > 2 * 10ms
  std::this_thread::sleep_for(std::chrono::milliseconds(21));

  int tries = 0;
  while (++tries < 1'000) {
    if (pool.numOpenConnections() == 0) {
      break;
    }
    pool.pruneConnections();

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  pool.drainConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);

    conn1->sendRequest(
      fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset),
      DoNothing);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_NE(conn1.get(), conn2.get());
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);

    conn2->sendRequest(
      fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset),
      DoNothing);

    auto conn3 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_NE(conn1.get(), conn3.get());
    ASSERT_NE(conn1.get(), conn2.get());
    ASSERT_EQ(pool.numOpenConnections(), 3);
    EXPECT_EQ(ExtractCurrentMetric(), 3ull);

    conn3->sendRequest(
      fuerte::CreateRequest(fuerte::RestVerb::Get, fuerte::ContentType::Unset),
      DoNothing);
  }
  ASSERT_EQ(pool.numOpenConnections(), 3);
  EXPECT_EQ(ExtractCurrentMetric(), 3ull);

  // 21ms > 2 * 10ms
  std::this_thread::sleep_for(std::chrono::milliseconds(21));

  tries = 0;
  while (++tries < 1'000) {
    if (pool.numOpenConnections() == 0) {
      break;
    }
    pool.pruneConnections();

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  pool.drainConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);
}

TEST_F(NetworkConnectionPoolTest, checking_expiration) {
  ConnectionPool::Config config(Metrics());
  config.num_io_threads = 1;
  config.max_open_connections = 2;
  config.idle_connection_milli = 10;  // extra small for testing
  config.verify_hosts = false;
  config.protocol = fuerte::ProtocolType::Http;

  ConnectionPool pool(config);

  bool is_from_pool;
  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);
  }
  ASSERT_EQ(pool.numOpenConnections(), 1);
  EXPECT_EQ(ExtractCurrentMetric(), 1ull);

  // 21ms > 2 * 10ms
  std::this_thread::sleep_for(std::chrono::milliseconds(21));

  // expires the connection
  pool.pruneConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  pool.drainConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);
  }
  ASSERT_EQ(pool.numOpenConnections(), 2);
  EXPECT_EQ(ExtractCurrentMetric(), 2ull);

  // 21ms > 2 * 10ms
  std::this_thread::sleep_for(std::chrono::milliseconds(21));

  // expires the connections
  pool.pruneConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  pool.drainConnections();
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);

    auto conn3 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 3);
    EXPECT_EQ(ExtractCurrentMetric(), 3ull);
  }
  ASSERT_EQ(pool.numOpenConnections(), 3);
  EXPECT_EQ(ExtractCurrentMetric(), 3ull);

  // 21ms > 2 * 10ms
  std::this_thread::sleep_for(std::chrono::milliseconds(21));

  // expires the connections
  pool.pruneConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  pool.drainConnections();
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);
}

TEST_F(NetworkConnectionPoolTest, checking_expiration_multiple_endpints) {
  ConnectionPool::Config config(Metrics());
  config.num_io_threads = 1;
  config.max_open_connections = 2;
  config.idle_connection_milli = 10;  // extra small for testing
  config.verify_hosts = false;
  config.protocol = fuerte::ProtocolType::Http;

  ConnectionPool pool(config);

  bool is_from_pool;
  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);
  }
  ASSERT_EQ(pool.numOpenConnections(), 2);
  EXPECT_EQ(ExtractCurrentMetric(), 2ull);

  // 21ms > 2 * 10ms
  std::this_thread::sleep_for(std::chrono::milliseconds(21));

  // expires the connection(s)
  pool.pruneConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  pool.drainConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.com:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);
  }
  ASSERT_EQ(pool.numOpenConnections(), 2);
  EXPECT_EQ(ExtractCurrentMetric(), 2ull);

  // 21ms > 2 * 10ms
  std::this_thread::sleep_for(std::chrono::milliseconds(21));

  // expires the connection
  pool.pruneConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  pool.drainConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);

    auto conn3 =
      pool.leaseConnection("tcp://example-invalid-url.com:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 3);
    EXPECT_EQ(ExtractCurrentMetric(), 3ull);
  }
  ASSERT_EQ(pool.numOpenConnections(), 3);
  EXPECT_EQ(ExtractCurrentMetric(), 3ull);

  // 21ms > 2 * 10ms
  std::this_thread::sleep_for(std::chrono::milliseconds(21));

  // expires the connections
  pool.pruneConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  pool.drainConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);

    auto conn3 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 3);
    EXPECT_EQ(ExtractCurrentMetric(), 3ull);
  }
  {
    auto conn4 =
      pool.leaseConnection("tcp://example-invalid-url.com:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 4);
    EXPECT_EQ(ExtractCurrentMetric(), 4ull);

    auto conn5 =
      pool.leaseConnection("tcp://example-invalid-url.com:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 5);
    EXPECT_EQ(ExtractCurrentMetric(), 5ull);

    // 21ms > 2 * 10ms
    std::this_thread::sleep_for(std::chrono::milliseconds(21));

    // expires the connections
    pool.pruneConnections();
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);
  }

  pool.drainConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.com:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);

    auto conn3 =
      pool.leaseConnection("tcp://example-invalid-url.net:80", is_from_pool);
    ASSERT_EQ(pool.numOpenConnections(), 3);
    EXPECT_EQ(ExtractCurrentMetric(), 3ull);
  }
  ASSERT_EQ(pool.numOpenConnections(), 3);
  EXPECT_EQ(ExtractCurrentMetric(), 3ull);

  // 21ms > 2 * 10ms
  std::this_thread::sleep_for(std::chrono::milliseconds(21));

  // expires the connections
  pool.pruneConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);

  pool.drainConnections();
  ASSERT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);
}

TEST_F(NetworkConnectionPoolTest, test_cancel_endpoint_all) {
  ConnectionPool::Config config(Metrics());
  config.num_io_threads = 1;
  config.max_open_connections = 2;
  config.idle_connection_milli = 10;  // extra small for testing
  config.verify_hosts = false;
  config.protocol = fuerte::ProtocolType::Http;

  ConnectionPool pool(config);

  bool is_from_pool;
  {
    auto conn1 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    EXPECT_FALSE(is_from_pool);
    EXPECT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);

    auto conn2 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    EXPECT_FALSE(is_from_pool);
    EXPECT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);

    auto conn3 =
      pool.leaseConnection("tcp://example-invalid-url.org:80", is_from_pool);
    EXPECT_FALSE(is_from_pool);
    EXPECT_EQ(pool.numOpenConnections(), 3);
    EXPECT_EQ(ExtractCurrentMetric(), 3ull);
  }
  EXPECT_EQ(pool.numOpenConnections(), 3);
  EXPECT_EQ(ExtractCurrentMetric(), 3ull);

  // cancel all connections
  pool.cancelConnections("tcp://example-invalid-url.org:80");
  EXPECT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);
}

TEST_F(NetworkConnectionPoolTest, test_cancel_endpoint_some) {
  std::string endpoint_a = "tcp://example-invalid-url.org:80";
  std::string endpoint_b = "tcp://example-invalid-url.org:800";
  ConnectionPool::Config config(Metrics());
  config.num_io_threads = 1;
  config.max_open_connections = 2;
  config.idle_connection_milli = 10;  // extra small for testing
  config.verify_hosts = false;
  config.protocol = fuerte::ProtocolType::Http;

  ConnectionPool pool(config);

  bool is_from_pool;
  {
    auto conn1 = pool.leaseConnection(endpoint_a, is_from_pool);
    EXPECT_FALSE(is_from_pool);
    EXPECT_EQ(pool.numOpenConnections(), 1);
    EXPECT_EQ(ExtractCurrentMetric(), 1ull);

    auto conn2 = pool.leaseConnection(endpoint_b, is_from_pool);
    EXPECT_FALSE(is_from_pool);
    EXPECT_EQ(pool.numOpenConnections(), 2);
    EXPECT_EQ(ExtractCurrentMetric(), 2ull);

    auto conn3 = pool.leaseConnection(endpoint_a, is_from_pool);
    EXPECT_FALSE(is_from_pool);
    EXPECT_EQ(pool.numOpenConnections(), 3);
    EXPECT_EQ(ExtractCurrentMetric(), 3ull);

    auto conn4 = pool.leaseConnection(endpoint_b, is_from_pool);
    EXPECT_FALSE(is_from_pool);
    EXPECT_EQ(pool.numOpenConnections(), 4);
    EXPECT_EQ(ExtractCurrentMetric(), 4ull);

    auto conn5 = pool.leaseConnection(endpoint_a, is_from_pool);
    EXPECT_FALSE(is_from_pool);
    EXPECT_EQ(pool.numOpenConnections(), 5);
    EXPECT_EQ(ExtractCurrentMetric(), 5ull);
  }
  // 3 from A, 2 from B
  EXPECT_EQ(pool.numOpenConnections(), 5);
  EXPECT_EQ(ExtractCurrentMetric(), 5ull);

  // cancel all connections from endpoint_a
  pool.cancelConnections(endpoint_a);
  // The connections to endpoint_b stay intact
  EXPECT_EQ(pool.numOpenConnections(), 2);
  EXPECT_EQ(ExtractCurrentMetric(), 2ull);

  // cancel all connections from endpoint_b
  pool.cancelConnections(endpoint_b);
  // No connections left
  EXPECT_EQ(pool.numOpenConnections(), 0);
  EXPECT_EQ(ExtractCurrentMetric(), 0ull);
}

}  // namespace tests
}  // namespace sdb
