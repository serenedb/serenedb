////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2021 ArangoDB GmbH, Cologne, Germany
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

#include <thread>

#include "app/app_server.h"
#include "app/communication_phase.h"
#include "endpoint/endpoint.h"
#include "gtest/gtest.h"
#include "http_client/connection_cache.h"
#include "http_client/general_client_connection.h"
#include "rest_server/serened.h"

using namespace sdb;
using namespace sdb::httpclient;

TEST(ConnectionCacheTest, testEmpty) {
  SerenedServer server(nullptr, nullptr);
  server.addFeature<app::CommunicationFeaturePhase>();

  ConnectionCache cache(server.getFeature<app::CommunicationFeaturePhase>(),
                        ConnectionCache::Options{5});

  const auto& connections = cache.connections();
  EXPECT_EQ(0, connections.size());
}

TEST(ConnectionCacheTest, testAcquireInvalidEndpoint) {
  SerenedServer server(nullptr, nullptr);
  server.addFeature<app::CommunicationFeaturePhase>();

  ConnectionCache cache(server.getFeature<app::CommunicationFeaturePhase>(),
                        ConnectionCache::Options{5});

  ConnectionLease lease;
  EXPECT_EQ(nullptr, lease.connection);

  try {
    lease = cache.acquire("piff", 10.0, 30.0, 10, 0);
    ASSERT_FALSE(true);
  } catch (...) {
    // must throw
  }

  EXPECT_EQ(nullptr, lease.connection);

  const auto& connections = cache.connections();
  EXPECT_EQ(0, connections.size());
}

TEST(ConnectionCacheTest, testAcquireAndReleaseClosedConnection) {
  SerenedServer server(nullptr, nullptr);
  server.addFeature<app::CommunicationFeaturePhase>();

  ConnectionCache cache(server.getFeature<app::CommunicationFeaturePhase>(),
                        ConnectionCache::Options{5});

  std::string endpoint = Endpoint::unifiedForm("tcp://127.0.0.1:9999");

  {
    ConnectionLease lease;
    EXPECT_EQ(nullptr, lease.connection);

    lease = cache.acquire(endpoint, 10.0, 30.0, 10, 0);
    EXPECT_NE(nullptr, lease.connection);

    const auto& connections = cache.connections();
    EXPECT_EQ(0, connections.size());
  }
  // lease will automatically return connection to cache, but
  // connection is still closed, so dropped

  const auto& connections = cache.connections();
  EXPECT_EQ(0, connections.size());
}

TEST(ConnectionCacheTest, testAcquireAndReleaseClosedConnectionForce) {
  SerenedServer server(nullptr, nullptr);
  server.addFeature<app::CommunicationFeaturePhase>();

  ConnectionCache cache(server.getFeature<app::CommunicationFeaturePhase>(),
                        ConnectionCache::Options{5});

  std::string endpoint = Endpoint::unifiedForm("tcp://127.0.0.1:9999");

  {
    ConnectionLease lease;
    EXPECT_EQ(nullptr, lease.connection);

    lease = cache.acquire(endpoint, 10.0, 30.0, 10, 0);
    EXPECT_NE(nullptr, lease.connection);

    const auto& connections = cache.connections();
    EXPECT_EQ(0, connections.size());

    cache.release(std::move(lease.connection), true);
  }

  const auto& connections = cache.connections();
  EXPECT_EQ(1, connections.size());

  EXPECT_NE(connections.find(endpoint), connections.end());
  EXPECT_EQ(1, connections.find(endpoint)->second.size());
}

TEST(ConnectionCacheTest, testAcquireAndReleaseRepeat) {
  SerenedServer server(nullptr, nullptr);
  server.addFeature<app::CommunicationFeaturePhase>();

  ConnectionCache cache(server.getFeature<app::CommunicationFeaturePhase>(),
                        ConnectionCache::Options{5});

  std::string endpoint = Endpoint::unifiedForm("tcp://127.0.0.1:9999");

  httpclient::GeneralClientConnection* gc1 = nullptr;
  {
    ConnectionLease lease = cache.acquire(endpoint, 10.0, 30.0, 10, 0);
    EXPECT_NE(nullptr, lease.connection);

    {
      const auto& connections = cache.connections();
      EXPECT_EQ(0, connections.size());
    }

    gc1 = lease.connection.get();

    cache.release(std::move(lease.connection), true);

    {
      const auto& connections = cache.connections();
      EXPECT_EQ(1, connections.size());

      EXPECT_NE(connections.find(endpoint), connections.end());
      EXPECT_EQ(1, connections.find(endpoint)->second.size());
    }
  }

  httpclient::GeneralClientConnection* gc2 = nullptr;
  {
    ConnectionLease lease = cache.acquire(endpoint, 10.0, 30.0, 10, 0);
    EXPECT_NE(nullptr, lease.connection);

    {
      const auto& connections = cache.connections();
      EXPECT_EQ(1, connections.size());
    }

    gc2 = lease.connection.get();

    cache.release(std::move(lease.connection), true);

    {
      const auto& connections = cache.connections();
      EXPECT_EQ(1, connections.size());

      EXPECT_NE(connections.find(endpoint), connections.end());
      EXPECT_EQ(1, connections.find(endpoint)->second.size());
    }
  }

  EXPECT_NE(gc1, nullptr);
  EXPECT_EQ(gc1, gc2);
}

TEST(ConnectionCacheTest, testSameEndpointMultipleLeases) {
  SerenedServer server(nullptr, nullptr);
  server.addFeature<app::CommunicationFeaturePhase>();

  ConnectionCache cache(server.getFeature<app::CommunicationFeaturePhase>(),
                        ConnectionCache::Options{5});

  std::string endpoint = Endpoint::unifiedForm("tcp://127.0.0.1:9999");

  ConnectionLease lease1 = cache.acquire(endpoint, 10.0, 30.0, 10, 0);
  EXPECT_NE(nullptr, lease1.connection);
  httpclient::GeneralClientConnection* gc1 = lease1.connection.get();

  {
    const auto& connections = cache.connections();
    EXPECT_EQ(0, connections.size());
  }

  ConnectionLease lease2 = cache.acquire(endpoint, 10.0, 30.0, 10, 0);
  EXPECT_NE(nullptr, lease2.connection);
  httpclient::GeneralClientConnection* gc2 = lease2.connection.get();

  EXPECT_NE(gc1, gc2);

  cache.release(std::move(lease1.connection), true);

  {
    const auto& connections = cache.connections();
    EXPECT_EQ(1, connections.size());

    EXPECT_NE(connections.find(endpoint), connections.end());
    EXPECT_EQ(1, connections.find(endpoint)->second.size());
    EXPECT_EQ(gc1, connections.find(endpoint)->second[0].connection.get());
  }

  cache.release(std::move(lease2.connection), true);

  {
    const auto& connections = cache.connections();
    EXPECT_EQ(1, connections.size());

    EXPECT_NE(connections.find(endpoint), connections.end());
    EXPECT_EQ(2, connections.find(endpoint)->second.size());
    EXPECT_EQ(gc1, connections.find(endpoint)->second[0].connection.get());
    EXPECT_EQ(gc2, connections.find(endpoint)->second[1].connection.get());
  }
}

TEST(ConnectionCacheTest, testDifferentEndpoints) {
  SerenedServer server(nullptr, nullptr);
  server.addFeature<app::CommunicationFeaturePhase>();

  ConnectionCache cache(server.getFeature<app::CommunicationFeaturePhase>(),
                        ConnectionCache::Options{5});

  std::string endpoint1 = Endpoint::unifiedForm("tcp://127.0.0.1:9999");
  std::string endpoint2 = Endpoint::unifiedForm("tcp://127.0.0.1:12345");

  ConnectionLease lease = cache.acquire(endpoint1, 10.0, 30.0, 10, 0);
  cache.release(std::move(lease.connection), true);

  {
    const auto& connections = cache.connections();
    EXPECT_EQ(1, connections.size());

    EXPECT_NE(connections.find(endpoint1), connections.end());
    EXPECT_EQ(1, connections.find(endpoint1)->second.size());

    EXPECT_EQ(connections.find(endpoint2), connections.end());
  }

  lease = cache.acquire(endpoint2, 10.0, 30.0, 10, 0);
  cache.release(std::move(lease.connection), true);

  {
    const auto& connections = cache.connections();
    EXPECT_EQ(2, connections.size());

    EXPECT_NE(connections.find(endpoint1), connections.end());
    EXPECT_EQ(1, connections.find(endpoint1)->second.size());

    EXPECT_NE(connections.find(endpoint2), connections.end());
    EXPECT_EQ(1, connections.find(endpoint2)->second.size());
  }
}

TEST(ConnectionCacheTest, testSameEndpointDifferentProtocols) {
  SerenedServer server(nullptr, nullptr);
  server.addFeature<app::CommunicationFeaturePhase>();

  ConnectionCache cache(server.getFeature<app::CommunicationFeaturePhase>(),
                        ConnectionCache::Options{5});

  std::string endpoint1 = Endpoint::unifiedForm("tcp://127.0.0.1:9999");
  std::string endpoint2 = Endpoint::unifiedForm("ssl://127.0.0.1:9999");

  ConnectionLease lease1 = cache.acquire(endpoint1, 10.0, 30.0, 10, 0);
  cache.release(std::move(lease1.connection), true);

  {
    const auto& connections = cache.connections();
    EXPECT_EQ(1, connections.size());

    EXPECT_NE(connections.find(endpoint1), connections.end());
    EXPECT_EQ(1, connections.find(endpoint1)->second.size());

    EXPECT_EQ(connections.find(endpoint2), connections.end());
  }

  ConnectionLease lease2 = cache.acquire(endpoint2, 10.0, 30.0, 10, 0);
  cache.release(std::move(lease2.connection), true);

  {
    const auto& connections = cache.connections();
    EXPECT_EQ(2, connections.size());

    EXPECT_NE(connections.find(endpoint1), connections.end());
    EXPECT_EQ(1, connections.find(endpoint1)->second.size());

    EXPECT_NE(connections.find(endpoint2), connections.end());
    EXPECT_EQ(1, connections.find(endpoint2)->second.size());
  }
}

TEST(ConnectionCacheTest, testDropSuperfluous) {
  SerenedServer server(nullptr, nullptr);
  server.addFeature<app::CommunicationFeaturePhase>();

  ConnectionCache cache(server.getFeature<app::CommunicationFeaturePhase>(),
                        ConnectionCache::Options{3});

  std::string endpoint1 = Endpoint::unifiedForm("tcp://127.0.0.1:9999");
  std::string endpoint2 = Endpoint::unifiedForm("tcp://127.0.0.1:12345");

  ConnectionLease lease1 = cache.acquire(endpoint1, 10.0, 30.0, 10, 0);
  ConnectionLease lease2 = cache.acquire(endpoint2, 10.0, 30.0, 10, 0);
  ConnectionLease lease3 = cache.acquire(endpoint1, 10.0, 30.0, 10, 0);
  ConnectionLease lease4 = cache.acquire(endpoint2, 10.0, 30.0, 10, 0);
  ConnectionLease lease5 = cache.acquire(endpoint1, 10.0, 30.0, 10, 0);
  ConnectionLease lease6 = cache.acquire(endpoint2, 10.0, 30.0, 10, 0);
  ConnectionLease lease7 = cache.acquire(endpoint1, 10.0, 30.0, 10, 0);
  ConnectionLease lease8 = cache.acquire(endpoint2, 10.0, 30.0, 10, 0);

  cache.release(std::move(lease1.connection), true);
  cache.release(std::move(lease2.connection), true);
  cache.release(std::move(lease3.connection), true);
  cache.release(std::move(lease4.connection), true);
  cache.release(std::move(lease5.connection), true);
  cache.release(std::move(lease6.connection), true);
  cache.release(std::move(lease7.connection), true);
  cache.release(std::move(lease8.connection), true);

  {
    const auto& connections = cache.connections();
    EXPECT_EQ(2, connections.size());

    EXPECT_NE(connections.find(endpoint1), connections.end());
    EXPECT_EQ(3, connections.find(endpoint1)->second.size());

    EXPECT_NE(connections.find(endpoint2), connections.end());
    EXPECT_EQ(3, connections.find(endpoint2)->second.size());
  }
}

TEST(ConnectionCacheTest, testSameEndpointMultipleLeasesOverExpiry) {
  SerenedServer server{nullptr, nullptr};
  server.addFeature<app::CommunicationFeaturePhase>();
  ConnectionCache cache(server.getFeature<app::CommunicationFeaturePhase>(),
                        ConnectionCache::Options{5, 3});
  std::string endpoint = Endpoint::unifiedForm("tcp://127.0.0.1:9999");
  ConnectionLease lease1 = cache.acquire(endpoint, 10.0, 30.0, 10, 0);
  EXPECT_NE(nullptr, lease1.connection);
  httpclient::GeneralClientConnection* gc1 = lease1.connection.get();
  gc1->connect();  // will fail, but that's ok, this produces error details!
  {
    const auto& connections = cache.connections();
    EXPECT_EQ(0, connections.size());
  }
  cache.release(std::move(lease1.connection), true);
  // Connection now in cache, wait for expiry:
  std::this_thread::sleep_for(std::chrono::seconds(5));
  // Connection is now expired!
  {
    const auto& connections = cache.connections();
    EXPECT_EQ(1, connections.size());
    EXPECT_NE(connections.find(endpoint), connections.end());
    EXPECT_EQ(1, connections.find(endpoint)->second.size());
    EXPECT_EQ(gc1, connections.find(endpoint)->second[0].connection.get());
  }
  lease1 = cache.acquire(endpoint, 10.0, 30.0, 10, 0);
  // We expect a new connection without error details:
  EXPECT_TRUE(lease1.connection->getErrorDetails().empty());
  {
    const auto& connections = cache.connections();
    EXPECT_EQ(1, connections.size());
    EXPECT_EQ(0, connections.find(endpoint)->second.size());
  }
}
