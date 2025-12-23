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
#include <fuerte/requests.h>

#include <yaclib/algo/wait_group.hpp>

#include "common.h"
#include "gtest/gtest.h"

namespace fu = ::sdb::fuerte;

namespace {

std::unique_ptr<fu::Request> SleepRequest(double sleep) {
  auto req = fu::CreateRequest(fu::RestVerb::Post, "/_api/cursor");
  {
    vpack::Builder builder;
    builder.openObject();
    builder.add("query", "RETURN SLEEP(@timeout)");
    builder.add("bindVars", vpack::Value(vpack::ValueType::Object));
    builder.add("timeout", sleep);
    builder.close();
    builder.close();
    req->addVPack(builder.slice());
  }
  return req;
}

void PerformRequests(fu::ProtocolType pt) {
  fu::EventLoopService loop;
  // Set connection parameters
  fu::ConnectionBuilder cbuilder;
  SetupEndpointFromEnv(cbuilder);
  SetupAuthenticationFromEnv(cbuilder);

  cbuilder.protocolType(pt);

  // make connection
  auto connection = cbuilder.connect(loop);

  // should fail after 1s
  auto req = ::SleepRequest(10.0);
  req->timeout(std::chrono::seconds(1));

  yaclib::WaitGroup<> wg{1};
  connection->sendRequest(std::move(req),
                          [&](fu::Error e, std::unique_ptr<fu::Request> req,
                              std::unique_ptr<fu::Response> res) {
                            absl::Cleanup done = [&] { wg.Done(); };
                            ASSERT_EQ(e, fu::Error::RequestTimeout);
                          });
  ASSERT_TRUE(wg.WaitFor(std::chrono::seconds(5)));
  wg.Reset(1);

  if (pt == fu::ProtocolType::Http) {
    ASSERT_EQ(connection->state(), fu::Connection::State::Closed);
    return;
  }
  // http 1.1 connection is broken after timeout, others must still work
  ASSERT_EQ(connection->state(), fu::Connection::State::Connected);

  req = fu::CreateRequest(fu::RestVerb::Post, "/_api/version");
  connection->sendRequest(
    std::move(req), [&](fu::Error e, std::unique_ptr<fu::Request> req,
                        std::unique_ptr<fu::Response> res) {
      absl::Cleanup done = [&] { wg.Done(); };
      ASSERT_TRUE(e == fu::Error::NoError) << fu::ToString(e);
      ASSERT_EQ(res->statusCode(), fu::kStatusOk);
      auto slice = res->slices().front();
      auto version = slice.get("version").copyString();
      auto server = slice.get("server").copyString();
      EXPECT_EQ(server, "serenedb");
      EXPECT_EQ(version[0], '1') << version;  // major version
    });
  wg.Wait();
  wg.Reset(1);

  for (int i = 0; i < 8; i++) {
    // should not fail
    req = ::SleepRequest(4.0);
    req->timeout(std::chrono::seconds(60));

    wg.Add();
    connection->sendRequest(std::move(req),
                            [&](fu::Error e, std::unique_ptr<fu::Request> req,
                                std::unique_ptr<fu::Response> res) {
                              absl::Cleanup done = [&] { wg.Done(); };
                              ASSERT_EQ(e, fu::Error::NoError);
                              ASSERT_TRUE(res != nullptr);
                            });

    // should fail
    req = ::SleepRequest(4.0);
    req->timeout(std::chrono::milliseconds(100));

    wg.Add();
    connection->sendRequest(std::move(req),
                            [&](fu::Error e, std::unique_ptr<fu::Request> req,
                                std::unique_ptr<fu::Response> res) {
                              absl::Cleanup done = [&] { wg.Done(); };
                              ASSERT_EQ(e, fu::Error::RequestTimeout);
                              ASSERT_EQ(res, nullptr);
                            });
  }

  wg.Done();
  ASSERT_TRUE(wg.WaitFor(std::chrono::seconds(120)));
}

}  // namespace

TEST(RequestTimeout, HTTP) { ::PerformRequests(fu::ProtocolType::Http); }

TEST(RequestTimeout, HTTP2) { ::PerformRequests(fu::ProtocolType::Http2); }
