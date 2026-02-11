////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
#include <vpack/builder.h>

#include <yaclib/algo/wait_group.hpp>

#include "connection_test.h"

namespace fu = ::sdb::fuerte;

TEST_P(ConnectionTestF, ApiVersionSync) {
  for (size_t rep = 0; rep < repeat(); rep++) {
    auto request = fu::CreateRequest(fu::RestVerb::Get, "/_api/version");
    auto result = _connection->sendRequest(std::move(request));
    ASSERT_EQ(result->statusCode(), fu::kStatusOk);
    auto slice = result->slices().front();
    auto version = slice.get("version").copyString();
    auto server = slice.get("server").copyString();
    ASSERT_EQ(server, "serenedb");
    ASSERT_EQ(version[0], kMajorSereneVersion);
  }
}

TEST_P(ConnectionTestF, ApiVersionASync) {
  yaclib::WaitGroup<> wg{1};
  auto cb = [&](fu::Error error, std::unique_ptr<fu::Request> req,
                std::unique_ptr<fu::Response> res) {
    absl::Cleanup done = [&] { wg.Done(); };
    if (error != fu::Error::NoError) {
      ASSERT_TRUE(false) << fu::ToString(error);
    } else {
      ASSERT_EQ(res->statusCode(), fu::kStatusOk);
      auto slice = res->slices().front();
      auto version = slice.get("version").stringView();
      auto server = slice.get("server").stringView();
      ASSERT_EQ(server, "serenedb");
      ASSERT_EQ(version[0], kMajorSereneVersion);
    }
  };
  for (size_t rep = 0; rep < repeat(); rep++) {
    auto request = fu::CreateRequest(fu::RestVerb::Get, "/_api/version");

    wg.Add();
    _connection->sendRequest(std::move(request), cb);
    if (wg.Count() >= 33) {
      wg.Done();
      wg.Wait();
      wg.Reset(1);
    }
  }
  wg.Done();
  wg.Wait();
}

TEST_P(ConnectionTestF, SimpleCursorSync) {
  auto request = fu::CreateRequest(fu::RestVerb::Post, "/_api/cursor");
  vpack::Builder builder;
  builder.openObject();
  builder.add("query", "FOR x IN 1..5 RETURN x");
  builder.close();
  request->addVPack(builder.slice());
  auto response = _connection->sendRequest(std::move(request));
  ASSERT_EQ(response->statusCode(), fu::kStatusCreated);
  auto slice = response->slices().front();

  ASSERT_TRUE(slice.isObject());
  auto result = slice.get("result");
  ASSERT_TRUE(result.isArray());
  ASSERT_TRUE(result.length() == 5);
}

TEST_P(ConnectionTestF, CreateDocumentSync) {
  dropCollection("test");
  createTable("test");

  auto request = fu::CreateRequest(fu::RestVerb::Post, "/_api/document/test");
  request->addVPack(vpack::Slice::emptyObjectSlice());
  auto response = _connection->sendRequest(std::move(request));
  ASSERT_EQ(response->statusCode(), fu::kStatusAccepted);
  auto slice = response->slices().front();

  ASSERT_TRUE(slice.get("_key").isString());
  ASSERT_TRUE(slice.get("_rev").isString());

  dropCollection("test");
}

TEST_P(ConnectionTestF, ShortAndLongASync) {
  yaclib::WaitGroup<> wg{2};
  fu::RequestCallback cb = [&](fu::Error error,
                               std::unique_ptr<fu::Request> req,
                               std::unique_ptr<fu::Response> res) {
    absl::Cleanup done = [&] { wg.Done(); };
    if (error != fu::Error::NoError) {
      ASSERT_TRUE(false) << fu::ToString(error);
    } else {
      ASSERT_EQ(res->statusCode(), fu::kStatusCreated);
      auto slice = res->slices().front();
      ASSERT_TRUE(slice.isObject());
      ASSERT_TRUE(slice.get("code").isInteger());
    }
  };

  auto request_short = fu::CreateRequest(fu::RestVerb::Post, "/_api/cursor");
  {
    vpack::Builder builder;
    builder.openObject();
    builder.add("query", "RETURN SLEEP(1)");
    builder.close();
    request_short->addVPack(builder.slice());
  }

  auto request_long = fu::CreateRequest(fu::RestVerb::Post, "/_api/cursor");
  {
    vpack::Builder builder;
    builder.openObject();
    builder.add("query", "RETURN SLEEP(2)");
    builder.close();
    request_long->addVPack(builder.slice());
  }

  _connection->sendRequest(std::move(request_long), cb);
  _connection->sendRequest(std::move(request_short), cb);
  wg.Wait();
}

// threads parameter has no effect in this testsuite
static const ConnectionTestParams kConnectionTestBasicParams[] = {
  {/*._protocol = */ fu::ProtocolType::Http, /*._threads=*/1,
   /*._repeat=*/100},
  {/*._protocol = */ fu::ProtocolType::Http2, /*._threads=*/1,
   /*._repeat=*/100},
  {/*._protocol = */ fu::ProtocolType::Http, /*._threads=*/1,
   /*._repeat=*/2500},
  {/*._protocol = */ fu::ProtocolType::Http2, /*._threads=*/1,
   /*._repeat=*/2500}};

INSTANTIATE_TEST_CASE_P(BasicConnectionTests, ConnectionTestF,
                        ::testing::ValuesIn(kConnectionTestBasicParams));
