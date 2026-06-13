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

#include <memory>
#include <string>
#include <utility>
#include <yaclib/async/future.hpp>
#include <yaclib/async/make.hpp>

#include "network/http/router.h"

using namespace sdb;
using namespace sdb::network;

namespace {

// Identifiable handler: these tests assert which route Match() selects and what
// params it captures, not handler execution, so Handle is an unused stub.
class NamedHandler final : public HttpHandler {
 public:
  explicit NamedHandler(std::string name) : name{std::move(name)} {}

  yaclib::Future<> Handle(RequestContext&, const HttpRequest&,
                          http::HttpResponseWriter&) override {
    return yaclib::MakeFuture();
  }

  std::string name;
};

const NamedHandler* MatchRoute(HttpRouter& router, HttpMethod method,
                               std::string target, HttpRequest& request) {
  request = HttpRequest{};
  request.method = method;
  request.target = std::move(target);
  return static_cast<const NamedHandler*>(router.Match(request));
}

}  // namespace

TEST(NetworkRouter, LiteralWildcardAndGroups) {
  HttpRouter router;
  router.Add(HttpMethod::Get, "/_cluster/health",
             std::make_unique<NamedHandler>("health"));
  router.Add(HttpMethod::Get, "/:index/_search",
             std::make_unique<NamedHandler>("search"));
  router.Add(HttpMethod::Post, "/:index/_doc/:id",
             std::make_unique<NamedHandler>("doc"));

  HttpRequest request;

  const auto* health =
    MatchRoute(router, HttpMethod::Get, "/_cluster/health", request);
  ASSERT_NE(health, nullptr);
  EXPECT_EQ(health->name, "health");

  const auto* search =
    MatchRoute(router, HttpMethod::Get, "/my-index/_search?q=x", request);
  ASSERT_NE(search, nullptr);
  EXPECT_EQ(search->name, "search");
  EXPECT_EQ(request.Param("index"), "my-index");

  const auto* doc =
    MatchRoute(router, HttpMethod::Post, "/books/_doc/42", request);
  ASSERT_NE(doc, nullptr);
  EXPECT_EQ(doc->name, "doc");
  EXPECT_EQ(request.Param("index"), "books");
  EXPECT_EQ(request.Param("id"), "42");
}

TEST(NetworkRouter, MethodAndPathMismatch) {
  HttpRouter router;
  router.Add(HttpMethod::Get, "/:index/_search",
             std::make_unique<NamedHandler>("search"));

  HttpRequest request;
  EXPECT_EQ(MatchRoute(router, HttpMethod::Post, "/my-index/_search", request),
            nullptr);
  EXPECT_EQ(MatchRoute(router, HttpMethod::Get, "/my-index/_count", request),
            nullptr);
}
