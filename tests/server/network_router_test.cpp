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

#include <absl/algorithm/container.h>
#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <yaclib/async/future.hpp>
#include <yaclib/async/make.hpp>
#include <yaclib/coro/task.hpp>
#include <yaclib/lazy/make.hpp>

#include "network/http/router.h"
#include "network/http/routes.h"

using namespace sdb;
using namespace sdb::network;

namespace {

// Identifiable handler: these tests assert which route Match() selects and what
// params it captures, not handler execution, so Handle is an unused stub.
class NamedHandler final : public HttpHandler {
 public:
  explicit NamedHandler(std::string name) : name{std::move(name)} {}

  yaclib::Task<> Handle(RequestContext&, const HttpRequest&,
                        http::HttpResponseWriter&) override {
    return yaclib::MakeTask();
  }

  std::string name;
};

const NamedHandler* MatchRoute(HttpRouter& router, HttpMethod method,
                               std::string target, HttpRequest& request) {
  request = HttpRequest{};
  request.method = method;
  request.target = std::move(target);
  return static_cast<const NamedHandler*>(router.Match(request).handler);
}

const http::Api* Api(std::string_view name) {
  return Registry<http::Api>::Find(name);
}

std::unique_ptr<HttpHandler> MakeRegisteredHandler() {
  return std::make_unique<NamedHandler>("registered");
}

constexpr std::array kRegisteredRoutes{
  http::Route{HttpMethod::Get, "/_registered/:item", MakeRegisteredHandler},
  http::Route{HttpMethod::Post, "/_registered/:item", MakeRegisteredHandler,
              SessionScope::Request},
};

SDB_HTTP_API kRouterTestApi{"router-test", kRegisteredRoutes, uint64_t{7}};

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

TEST(NetworkRouter, LiteralRoutesWinOverParams) {
  HttpRouter router;
  router.Add(HttpMethod::Put, "/:index", std::make_unique<NamedHandler>("put"));
  router.Add(HttpMethod::Put, "/_bulk",
             std::make_unique<NamedHandler>("put bulk"));
  router.Add(HttpMethod::Post, "/_bulk",
             std::make_unique<NamedHandler>("bulk"));
  router.Add(HttpMethod::Post, "/:index/_doc",
             std::make_unique<NamedHandler>("doc"));

  HttpRequest request;
  const auto* put_bulk = MatchRoute(router, HttpMethod::Put, "/_bulk", request);
  ASSERT_NE(put_bulk, nullptr);
  EXPECT_EQ(put_bulk->name, "put bulk");

  const auto* bulk = MatchRoute(router, HttpMethod::Post, "/_bulk", request);
  ASSERT_NE(bulk, nullptr);
  EXPECT_EQ(bulk->name, "bulk");

  // No literal claims it, so a param captures it -- the handler decides
  // whether the name is legal rather than the router answering 404.
  const auto* put = MatchRoute(router, HttpMethod::Put, "/_leading", request);
  ASSERT_NE(put, nullptr);
  EXPECT_EQ(put->name, "put");
  EXPECT_EQ(request.Param("index"), "_leading");

  const auto* doc = MatchRoute(router, HttpMethod::Post, "/_odd/_doc", request);
  ASSERT_NE(doc, nullptr);
  EXPECT_EQ(doc->name, "doc");
  EXPECT_EQ(request.Param("index"), "_odd");
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

TEST(NetworkRouter, UrlPatternSyntax) {
  HttpRouter router;
  router.Add(HttpMethod::Get, "/_doc/:id(\\d+)",
             std::make_unique<NamedHandler>("numeric"));
  router.Add(HttpMethod::Get, "/:index/_stats{/:metric}?",
             std::make_unique<NamedHandler>("stats"));

  HttpRequest request;
  const auto* numeric =
    MatchRoute(router, HttpMethod::Get, "/_doc/42", request);
  ASSERT_NE(numeric, nullptr);
  EXPECT_EQ(request.Param("id"), "42");
  EXPECT_EQ(MatchRoute(router, HttpMethod::Get, "/_doc/abc", request), nullptr);

  const auto* all =
    MatchRoute(router, HttpMethod::Get, "/books/_stats", request);
  ASSERT_NE(all, nullptr);
  EXPECT_EQ(request.Param("index"), "books");
  EXPECT_EQ(request.Param("metric"), "");
  const auto* one =
    MatchRoute(router, HttpMethod::Get, "/books/_stats/docs", request);
  ASSERT_NE(one, nullptr);
  EXPECT_EQ(request.Param("metric"), "docs");
}

TEST(NetworkRouter, RouteTableIsFilteredByApi) {
  for (const auto name : Registry<http::Api>::Names()) {
    for (const auto& route : Api(name)->routes) {
      HttpRouter router;
      router.Add(route.method, route.pattern, route.make());
    }
  }

  HttpRouter otel;
  const std::array otel_only{Api("otel")};
  http::AddRoutes(otel, otel_only);
  HttpRequest request;
  request.method = HttpMethod::Post;
  request.target = "/v1/logs";
  EXPECT_NE(otel.Match(request).handler, nullptr);
  request.target = "/mcp";
  EXPECT_EQ(otel.Match(request).handler, nullptr);
  request.target = "/_bulk";
  EXPECT_EQ(otel.Match(request).handler, nullptr);

  HttpRouter both;
  const std::array otel_and_mcp{Api("otel"), Api("mcp")};
  http::AddRoutes(both, otel_and_mcp);
  request.target = "/mcp";
  EXPECT_NE(both.Match(request).handler, nullptr);
  request.target = "/v1/traces";
  EXPECT_NE(both.Match(request).handler, nullptr);
  request.method = HttpMethod::Put;
  request.target = "/my-index";
  EXPECT_EQ(both.Match(request).handler, nullptr);
}

TEST(NetworkRouter, RoutesKeepTheirSessionScope) {
  HttpRouter router;
  http::AddRoutes(router, std::array{Api("router-test")});
  HttpRequest request;
  request.method = HttpMethod::Get;
  request.target = "/_registered/apples";
  const auto get = router.Match(request);
  ASSERT_NE(get.handler, nullptr);
  EXPECT_EQ(get.session, SessionScope::Connection);
  request.method = HttpMethod::Post;
  const auto post = router.Match(request);
  ASSERT_NE(post.handler, nullptr);
  EXPECT_EQ(post.session, SessionScope::Request);
  request.method = HttpMethod::Put;
  EXPECT_EQ(router.Match(request).handler, nullptr);
}

TEST(NetworkRouter, RegisteredApiServesItsRoutes) {
  const auto* api = Api("router-test");
  ASSERT_TRUE(api);
  EXPECT_FALSE(Api("router-test-missing"));
  EXPECT_TRUE(
    absl::c_linear_search(Registry<http::Api>::Names(), "router-test"));
  EXPECT_EQ(http::MaxBodyBytes(std::array{api}), 7u);
  EXPECT_EQ(http::MaxBodyBytes(std::array{api, Api("mcp")}), uint64_t{4} << 20);

  HttpRouter router;
  http::AddRoutes(router, std::array{api});
  HttpRequest request;
  const auto* handler =
    MatchRoute(router, HttpMethod::Get, "/_registered/apples", request);
  ASSERT_NE(handler, nullptr);
  EXPECT_EQ(handler->name, "registered");
  EXPECT_EQ(request.Param("item"), "apples");
  EXPECT_EQ(MatchRoute(router, HttpMethod::Get, "/mcp", request), nullptr);
}

TEST(NetworkRouter, BodyLimitIsTheLargestOfTheApis) {
  EXPECT_EQ(http::MaxBodyBytes(std::array{Api("es")}), uint64_t{100} << 20);
  EXPECT_EQ(http::MaxBodyBytes(std::array{Api("otel")}), uint64_t{100} << 20);
  EXPECT_EQ(http::MaxBodyBytes(std::array{Api("mcp")}), uint64_t{4} << 20);
  EXPECT_EQ(http::MaxBodyBytes(std::array{Api("otel"), Api("mcp")}),
            uint64_t{100} << 20);
  EXPECT_EQ(http::MaxBodyBytes(std::array{Api("mcp"), Api("test")}),
            uint64_t{100} << 20);
}
