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
#include <yaclib/async/make.hpp>

#include "network/http/router.h"

using namespace sdb;
using namespace sdb::network;

namespace {

class RecordingHandler final : public HttpHandler {
 public:
  RecordingHandler(std::string name, std::vector<std::string>& log)
    : _name{std::move(name)}, _log{log} {}

  yaclib::Future<HttpResponse> Handle(const HttpRequest& request) override {
    _log.push_back(_name);
    std::string body = _name;
    for (const auto& [key, value] : request.params) {
      body += ";" + key + "=" + value;
    }
    return yaclib::MakeFuture(HttpResponse::Json(200, "OK", std::move(body)));
  }

 private:
  std::string _name;
  std::vector<std::string>& _log;
};

HttpResponse RunRoute(HttpRouter& router, HttpMethod method,
                      std::string target) {
  HttpRequest request;
  request.method = method;
  request.target = std::move(target);
  return std::move(router.Route(request)).Get().Ok();
}

}  // namespace

TEST(NetworkRouter, LiteralWildcardAndGroups) {
  std::vector<std::string> log;
  HttpRouter router;
  router.Add(HttpMethod::Get, "/_cluster/health",
             std::make_unique<RecordingHandler>("health", log));
  router.Add(HttpMethod::Get, "/:index/_search",
             std::make_unique<RecordingHandler>("search", log));
  router.Add(HttpMethod::Post, "/:index/_doc/:id",
             std::make_unique<RecordingHandler>("doc", log));

  const HttpResponse health =
    RunRoute(router, HttpMethod::Get, "/_cluster/health");
  EXPECT_EQ(health.status, 200);
  EXPECT_EQ(health.body, "health");

  const HttpResponse search =
    RunRoute(router, HttpMethod::Get, "/my-index/_search?q=x");
  EXPECT_EQ(search.status, 200);
  EXPECT_EQ(search.body, "search;index=my-index");

  const HttpResponse doc = RunRoute(router, HttpMethod::Post, "/books/_doc/42");
  EXPECT_EQ(doc.status, 200);
  EXPECT_NE(doc.body.find("index=books"), std::string::npos);
  EXPECT_NE(doc.body.find("id=42"), std::string::npos);
}

TEST(NetworkRouter, MethodAndPathMismatch) {
  std::vector<std::string> log;
  HttpRouter router;
  router.Add(HttpMethod::Get, "/:index/_search",
             std::make_unique<RecordingHandler>("search", log));

  const HttpResponse wrong_method =
    RunRoute(router, HttpMethod::Post, "/my-index/_search");
  EXPECT_EQ(wrong_method.status, 404);

  const HttpResponse wrong_path =
    RunRoute(router, HttpMethod::Get, "/my-index/_count");
  EXPECT_EQ(wrong_path.status, 404);

  EXPECT_TRUE(log.empty());
}
