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

#include "network/http/tier0_handlers.h"

#include <memory>
#include <utility>
#include <yaclib/async/make.hpp>

#include "network/http/handler.h"

namespace sdb::network {
namespace {

class RootHandler final : public HttpHandler {
 public:
  yaclib::Future<HttpResponse> Handle(const HttpRequest&) override {
    return yaclib::MakeFuture(HttpResponse::Json(
      200, "OK",
      R"({"name":"serenedb","cluster_name":"serenedb","version":{)"
      R"("number":"8.11.0","build_flavor":"default"},)"
      R"("tagline":"You Know, for Search"})"));
  }
};

class HealthHandler final : public HttpHandler {
 public:
  yaclib::Future<HttpResponse> Handle(const HttpRequest&) override {
    return yaclib::MakeFuture(HttpResponse::Json(
      200, "OK",
      R"({"cluster_name":"serenedb","status":"green","number_of_nodes":1})"));
  }
};

class PingHandler final : public HttpHandler {
 public:
  yaclib::Future<HttpResponse> Handle(const HttpRequest&) override {
    HttpResponse response = HttpResponse::Json(200, "OK", "");
    response.content_type = "text/plain; charset=UTF-8";
    return yaclib::MakeFuture(std::move(response));
  }
};

}  // namespace

void RegisterTier0(HttpRouter& router) {
  router.Add(HttpMethod::Get, "/", std::make_unique<RootHandler>());
  router.Add(HttpMethod::Get, "/_cluster/health",
             std::make_unique<HealthHandler>());
  router.Add(HttpMethod::Get, "/ping", std::make_unique<PingHandler>());
}

}  // namespace sdb::network
