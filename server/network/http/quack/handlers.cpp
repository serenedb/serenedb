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

#include "network/http/quack/handlers.h"

#include <yaclib/lazy/make.hpp>

#include "network/http/quack/service.h"
#include "network/server.h"

namespace sdb::network::http::quack {
namespace {

class RpcHandler final : public HttpHandler {
 public:
  bool AuthenticatesItself() const final { return true; }

  yaclib::Task<> Handle(RequestContext& context, const HttpRequest& request,
                        HttpResponseWriter& writer) final {
    return Server::instance().Quack()->Handle(context, request, writer);
  }
};

class BannerHandler final : public HttpHandler {
 public:
  bool AuthenticatesItself() const final { return true; }

  yaclib::Task<> Handle(RequestContext&, const HttpRequest&,
                        HttpResponseWriter& writer) final {
    writer.Text(HttpStatus::Ok,
                "This is a SereneDB Quack endpoint. Use ATTACH 'quack:...' to "
                "connect here.\n");
    return yaclib::MakeTask();
  }
};

}  // namespace

std::unique_ptr<HttpHandler> Make(Endpoint endpoint) {
  switch (endpoint) {
    case Endpoint::Rpc:
      return std::make_unique<RpcHandler>();
    case Endpoint::Banner:
      return std::make_unique<BannerHandler>();
  }
  SDB_UNREACHABLE();
}

}  // namespace sdb::network::http::quack
