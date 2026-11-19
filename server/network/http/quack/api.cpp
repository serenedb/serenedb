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

#include "network/http/api.h"

#include <array>
#include <memory>

#include "network/http/common.h"
#include "network/http/quack/handlers.h"
#include "network/http/quack/service.h"

namespace sdb::network::http::quack {
namespace {

template<Endpoint E>
std::unique_ptr<HttpHandler> Handler() {
  return Make(E);
}

constexpr auto kRoutes = std::to_array<Route>({
  {HttpMethod::Post, "/quack", Handler<Endpoint::Rpc>},
  {HttpMethod::Get, "/quack", Handler<Endpoint::Banner>},
});

std::shared_ptr<void> Start(const ListenContext& context,
                            const ListenSpec& spec) {
  return Service::Open(context.cancel, context.sessions, spec.url);
}

SDB_HTTP_API kQuack{"quack", kRoutes, kQuackMaxBodyBytes, &Start};

}  // namespace
}  // namespace sdb::network::http::quack
