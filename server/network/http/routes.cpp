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

#include "network/http/routes.h"

#include <array>

#include "network/http/es/handlers.h"
#include "network/http/mcp/handlers.h"
#include "network/http/otel/handlers.h"
#include "network/http/test/handlers.h"

namespace sdb::network::http {
namespace {

template<auto E>
std::unique_ptr<HttpHandler> Make() {
  if constexpr (std::is_same_v<decltype(E), es::Endpoint>) {
    return es::Make(E);
  } else if constexpr (std::is_same_v<decltype(E), mcp::Endpoint>) {
    return mcp::Make(E);
  } else if constexpr (std::is_same_v<decltype(E), test::Endpoint>) {
    return test::Make(E);
  } else {
    return otel::Make(E);
  }
}

constexpr auto kRoutes = std::to_array<Route>({
  {HttpApi::Es, HttpMethod::Get, "/", Make<es::Endpoint::Root>},
  {HttpApi::Es, HttpMethod::Head, "/", Make<es::Endpoint::Root>},
  {HttpApi::Es, HttpMethod::Get, "/_cluster/health",
   Make<es::Endpoint::Health>},
  {HttpApi::Es, HttpMethod::Get, "/_cat/indices",
   Make<es::Endpoint::CatIndices>},
  {HttpApi::Es, HttpMethod::Get, "/_cat/count", Make<es::Endpoint::CatCount>},
  {HttpApi::Es, HttpMethod::Post, "/_bulk", Make<es::Endpoint::Bulk>},
  {HttpApi::Es, HttpMethod::Put, "/_bulk", Make<es::Endpoint::Bulk>},
  {HttpApi::Es, HttpMethod::Get, "/_nodes/stats",
   Make<es::Endpoint::NodesStats>},
  {HttpApi::Es, HttpMethod::Get, "/_nodes/stats/:metric",
   Make<es::Endpoint::NodesStats>},
  {HttpApi::Es, HttpMethod::Put, "/_cluster/settings",
   Make<es::Endpoint::ClusterSettings>},
  {HttpApi::Es, HttpMethod::Get, "/_cluster/settings",
   Make<es::Endpoint::ClusterSettings>},
  {HttpApi::Es, HttpMethod::Get, "/_cluster/health/:index",
   Make<es::Endpoint::Health>},
  {HttpApi::Es, HttpMethod::Get, "/_stats", Make<es::Endpoint::IndexStats>},
  {HttpApi::Es, HttpMethod::Get, "/:index/_stats",
   Make<es::Endpoint::IndexStats>},
  {HttpApi::Es, HttpMethod::Get, "/:index/_stats/:metric",
   Make<es::Endpoint::IndexStats>},
  {HttpApi::Es, HttpMethod::Post, "/_forcemerge",
   Make<es::Endpoint::ForceMerge>},
  {HttpApi::Es, HttpMethod::Post, "/:index/_forcemerge",
   Make<es::Endpoint::ForceMerge>},
  {HttpApi::Es, HttpMethod::Post, "/_refresh", Make<es::Endpoint::Refresh>},
  {HttpApi::Es, HttpMethod::Get, "/_refresh", Make<es::Endpoint::Refresh>},
  {HttpApi::Es, HttpMethod::Put, "/:index", Make<es::Endpoint::CreateIndex>},
  {HttpApi::Es, HttpMethod::Delete, "/:index", Make<es::Endpoint::DeleteIndex>},
  {HttpApi::Es, HttpMethod::Head, "/:index", Make<es::Endpoint::IndexExists>},
  {HttpApi::Es, HttpMethod::Get, "/:index", Make<es::Endpoint::IndexInfo>},
  {HttpApi::Es, HttpMethod::Get, "/:index/_mapping",
   Make<es::Endpoint::Mapping>},
  {HttpApi::Es, HttpMethod::Post, "/:index/_bulk", Make<es::Endpoint::Bulk>},
  {HttpApi::Es, HttpMethod::Put, "/:index/_bulk", Make<es::Endpoint::Bulk>},
  {HttpApi::Es, HttpMethod::Post, "/:index/_doc", Make<es::Endpoint::Doc>},
  {HttpApi::Es, HttpMethod::Post, "/:index/_doc/:id", Make<es::Endpoint::Doc>},
  {HttpApi::Es, HttpMethod::Put, "/:index/_doc/:id", Make<es::Endpoint::Doc>},
  {HttpApi::Es, HttpMethod::Get, "/:index/_doc/:id",
   Make<es::Endpoint::GetDoc>},
  {HttpApi::Es, HttpMethod::Head, "/:index/_doc/:id",
   Make<es::Endpoint::ExistsDoc>},
  {HttpApi::Es, HttpMethod::Get, "/:index/_source/:id",
   Make<es::Endpoint::GetSource>},
  {HttpApi::Es, HttpMethod::Get, "/:index/_mget", Make<es::Endpoint::Mget>},
  {HttpApi::Es, HttpMethod::Post, "/:index/_mget", Make<es::Endpoint::Mget>},
  {HttpApi::Es, HttpMethod::Post, "/:index/_refresh",
   Make<es::Endpoint::Refresh>},
  {HttpApi::Es, HttpMethod::Get, "/:index/_refresh",
   Make<es::Endpoint::Refresh>},
  {HttpApi::Es, HttpMethod::Get, "/:index/_count", Make<es::Endpoint::Count>},
  {HttpApi::Es, HttpMethod::Post, "/:index/_count", Make<es::Endpoint::Count>},
  {HttpApi::Es, HttpMethod::Get, "/:index/_search", Make<es::Endpoint::Search>},
  {HttpApi::Es, HttpMethod::Post, "/:index/_search",
   Make<es::Endpoint::Search>},
  {HttpApi::Es, HttpMethod::Get, "/_search/scroll", Make<es::Endpoint::Scroll>},
  {HttpApi::Es, HttpMethod::Post, "/_search/scroll",
   Make<es::Endpoint::Scroll>},
  {HttpApi::Es, HttpMethod::Delete, "/_search/scroll",
   Make<es::Endpoint::ClearScroll>},
  {HttpApi::Test, HttpMethod::Post, "/_test/echo", Make<test::Endpoint::Echo>},
  {HttpApi::Test, HttpMethod::Get, "/_test/ping", Make<test::Endpoint::Ping>},
  {HttpApi::Test, HttpMethod::Get, "/_test/bytes", Make<test::Endpoint::Bytes>},
  {HttpApi::Test, HttpMethod::Post, "/_test/fuzz", Make<test::Endpoint::Fuzz>},
  {HttpApi::Test, HttpMethod::Get, "/_test/status",
   Make<test::Endpoint::Status>},
  {HttpApi::Test, HttpMethod::Get, "/_test/session_user",
   Make<test::Endpoint::SessionUser>},
  {HttpApi::Mcp, HttpMethod::Post, "/_mcp", Make<mcp::Endpoint::Rpc>},
  {HttpApi::Mcp, HttpMethod::Get, "/_mcp",
   Make<mcp::Endpoint::MethodNotAllowed>},
  {HttpApi::Mcp, HttpMethod::Delete, "/_mcp",
   Make<mcp::Endpoint::MethodNotAllowed>},
  {HttpApi::Mcp, HttpMethod::Put, "/_mcp",
   Make<mcp::Endpoint::MethodNotAllowed>},
  {HttpApi::Mcp, HttpMethod::Head, "/_mcp",
   Make<mcp::Endpoint::MethodNotAllowed>},
  {HttpApi::Otel, HttpMethod::Post, "/v1/logs", Make<otel::Endpoint::Logs>},
  {HttpApi::Otel, HttpMethod::Post, "/v1/traces", Make<otel::Endpoint::Traces>},
  {HttpApi::Otel, HttpMethod::Post, "/v1/metrics",
   Make<otel::Endpoint::Metrics>},
});

}  // namespace

std::span<const Route> Routes() { return kRoutes; }

void AddRoutes(HttpRouter& router, std::span<const HttpApi> apis) {
  for (const auto api : apis) {
    for (const auto& route : kRoutes) {
      if (route.api == api) {
        router.Add(route.method, route.pattern, route.make());
      }
    }
  }
}

}  // namespace sdb::network::http
