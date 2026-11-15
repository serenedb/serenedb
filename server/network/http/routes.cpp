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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <array>
#include <iresearch/utils/assert.hpp>
#include <limits>
#include <type_traits>
#include <vector>

#include "network/http/common.h"
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

constexpr auto kEsRoutes = std::to_array<Route>({
  {HttpMethod::Get, "/", Make<es::Endpoint::Root>},
  {HttpMethod::Head, "/", Make<es::Endpoint::Root>},
  {HttpMethod::Get, "/_cluster/health", Make<es::Endpoint::Health>},
  {HttpMethod::Get, "/_cat/indices", Make<es::Endpoint::CatIndices>},
  {HttpMethod::Get, "/_cat/count", Make<es::Endpoint::CatCount>},
  {HttpMethod::Post, "/_bulk", Make<es::Endpoint::Bulk>},
  {HttpMethod::Put, "/_bulk", Make<es::Endpoint::Bulk>},
  {HttpMethod::Get, "/_nodes/stats", Make<es::Endpoint::NodesStats>},
  {HttpMethod::Get, "/_nodes/stats/:metric", Make<es::Endpoint::NodesStats>},
  {HttpMethod::Put, "/_cluster/settings", Make<es::Endpoint::ClusterSettings>},
  {HttpMethod::Get, "/_cluster/settings", Make<es::Endpoint::ClusterSettings>},
  {HttpMethod::Get, "/_cluster/health/:index", Make<es::Endpoint::Health>},
  {HttpMethod::Get, "/_stats", Make<es::Endpoint::IndexStats>},
  {HttpMethod::Get, "/:index/_stats", Make<es::Endpoint::IndexStats>},
  {HttpMethod::Get, "/:index/_stats/:metric", Make<es::Endpoint::IndexStats>},
  {HttpMethod::Post, "/_forcemerge", Make<es::Endpoint::ForceMerge>},
  {HttpMethod::Post, "/:index/_forcemerge", Make<es::Endpoint::ForceMerge>},
  {HttpMethod::Post, "/_refresh", Make<es::Endpoint::Refresh>},
  {HttpMethod::Get, "/_refresh", Make<es::Endpoint::Refresh>},
  {HttpMethod::Put, "/:index", Make<es::Endpoint::CreateIndex>},
  {HttpMethod::Delete, "/:index", Make<es::Endpoint::DeleteIndex>},
  {HttpMethod::Head, "/:index", Make<es::Endpoint::IndexExists>},
  {HttpMethod::Get, "/:index", Make<es::Endpoint::IndexInfo>},
  {HttpMethod::Get, "/:index/_mapping", Make<es::Endpoint::Mapping>},
  {HttpMethod::Post, "/:index/_bulk", Make<es::Endpoint::Bulk>},
  {HttpMethod::Put, "/:index/_bulk", Make<es::Endpoint::Bulk>},
  {HttpMethod::Post, "/:index/_doc", Make<es::Endpoint::Doc>},
  {HttpMethod::Post, "/:index/_doc/:id", Make<es::Endpoint::Doc>},
  {HttpMethod::Put, "/:index/_doc/:id", Make<es::Endpoint::Doc>},
  {HttpMethod::Get, "/:index/_doc/:id", Make<es::Endpoint::GetDoc>},
  {HttpMethod::Head, "/:index/_doc/:id", Make<es::Endpoint::ExistsDoc>},
  {HttpMethod::Get, "/:index/_source/:id", Make<es::Endpoint::GetSource>},
  {HttpMethod::Get, "/:index/_mget", Make<es::Endpoint::Mget>},
  {HttpMethod::Post, "/:index/_mget", Make<es::Endpoint::Mget>},
  {HttpMethod::Post, "/:index/_refresh", Make<es::Endpoint::Refresh>},
  {HttpMethod::Get, "/:index/_refresh", Make<es::Endpoint::Refresh>},
  {HttpMethod::Get, "/:index/_count", Make<es::Endpoint::Count>},
  {HttpMethod::Post, "/:index/_count", Make<es::Endpoint::Count>},
  {HttpMethod::Get, "/:index/_search", Make<es::Endpoint::Search>},
  {HttpMethod::Post, "/:index/_search", Make<es::Endpoint::Search>},
  {HttpMethod::Get, "/_search/scroll", Make<es::Endpoint::Scroll>},
  {HttpMethod::Post, "/_search/scroll", Make<es::Endpoint::Scroll>},
  {HttpMethod::Delete, "/_search/scroll", Make<es::Endpoint::ClearScroll>},
});

constexpr auto kTestRoutes = std::to_array<Route>({
  {HttpMethod::Post, "/_test/echo", Make<test::Endpoint::Echo>},
  {HttpMethod::Get, "/_test/ping", Make<test::Endpoint::Ping>},
  {HttpMethod::Get, "/_test/bytes", Make<test::Endpoint::Bytes>},
  {HttpMethod::Post, "/_test/fuzz", Make<test::Endpoint::Fuzz>},
  {HttpMethod::Get, "/_test/status", Make<test::Endpoint::Status>},
  {HttpMethod::Get, "/_test/session_user", Make<test::Endpoint::SessionUser>},
});

constexpr auto kMcpRoutes = std::to_array<Route>({
  {HttpMethod::Post, "/_mcp", Make<mcp::Endpoint::Rpc>},
  {HttpMethod::Get, "/_mcp", Make<mcp::Endpoint::MethodNotAllowed>},
  {HttpMethod::Delete, "/_mcp", Make<mcp::Endpoint::MethodNotAllowed>},
  {HttpMethod::Put, "/_mcp", Make<mcp::Endpoint::MethodNotAllowed>},
  {HttpMethod::Head, "/_mcp", Make<mcp::Endpoint::MethodNotAllowed>},
});

constexpr auto kOtelRoutes = std::to_array<Route>({
  {HttpMethod::Post, "/v1/logs", Make<otel::Endpoint::Logs>},
  {HttpMethod::Post, "/v1/traces", Make<otel::Endpoint::Traces>},
  {HttpMethod::Post, "/v1/metrics", Make<otel::Endpoint::Metrics>},
});

std::vector<Api>& Registry() {
  static std::vector<Api> apis{
    {.name = "es", .routes = kEsRoutes, .max_body_bytes = kEsMaxBodyBytes},
    {.name = "test", .routes = kTestRoutes, .max_body_bytes = kEsMaxBodyBytes},
    {.name = "mcp", .routes = kMcpRoutes, .max_body_bytes = kMcpMaxBodyBytes},
    {.name = "otel",
     .routes = kOtelRoutes,
     .max_body_bytes = kOtelMaxBodyBytes},
  };
  return apis;
}

}  // namespace

HttpApi RegisterApi(const Api& api) {
  auto& apis = Registry();
  SDB_ASSERT(!FindApi(api.name));
  SDB_ASSERT(apis.size() <
             std::numeric_limits<std::underlying_type_t<HttpApi>>::max());
  apis.push_back(api);
  return static_cast<HttpApi>(apis.size() - 1);
}

std::optional<HttpApi> FindApi(std::string_view name) {
  const auto& apis = Registry();
  const auto it =
    absl::c_find_if(apis, [&](const Api& api) { return api.name == name; });
  if (it == apis.end()) {
    return std::nullopt;
  }
  return static_cast<HttpApi>(it - apis.begin());
}

const Api& GetApi(HttpApi api) {
  const auto& apis = Registry();
  const auto index = static_cast<size_t>(api);
  SDB_ASSERT(index < apis.size());
  return apis[index];
}

std::string ApiNames() {
  return absl::StrJoin(Registry(), ", ", [](std::string* out, const Api& api) {
    absl::StrAppend(out, api.name);
  });
}

void AddRoutes(HttpRouter& router, std::span<const HttpApi> apis) {
  for (const auto api : apis) {
    for (const auto& route : GetApi(api).routes) {
      router.Add(route.method, route.pattern, route.make());
    }
  }
}

uint64_t MaxBodyBytes(std::span<const HttpApi> apis) noexcept {
  uint64_t bytes = 0;
  for (const auto api : apis) {
    bytes = std::max(bytes, GetApi(api).max_body_bytes);
  }
  return bytes;
}

}  // namespace sdb::network::http
