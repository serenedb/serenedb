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
#include "network/http/es/handlers.h"

namespace sdb::network::http::es {
namespace {

template<Endpoint E>
std::unique_ptr<HttpHandler> Handler() {
  return Make(E);
}

constexpr auto kRoutes = std::to_array<Route>({
  {HttpMethod::Get, "/", Handler<Endpoint::Root>},
  {HttpMethod::Head, "/", Handler<Endpoint::Root>},
  {HttpMethod::Get, "/_cluster/health", Handler<Endpoint::Health>},
  {HttpMethod::Get, "/_cat/indices", Handler<Endpoint::CatIndices>},
  {HttpMethod::Get, "/_cat/count", Handler<Endpoint::CatCount>},
  {HttpMethod::Post, "/_bulk", Handler<Endpoint::Bulk>},
  {HttpMethod::Put, "/_bulk", Handler<Endpoint::Bulk>},
  {HttpMethod::Get, "/_nodes/stats", Handler<Endpoint::NodesStats>},
  {HttpMethod::Get, "/_nodes/stats/:metric", Handler<Endpoint::NodesStats>},
  {HttpMethod::Put, "/_cluster/settings", Handler<Endpoint::ClusterSettings>},
  {HttpMethod::Get, "/_cluster/settings", Handler<Endpoint::ClusterSettings>},
  {HttpMethod::Get, "/_cluster/health/:index", Handler<Endpoint::Health>},
  {HttpMethod::Get, "/_stats", Handler<Endpoint::IndexStats>},
  {HttpMethod::Get, "/:index/_stats", Handler<Endpoint::IndexStats>},
  {HttpMethod::Get, "/:index/_stats/:metric", Handler<Endpoint::IndexStats>},
  {HttpMethod::Post, "/_forcemerge", Handler<Endpoint::ForceMerge>},
  {HttpMethod::Post, "/:index/_forcemerge", Handler<Endpoint::ForceMerge>},
  {HttpMethod::Post, "/_refresh", Handler<Endpoint::Refresh>},
  {HttpMethod::Get, "/_refresh", Handler<Endpoint::Refresh>},
  {HttpMethod::Put, "/:index", Handler<Endpoint::CreateIndex>},
  {HttpMethod::Delete, "/:index", Handler<Endpoint::DeleteIndex>},
  {HttpMethod::Head, "/:index", Handler<Endpoint::IndexExists>},
  {HttpMethod::Get, "/:index", Handler<Endpoint::IndexInfo>},
  {HttpMethod::Get, "/:index/_mapping", Handler<Endpoint::Mapping>},
  {HttpMethod::Post, "/:index/_bulk", Handler<Endpoint::Bulk>},
  {HttpMethod::Put, "/:index/_bulk", Handler<Endpoint::Bulk>},
  {HttpMethod::Post, "/:index/_doc", Handler<Endpoint::Doc>},
  {HttpMethod::Post, "/:index/_doc/:id", Handler<Endpoint::Doc>},
  {HttpMethod::Put, "/:index/_doc/:id", Handler<Endpoint::Doc>},
  {HttpMethod::Get, "/:index/_doc/:id", Handler<Endpoint::GetDoc>},
  {HttpMethod::Head, "/:index/_doc/:id", Handler<Endpoint::ExistsDoc>},
  {HttpMethod::Get, "/:index/_source/:id", Handler<Endpoint::GetSource>},
  {HttpMethod::Get, "/:index/_mget", Handler<Endpoint::Mget>},
  {HttpMethod::Post, "/:index/_mget", Handler<Endpoint::Mget>},
  {HttpMethod::Post, "/:index/_refresh", Handler<Endpoint::Refresh>},
  {HttpMethod::Get, "/:index/_refresh", Handler<Endpoint::Refresh>},
  {HttpMethod::Get, "/:index/_count", Handler<Endpoint::Count>},
  {HttpMethod::Post, "/:index/_count", Handler<Endpoint::Count>},
  {HttpMethod::Get, "/:index/_search", Handler<Endpoint::Search>},
  {HttpMethod::Post, "/:index/_search", Handler<Endpoint::Search>},
  {HttpMethod::Get, "/_search/scroll", Handler<Endpoint::Scroll>},
  {HttpMethod::Post, "/_search/scroll", Handler<Endpoint::Scroll>},
  {HttpMethod::Delete, "/_search/scroll", Handler<Endpoint::ClearScroll>},
});

SDB_HTTP_API kEs{"es", kRoutes, kEsMaxBodyBytes};

}  // namespace
}  // namespace sdb::network::http::es
