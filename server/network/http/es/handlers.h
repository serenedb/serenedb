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

#pragma once

#include <cstdint>
#include <memory>

#include "network/http/handler.h"

namespace sdb::network::http::es {

// The standard Elasticsearch API surface (spec: rest-api-spec).
enum class Endpoint : uint8_t {
  Root,
  Health,
  CatIndices,
  CatCount,
  Bulk,
  NodesStats,
  ClusterSettings,
  IndexStats,
  ForceMerge,
  Refresh,
  CreateIndex,
  DeleteIndex,
  IndexExists,
  IndexInfo,
  Mapping,
  Doc,
  GetDoc,
  ExistsDoc,
  GetSource,
  Mget,
  Count,
  Search,
  Scroll,
  ClearScroll,
};

std::unique_ptr<HttpHandler> Make(Endpoint endpoint);

}  // namespace sdb::network::http::es
