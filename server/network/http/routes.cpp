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

#include <algorithm>

namespace sdb::network::http {

void AddRoutes(HttpRouter& router, std::span<const Api* const> apis) {
  for (const auto* api : apis) {
    for (const auto& route : api->routes) {
      router.Add(route.method, route.pattern, route.make(), route.session);
    }
  }
}

uint64_t MaxBodyBytes(std::span<const Api* const> apis) noexcept {
  uint64_t bytes = 0;
  for (const auto* api : apis) {
    bytes = std::max(bytes, api->max_body_bytes);
  }
  return bytes;
}

}  // namespace sdb::network::http
