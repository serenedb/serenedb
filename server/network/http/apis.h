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
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "network/http/request.h"
#include "network/listen_spec.h"

namespace sdb::network {

class HttpHandler;

}  // namespace sdb::network
namespace sdb::network::http {

struct Route {
  HttpMethod method;
  std::string_view pattern;
  std::unique_ptr<HttpHandler> (*make)();
};

struct Api {
  std::string_view name;
  std::span<const Route> routes;
  uint64_t max_body_bytes = 0;
};

HttpApi RegisterApi(const Api& api);

std::optional<HttpApi> FindApi(std::string_view name);

const Api& GetApi(HttpApi api);

std::string ApiNames();

}  // namespace sdb::network::http
