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
#include <span>
#include <string_view>

#include "network/http/handler.h"
#include "network/listen_spec.h"
#include "server/utils/registry.h"

namespace sdb::network::http {

struct Route {
  HttpMethod method;
  std::string_view pattern;
  std::unique_ptr<HttpHandler> (*make)();
};

struct Api {
  static constexpr std::string_view kKind = "http api";

  std::span<const Route> routes;
  uint64_t max_body_bytes = 0;
  // Runs once a listener serving the API has its database. The listener keeps
  // what it returns alive, such as a service the API's listeners share.
  std::shared_ptr<void> (*start)(const ListenContext& context,
                                 const ListenSpec& spec) = nullptr;
};

}  // namespace sdb::network::http

#define SDB_HTTP_API const ::sdb::Registry<::sdb::network::http::Api>::Register
