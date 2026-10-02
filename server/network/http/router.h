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

#include <ada.h>

#include <array>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "network/ada_re2_provider.h"
#include "network/http/handler.h"

namespace sdb::network {

class HttpRouter {
 public:
  // `pattern` is a WHATWG URLPattern pathname (`/:index/_search`).
  void Add(HttpMethod method, std::string_view pattern,
           std::unique_ptr<HttpHandler> handler);

  // Parses the query string into request.query, matches method + path,
  // fills request.params from the pattern's named groups. nullptr = no
  // route. Routes are tried in insertion order, first match wins.
  HttpHandler* Match(HttpRequest& request);

 private:
  using PathPattern = ada::url_pattern_component<AdaRe2Provider>;

  static constexpr size_t kMethods = magic_enum::enum_count<HttpMethod>();

  // Fully literal routes are matched first, so one api's `/:index` cannot
  // swallow another's reserved `/_mcp` whichever order the apis were
  // registered in; within each of the two classes, insertion order still
  // decides.
  struct Entry {
    HttpMethod method;
    PathPattern path;
    HttpHandler* handler;
  };

  std::vector<std::unique_ptr<HttpHandler>> _handlers;
  irs::containers::FlatHashMap<std::string, std::array<HttpHandler*, kMethods>>
    _literal;
  std::vector<Entry> _parameterized;
};

}  // namespace sdb::network
