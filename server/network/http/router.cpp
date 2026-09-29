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

#include "network/http/router.h"

#include <ada.h>

#include <iresearch/utils/assert.hpp>
#include <string>
#include <string_view>
#include <utility>

namespace sdb::network {

void HttpRouter::Add(HttpMethod method, std::string_view pattern,
                     std::unique_ptr<HttpHandler> handler) {
  SDB_VERIFY(!pattern.empty() && pattern.front() == '/',
             "HTTP route pattern must start with '/': '", pattern, "'");
  ada::url_pattern_init init{};
  init.pathname = std::string{pattern};
  auto parsed = ada::parse_url_pattern<AdaRe2Provider>(std::move(init));
  SDB_VERIFY(parsed.has_value(), "invalid HTTP route pattern: '", pattern, "'");
  const bool literal =
    pattern.find_first_of(":*(){}?+") == std::string_view::npos;
  auto& routes = literal ? _literal : _parameterized;
  routes.push_back({method, std::move(*parsed), std::move(handler)});
}

HttpHandler* HttpRouter::MatchIn(std::vector<Entry>& routes,
                                 const ada::url_pattern_init& path,
                                 HttpRequest& request) {
  for (auto& route : routes) {
    if (route.method != request.method) {
      continue;
    }
    const auto hit = route.pattern.test(path, nullptr);
    if (!hit || !*hit) {
      continue;
    }
    const auto result = route.pattern.exec(path, nullptr);
    if (!result || !*result) {
      continue;
    }
    request.params.clear();
    for (const auto& [name, value] : (*result)->pathname.groups) {
      if (value) {
        request.params.emplace_back(name, *value);
      }
    }
    return route.handler.get();
  }
  return nullptr;
}

HttpHandler* HttpRouter::Match(HttpRequest& request) {
  std::string_view target = request.target;
  const size_t cut = target.find_first_of("?#");
  const std::string_view path =
    cut == std::string_view::npos ? target : target.substr(0, cut);
  // Parse the query string (request-scoped) into request.query, percent-decoded
  // by ada; path matching below uses `path` only, so it is unaffected.
  request.query.clear();
  if (cut != std::string_view::npos && target[cut] == '?') {
    const size_t frag = target.find('#', cut);
    const std::string_view raw_query = target.substr(
      cut + 1,
      frag == std::string_view::npos ? std::string_view::npos : frag - cut - 1);
    ada::url_search_params parsed{raw_query};
    for (const auto& [key, value] : parsed) {
      request.query.emplace_back(key, value);
    }
  }
  if (path.empty() || path.front() != '/') {
    return nullptr;
  }
  ada::url_pattern_init input{};
  input.pathname = std::string{path};
  if (auto* handler = MatchIn(_literal, input, request)) {
    return handler;
  }
  if (auto* handler = MatchIn(_parameterized, input, request)) {
    return handler;
  }
  request.params.clear();
  return nullptr;
}

}  // namespace sdb::network
