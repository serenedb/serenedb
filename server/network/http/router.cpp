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

#include <absl/strings/str_cat.h>
#include <ada.h>

#include <string>
#include <utility>
#include <variant>
#include <yaclib/async/make.hpp>

#include "basics/assert.h"

namespace sdb::network {

void HttpRouter::Add(HttpMethod method, std::string_view pattern,
                     std::unique_ptr<HttpHandler> handler) {
  ada::url_pattern_init init;
  init.pathname = std::string{pattern};
  auto compiled = ada::parse_url_pattern<AdaRe2Provider>(
    std::variant<std::string_view, ada::url_pattern_init>{std::move(init)});
  SDB_VERIFY(compiled.has_value(), "failed to compile HTTP route pattern '",
             pattern, "'");
  _routes.push_back({method, std::move(compiled.value()), std::move(handler)});
}

yaclib::Future<HttpResponse> HttpRouter::Route(HttpRequest& request) {
  std::string_view target = request.target;
  const size_t cut = target.find_first_of("?#");
  const std::string_view path =
    cut == std::string_view::npos ? target : target.substr(0, cut);
  // Parse the query string (request-scoped) into request.query, percent-decoded
  // by ada; path matching below still uses `path` only, so it is unaffected.
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
    return yaclib::MakeFuture(HttpResponse::NotFound());
  }
  const std::string url = absl::StrCat("http://serenedb", path);
  for (auto& route : _routes) {
    if (route.method != request.method) {
      continue;
    }
    auto matched =
      route.pattern.exec(ada::url_pattern_input{std::string_view{url}});
    if (!matched.has_value() || !matched.value().has_value()) {
      continue;
    }
    request.params.clear();
    for (auto& [name, value] : matched.value().value().pathname.groups) {
      if (value.has_value()) {
        request.params.emplace_back(name, std::move(value.value()));
      }
    }
    return route.handler->Handle(request);
  }
  return yaclib::MakeFuture(HttpResponse::NotFound());
}

}  // namespace sdb::network
