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
  auto* raw = _handlers.emplace_back(std::move(handler)).get();
  auto& slot = _methods[HandlersOf(pattern)][static_cast<size_t>(method)];
  if (slot == nullptr) {
    slot = raw;
  }
}

size_t HttpRouter::HandlersOf(std::string_view pattern) {
  if (const auto it = _patterns.find(pattern); it != _patterns.end()) {
    return it->second;
  }
  ada::url_pattern_init init{};
  init.pathname = std::string{pattern};
  auto parsed = ada::parse_url_pattern<AdaRe2Provider>(std::move(init));
  SDB_VERIFY(parsed.has_value(), "invalid HTTP route pattern: '", pattern, "'");
  auto& path = parsed->pathname_component;
  size_t index = _methods.size();
  if (path.type == ada::url_pattern_component_type::EXACT_MATCH) {
    index = _literal.try_emplace(path.exact_match_value, index).first->second;
  } else {
    _parameterized.push_back({std::move(path), index});
  }
  if (index == _methods.size()) {
    _methods.emplace_back();
  }
  _patterns.emplace(std::string{pattern}, index);
  return index;
}

HttpHandler* HttpRouter::Match(HttpRequest& request) {
  std::string_view target = request.target;
  const size_t cut = target.find_first_of("?#");
  const std::string_view raw_path =
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
  request.params.clear();
  if (raw_path.empty() || raw_path.front() != '/') {
    return nullptr;
  }
  const auto canonical =
    ada::url_pattern_helpers::canonicalize_pathname(raw_path);
  if (!canonical) {
    return nullptr;
  }
  const std::string_view path = *canonical;
  const auto method = static_cast<size_t>(request.method);
  if (const auto it = _literal.find(path); it != _literal.end()) {
    if (auto* handler = _methods[it->second][method]) {
      return handler;
    }
  }
  for (auto& route : _parameterized) {
    auto* handler = _methods[route.handlers][method];
    if (handler == nullptr || !route.path.fast_test(path)) {
      continue;
    }
    auto groups = route.path.fast_match(path);
    if (!groups) {
      continue;
    }
    const auto& names = route.path.group_name_list;
    for (size_t i = 0; i < groups->size() && i < names.size(); ++i) {
      if (auto& value = (*groups)[i]) {
        request.params.emplace_back(names[i], std::move(*value));
      }
    }
    return handler;
  }
  return nullptr;
}

}  // namespace sdb::network
