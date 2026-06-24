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

#include <string>
#include <string_view>
#include <utility>

#include "basics/assert.h"

namespace sdb::network {

void HttpRouter::Add(HttpMethod method, std::string_view pattern,
                     std::unique_ptr<HttpHandler> handler) {
  SDB_VERIFY(!pattern.empty() && pattern.front() == '/',
             "HTTP route pattern must start with '/': '", pattern, "'");
  std::vector<Segment> segments;
  std::string_view rest = pattern.substr(1);
  // "/" is the root: zero segments. Any other pattern splits on '/'.
  if (!rest.empty()) {
    for (;;) {
      const size_t slash = rest.find('/');
      const std::string_view seg =
        slash == std::string_view::npos ? rest : rest.substr(0, slash);
      if (!seg.empty() && seg.front() == ':') {
        segments.push_back({std::string{seg.substr(1)}, true});
      } else {
        segments.push_back({std::string{seg}, false});
      }
      if (slash == std::string_view::npos) {
        break;
      }
      rest = rest.substr(slash + 1);
    }
  }
  _routes.push_back({method, std::move(segments), std::move(handler)});
}

bool HttpRouter::MatchPath(const std::vector<Segment>& segments,
                           std::string_view path, HttpRequest& request) {
  // `path` is non-empty with a leading '/'. Walk segments in lockstep with
  // '/'-delimited path pieces; a trailing '/' leaves an empty piece that only
  // matches an empty literal (never registered), so "/x/" != "/x".
  std::string_view rest = path.substr(1);
  if (rest.empty()) {
    return segments.empty();
  }
  size_t i = 0;
  for (;;) {
    if (i >= segments.size()) {
      return false;
    }
    const size_t slash = rest.find('/');
    const std::string_view seg =
      slash == std::string_view::npos ? rest : rest.substr(0, slash);
    const Segment& pat = segments[i];
    if (pat.param) {
      request.params.emplace_back(pat.text, std::string{seg});
    } else if (seg != pat.text) {
      return false;
    }
    ++i;
    if (slash == std::string_view::npos) {
      break;
    }
    rest = rest.substr(slash + 1);
  }
  return i == segments.size();
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
  for (auto& route : _routes) {
    if (route.method != request.method) {
      continue;
    }
    request.params.clear();
    if (MatchPath(route.segments, path, request)) {
      return route.handler.get();
    }
  }
  return nullptr;
}

}  // namespace sdb::network
