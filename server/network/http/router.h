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

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "network/http/handler.h"

namespace sdb::network {

class HttpRouter {
 public:
  void Add(HttpMethod method, std::string_view pattern,
           std::unique_ptr<HttpHandler> handler);

  // Parses the query string into request.query, matches method + path,
  // fills request.params from the pattern's `:name` segments. nullptr = no
  // route. Routes are tried in insertion order, first match wins.
  HttpHandler* Match(HttpRequest& request);

 private:
  // A route pattern is a list of '/'-delimited segments; `param` segments
  // (`:name`) capture the request segment verbatim, the rest match literally.
  // No regex, optionals, or wildcards -- every ES/OS route is this shape, so a
  // segment walk beats per-request URL parsing + regex by ~30% of server CPU.
  struct Segment {
    std::string text;
    bool param;
  };
  struct Entry {
    HttpMethod method;
    std::vector<Segment> segments;
    std::unique_ptr<HttpHandler> handler;
  };

  static bool MatchPath(const std::vector<Segment>& segments,
                        std::string_view path, HttpRequest& request);

  std::vector<Entry> _routes;
};

}  // namespace sdb::network
