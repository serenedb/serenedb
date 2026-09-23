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
  // (`:name`) capture any request segment, the rest match literally. Fully
  // literal routes are matched first, so one api's `/:index` cannot swallow
  // another's reserved `/_mcp` whichever order the apis were registered in;
  // within each of the two classes, insertion order still decides.
  //
  // A param does capture an `_`-prefixed segment once no literal claims it, so
  // a name the caller should not have used reaches the handler that can reject
  // it properly rather than looking like a missing route.
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

  static HttpHandler* MatchIn(std::vector<Entry>& routes, std::string_view path,
                              HttpRequest& request);

  std::vector<Entry> _literal;
  std::vector<Entry> _parameterized;
};

}  // namespace sdb::network
