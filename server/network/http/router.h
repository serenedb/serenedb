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

#include <memory>
#include <string_view>
#include <vector>
#include <yaclib/async/future.hpp>

#include "network/ada_re2_provider.h"
#include "network/http/handler.h"

namespace sdb::network {

class HttpRouter {
 public:
  void Add(HttpMethod method, std::string_view pattern,
           std::unique_ptr<HttpHandler> handler);

  yaclib::Future<HttpResponse> Route(HttpRequest& request);

 private:
  using Pattern = ada::url_pattern<AdaRe2Provider>;

  struct Entry {
    HttpMethod method;
    Pattern pattern;
    std::unique_ptr<HttpHandler> handler;
  };

  std::vector<Entry> _routes;
};

}  // namespace sdb::network
