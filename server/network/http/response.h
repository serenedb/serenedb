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

#include <string>
#include <utility>
#include <vector>

namespace sdb::network {

struct HttpResponse {
  int status = 200;
  std::string reason = "OK";
  std::string content_type = "application/json";
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;

  static HttpResponse Json(int status, std::string reason, std::string body) {
    HttpResponse response;
    response.status = status;
    response.reason = std::move(reason);
    response.content_type = "application/json";
    response.body = std::move(body);
    return response;
  }

  static HttpResponse NotFound() {
    return Json(404, "Not Found", R"({"error":"not_found"})");
  }

  static HttpResponse Error(int status) {
    switch (status) {
      case 400:
        return Json(400, "Bad Request", R"({"error":"bad_request"})");
      case 413:
        return Json(413, "Content Too Large", R"({"error":"too_large"})");
      case 417:
        return Json(417, "Expectation Failed",
                    R"({"error":"expectation_failed"})");
      case 431:
        return Json(431, "Request Header Fields Too Large",
                    R"({"error":"headers_too_large"})");
      case 500:
        return Json(500, "Internal Server Error", R"({"error":"internal"})");
      default:
        return Json(status, "Error", R"({"error":"error"})");
    }
  }
};

}  // namespace sdb::network
