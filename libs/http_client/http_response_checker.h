////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string_view>

#include "basics/result.h"
#include "http_client/http_result.h"

namespace sdb {
class HttpResponseChecker {
 public:
  HttpResponseChecker() = delete;
  enum class PayloadType { JSON, VPACK, JSONL };
  static sdb::Result check(const std::string& client_error_msg,
                           const sdb::httpclient::HttpResult* const response);
  static sdb::Result check(const std::string& client_error_msg,
                           const sdb::httpclient::HttpResult* const response,
                           const std::string& action_msg,
                           std::string_view request_payload, PayloadType type);

 private:
  static void trimPayload(vpack::Slice input, vpack::Builder& output);
};

}  // namespace sdb
