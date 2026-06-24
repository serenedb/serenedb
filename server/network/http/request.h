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

#include <absl/strings/match.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/message_sequence_view.h"
#include "network/http/header.h"

namespace sdb::network {

enum class HttpMethod : uint8_t {
  Get,
  Post,
  Put,
  Delete,
  Head,
  Options,
  Other,
};

struct HttpRequest {
  struct Field {
    HttpHeader name = HttpHeader::Unknown;
    std::string raw;
    std::string value;
  };

  HttpMethod method = HttpMethod::Other;
  std::string target;
  std::vector<Field> headers;
  std::vector<std::pair<std::string, std::string>> params;
  std::vector<std::pair<std::string, std::string>> query;
  message::SequenceView body;
  bool keep_alive = true;

  std::string_view Header(HttpHeader name) const {
    for (const auto& field : headers) {
      if (field.name == name) {
        return field.value;
      }
    }
    return {};
  }

  std::string_view Header(std::string_view name) const {
    const HttpHeader interned = InternHeader(name);
    if (interned != HttpHeader::Unknown) {
      return Header(interned);
    }
    for (const auto& field : headers) {
      if (field.name == HttpHeader::Unknown &&
          absl::EqualsIgnoreCase(field.raw, name)) {
        return field.value;
      }
    }
    return {};
  }

  std::string_view Param(std::string_view name) const {
    for (const auto& [key, value] : params) {
      if (key == name) {
        return value;
      }
    }
    return {};
  }

  // Query-string parameters (percent-decoded). Returns the first match, like
  // Param; repeated keys keep wire order.
  std::string_view Query(std::string_view name) const {
    for (const auto& [key, value] : query) {
      if (key == name) {
        return value;
      }
    }
    return {};
  }
};

}  // namespace sdb::network
