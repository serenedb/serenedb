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

#include <simdjson.h>

#include <cstdint>
#include <string>
#include <string_view>

namespace irs {

enum class JsonLeafKind : uint8_t {
  None,
  String,
  Number,
  Bool,
  Null,
  Composite,
};

struct JsonLeaf {
  JsonLeafKind kind = JsonLeafKind::None;
  union {
    std::string_view string{};
    double number;
    bool boolean;
  };
};

class JsonLeafReader {
 public:
  JsonLeaf Read(std::string_view json) {
    if (json.size() > 1 && json.front() == '"' &&
        json.find('"', 1) == json.size() - 1 &&
        json.find('\\') == std::string_view::npos) {
      return {.kind = JsonLeafKind::String,
              .string = json.substr(1, json.size() - 2)};
    }
    _buffer.assign(json);
    _buffer.append(simdjson::SIMDJSON_PADDING, '\0');
    simdjson::ondemand::document doc;
    simdjson::ondemand::json_type type{};
    if (_parser
            .iterate(simdjson::padded_string_view{_buffer.data(), json.size(),
                                                  _buffer.size()})
            .get(doc) != simdjson::SUCCESS ||
        doc.type().get(type) != simdjson::SUCCESS) {
      return {};
    }
    switch (type) {
      case simdjson::ondemand::json_type::string:
        if (std::string_view string;
            doc.get_string().get(string) == simdjson::SUCCESS) {
          return {.kind = JsonLeafKind::String, .string = string};
        }
        break;
      case simdjson::ondemand::json_type::number:
        if (double number; doc.get_double().get(number) == simdjson::SUCCESS) {
          return {.kind = JsonLeafKind::Number, .number = number};
        }
        break;
      case simdjson::ondemand::json_type::boolean:
        if (bool boolean; doc.get_bool().get(boolean) == simdjson::SUCCESS) {
          return {.kind = JsonLeafKind::Bool, .boolean = boolean};
        }
        break;
      case simdjson::ondemand::json_type::null:
        return {.kind = JsonLeafKind::Null};
      case simdjson::ondemand::json_type::object:
      case simdjson::ondemand::json_type::array:
        return {.kind = JsonLeafKind::Composite};
      default:
        break;
    }
    return {};
  }

 private:
  simdjson::ondemand::parser _parser;
  std::string _buffer;
};

}  // namespace irs
