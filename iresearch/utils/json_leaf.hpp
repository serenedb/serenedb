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

#include <absl/cleanup/cleanup.h>
#include <simdjson.h>

#include <cstdint>
#include <cstring>
#include <duckdb/storage/arena_allocator.hpp>
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
  explicit JsonLeafReader(duckdb::ArenaAllocator& strings) noexcept
    : _strings{strings} {}

  JsonLeaf Read(std::string_view json) {
    if (json.size() > 1 && json.front() == '"' && json.back() == '"') {
      return StringOf(json.substr(1, json.size() - 2));
    }
    constexpr std::string_view kSpace = " \t\n\r";
    const auto first = json.find_first_not_of(kSpace);
    if (first == std::string_view::npos || json[first] != '"') {
      return ValueOf(json);
    }
    const auto last = json.find_last_not_of(kSpace);
    if (first == last || json[last] != '"') {
      return {};
    }
    return StringOf(json.substr(first + 1, last - first - 1));
  }

 private:
  JsonLeaf StringOf(std::string_view body) {
    if (body.find('\\') == std::string_view::npos) {
      return {.kind = JsonLeafKind::String, .string = body};
    }
    if (_parser.capacity() == 0 &&
        _parser.allocate(simdjson::SIMDJSON_PADDING) != simdjson::SUCCESS) {
      return {};
    }
    const auto capacity = body.size() + simdjson::SIMDJSON_PADDING;
    const auto size = 2 * capacity + 2;
    auto* const out = _strings.Allocate(size);
    auto* const in = out + capacity;
    std::memcpy(in, body.data(), body.size());
    in[body.size()] = '"';
    in[body.size() + 1] = '"';
    std::memset(in + body.size() + 2, 0, simdjson::SIMDJSON_PADDING);
    const auto string = Unescape(in, out);
    _strings.ShrinkHead(size - string.size());
    if (!string.data()) {
      return {};
    }
    return {.kind = JsonLeafKind::String, .string = string};
  }

  std::string_view Unescape(const uint8_t* in, uint8_t* out) {
    std::string_view string;
    if (_parser.unescape(simdjson::ondemand::raw_json_string{in}, out)
          .get(string) != simdjson::SUCCESS) {
      return {};
    }
    return string;
  }

  JsonLeaf ValueOf(std::string_view json) {
    const auto size = json.size() + simdjson::SIMDJSON_PADDING;
    auto* const padded = reinterpret_cast<char*>(_strings.Allocate(size));
    std::memcpy(padded, json.data(), json.size());
    std::memset(padded + json.size(), 0, simdjson::SIMDJSON_PADDING);
    absl::Cleanup release = [&] { _strings.ShrinkHead(size); };
    simdjson::ondemand::document doc;
    simdjson::ondemand::json_type type{};
    if (_parser.iterate(simdjson::padded_string_view{padded, json.size(), size})
            .get(doc) != simdjson::SUCCESS ||
        doc.type().get(type) != simdjson::SUCCESS) {
      return {};
    }
    switch (type) {
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

  simdjson::ondemand::parser _parser;
  duckdb::ArenaAllocator& _strings;
};

}  // namespace irs
