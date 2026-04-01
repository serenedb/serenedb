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

#include <absl/strings/numbers.h>
#include <simdjson.h>
#include <velox/type/StringView.h>

#include "basics/fwd.h"

namespace sdb::pg::functions {

class JsonParser {
 public:
  enum class OutputType {
    JSON,
    TEXT,
  };

  void PrepareJson(std::string_view json);

  template<OutputType Output, typename Range>
  std::string_view Extract(const Range& path) {
    simdjson::ondemand::value value;
    auto doc = GetJsonDocument();
    if (doc.get_value().get(value)) {
      return {};
    }
    for (const std::optional<velox::StringView>& element : path) {
      if (!element) {
        return {};
      }
      auto key = static_cast<std::string_view>(*element);

      if (value.type() == simdjson::ondemand::json_type::array) {
        int64_t index;
        if (!absl::SimpleAtoi(key, &index)) {
          return {};
        }
        simdjson::ondemand::array arr;
        if (value.get_array().get(arr)) {
          return {};
        }
        if (GetByIndex(arr, index).get(value)) {
          return {};
        }
      } else if (value.type() == simdjson::ondemand::json_type::object) {
        auto res = value.find_field(key);
        if (res.get(value)) {
          return {};
        }
      } else {
        return {};
      }
    }
    return ProcessOutput<Output>(value);
  }

  template<OutputType Output>
  std::string_view ExtractByIndex(int64_t index) {
    simdjson::ondemand::value value;
    auto doc = GetJsonDocument();
    if (doc.get_value().get(value)) {
      return {};
    }
    if (value.type() != simdjson::ondemand::json_type::array) {
      return {};
    }
    simdjson::ondemand::array arr;
    if (value.get_array().get(arr)) {
      return {};
    }
    if (GetByIndex(arr, index).get(value)) {
      return {};
    }
    return ProcessOutput<Output>(value);
  }

  template<OutputType Output>
  std::string_view ExtractByField(std::string_view field) {
    simdjson::ondemand::value value;
    auto doc = GetJsonDocument();
    if (doc.get_value().get(value)) {
      return {};
    }
    if (value.type() != simdjson::ondemand::json_type::object) {
      return {};
    }
    auto res = value.find_field(field);
    if (res.get(value)) {
      return {};
    }
    return ProcessOutput<Output>(value);
  }

 private:
  template<OutputType Output>
  std::string_view ProcessOutput(simdjson::ondemand::value& value) {
    if constexpr (Output == OutputType::TEXT) {
      if (value.type() == simdjson::ondemand::json_type::string) {
        return value.get_string().value();
      }
    }
    std::string_view str;
    if (simdjson::to_json_string(value).get(str)) {
      return {};
    }
    return str;
  }

  simdjson::ondemand::document GetJsonDocument();

  simdjson::simdjson_result<simdjson::ondemand::value> GetByIndex(
    simdjson::ondemand::array arr, int64_t relative_index);

  // TODO(codeworse): Try to reuse parser between calls
  simdjson::ondemand::parser _parser;
  simdjson::padded_string _padded_input;
};

}  // namespace sdb::pg::functions
