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

#include <duckdb/common/types/string_type.hpp>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

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
    for (const std::optional<duckdb::string_t>& element : path) {
      if (!element) {
        return {};
      }
      std::string_view key{element->GetData(), element->GetSize()};

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
      if (value.type() == simdjson::ondemand::json_type::null) {
        return std::string_view{};
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

inline simdjson::simdjson_result<simdjson::ondemand::value>
JsonParser::GetByIndex(simdjson::ondemand::array arr, int64_t relative_index) {
  size_t size, index;
  auto ec = arr.count_elements().get(size);
  SDB_ASSERT(ec == simdjson::SUCCESS);

  if (relative_index < 0) {
    if (static_cast<size_t>(-relative_index) > size) {
      return simdjson::simdjson_result<simdjson::ondemand::value>(
        simdjson::OUT_OF_BOUNDS);
    }
    index = size + relative_index;
  } else {
    index = static_cast<size_t>(relative_index);
  }

  return arr.at(index);
}

inline void JsonParser::PrepareJson(std::string_view json) {
  // TODO(codeworse):
  // https://github.com/simdjson/simdjson/blob/master/doc/performance.md#free-padding
  _padded_input = simdjson::padded_string{json};
}

inline simdjson::ondemand::document JsonParser::GetJsonDocument() {
  simdjson::ondemand::document doc;
  auto ec = _parser.iterate(_padded_input).get(doc);
  if (ec != simdjson::SUCCESS) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Invalid JSON input: ", simdjson::error_message(ec)));
  }
  if (doc.type().error()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Invalid JSON input: tape error"));
  }
  return doc;
}

}  // namespace sdb::pg::functions
