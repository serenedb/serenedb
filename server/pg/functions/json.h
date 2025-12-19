////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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
#include <velox/functions/Macros.h>
#include <velox/functions/prestosql/json/JsonStringUtil.h>
#include <velox/functions/prestosql/types/JsonType.h>
#include <velox/type/SimpleFunctionApi.h>

#include <functional>
#include <iterator>
#include <utility>

#include "basics/fwd.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {

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
      if (!element.has_value()) {
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

  static bool CheckQuoted(std::string_view str) {
    return !str.empty() && str.front() == '"' && str.back() == '"';
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

  simdjson::ondemand::parser _parser;
  simdjson::padded_string _padded_input;
};

template<typename T>
struct PgJsonExtractPathText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  bool call(  // NOLINT
    out_type<velox::Varchar>& result, const arg_type<velox::Json>& json,
    const arg_type<int64_t>& index) {
    _parser.PrepareJson({json});
    auto str = _parser.ExtractByIndex<JsonParser::OutputType::TEXT>(index);
    if (!str.data()) {
      return false;
    }
    result = str;
    return true;
  }

  // call for ->> operator (field text)
  bool call(out_type<velox::Varchar>& result, const arg_type<velox::Json>& json,
            const arg_type<velox::Varchar>& field) {
    _parser.PrepareJson({json});
    auto str = _parser.ExtractByField<JsonParser::OutputType::TEXT>(
      {field.data(), field.size()});
    if (!str.data()) {
      return false;
    }
    result = str;
    return true;
  }

  // call for #>> operator (path text)
  bool call(out_type<velox::Varchar>& result, const arg_type<velox::Json>& json,
            const arg_type<velox::Array<velox::Varchar>>& path) {
    _parser.PrepareJson({json});

    auto str = _parser.Extract<JsonParser::OutputType::TEXT>(path);
    if (!str.data()) {
      return false;
    }
    result = str;
    return true;
  }

 private:
  JsonParser _parser;
};

template<typename T>
struct PgJsonExtractPath {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // call for -> operator (index json)
  bool call(  // NOLINT
    out_type<velox::Json>& result, const arg_type<velox::Json>& json,
    const arg_type<int64_t>& index) {
    _parser.PrepareJson({json});
    auto str = _parser.ExtractByIndex<JsonParser::OutputType::JSON>(index);
    if (!str.data()) {
      return false;
    }
    result = str;
    return true;
  }

  // call for -> operator (field json)
  bool call(out_type<velox::Json>& result, const arg_type<velox::Json>& json,
            const arg_type<velox::Varchar>& field) {
    _parser.PrepareJson({json});
    auto str = _parser.ExtractByField<JsonParser::OutputType::JSON>(
      {field.data(), field.size()});
    if (!str.data()) {
      return false;
    }
    result = str;
    return true;
  }

  // call for #>> operator (path json)
  bool call(out_type<velox::Json>& result, const arg_type<velox::Json>& json,
            const arg_type<velox::Array<velox::Varchar>>& path) {
    _parser.PrepareJson({json});
    auto str = _parser.Extract<JsonParser::OutputType::JSON>(path);
    if (!str.data()) {
      return false;
    }
    result = str;
    return true;
  }

 private:
  JsonParser _parser;
};

}  // namespace sdb::pg
