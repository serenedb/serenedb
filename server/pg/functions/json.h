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
namespace {

template<typename T>
bool ValidateJson(T& value) {
  switch (value.type()) {
    case simdjson::ondemand::json_type::array: {
      simdjson::ondemand::array arr;
      if (value.get_array().get(arr)) {
        return false;
      }
      for (auto element : arr) {
        simdjson::ondemand::value element_value;
        if (element.get(element_value)) {
          return false;
        }
        if (!ValidateJson(element_value)) {
          return false;
        }
      }
      return true;
    }
    case simdjson::ondemand::json_type::object: {
      simdjson::ondemand::object obj;
      if (value.get_object().get(obj)) {
        return false;
      }
      for (auto field : obj) {
        simdjson::ondemand::value field_value;
        if (field.value().get(field_value)) {
          return false;
        }
        if (!ValidateJson(field_value)) {
          return false;
        }
      }
      return true;
    }
    case simdjson::ondemand::json_type::string: {
      return value.get_string().error() == simdjson::SUCCESS;
    }
    case simdjson::ondemand::json_type::number: {
      auto num = value.get_number();
      return num.error() == simdjson::SUCCESS ||
             num.error() == simdjson::BIGINT_ERROR;
    }
    case simdjson::ondemand::json_type::boolean: {
      return value.get_bool().error() == simdjson::SUCCESS;
    }
    case simdjson::ondemand::json_type::null:
      return value.is_null();
    default:
      return false;
  }
}
}  // namespace

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

// Process -> operator (by index) and ->> operator (by index text)
template<JsonParser::OutputType Output, typename T>
struct PgJsonExtractIndexBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  using ResultVeloxType =
    std::conditional_t<Output == JsonParser::OutputType::TEXT, velox::Varchar,
                       velox::Json>;

  bool call(  // NOLINT
    out_type<ResultVeloxType>& result, const arg_type<velox::Json>& json,
    const arg_type<int64_t>& index) {
    _parser.PrepareJson({json});
    auto str = _parser.ExtractByIndex<Output>(index);
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
using PgJsonExtractIndex =
  PgJsonExtractIndexBase<JsonParser::OutputType::JSON, T>;

template<typename T>
using PgJsonExtractIndexText =
  PgJsonExtractIndexBase<JsonParser::OutputType::TEXT, T>;

// Process -> operator (by field) and ->> operator (by field text)
template<JsonParser::OutputType Output, typename T>
struct PgJsonExtractFieldBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  using ResultVeloxType =
    std::conditional_t<Output == JsonParser::OutputType::TEXT, velox::Varchar,
                       velox::Json>;

  bool call(  // NOLINT
    out_type<ResultVeloxType>& result, const arg_type<velox::Json>& json,
    const arg_type<velox::Varchar>& field) {
    _parser.PrepareJson({json});
    auto str = _parser.ExtractByField<Output>({field.data(), field.size()});
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
using PgJsonExtractField =
  PgJsonExtractFieldBase<JsonParser::OutputType::JSON, T>;

template<typename T>
using PgJsonExtractFieldText =
  PgJsonExtractFieldBase<JsonParser::OutputType::TEXT, T>;

// Process #> operator (path json) and #>> operator (path text)
template<JsonParser::OutputType Output, typename T>
struct PgJsonExtractPathBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  using ResultVeloxType =
    std::conditional_t<Output == JsonParser::OutputType::TEXT, velox::Varchar,
                       velox::Json>;

  // call for #>> operator (path text)
  bool call(out_type<ResultVeloxType>& result,
            const arg_type<velox::Json>& json,
            const arg_type<velox::Array<velox::Varchar>>& path) {
    _parser.PrepareJson({json});

    auto str = _parser.Extract<Output>(path);
    if (!str.data()) {
      return false;
    }
    result = str;
    return true;
  }

  bool call(out_type<ResultVeloxType>& result,
            const arg_type<velox::Json>& json,
            const arg_type<velox::Variadic<velox::Varchar>>& segments) {
    _parser.PrepareJson({json});

    auto str = _parser.Extract<Output>(segments);
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
using PgJsonExtractPath =
  PgJsonExtractPathBase<JsonParser::OutputType::JSON, T>;

template<typename T>
using PgJsonExtractPathText =
  PgJsonExtractPathBase<JsonParser::OutputType::TEXT, T>;

template<typename T>
struct PgJsonInFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int32_t reuse_strings_from_arg = 0;

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<velox::Json>& result, const arg_type<velox::Varchar>& input) {
    // Even though we need only validation here, we still parse the input:
    // https://github.com/simdjson/simdjson/discussions/1393

    std::string_view input_view{input.data(), input.size()};
    simdjson::ondemand::parser parser;
    simdjson::padded_string padded_input{input_view};
    simdjson::ondemand::document doc;
    auto ec = parser.iterate(padded_input).get(doc);
    if (ec != simdjson::SUCCESS || !ValidateJson(doc)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_JSON_TEXT),
        ERR_MSG("Invalid input syntax for type json: ", input_view));
    } else {
      result.setNoCopy(input);
    }
  }
};

template<typename T>
struct PgJsonOutFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int32_t reuse_strings_from_arg = 0;

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<velox::Varchar>& result, const arg_type<velox::Json>& input) {
    result.setNoCopy(input);
  }
};

}  // namespace sdb::pg
