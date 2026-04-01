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

#include "pg/functions/json.h"

#include <absl/strings/numbers.h>
#include <simdjson.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/prestosql/json/JsonStringUtil.h>
#include <velox/functions/prestosql/types/JsonType.h>
#include <velox/type/SimpleFunctionApi.h>

#include <functional>
#include <iterator>
#include <utility>

#include "basics/fwd.h"
#include "pg/functions/json.tpp"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {
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

// Process -> operator (by index) and ->> operator (by index text)
template<JsonParser::OutputType Output, typename T>
struct PgJsonExtractIndexBase {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  using ResultVeloxType =
    std::conditional_t<Output == JsonParser::OutputType::TEXT, velox::Varchar,
                       velox::Json>;

  bool call(out_type<ResultVeloxType>& result,
            const arg_type<velox::Json>& json, const arg_type<int64_t>& index) {
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

  bool call(out_type<ResultVeloxType>& result,
            const arg_type<velox::Json>& json,
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

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Json>& result,
                                const arg_type<velox::Varchar>& input) {
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

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Json>& input) {
    result.setNoCopy(input);
  }
};

// Returns the type of the outermost JSON value as a text string:
// object, array, string, number, boolean, or null.
template<typename T>
struct PgJsonTypeof {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Json>& input) {
    std::string_view sv(input.data(), input.size());
    simdjson::ondemand::parser parser;
    simdjson::padded_string padded(sv);
    auto doc = parser.iterate(padded);
    auto tp = doc.type();
    if (tp.error()) {
      result.copy_from("null");
      return;
    }
    switch (tp.value()) {
      case simdjson::ondemand::json_type::object:
        result.copy_from("object");
        break;
      case simdjson::ondemand::json_type::array:
        result.copy_from("array");
        break;
      case simdjson::ondemand::json_type::string:
        result.copy_from("string");
        break;
      case simdjson::ondemand::json_type::number:
        result.copy_from("number");
        break;
      case simdjson::ondemand::json_type::boolean:
        result.copy_from("boolean");
        break;
      default:
        result.copy_from("null");
        break;
    }
  }
};

// Recursively removes all object fields with null values.
template<typename T>
struct PgJsonStripNulls {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Json>& result,
                                const arg_type<velox::Json>& input) {
    std::string_view sv(input.data(), input.size());
    simdjson::ondemand::parser parser;
    simdjson::padded_string padded(sv);
    simdjson::ondemand::document doc;
    if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("invalid JSON"));
    }
    std::string out;
    writeValue(doc, out);
    result.resize(out.size());
    std::memcpy(result.data(), out.data(), out.size());
  }

 private:
  void writeValue(simdjson::ondemand::value val, std::string& out) {
    switch (val.type().value()) {
      case simdjson::ondemand::json_type::object:
        writeObject(val.get_object().value(), out);
        break;
      case simdjson::ondemand::json_type::array:
        writeArray(val.get_array().value(), out);
        break;
      default:
        out += val.raw_json_token();
        break;
    }
  }

  void writeValue(simdjson::ondemand::document& doc, std::string& out) {
    switch (doc.type().value()) {
      case simdjson::ondemand::json_type::object:
        writeObject(doc.get_object().value(), out);
        break;
      case simdjson::ondemand::json_type::array:
        writeArray(doc.get_array().value(), out);
        break;
      default:
        out += doc.raw_json_token().value();
        break;
    }
  }

  void writeObject(simdjson::ondemand::object obj, std::string& out) {
    out += '{';
    bool first = true;
    for (auto field : obj) {
      simdjson::ondemand::value val = field.value();
      if (val.type().value() == simdjson::ondemand::json_type::null) {
        continue;  // Skip null fields
      }
      if (!first) {
        out += ',';
      }
      first = false;
      out += '"';
      out += field.escaped_key().value();
      out += '"';
      out += ':';
      writeValue(val, out);
    }
    out += '}';
  }

  void writeArray(simdjson::ondemand::array arr, std::string& out) {
    out += '[';
    bool first = true;
    for (simdjson::ondemand::value elem : arr) {
      if (!first) {
        out += ',';
      }
      first = false;
      writeValue(elem, out);
    }
    out += ']';
  }
};

}  // namespace

simdjson::simdjson_result<simdjson::ondemand::value> JsonParser::GetByIndex(
  simdjson::ondemand::array arr, int64_t relative_index) {
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

void JsonParser::PrepareJson(std::string_view json) {
  // TODO(codeworse):
  // https://github.com/simdjson/simdjson/blob/master/doc/performance.md#free-padding
  _padded_input = simdjson::padded_string{json};
}

simdjson::ondemand::document JsonParser::GetJsonDocument() {
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

void registerJsonFunctions(const std::string& prefix) {
  velox::registerFunction<PgJsonExtractIndex, velox::Json, velox::Json,
                          int64_t>({prefix + "json_extract_index"});
  velox::registerFunction<PgJsonExtractIndexText, velox::Varchar, velox::Json,
                          int64_t>({prefix + "json_extract_index_text"});
  velox::registerFunction<PgJsonExtractField, velox::Json, velox::Json,
                          velox::Varchar>({prefix + "json_extract_field"});
  velox::registerFunction<PgJsonExtractFieldText, velox::Varchar, velox::Json,
                          velox::Varchar>({prefix + "json_extract_field_text"});
  velox::registerFunction<PgJsonExtractPath, velox::Json, velox::Json,
                          velox::Array<velox::Varchar>>(
    {prefix + "json_extract_path"});
  velox::registerFunction<PgJsonExtractPath, velox::Json, velox::Json,
                          velox::Variadic<velox::Varchar>>(
    {prefix + "json_extract_path"});
  velox::registerFunction<PgJsonExtractPathText, velox::Varchar, velox::Json,
                          velox::Array<velox::Varchar>>(
    {prefix + "json_extract_path_text"});
  velox::registerFunction<PgJsonExtractPathText, velox::Varchar, velox::Json,
                          velox::Variadic<velox::Varchar>>(
    {prefix + "json_extract_path_text"});
  velox::registerFunction<PgJsonInFunction, velox::Json, velox::Varchar>(
    {prefix + "jsonin"});
  velox::registerFunction<PgJsonOutFunction, velox::Varchar, velox::Json>(
    {prefix + "jsonout"});
  velox::registerFunction<PgJsonTypeof, velox::Varchar, velox::Json>(
    {prefix + "json_typeof"});
  velox::registerFunction<PgJsonStripNulls, velox::Json, velox::Json>(
    {prefix + "json_strip_nulls"});
}

}  // namespace sdb::pg::functions
