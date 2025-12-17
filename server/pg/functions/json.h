#pragma once

#include <absl/strings/numbers.h>
#include <simdjson.h>
#include <velox/functions/Macros.h>
#include <velox/functions/prestosql/json/JsonStringUtil.h>
#include <velox/functions/prestosql/types/JsonType.h>
#include <velox/type/SimpleFunctionApi.h>

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
  void PrepareJson(std::string_view json);
  simdjson::simdjson_result<simdjson::ondemand::value> Extract(
    std::span<const std::string> path);
  simdjson::simdjson_result<simdjson::ondemand::value> ExtractByIndex(
    int64_t index);
  simdjson::simdjson_result<simdjson::ondemand::value> ExtractByField(
    std::string_view field);
  static bool CheckQuoted(std::string_view str) {
    return !str.empty() && str.front() == '"' && str.back() == '"';
  }

 private:
  simdjson::ondemand::document GetJsonDocument();

  simdjson::ondemand::parser _parser;
  simdjson::padded_string _padded_input;
  simdjson::ondemand::document _doc;
};

template<typename T>
struct PgJsonExtractPathText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  bool call(  // NOLINT
    out_type<velox::Varchar>& result, const arg_type<velox::Json>& json,
    const arg_type<int64_t>& index) {
    _parser.PrepareJson({json.data(), json.size()});
    simdjson::ondemand::value value;
    if (_parser.ExtractByIndex(index).get(value)) {
      return false;
    }
    if (value.type() == simdjson::ondemand::json_type::string) {
      result = value.get_string().value();
    } else {
      result = simdjson::to_json_string(value).value();
    }
    return true;
  }

  // call for ->> operator (field text)
  bool call(out_type<velox::Varchar>& result, const arg_type<velox::Json>& json,
            const arg_type<velox::Varchar>& field) {
    _parser.PrepareJson({json.data(), json.size()});
    simdjson::ondemand::value value;
    if (_parser.ExtractByField({field.data(), field.size()}).get(value)) {
      return false;
    }
    if (value.type() == simdjson::ondemand::json_type::string) {
      result = value.get_string().value();
    } else {
      result = simdjson::to_json_string(value).value();
    }
    return true;
  }

  // call for #>> operator (path text)
  bool call(out_type<velox::Varchar>& result, const arg_type<velox::Json>& json,
            const arg_type<velox::Array<velox::Varchar>>& path) {
    _parser.PrepareJson({json.data(), json.size()});
    std::vector<std::string> keys;
    keys.reserve(path.size());
    for (size_t i = 0; i < path.size(); ++i) {
      if (!path[i]) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("JSON path element at position ", i + 1, " is null"));
      }
      const auto& key_value = *path[i];
      keys.emplace_back(std::string_view{key_value});
    }

    simdjson::ondemand::value value;
    if (_parser.Extract(keys).get(value)) {
      return false;
    }
    if (value.type() == simdjson::ondemand::json_type::string) {
      result = value.get_string().value();
    } else {
      result = simdjson::to_json_string(value).value();
    }
    return true;
  }

 private:
  // void StoreTextValue(out_type<velox::Varchar>& result,
  //                     simdjson::ondemand::value& value) {
  //   if (value.type() != simdjson::ondemand::json_type::string) {
  //     result = simdjson::to_json_string(value).value();
  //     return;
  //   }
  //   std::string_view str = value.get_string().value();
  //   if (!JsonParser::CheckQuoted(str)) {
  //     THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
  //                     ERR_MSG("JSON string value must be quoted: ", str));
  //   }
  //   result.resize(velox::unescapeSizeForJsonCast(str.data(), str.size()));
  //   velox::unescapeForJsonCast(str.data(), str.size(), result.data());
  // }

  JsonParser _parser;
};

template<typename T>
struct PgJsonExtractPath {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // call for -> operator (index json)
  bool call(  // NOLINT
    out_type<velox::Json>& result, const arg_type<velox::Json>& json,
    const arg_type<int64_t>& index) {
    _parser.PrepareJson({json.data(), json.size()});
    simdjson::ondemand::value value;
    if (_parser.ExtractByIndex(index).get(value)) {
      return false;
    }
    result = simdjson::to_json_string(value).value();
    return true;
  }

  // call for -> operator (field json)
  bool call(out_type<velox::Json>& result, const arg_type<velox::Json>& json,
            const arg_type<velox::Varchar>& field) {
    _parser.PrepareJson({json.data(), json.size()});
    simdjson::ondemand::value value;
    if (_parser.ExtractByField({field.data(), field.size()}).get(value)) {
      return false;
    }
    result = simdjson::to_json_string(value).value();
    return true;
  }

  // call for #>> operator (path json)
  bool call(out_type<velox::Json>& result, const arg_type<velox::Json>& json,
            const arg_type<velox::Array<velox::Varchar>>& path) {
    _parser.PrepareJson({json.data(), json.size()});
    std::vector<std::string> keys;
    keys.reserve(path.size());
    for (size_t i = 0; i < path.size(); ++i) {
      if (!path[i]) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("JSON path element at position ", i + 1, " is null"));
      }
      const auto key_value = *path[i];
      keys.emplace_back(key_value.data(), key_value.size());
    }
    simdjson::ondemand::value value;
    if (_parser.Extract(keys).get(value)) {
      return false;
    }
    result = simdjson::to_json_string(value).value();
    return true;
  }

 private:
  JsonParser _parser;
};

}  // namespace sdb::pg
