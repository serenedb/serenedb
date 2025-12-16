#pragma once

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

namespace {
simdjson::ondemand::document GetJsonDocument(
  simdjson::padded_string_view json, simdjson::ondemand::parser& parser) {
  simdjson::ondemand::document doc;
  auto ec = parser.iterate(json).get(doc);
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
}  // namespace

template<typename T>
struct PgJsonExtractIndex {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(out_type<velox::Varchar>& result, const arg_type<velox::Json>& json,
            const arg_type<int64_t>& index) {
    simdjson::ondemand::parser parser;
    if (json.data()[0] != '"' && json.data()[json.size() - 1] != '"') {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("JSON input must be a valid JSON string"));
    }
    simdjson::padded_string padded_input(json.data() + 1, json.size() - 2);
    auto doc = GetJsonDocument(padded_input, parser);
    auto value = doc.get_value();
    if (value.error() || doc.type() != simdjson::ondemand::json_type::array) {
      return;
    }
    size_t size;
    auto ec = value.count_elements().get(size);
    SDB_ASSERT(ec == simdjson::SUCCESS);

    if (index < 0) {
      if (static_cast<size_t>(-index) > size) {
        return;
      }
      value = value.at(size + index);
    } else {
      value = value.at(index);
    }
    SDB_ASSERT(!value.error());
    result += "\"";
    result += simdjson::to_json_string(value).value();
    result += "\"";
  }
};

template<typename T>
struct PgJsonExtractPath {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(out_type<velox::Varchar>& result, const arg_type<velox::Json>& json,
            const arg_type<velox::Array<velox::Varchar>>& path) {
    simdjson::ondemand::parser parser;
    if (json.data()[0] != '"' && json.data()[json.size() - 1] != '"') {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("JSON input must be a valid JSON string"));
    }
    size_t length = velox::unescapeSizeForJsonFunctions(json.data() + 1,
                                                        json.size() - 2, true);

    simdjson::padded_string padded_input(length);
    velox::unescapeForJsonFunctions(json.data() + 1, json.size() - 2,
                                    padded_input.data(), true);
    if (path.size() == 0) {
      result = std::string_view{json.data(), json.size()};
      return;
    }
    auto doc = GetJsonDocument(padded_input, parser);
    auto value = doc.get_value();
    for (size_t i = 0; i < path.size(); ++i) {
      if (value.error()) {
        return;
      }
      const auto& key = path[i];
      if (!key.has_value()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("JSON path element at position ", i + 1, " is null"));
      }
      const auto& key_value = key.value();

      if (doc.type() == simdjson::ondemand::json_type::array) {
        int64_t index;
        try {
          index = folly::to<int64_t>(std::string_view{key_value});
        } catch (const folly::ConversionError&) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                          ERR_MSG("Invalid JSON path element for array: \"",
                                  std::string_view{key_value}, "\""));
        }
        value = value.at(index);
      } else if (doc.type() == simdjson::ondemand::json_type::object) {
        value = value.find_field(std::string_view{key_value});
      }
    }
    if (value.error()) {
      return;
    }

    result += simdjson::to_json_string(value).value();
  }
};

}  // namespace sdb::pg
