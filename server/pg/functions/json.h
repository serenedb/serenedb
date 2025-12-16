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
void CheckQuoted(std::string_view str) {
  if (str.empty() || str.front() != '"' || str.back() != '"') {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("JSON input must be a valid JSON string"));
  }
}

auto GetByIndex(simdjson::ondemand::array arr, int64_t index) {
  size_t size;
  auto ec = arr.count_elements().get(size);
  SDB_ASSERT(ec == simdjson::SUCCESS);

  if (index < 0) {
    if (static_cast<size_t>(-index) > size) {
      return simdjson::simdjson_result<simdjson::ondemand::value>(
        simdjson::OUT_OF_BOUNDS);
    }
    return arr.at(size + index);
  }

  return arr.at(index);
}

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

auto PrepareJson(std::string_view json) {
  simdjson::ondemand::parser parser;
  CheckQuoted(json);
  size_t length =
    velox::unescapeSizeForJsonFunctions(json.data() + 1, json.size() - 2, true);
  simdjson::padded_string padded_input(length);
  velox::unescapeForJsonFunctions(json.data() + 1, json.size() - 2,
                                  padded_input.data(), true);
  return std::tuple(std::move(parser), std::move(padded_input));
}
}  // namespace

template<typename T>
struct PgJsonExtractText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  bool call(  // NOLINT
    out_type<velox::Varchar>& result, const arg_type<velox::Json>& json,
    const arg_type<int64_t>& index) {
    auto [parser, padded_input] = PrepareJson({json.data(), json.size()});
    auto doc = GetJsonDocument(padded_input, parser);
    simdjson::ondemand::array arr;
    if (doc.get_array().get(arr)) {
      return false;
    }
    simdjson::ondemand::value value;
    if (GetByIndex(arr, index).get(value)) {
      return false;
    }
    result = simdjson::to_json_string(value).value();
    return true;
  }

  bool call(out_type<velox::Varchar>& result, const arg_type<velox::Json>& json,
            const arg_type<velox::Varchar>& field) {
    auto [parser, padded_input] = PrepareJson({json.data(), json.size()});
    auto doc = GetJsonDocument(padded_input, parser);
    simdjson::ondemand::value value;
    if (doc.get_value().get(value)) {
      return false;
    }
    auto res = value.find_field({field.data(), field.size()});
    if (res.get(value)) {
      return false;
    }
    result = simdjson::to_json_string(value).value();
    return true;
  }

  bool call(out_type<velox::Varchar>& result, const arg_type<velox::Json>& json,
            const arg_type<velox::Array<velox::Varchar>>& path) {
    auto [parser, padded_input] = PrepareJson({json.data(), json.size()});
    if (path.size() == 0) {
      result = std::string_view{json.data(), json.size()};
      return false;
    }
    auto doc = GetJsonDocument(padded_input, parser);
    simdjson::ondemand::value value;
    if (doc.get_value().get(value)) {
      return false;
    }
    for (size_t i = 0; i < path.size(); ++i) {
      if (!path[i].has_value()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("JSON path element at position ", i + 1, " is null"));
      }
      const auto& key_value = path[i].value();
      std::string_view key = std::string_view{key_value};

      if (value.type() == simdjson::ondemand::json_type::array) {
        int64_t index;
        if (!absl::SimpleAtoi(key, &index)) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG("Invalid JSON path element for array: \"", key, "\""));
        }
        if (GetByIndex(value, index).get(value)) {
          return false;
        }
      } else if (value.type() == simdjson::ondemand::json_type::object) {
        auto res = value.find_field(key);
        if (res.get(value)) {
          return false;
        }
      } else {
        return false;
      }
    }
    result = simdjson::to_json_string(value).value();
    return true;
  }
};

}  // namespace sdb::pg
