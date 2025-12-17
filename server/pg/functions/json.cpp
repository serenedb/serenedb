#include "pg/functions/json.h"

namespace sdb::pg {

namespace {

void AssertQuoted(std::string_view str) {
  if (!JsonParser::CheckQuoted(str)) {
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

}  // namespace

void JsonParser::PrepareJson(std::string_view json) {
  AssertQuoted(json);
  size_t length =
    velox::unescapeSizeForJsonFunctions(json.data() + 1, json.size() - 2, true);
  _padded_input = simdjson::padded_string(length);
  velox::unescapeForJsonFunctions(json.data() + 1, json.size() - 2,
                                  _padded_input.data(), true);
  _doc = GetJsonDocument();
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

simdjson::simdjson_result<simdjson::ondemand::value> JsonParser::ExtractByIndex(
  int64_t index) {
  simdjson::ondemand::value value;
  if (auto ec = _doc.get_value().get(value)) {
    return {ec};
  }
  if (value.type() != simdjson::ondemand::json_type::array) {
    return {simdjson::INCORRECT_TYPE};
  }
  auto arr = value.get_array();
  return GetByIndex(std::move(arr), index);
}

simdjson::simdjson_result<simdjson::ondemand::value> JsonParser::ExtractByField(
  std::string_view field) {
  simdjson::ondemand::value value;
  if (auto ec = _doc.get_value().get(value)) {
    return {ec};
  }
  if (value.type() != simdjson::ondemand::json_type::object) {
    return {simdjson::INCORRECT_TYPE};
  }
  auto res = value.find_field(field);
  return res;
}

simdjson::simdjson_result<simdjson::ondemand::value> JsonParser::Extract(
  std::span<const std::string> path) {
  simdjson::ondemand::value value;
  if (auto ec = _doc.get_value().get(value)) {
    return {ec};
  }
  for (size_t i = 0; i < path.size(); ++i) {
    if (!path[i].data()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("JSON path element at position ", i + 1, " is null"));
    }
    const auto& key_value = path[i];
    auto key = std::string_view{key_value};

    if (value.type() == simdjson::ondemand::json_type::array) {
      int64_t index;
      if (!absl::SimpleAtoi(key, &index)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("Invalid JSON path element for array: \"", key, "\""));
      }
      simdjson::ondemand::array arr;
      if (auto ec = value.get_array().get(arr)) {
        return {ec};
      }
      if (auto ec = GetByIndex(arr, index).get(value)) {
        return {ec};
      }
    } else if (value.type() == simdjson::ondemand::json_type::object) {
      auto res = value.find_field(key);
      if (auto ec = res.get(value)) {
        return {ec};
      }
    } else {
      return {simdjson::INCORRECT_TYPE};
    }
  }
  return value;
}
}  // namespace sdb::pg
