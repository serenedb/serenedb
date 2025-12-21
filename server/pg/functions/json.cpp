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

namespace sdb::pg {
namespace {

void AssertQuoted(std::string_view str) {
  if (!JsonParser::CheckQuoted(str)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("JSON input must be a valid JSON string"));
  }
}

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
  // TODO(codeworse): erase unquote after erasing quotes in velox Json type
  AssertQuoted(json);
  size_t length =
    velox::unescapeSizeForJsonFunctions(json.data() + 1, json.size() - 2, true);
  _padded_input = simdjson::padded_string(length);
  velox::unescapeForJsonFunctions(json.data() + 1, json.size() - 2,
                                  _padded_input.data(), true);
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

}  // namespace sdb::pg
