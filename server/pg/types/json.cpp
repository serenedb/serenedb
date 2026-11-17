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

#include "pg/types/json.h"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <cstring>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

namespace sdb::pg {
namespace {

namespace ondemand = simdjson::ondemand;

size_t SkipDigits(std::string_view text, size_t at) {
  while (at < text.size() && absl::ascii_isdigit(text[at])) {
    ++at;
  }
  return at;
}

bool JsonNumber(std::string_view text) {
  text = absl::StripTrailingAsciiWhitespace(text);
  size_t at = text.starts_with('-') ? 1 : 0;
  if (const auto word = text.substr(at);
      absl::EqualsIgnoreCase(word, "nan") ||
      absl::EqualsIgnoreCase(word, "inf") ||
      absl::EqualsIgnoreCase(word, "infinity")) {
    return true;
  }
  if (at < text.size() && text[at] == '0') {
    ++at;
  } else if (const auto end = SkipDigits(text, at); end != at) {
    at = end;
  } else {
    return false;
  }
  if (at < text.size() && text[at] == '.') {
    const auto end = SkipDigits(text, at + 1);
    if (end == at + 1) {
      return false;
    }
    at = end;
  }
  if (at < text.size() && (text[at] == 'e' || text[at] == 'E')) {
    ++at;
    if (at < text.size() && (text[at] == '+' || text[at] == '-')) {
      ++at;
    }
    const auto end = SkipDigits(text, at);
    if (end == at) {
      return false;
    }
    at = end;
  }
  return at == text.size();
}

simdjson::error_code Walk(ondemand::value value);

simdjson::error_code WalkArray(ondemand::array array) {
  for (auto element : array) {
    ondemand::value value;
    if (const auto error = element.get(value)) {
      return error;
    }
    if (const auto error = Walk(value)) {
      return error;
    }
  }
  return simdjson::SUCCESS;
}

simdjson::error_code WalkObject(ondemand::object object) {
  for (auto field : object) {
    std::string_view key;
    if (const auto error = field.unescaped_key(true).get(key)) {
      return error;
    }
    ondemand::value value;
    if (const auto error = field.value().get(value)) {
      return error;
    }
    if (const auto error = Walk(value)) {
      return error;
    }
  }
  return simdjson::SUCCESS;
}

template<typename Value>
simdjson::error_code Null(Value& value) {
  bool null = false;
  if (const auto error = value.is_null().get(null)) {
    return error;
  }
  return null ? simdjson::SUCCESS : simdjson::N_ATOM_ERROR;
}

simdjson::error_code Walk(ondemand::value value) {
  ondemand::json_type type;
  if (const auto error = value.type().get(type)) {
    return error;
  }
  switch (type) {
    case ondemand::json_type::array: {
      ondemand::array array;
      if (const auto error = value.get_array().get(array)) {
        return error;
      }
      return WalkArray(array);
    }
    case ondemand::json_type::object: {
      ondemand::object object;
      if (const auto error = value.get_object().get(object)) {
        return error;
      }
      return WalkObject(object);
    }
    case ondemand::json_type::string: {
      std::string_view text;
      return value.get_string(true).get(text);
    }
    case ondemand::json_type::number:
      return JsonNumber(value.raw_json_token()) ? simdjson::SUCCESS
                                                : simdjson::NUMBER_ERROR;
    case ondemand::json_type::boolean: {
      bool flag = false;
      return value.get_bool().get(flag);
    }
    case ondemand::json_type::null:
      return Null(value);
    default:
      return simdjson::TAPE_ERROR;
  }
}

simdjson::error_code CheckDocument(ondemand::document& document,
                                   std::string_view json) {
  ondemand::json_type type;
  if (const auto error = document.type().get(type)) {
    return error;
  }
  switch (type) {
    case ondemand::json_type::array:
    case ondemand::json_type::object: {
      ondemand::value value;
      if (const auto error = document.get_value().get(value)) {
        return error;
      }
      if (const auto error = Walk(value)) {
        return error;
      }
      return document.at_end() ? simdjson::SUCCESS : simdjson::TRAILING_CONTENT;
    }
    case ondemand::json_type::string: {
      std::string_view text;
      return document.get_string(true).get(text);
    }
    case ondemand::json_type::number: {
      std::string_view token;
      if (const auto error = document.raw_json_token().get(token)) {
        return error;
      }
      if (!JsonNumber(token)) {
        return simdjson::NUMBER_ERROR;
      }
      const auto end =
        static_cast<size_t>(token.data() - json.data()) + token.size();
      return absl::StripLeadingAsciiWhitespace(json.substr(end)).empty()
               ? simdjson::SUCCESS
               : simdjson::TRAILING_CONTENT;
    }
    case ondemand::json_type::boolean: {
      bool flag = false;
      return document.get_bool().get(flag);
    }
    case ondemand::json_type::null:
      return Null(document);
    default:
      return simdjson::TAPE_ERROR;
  }
}

}  // namespace

JsonInput::JsonInput() {
  _parser.allow_trailing_commas = true;
  _parser.allow_inf_and_nan = true;
}

simdjson::error_code JsonInput::Check(std::string_view text) {
  if (const auto size = text.size() + simdjson::SIMDJSON_PADDING;
      _buffer.size() < size) {
    _buffer.resize(size);
  }
  std::memcpy(_buffer.data(), text.data(), text.size());
  ondemand::document document;
  if (const auto error = _parser
                           .iterate(simdjson::padded_string_view{
                             _buffer.data(), text.size(), _buffer.size()})
                           .get(document)) {
    return error;
  }
  return CheckDocument(document, {_buffer.data(), text.size()});
}

void ThrowInvalidJson(simdjson::error_code error) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                  ERR_MSG("invalid input syntax for type json"),
                  ERR_DETAIL(simdjson::error_message(error)));
}

}  // namespace sdb::pg
