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

#include <absl/strings/ascii.h>

#include <string>
#include <string_view>

namespace sdb::pg {

// ParsePgTextRecord: parse a PostgreSQL text-format composite (record) literal.
//
// Format: (field1,field2,...)
//   - A field can be quoted with double quotes; inside quotes, "" -> " and
//     \X -> X (backslash escapes the next character).
//   - An empty unquoted field is SQL NULL. An empty quoted field ("") is the
//     empty string.
//   - Outside quotes, \X also escapes the next character.
//
// Calls on_field(token, is_null) for each field:
//   - token: the field value (unescaped); empty and ignored when is_null=true.
//   - is_null: true for an empty unquoted field.
//
// Calls on_error(msg) on malformed input. on_error must not return normally
// (it should throw). If it does return, parsing stops immediately.
template<typename OnField, typename OnError>
void ParsePgTextRecord(std::string_view input, OnField&& on_field,
                       OnError&& on_error) {
  while (!input.empty() && absl::ascii_isspace(input.front())) {
    input.remove_prefix(1);
  }
  while (!input.empty() && absl::ascii_isspace(input.back())) {
    input.remove_suffix(1);
  }

  if (input.size() < 2 || input.front() != '(' || input.back() != ')') {
    on_error("malformed record literal: must be wrapped in ()");
    return;
  }
  input.remove_prefix(1);
  input.remove_suffix(1);

  // PG accepts () for a zero-field record. For a record type that has fields,
  // this case will fall out as a field-count mismatch in the caller.
  if (input.empty()) {
    return;
  }

  size_t i = 0;
  while (true) {
    std::string buf;
    bool is_null = false;

    if (i < input.size() && input[i] == '"') {
      // Quoted field: "" -> ", \X -> X. Closing quote ends the field.
      ++i;
      while (i < input.size()) {
        if (input[i] == '"') {
          if (i + 1 < input.size() && input[i + 1] == '"') {
            buf += '"';
            i += 2;
          } else {
            ++i;  // closing '"'
            break;
          }
        } else if (input[i] == '\\' && i + 1 < input.size()) {
          buf += input[++i];
          ++i;
        } else {
          buf += input[i++];
        }
      }
    } else {
      // Unquoted field: ends at ','. Empty -> NULL. Backslash escapes.
      while (i < input.size() && input[i] != ',') {
        if (input[i] == '\\' && i + 1 < input.size()) {
          buf += input[++i];
          ++i;
        } else {
          buf += input[i++];
        }
      }
      if (buf.empty()) {
        is_null = true;
      }
    }

    on_field(std::string_view{buf}, is_null);

    if (i >= input.size()) {
      return;
    }
    if (input[i] != ',') {
      on_error("malformed record literal: unexpected character after field");
      return;
    }
    ++i;  // skip comma
  }
}

}  // namespace sdb::pg
