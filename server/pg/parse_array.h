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

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <string>
#include <string_view>

namespace sdb::pg {

// ParsePgTextArray: parse a PostgreSQL text-format 1D array literal.
//
// Format: {elem1,elem2,...}
//   - Elements may be quoted with double quotes (supporting \-escapes inside).
//   - Unquoted elements have leading/trailing whitespace trimmed.
//   - Unquoted "NULL" (case-insensitive, no backslash escapes) is a SQL NULL.
//
// Calls on_element(token, is_null) for each element:
//   - token: the element value (unescaped); empty and ignored when
//   is_null=true.
//   - is_null: true for SQL NULL elements.
//
// Calls on_error(msg) on malformed input. on_error must not return normally
// (it should throw). If it does return, parsing stops immediately.
template<typename OnElement, typename OnError>
void ParsePgTextArray(std::string_view input, OnElement&& on_element,
                      OnError&& on_error) {
  while (!input.empty() && absl::ascii_isspace(input.front())) {
    input.remove_prefix(1);
  }
  while (!input.empty() && absl::ascii_isspace(input.back())) {
    input.remove_suffix(1);
  }

  if (input.size() < 2 || input.front() != '{' || input.back() != '}') {
    on_error("malformed array literal: must be wrapped in {}");
    return;
  }
  input.remove_prefix(1);
  input.remove_suffix(1);

  size_t i = 0;
  while (i < input.size() && absl::ascii_isspace(input[i])) {
    ++i;
  }
  if (i == input.size()) {
    return;  // empty array
  }

  while (true) {
    while (i < input.size() && absl::ascii_isspace(input[i])) {
      ++i;
    }

    if (i >= input.size()) {
      on_error("malformed array literal: missing element after delimiter");
      return;
    }

    if (input[i] == '"') {
      // Quoted element: backslash-escape aware, preserves internal whitespace.
      ++i;
      std::string buf;
      while (i < input.size() && input[i] != '"') {
        if (input[i] == '\\' && i + 1 < input.size()) {
          buf += input[++i];
        } else {
          buf += input[i];
        }
        ++i;
      }
      if (i >= input.size()) {
        on_error("malformed array literal: unterminated quoted element");
        return;
      }
      ++i;  // closing '"'
      while (i < input.size() && absl::ascii_isspace(input[i])) {
        ++i;
      }
      on_element(std::string_view{buf}, false);
    } else {
      // Unquoted element: trim trailing whitespace, support backslash escapes.
      // Backslash escapes suppress the NULL check.
      bool has_escapes = false;
      std::string buf;
      size_t non_ws_len = 0;
      while (i < input.size() && input[i] != ',') {
        if (input[i] == '\\' && i + 1 < input.size()) {
          buf += input[++i];
          ++i;
          has_escapes = true;
          non_ws_len = buf.size();
        } else {
          if (!absl::ascii_isspace(input[i])) {
            non_ws_len = buf.size() + 1;
          }
          buf += input[i++];
        }
      }
      buf.resize(non_ws_len);

      // Case-insensitive NULL only for unescaped unquoted values.
      const bool is_null = !has_escapes && absl::EqualsIgnoreCase(buf, "NULL");
      on_element(std::string_view{buf}, is_null);
    }

    if (i < input.size()) {
      if (input[i] != ',') {
        on_error("malformed array literal: unexpected character after element");
        return;
      }
      ++i;  // skip comma
    } else {
      break;
    }
  }
}

}  // namespace sdb::pg
