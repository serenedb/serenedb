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

#include <string_view>

namespace sdb::connector {

// Recognises the names of every registered JSON-extract scalar function in
// this codebase (operator forms `->` / `->>`, plus the spelled-out aliases
// kept for PG and DuckDB compatibility -- see server/connector/functions/
// json.cpp). The two predicates partition the set by leaf return type:
//   * IsJsonExtractString  -> leaf is VARCHAR (`->>`, json_extract_field_text)
//   * IsJsonExtractJson    -> leaf is JSON    (`->` , json_extract,
//                                              json_extract_field)
// They are disjoint; together they cover every name we accept.

inline bool IsJsonExtractString(std::string_view name) noexcept {
  return name == "->>" || name == "json_extract_field_text";
}

inline bool IsJsonExtractJson(std::string_view name) noexcept {
  return name == "->" || name == "json_extract" || name == "json_extract_field";
}

inline bool IsJsonExtract(std::string_view name) noexcept {
  return IsJsonExtractString(name) || IsJsonExtractJson(name);
}

}  // namespace sdb::connector
