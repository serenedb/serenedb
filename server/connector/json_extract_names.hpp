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

#include <absl/strings/str_cat.h>

#include <duckdb/common/types/value.hpp>
#include <string>
#include <string_view>
#include <vector>

namespace sdb::connector {

// Recognises the names of every registered JSON-extract scalar function in
// this codebase (operator forms `->` / `->>`, plus the spelled-out aliases
// kept for PG and DuckDB compatibility -- see server/connector/functions/
// json.cpp). The two predicates partition the set by leaf return type:
//   * IsJsonExtractString  -> leaf is VARCHAR (`->>`, json_extract_field_text,
//                                              json_extract_index_text)
//   * IsJsonExtractJson    -> leaf is JSON    (`->` , json_extract,
//                                              json_extract_field,
//                                              json_extract_index)
// They are disjoint; together they cover every name we accept.
//
// Both _field variants take a VARCHAR key (object lookup) and both _index
// variants take a BIGINT key (array lookup). The operator forms `->` / `->>`
// are polymorphic and accept either kind of key.

inline bool IsJsonExtractString(std::string_view name) noexcept {
  return name == "->>" || name == "json_extract_field_text" ||
         name == "json_extract_index_text";
}

inline bool IsJsonExtractJson(std::string_view name) noexcept {
  return name == "->" || name == "json_extract" ||
         name == "json_extract_field" || name == "json_extract_index";
}

inline bool IsJsonExtract(std::string_view name) noexcept {
  return IsJsonExtractString(name) || IsJsonExtractJson(name);
}

// Stringifies one key constant from a `->` / `->>` chain into a JSON Pointer
// path segment. Returns true on success and appends to `out_path`. Accepts
// VARCHAR (object key) and integer types (array index, stringified). Returns
// false for any other type. Caller must have already verified that `key` is
// non-null. Shared by the bound-tree and parser-tree path lifters.
inline bool AppendJsonPathKey(const duckdb::Value& key,
                              std::vector<std::string>& out_path) {
  switch (key.type().id()) {
    case duckdb::LogicalTypeId::VARCHAR:
      out_path.emplace_back(key.GetValue<std::string>());
      return true;
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
      // Integer/array-index key like `content->0`: stringify so the path
      // segment becomes "0", which `simdjson::at_pointer` interprets as
      // an array index when the parent is an array.
      out_path.emplace_back(absl::StrCat(key.GetValue<int64_t>()));
      return true;
    default:
      return false;
  }
}

}  // namespace sdb::connector
