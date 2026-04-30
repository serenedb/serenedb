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

#include <span>
#include <string>

#include "basics/endian.h"
#include "basics/string_utils.h"
#include "catalog/table_options.h"

namespace sdb::connector {

// Builds the iresearch field name for a column, optionally qualified by a
// JSON path.
//
// Layout: [8 bytes BE column_id] + "/" + escape(path[0]) + "/" + escape(path[1])
//                                + ... + (caller-applied mangle byte)
//
// The path-segment portion is a valid RFC 6901 JSON Pointer, so the sink can
// pass `name.substr(sizeof(column_id), name.size() - sizeof(column_id) - 1)`
// directly to `simdjson::ondemand::document::at_pointer` without rebuilding
// any string. Each key has `~` escaped as `~0` and `/` as `~1`, so keys
// containing those characters round-trip cleanly.
//
// Caller is expected to apply the appropriate `search::mangling::Mangle*` on
// the result before using it as an iresearch field name.
inline void MakeColumnFieldName(catalog::Column::Id column_id,
                                std::span<const std::string> path,
                                std::string& out) {
  basics::StrResize(out, sizeof(column_id));
  absl::big_endian::Store(out.data(), column_id);
  for (const auto& key : path) {
    out.push_back('/');
    for (char c : key) {
      if (c == '~') {
        out.append("~0");
      } else if (c == '/') {
        out.append("~1");
      } else {
        out.push_back(c);
      }
    }
  }
}

// Returns the JSON Pointer view of a field name produced by
// MakeColumnFieldName + a 1-byte type-mangle suffix. The view is valid as
// long as the underlying field-name string is.
inline std::string_view JsonPointerOf(std::string_view field_name) {
  constexpr size_t kPrefix = sizeof(catalog::Column::Id);
  constexpr size_t kMangle = 1;
  if (field_name.size() < kPrefix + kMangle) {
    return {};
  }
  return field_name.substr(kPrefix, field_name.size() - kPrefix - kMangle);
}

}  // namespace sdb::connector
