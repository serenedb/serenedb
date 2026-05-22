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
#include <string_view>

#include "basics/string_utils.h"
#include "catalog/table_options.h"

namespace sdb::connector {

inline std::string EncodeJsonPointer(std::span<const std::string> path) {
  std::string out;
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
  return out;
}

inline void MakeColumnFieldName(catalog::Column::Id column_id,
                                std::string_view json_pointer,
                                std::string& out) {
  basics::StrResize(out, sizeof(column_id));
  absl::big_endian::Store(out.data(), column_id);
  out.append(json_pointer);
}

}  // namespace sdb::connector
