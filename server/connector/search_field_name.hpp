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

#include <cstring>
#include <string>

#include "basics/string_utils.h"
#include "catalog/table_options.h"

namespace sdb::connector {

inline void MakeColumnFieldName(catalog::Column::Id column_id,
                                std::string& out) {
  basics::StrResize(out, sizeof(column_id));
  absl::big_endian::Store(out.data(), column_id);
}

inline void MakeExpressionFieldName(std::string_view serialized_expr,
                                    std::string& out) {
  basics::StrResize(out, serialized_expr.size());
  if (!serialized_expr.empty()) {
    std::memcpy(out.data(), serialized_expr.data(), serialized_expr.size());
  }
}

}  // namespace sdb::connector
