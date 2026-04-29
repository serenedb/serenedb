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

#include "sql_utils.h"

#include "pg/sql_collector.h"

namespace sdb::pg {

std::string_view ToPgObjectTypeName(duckdb::CatalogType t) noexcept {
  switch (t) {
    using enum duckdb::CatalogType;
    case TABLE_ENTRY:
      return "table";
    case SCHEMA_ENTRY:
      return "schema";
    case VIEW_ENTRY:
      return "view";
    case INDEX_ENTRY:
      return "index";
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      return "function";
    case TYPE_ENTRY:
      return "type";
    default:
      return "object";
  }
}

Objects::ObjectName ParseObjectName(std::string_view name,
                                    std::string_view default_schema) {
  const auto pos = name.find('.');
  auto schema_name =
    pos == std::string_view::npos ? default_schema : name.substr(0, pos);
  auto object_name =
    pos == std::string_view::npos ? name : name.substr(pos + 1);
  return {.schema = schema_name, .relation = object_name};
}

}  // namespace sdb::pg
