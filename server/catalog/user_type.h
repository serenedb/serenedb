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

#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::catalog {

inline constexpr std::string kPgSqlTypeOidProp = "sdb_oid";

// The array type PG pairs with a scalar one. Allocated together with the
// scalar, one below it, so neither has to be written down twice.
constexpr ObjectId TypeArrayOid(ObjectId scalar) noexcept {
  return ObjectId{scalar.id() - 1};
}

// Stamps the type's own name and stable id into its extension info, so a
// LogicalType read back from a column, a parameter or a wire description still
// names the catalog row it came from.
duckdb::LogicalType StampUserType(const duckdb::LogicalType& type,
                                  std::string_view name, ObjectId id);

}  // namespace sdb::catalog
