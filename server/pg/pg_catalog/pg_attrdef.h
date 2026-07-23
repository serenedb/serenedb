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

#include "pg/system_table.h"

namespace sdb::pg {

// https://www.postgresql.org/docs/18/catalog-pg-attrdef.html
// NOLINTBEGIN
struct PgAttrdef {
  static constexpr uint64_t kId = 105;
  static constexpr std::string_view kName = "pg_attrdef";

  Oid oid;
  Oid adrelid;
  int16_t adnum;
  PgNodeTree adbin;
};
// NOLINTEND

inline constexpr uint64_t kAttrdefBit = uint64_t{1} << 59;

inline constexpr Oid AttrdefOid(uint64_t column_oid) {
  return Oid{column_oid | kAttrdefBit};
}

template<>
catalog::MaterializedData SystemTableSnapshot<PgAttrdef>::GetTableData();

}  // namespace sdb::pg
