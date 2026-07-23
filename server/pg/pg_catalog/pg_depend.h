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

// https://www.postgresql.org/docs/18/catalog-pg-depend.html
// NOLINTBEGIN
struct PgDepend {
  static constexpr uint64_t kId = 117;
  static constexpr std::string_view kName = "pg_depend";

  enum class Deptype : char {
    Normal = 'n',
    Auto = 'a',
    Internal = 'i',
    PartitionPri = 'P',
    PartitionSec = 'S',
    Extension = 'e',
    AutoExtension = 'x',
  };

  Oid classid;
  Oid objid;
  int32_t objsubid;
  Oid refclassid;
  Oid refobjid;
  int32_t refobjsubid;
  Deptype deptype;
};
// NOLINTEND

template<>
catalog::MaterializedData SystemTableSnapshot<PgDepend>::GetTableData();

}  // namespace sdb::pg
