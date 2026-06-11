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

// https://www.postgresql.org/docs/18/catalog-pg-extension.html
// NOLINTBEGIN
struct PgExtension {
  static constexpr uint64_t kId = 121;
  static constexpr std::string_view kName = "pg_extension";

  Oid oid;
  Name extname;
  Oid extowner;
  Oid extnamespace;
  bool extrelocatable;
  Text extversion;
  Array<Oid> extconfig;
  Array<Text> extcondition;
};
// NOLINTEND

template<>
catalog::MaterializedData SystemTableSnapshot<PgExtension>::GetTableData();

}  // namespace sdb::pg
