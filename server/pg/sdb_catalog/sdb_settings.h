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

#include "pg/system_table.h"

namespace sdb::pg {

// pg_settings-shaped view over the abseil flag registry: one row per flag.
// Abseil flags are process-start only, so context is always "postmaster" and
// pending_restart is always false; columns with no abseil equivalent are NULL.
// NOLINTBEGIN
struct SdbSettings {
  static constexpr uint64_t kId = 999994;  // TODO(codeworse): assign proper OID
  static constexpr std::string_view kName = "sdb_settings";

  Text name;
  Text setting;
  Text unit;
  Text category;
  Text short_desc;
  Text context;
  Text vartype;
  Text source;
  Text min_val;
  Text max_val;
  Text boot_val;
  Text reset_val;
  bool pending_restart;
};
// NOLINTEND

template<>
catalog::MaterializedData SystemTableSnapshot<SdbSettings>::GetTableData();

}  // namespace sdb::pg
