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

#include <duckdb/common/constants.hpp>
#include <string_view>

#include "pg/catalog/oids.h"

namespace sdb::pg {

struct SystemView {
  std::string_view schema;
  std::string_view name;
  duckdb::idx_t oid;
  bool superuser_only;
  std::string_view sql;
};

inline constexpr SystemView kExternalViews[] = {
#include "pg/catalog/views/system_views.gen.inc"
  {"pg_catalog", "pg_stat_progress_create_table_as", kMinSystem + 403, false,
   R"(SELECT
          S.pid AS pid, S.datid AS datid, S.datname AS datname,
          S.relid AS relid,
          S.command AS command,
          S.phase AS phase,
          S.tuples_processed AS tuples_processed,
          S.bytes_processed AS bytes_processed,
          S.tuples_total AS tuples_total
      FROM sdb_progress S WHERE S.command = 'CREATE TABLE AS')"},
};

}  // namespace sdb::pg
