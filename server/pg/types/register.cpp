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

#include "pg/types/register.h"

#include <duckdb/main/config.hpp>

#include "pg/types/bytea.h"
#include "pg/types/inet.h"
#include "pg/types/mac.h"
#include "pg/types/pg_lsn.h"
#include "pg/types/reg.h"

namespace sdb::pg {

void RegisterTypes(duckdb::DatabaseInstance& db) {
  auto& casts = duckdb::DBConfig::GetConfig(db).GetCastFunctions();
  RegisterRegTypes(casts);
  RegisterPgLsn(casts);
  RegisterInet(casts);
  RegisterBytea(casts);
  RegisterMacaddr(db);
}

}  // namespace sdb::pg
