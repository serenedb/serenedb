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
#include "pg/types/char.h"
#include "pg/types/function.h"
#include "pg/types/geometric.h"
#include "pg/types/mac.h"
#include "pg/types/money.h"
#include "pg/types/pg_lsn.h"
#include "pg/types/range.h"
#include "pg/types/reg.h"
#include "pg/types/tid.h"
#include "pg/types/vector.h"
#include "pg/types/xml.h"

namespace sdb::pg {

void RegisterTypes(duckdb::DatabaseInstance& db) {
  auto& casts = duckdb::DBConfig::GetConfig(db).GetCastFunctions();
  Functions functions;
  RegisterRegTypes(casts);
  RegisterPgLsn(casts, functions);
  RegisterBytea(casts);
  RegisterChar(casts);
  RegisterVectors(casts);
  RegisterTid(casts);
  RegisterMacaddr(casts, functions);
  RegisterMoney(casts, functions);
  RegisterXml(casts, functions);
  RegisterRange(casts, functions);
  RegisterGeometric(casts, functions);
  RegisterFunctions(db, std::move(functions));
}

}  // namespace sdb::pg
