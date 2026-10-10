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

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "server/utils/build.h"

namespace sdb::pg {
namespace {

// clang-format off
constexpr SystemCell kRows[] = {
  "10003", "CATALOG NAME", std::nullopt, "Y", std::nullopt,
  "10004", "COLLATING SEQUENCE", std::nullopt, "C.UTF-8", std::nullopt,
  "13", "SERVER NAME", std::nullopt, "", std::nullopt,
  "17", "DBMS NAME", std::nullopt, "SereneDB", std::nullopt,
  "18", "DBMS VERSION", std::nullopt, SERENEDB_VERSION, std::nullopt,
  "2", "DATA SOURCE NAME", std::nullopt, "", std::nullopt,
  "23", "CURSOR COMMIT BEHAVIOR", "1", std::nullopt, "close cursors and retain prepared statements",
  "26", "DEFAULT TRANSACTION ISOLATION", "4", std::nullopt, "REPEATABLE READ; user-settable",
  "28", "IDENTIFIER CASE", "3", std::nullopt, "stored in mixed case - case sensitive",
  "46", "TRANSACTION CAPABLE", "2", std::nullopt, "both DML and DDL",
  "85", "NULL COLLATION", "4", std::nullopt, "nulls sort at the end",
  "94", "SPECIAL CHARACTERS", std::nullopt, "", "all non-ASCII characters allowed",
};
// clang-format on

}  // namespace

SystemTable gInfoSqlImplementationInfo{kInfoSqlImplementationInfoSql, kRows};

}  // namespace sdb::pg
