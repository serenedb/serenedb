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

namespace sdb::pg {
namespace {

constexpr std::string_view kFullNames = "SereneDB keeps longer names in full.";

// clang-format off
constexpr SystemCell kRows[] = {
  "0", "MAXIMUM DRIVER CONNECTIONS", std::nullopt, std::nullopt,
  "1", "MAXIMUM CONCURRENT ACTIVITIES", "0", std::nullopt,
  "30", "MAXIMUM COLUMN NAME LENGTH", "63", kFullNames,
  "31", "MAXIMUM CURSOR NAME LENGTH", "63", kFullNames,
  "32", "MAXIMUM SCHEMA NAME LENGTH", "63", kFullNames,
  "34", "MAXIMUM CATALOG NAME LENGTH", "63", kFullNames,
  "35", "MAXIMUM TABLE NAME LENGTH", "63", kFullNames,
  "97", "MAXIMUM COLUMNS IN GROUP BY", "0", std::nullopt,
  "99", "MAXIMUM COLUMNS IN ORDER BY", "0", std::nullopt,
  "100", "MAXIMUM COLUMNS IN SELECT", "1664", std::nullopt,
  "101", "MAXIMUM COLUMNS IN TABLE", "1600", std::nullopt,
  "106", "MAXIMUM TABLES IN SELECT", "0", std::nullopt,
  "107", "MAXIMUM USER NAME LENGTH", "63", kFullNames,
  "10005", "MAXIMUM IDENTIFIER LENGTH", "63", kFullNames,
  "20000", "MAXIMUM STATEMENT OCTETS", "0", std::nullopt,
  "20001", "MAXIMUM STATEMENT OCTETS DATA", "0", std::nullopt,
  "20002", "MAXIMUM STATEMENT OCTETS SCHEMA", "0", std::nullopt,
  "25000", "MAXIMUM CURRENT DEFAULT TRANSFORM GROUP LENGTH", std::nullopt, std::nullopt,
  "25001", "MAXIMUM CURRENT TRANSFORM GROUP LENGTH", std::nullopt, std::nullopt,
  "25002", "MAXIMUM CURRENT PATH LENGTH", "0", std::nullopt,
  "25003", "MAXIMUM CURRENT ROLE LENGTH", std::nullopt, std::nullopt,
  "25004", "MAXIMUM SESSION USER LENGTH", "63", kFullNames,
  "25005", "MAXIMUM SYSTEM USER LENGTH", "63", kFullNames,
};
// clang-format on

}  // namespace

SystemTable gInfoSqlSizing{kInfoSqlSizingSql, kRows};

}  // namespace sdb::pg
