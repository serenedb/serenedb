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

// clang-format off
constexpr SystemCell kRows[] = {
  "1", "Framework (SQL/Framework)", "NO", std::nullopt, "",
  "10", "Object Language Bindings (SQL/OLB)", "NO", std::nullopt, "",
  "11", "Information and Definition Schema (SQL/Schemata)", "NO", std::nullopt, "",
  "13", "Routines and Types Using the Java Programming Language (SQL/JRT)", "NO", std::nullopt, "",
  "14", "XML-Related Specifications (SQL/XML)", "NO", std::nullopt, "",
  "15", "Multi-Dimensional Arrays (SQL/MDA)", "NO", std::nullopt, "",
  "16", "Property Graph Queries (SQL/PGQ)", "NO", std::nullopt, "",
  "2", "Foundation (SQL/Foundation)", "NO", std::nullopt, "",
  "3", "Call-Level Interface (SQL/CLI)", "NO", std::nullopt, "",
  "4", "Persistent Stored Modules (SQL/PSM)", "NO", std::nullopt, "",
  "9", "Management of External Data (SQL/MED)", "NO", std::nullopt, "",
};
// clang-format on

}  // namespace

SystemTable gInfoSqlParts{kInfoSqlPartsSql, kRows};

}  // namespace sdb::pg
