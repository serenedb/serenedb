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

#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

struct SqlPart {
  std::string_view id;
  std::string_view name;
};

constexpr std::array kParts{
  SqlPart{"1", "Framework (SQL/Framework)"},
  SqlPart{"10", "Object Language Bindings (SQL/OLB)"},
  SqlPart{"11", "Information and Definition Schema (SQL/Schemata)"},
  SqlPart{"13",
          "Routines and Types Using the Java Programming Language (SQL/JRT)"},
  SqlPart{"14", "XML-Related Specifications (SQL/XML)"},
  SqlPart{"15", "Multi-Dimensional Arrays (SQL/MDA)"},
  SqlPart{"16", "Property Graph Queries (SQL/PGQ)"},
  SqlPart{"2", "Foundation (SQL/Foundation)"},
  SqlPart{"3", "Call-Level Interface (SQL/CLI)"},
  SqlPart{"4", "Persistent Stored Modules (SQL/PSM)"},
  SqlPart{"9", "Management of External Data (SQL/MED)"},
};

constexpr auto kPart = Shape<kInfoSqlPartsSql, const SqlPart>(
  Col<"feature_id">(&SqlPart::id), Col<"feature_name">(&SqlPart::name),
  Col<"is_supported">([](const auto&) { return std::string_view{"NO"}; }),
  Col<"comments">([](const auto&) { return std::string_view{}; }));

}  // namespace

SystemTable gInfoSqlParts = SystemTableOf<StaticTable<kPart, kParts>>();

}  // namespace sdb::pg
