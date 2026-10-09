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

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

struct Tablespace {
  duckdb::idx_t oid;
  std::string_view name;
};

constexpr std::array kTablespaces{
  Tablespace{kPgDefaultTablespace, "pg_default"},
  Tablespace{kPgGlobalTablespace, "pg_global"},
};

class PgTablespace final : public SystemTableScan<kPgTablespaceSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<Tablespace>{&LoadStatic<kTablespaces>, {}}};

  static constexpr auto kTablespace = Shape<kSql, const Tablespace>(
    Col<"oid">(&Tablespace::oid), Col<"spcname">(&Tablespace::name),
    Col<"spcowner">([](const auto&) { return kRootUser; }));

  void Row(const Tablespace& tablespace) { Emit<kTablespace>(tablespace); }
};

}  // namespace

SystemTable gPgTablespace = SystemTableOf<PgTablespace>();

}  // namespace sdb::pg
