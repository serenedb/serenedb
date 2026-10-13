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

class PgTablespace final : public SystemTableScan<kPgTablespaceSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr duckdb::idx_t kRows = 10;

  static constexpr std::tuple kSources{
    ArraySource<SystemObject>{&LoadStatic<kSystemTablespaces>, {}, {}}};

  static constexpr auto kTablespace = Shape<kSql, const SystemObject>(
    Col<"oid">(&SystemObject::oid), Col<"spcname">(&SystemObject::name),
    Col<"spcowner">([](const auto&) { return kRootUser; }));

  void Row(const SystemObject& tablespace) { Emit<kTablespace>(tablespace); }
};

}  // namespace

SystemTable gPgTablespace = SystemTableOf<PgTablespace>();

}  // namespace sdb::pg
