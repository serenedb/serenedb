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

#include <span>

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

struct Wrapper {
  const SystemObject& object;
  std::optional<std::span<const duckdb::AclItem>> acl;
};

class PgForeignDataWrapper final
  : public SystemTableScan<kPgForeignDataWrapperSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr duckdb::idx_t kRows = std::size(kForeignDataWrappers);

  static constexpr std::tuple kSources{
    ArraySource<SystemObject>{&LoadStatic<kForeignDataWrappers>, {}, {}}};

  static constexpr auto kWrapper = Shape<kSql, const Wrapper>(
    Col<"oid">([](const auto& row) { return row.object.oid; }),
    Col<"fdwname">([](const auto& row) { return row.object.name; }),
    Col<"fdwowner">([](const auto&) { return kRootUser; }),
    Col<"fdwacl">([](const auto& row) { return row.acl; }));

  void Row(const SystemObject& wrapper) {
    const auto entry = Database().LookupEntry(
      Transaction(), duckdb::CatalogType::FOREIGN_DATA_WRAPPER_ENTRY,
      duckdb::Identifier{wrapper.name});
    Emit<kWrapper>({wrapper, entry ? AclOf(entry->permissions) : std::nullopt});
  }
};

}  // namespace

SystemTable gPgForeignDataWrapper = SystemTableOf<PgForeignDataWrapper>();

}  // namespace sdb::pg
