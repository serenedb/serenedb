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

#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

SystemRows<ReplicationOrigin> LoadOrigins(SystemScan& scan) {
  return ReplicationOrigins(*scan.Context().db);
}

class PgReplicationOrigin final
  : public SystemTableScan<kPgReplicationOriginSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<ReplicationOrigin>{&LoadOrigins, {}, {}}};

  static constexpr auto kOrigin = Shape<kSql, const ReplicationOrigin>(
    Col<"roident">(&ReplicationOrigin::id),
    Col<"roname">(&ReplicationOrigin::name));

  void Row(const ReplicationOrigin& origin) { Emit<kOrigin>(origin); }
};

}  // namespace

SystemTable gPgReplicationOrigin = SystemTableOf<PgReplicationOrigin>();

}  // namespace sdb::pg
