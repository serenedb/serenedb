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

#include "pg/pg_catalog/pg_foreign_server.h"

#include <deque>
#include <string>
#include <string_view>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/duckdb_catalog_sets.h"
#include "catalog/foreign_server.h"
#include "catalog/identifiers/object_id.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgForeignServer::srvtype),
  GetIndex(&PgForeignServer::srvversion),
  GetIndex(&PgForeignServer::srvacl),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgForeignServer>::GetTableData() {
  const auto database_id = GetDatabaseId();

  // srvoptions is an Array<Text> spanning opt_views, whose Text elements point
  // into the owned "key=value" strings in opt_bytes; both outlive the WriteData
  // loop and neither may reallocate its elements (hence the deques). Rendered
  // verbatim (unlike PG's world-readable pg_foreign_server): our options may
  // carry credentials, so this whole catalog is superuser-only instead of
  // redacting values.
  std::deque<std::vector<std::string>> opt_bytes;
  std::deque<std::vector<Text>> opt_views;
  std::vector<PgForeignServer> values;

  catalog::VisitForeignServers(
    &_config.GetClientContext(), database_id,
    [&](const catalog::CreateForeignServerInfo& server,
        const catalog::Permissions& perm) {
      const auto& bytes = opt_bytes.emplace_back(server.GetStringOptions());
      const auto& views = opt_views.emplace_back(bytes.begin(), bytes.end());
      values.push_back(PgForeignServer{
        .oid = server.GetId().id(),
        .srvname = server.GetName(),
        .srvowner = perm.owner,
        .srvfdw = 0,
        .srvtype = {},
        .srvversion = {},
        .srvacl = {catalog::AclView{perm.acl}},
        .srvoptions = views,
      });
    });

  auto result = CreateColumns<PgForeignServer>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row,
              *sdb::auth::RolesOf(&_config.GetClientContext()));
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
