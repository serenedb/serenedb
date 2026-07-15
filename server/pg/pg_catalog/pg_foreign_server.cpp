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

#include <absl/strings/ascii.h>

#include <string>
#include <string_view>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/foreign_server.h"
#include "catalog/identifiers/object_id.h"
#include "pg/pg_catalog/fwd.h"
#include "catalog/options.h"

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
  auto catalog = _config.CatalogSnapshot();
  const auto database_id = GetDatabaseId();

  std::vector<PgForeignServer> values;

  for (const auto& server : catalog->GetForeignServers(database_id)) {
    PgForeignServer row{
      .oid = server->GetId().id(),
      .srvname = server->GetName(),
      .srvowner = server->GetOwner().id(),
      .srvfdw = 0,
      .srvtype = {},
      .srvversion = {},
      .srvacl = {},
      // Redacted: this table is world-readable like PG's, but unlike PG our
      // server options may carry credentials (the eager attach takes them),
      // so a secret VALUE never renders here.
      .srvoptions = server->GetOptions().ToStrings(/*redact_secrets=*/true),
    };
    values.push_back(std::move(row));
  }

  auto result = CreateColumns<PgForeignServer>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, *catalog);
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
