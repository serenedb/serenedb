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

#include "pg/pg_catalog/pg_user_mapping.h"

#include <absl/strings/ascii.h>

#include <deque>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog/catalog.h"
#include "catalog/user_mapping.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/pg_catalog/make_options.h"

namespace sdb::pg {

namespace {}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgUserMapping>::GetTableData() {
  auto catalog = _config.CatalogSnapshot();
  const auto database_id = GetDatabaseId();

  std::vector<PgUserMapping> values;
  std::deque<std::string> option_storage;
  std::deque<std::vector<std::string_view>> option_spans;

  for (const auto& schema : catalog->GetSchemas(database_id)) {
    // Resolve umserver by name: foreign servers live in the same (db, schema)
    // as their mappings, so build a name -> oid lookup first.
    containers::FlatHashMap<std::string_view, uint64_t> server_oids;
    for (const auto& server :
         catalog->GetForeignServers(database_id, schema->GetName())) {
      server_oids.emplace(server->GetName(), server->GetId().id());
    }

    for (const auto& mapping :
         catalog->GetUserMappings(database_id, schema->GetName())) {
      uint64_t umserver = 0;
      if (auto it = server_oids.find(mapping->GetServerName());
          it != server_oids.end()) {
        umserver = it->second;
      }
      // PG uses 0 for PUBLIC; role oids are not resolvable here either.
      PgUserMapping row{
        .oid = mapping->GetId().id(),
        .umuser = 0,
        .umserver = umserver,
        .umoptions = MakeOptions(*mapping, option_storage, option_spans),
      };
      values.push_back(std::move(row));
    }
  }

  auto result = CreateColumns<PgUserMapping>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], /*null_mask=*/0, row, *catalog);
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
