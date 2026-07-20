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

#include "pg/pg_catalog/pg_policy.h"

#include "app/app_server.h"
#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/policy.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/system_catalog.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&PgPolicy::oid),
  GetIndex(&PgPolicy::polname),
  GetIndex(&PgPolicy::polrelid),
  GetIndex(&PgPolicy::polcmd),
  GetIndex(&PgPolicy::polpermissive),
  GetIndex(&PgPolicy::polroles),
});

PgPolicy::Polcmd ToPolcmd(catalog::persistence::PolicyCommand cmd) {
  switch (cmd) {
    case catalog::persistence::PolicyCommand::Select:
      return PgPolicy::Polcmd::Select;
    case catalog::persistence::PolicyCommand::Insert:
      return PgPolicy::Polcmd::Insert;
    case catalog::persistence::PolicyCommand::Update:
      return PgPolicy::Polcmd::Update;
    case catalog::persistence::PolicyCommand::Delete:
      return PgPolicy::Polcmd::Delete;
    case catalog::persistence::PolicyCommand::All:
      return PgPolicy::Polcmd::All;
  }
  return PgPolicy::Polcmd::All;
}

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgPolicy>::GetTableData() {
  auto catalog = _config.CatalogSnapshot();

  std::vector<PgPolicy> values;
  // Stable backing storage for the polroles spans referenced by `values`.
  std::vector<std::vector<Oid>> roles_storage;

  for (const auto& schema : catalog->GetSchemas(GetDatabaseId())) {
    SDB_ASSERT(schema);
    for (const auto& table :
         catalog->GetTables(GetDatabaseId(), schema->GetName())) {
      for (auto policy_id : catalog->PolicyIds(table->GetId())) {
        auto policy = catalog->GetObject<catalog::Policy>(policy_id);
        if (!policy) {
          continue;
        }
        std::vector<Oid> roles;
        // PUBLIC is rendered as the singleton role oid 0 (PG convention).
        if (policy->AppliesToPublic()) {
          roles.push_back(Oid{0});
        } else {
          for (auto role_id : policy->Roles()) {
            roles.push_back(Oid{role_id.id()});
          }
        }
        roles_storage.push_back(std::move(roles));
        values.push_back({
          .oid = policy->GetId().id(),
          .polname = policy->GetName(),
          .polrelid = table->GetId().id(),
          .polcmd = ToPolcmd(policy->Command()),
          .polpermissive = policy->Permissive(),
          .polroles = roles_storage.back(),
        });
      }
    }
  }

  auto result = CreateColumns<PgPolicy>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, *_config.CatalogSnapshot());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
