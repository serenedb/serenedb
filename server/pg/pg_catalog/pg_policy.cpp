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
  GetIndex(&PgPolicy::polqual),
  GetIndex(&PgPolicy::polwithcheck),
});

constexpr uint64_t kQualNull = MaskFromNulls({GetIndex(&PgPolicy::polqual)});
constexpr uint64_t kCheckNull =
  MaskFromNulls({GetIndex(&PgPolicy::polwithcheck)});

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
    for (const auto& table :
         catalog->GetTables(GetDatabaseId(), schema->GetName())) {
      for (auto policy_id : catalog->PolicyIds(table->GetId())) {
        auto policy = catalog->GetObject<catalog::Policy>(policy_id);
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

        // The stored USING/CHECK text is already parenthesized, matching how
        // pg_get_expr renders a policy expression, e.g. "(v > 0)", so it is
        // never empty when present -- empty means the clause is absent and the
        // row's null mask marks the column NULL.
        Text qual;
        Text with_check;
        if (policy->HasUsing()) {
          qual = policy->UsingText();
        }
        if (policy->HasCheck()) {
          with_check = policy->CheckText();
        }
        values.push_back({
          .oid = policy->GetId().id(),
          .polname = policy->GetName(),
          .polrelid = table->GetId().id(),
          .polcmd = ToPolcmd(policy->Command()),
          .polpermissive = policy->Permissive(),
          .polroles = roles_storage.back(),
          .polqual = qual,
          .polwithcheck = with_check,
        });
      }
    }
  }

  auto result = CreateColumns<PgPolicy>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    const auto& value = values[row];
    const uint64_t mask = kNullMask |
                          (value.polqual.empty() ? kQualNull : 0) |
                          (value.polwithcheck.empty() ? kCheckNull : 0);
    WriteData(result, value, mask, row, *_config.CatalogSnapshot());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
