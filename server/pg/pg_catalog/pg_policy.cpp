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

#include "pg/pg_catalog/pg_policy.h"

#include <deque>
#include <duckdb/catalog/catalog_entry/policy_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/row_security.hpp>
#include <string>
#include <vector>

#include "pg/pg_catalog/fwd.h"
#include "pg/system_catalog.h"

namespace sdb::pg {
namespace {

constexpr uint64_t Bit(size_t index) { return uint64_t{1} << index; }

PgPolicy::Polcmd PolicyCommand(duckdb::PolicyCommand command) {
  switch (command) {
    case duckdb::PolicyCommand::ALL:
      return PgPolicy::Polcmd::All;
    case duckdb::PolicyCommand::SELECT:
      return PgPolicy::Polcmd::Select;
    case duckdb::PolicyCommand::INSERT:
      return PgPolicy::Polcmd::Insert;
    case duckdb::PolicyCommand::UPDATE:
      return PgPolicy::Polcmd::Update;
    case duckdb::PolicyCommand::DELETE:
      return PgPolicy::Polcmd::Delete;
  }
  return PgPolicy::Polcmd::All;
}

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgPolicy>::GetTableData() {
  std::vector<PgPolicy> values;
  std::vector<uint64_t> masks;
  std::deque<std::string> text_storage;
  std::deque<std::vector<Oid>> roles_storage;

  const auto add_policies = [&](duckdb::StandardEntry& relation) {
    duckdb::RowSecurity::Get(relation)->ScanPolicies(
      relation.ParentCatalog().GetCatalogTransaction(_context),
      [&](duckdb::PolicyCatalogEntry& policy) {
        uint64_t mask = 0;
        auto& roles = roles_storage.emplace_back();
        for (const auto role : policy.roles) {
          roles.emplace_back(role);
        }
        auto& name = text_storage.emplace_back(policy.name.GetIdentifierName());
        auto row = PgPolicy{
          .oid = policy.oid,
          .polname = name,
          .polrelid = relation.oid,
          .polcmd = PolicyCommand(policy.command),
          .polpermissive = policy.permissive,
          .polroles = roles,
        };
        if (policy.using_expr) {
          row.polqual =
            text_storage.emplace_back(policy.using_expr->ToString());
        } else {
          mask |= Bit(GetIndex(&PgPolicy::polqual));
        }
        if (policy.check_expr) {
          row.polwithcheck =
            text_storage.emplace_back(policy.check_expr->ToString());
        } else {
          mask |= Bit(GetIndex(&PgPolicy::polwithcheck));
        }
        values.push_back(row);
        masks.push_back(mask);
      });
  };
  VisitEntries<duckdb::TableCatalogEntry>(_context, GetDatabase(),
                                          add_policies);
  VisitEntries<duckdb::ViewCatalogEntry>(_context, GetDatabase(), add_policies);

  auto result = CreateColumns<PgPolicy>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], masks[row], row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
