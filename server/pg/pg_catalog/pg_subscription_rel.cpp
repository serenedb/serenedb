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

#include "pg/pg_catalog/pg_subscription_rel.h"

#include <deque>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/subscription.h"
#include "pg/commands/create_subscription.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgSubscriptionRel::srsublsn),
});

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgSubscriptionRel>::GetTableData() {
  std::deque<std::string> lsns;
  std::vector<PgSubscriptionRel> values;
  std::vector<uint64_t> null_masks;
  auto& database = GetDatabase().Cast<catalog::SereneDBCatalog>();
  const auto catalog_name = database.GetName();
  database.GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY)
    .Scan(database.GetCatalogTransaction(_context),
          [&](duckdb::CatalogEntry& entry) {
            const auto& subscription =
              entry.Cast<catalog::SubscriptionCatalogEntry>();
            for (const auto& relation : subscription.Relations()) {
              auto table = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
                _context,
                duckdb::QualifiedName::FromCatalogSchema(
                  catalog_name, {duckdb::Identifier{relation.schema}},
                  duckdb::Identifier{relation.table}),
                duckdb::OnEntryNotFound::RETURN_NULL);
              if (!table) {
                continue;
              }
              const auto& lsn = lsns.emplace_back(
                relation.lsn == 0 ? std::string{} : FormatLsn(relation.lsn));
              values.push_back(PgSubscriptionRel{
                .srsubid = subscription.oid,
                .srrelid = table->oid,
                .srsubstate =
                  static_cast<PgSubscriptionRel::Srsubstate>(relation.state),
                .srsublsn = lsn,
              });
              null_masks.push_back(relation.lsn == 0 ? kNullMask : 0);
            }
          });
  auto result = CreateColumns<PgSubscriptionRel>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], null_masks[row], row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
