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

#include "catalog/entry/subscription.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

struct SubscribedRelation {
  duckdb::idx_t subscription;
  duckdb::idx_t relation;
  char state;
  uint64_t lsn;
};

class PgSubscriptionRel final : public SystemTableScan<kPgSubscriptionRelSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Database, duckdb::CatalogType::SUBSCRIPTION_ENTRY, {}}};

  static constexpr auto kRelation = Shape<kSql, const SubscribedRelation>(
    Col<"srsubid">(&SubscribedRelation::subscription),
    Col<"srrelid">(&SubscribedRelation::relation),
    Col<"srsubstate">(&SubscribedRelation::state),
    Col<"srsublsn">([](const auto& row) {
      return row.lsn == 0 ? std::nullopt : std::optional<uint64_t>{row.lsn};
    }));

  void Row(const catalog::SubscriptionCatalogEntry& subscription) {
    for (const auto& relation : subscription.Relations()) {
      auto schema =
        Database().GetSchema(Transaction(), duckdb::Identifier{relation.schema},
                             duckdb::OnEntryNotFound::RETURN_NULL);
      if (!schema) {
        continue;
      }
      auto table =
        FindMember(Transaction(), *schema, duckdb::CatalogType::TABLE_ENTRY,
                   duckdb::Identifier{relation.table});
      if (!table) {
        continue;
      }
      Emit<kRelation>({.subscription = subscription.oid,
                       .relation = table->oid,
                       .state = static_cast<char>(relation.state),
                       .lsn = relation.lsn});
    }
  }
};

}  // namespace

SystemTable gPgSubscriptionRel = SystemTableOf<PgSubscriptionRel>();

}  // namespace sdb::pg
