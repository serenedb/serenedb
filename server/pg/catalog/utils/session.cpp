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

#include "pg/catalog/utils/session.h"

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry_retriever.hpp>
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/main/client_data.hpp>
#include <iresearch/utils/static_strings.hpp>

#include "catalog/catalog.h"
#include "pg/catalog/oids.h"
#include "pg/catalog/utils/lookup.h"

namespace sdb::pg {

Session MakeSession(
  duckdb::ClientContext* context, duckdb::optional_idx caller,
  duckdb::optional_ptr<const duckdb::vector<duckdb::CatalogSearchEntry>>
    body_search_path) {
  Session session{
    .context = context,
    .role = context ? duckdb::Role(*context, caller) : duckdb::RoleAccess{},
    .database =
      context ? catalog::DefaultSereneDBCatalog(*context).get() : nullptr,
    .transaction = std::nullopt,
    .search_path = {},
    .temporary = std::nullopt,
    .reg_out = {},
    .reg_out_texts = {},
    .reg_out_recent = {},
    .usable_schemas = {}};
  if (!context) {
    return session;
  }
  if (session.database) {
    session.transaction.emplace(
      session.database->GetCatalogTransaction(*context));
  }
  const auto added = [&](std::string_view name) {
    return absl::c_any_of(
      session.search_path,
      [&](const SessionSchema& schema) { return schema.name == name; });
  };
  if (session.database) {
    duckdb::CatalogEntryRetriever retriever{*context, session.role};
    if (body_search_path) {
      retriever.ReplaceSearchPath(*body_search_path);
    }
    retriever.VisitSearchSchemas([&](const duckdb::Identifier& identifier,
                                     duckdb::SchemaCatalogEntry& schema) {
      const auto& name = identifier.GetIdentifierName();
      if (!added(name)) {
        session.search_path.emplace_back(
          name, &schema,
          static_cast<bool>(FindSystemObject(kSystemNamespaces, name)));
      }
    });
  }
  if (!added(irs::StaticStrings::kPgCatalogSchema)) {
    session.search_path.emplace(
      session.search_path.begin(),
      std::string{irs::StaticStrings::kPgCatalogSchema},
      session.database
        ? session.database->GetSchema(
            *session.transaction,
            duckdb::Identifier{irs::StaticStrings::kPgCatalogSchema},
            duckdb::OnEntryNotFound::RETURN_NULL)
        : nullptr,
      true);
  }
  auto& temporary =
    duckdb::ClientData::Get(*context).temporary_objects->GetCatalog();
  const auto transaction = temporary.GetCatalogTransaction(*context);
  auto schema = temporary.GetSchema(transaction, *temporary.GetDefaultSchema(),
                                    duckdb::OnEntryNotFound::RETURN_NULL);
  SDB_ASSERT(schema);
  for (const auto type : kRelationSets) {
    schema->Cast<catalog::Schema>().Set(type).Scan(
      transaction, [&](duckdb::CatalogEntry& entry) {
        if (!entry.internal && !session.temporary) {
          session.temporary.emplace(transaction, *schema);
        }
      });
  }
  return session;
}

}  // namespace sdb::pg
