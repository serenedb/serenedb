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
#include <duckdb/catalog/catalog_search_path.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/static_strings.hpp>

#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "pg/catalog/utils/oids.h"

namespace sdb::pg {

duckdb::optional_ptr<catalog::SereneDBCatalog> SessionCatalog(
  duckdb::ClientContext& context) {
  if (!connector::GetSereneDBContextPtr(context)) {
    return nullptr;
  }
  auto& catalog = duckdb::Catalog::GetCatalog(
    context, duckdb::DatabaseManager::GetDefaultDatabase(context));
  if (catalog.GetCatalogType() != catalog::SereneDBCatalog::kStorageType) {
    return nullptr;
  }
  return catalog.Cast<catalog::SereneDBCatalog>();
}

Session MakeSession(duckdb::ClientContext* context) {
  Session session{
    .context = context,
    .database = context ? SessionCatalog(*context).get() : nullptr,
    .transaction = std::nullopt,
    .retriever = std::nullopt,
    .search_path = {},
    .reg_out = {},
    .reg_out_texts = {},
    .reg_out_recent = {}};
  if (!context) {
    return session;
  }
  session.retriever.emplace(*context);
  if (session.database) {
    session.transaction.emplace(
      session.database->GetCatalogTransaction(*context));
  }
  const auto add = [&](std::string_view name) {
    if (absl::c_any_of(session.search_path, [&](const SessionSchema& schema) {
          return schema.name == name;
        })) {
      return;
    }
    session.search_path.emplace_back(SessionSchema{
      .name = std::string{name},
      .entry = session.database
                 ? session.database->GetSchema(
                     *session.transaction, duckdb::Identifier{name},
                     duckdb::OnEntryNotFound::RETURN_NULL)
                 : nullptr,
      .system = FindSystemObject(kSystemNamespaces, name) != nullptr});
  };
  const auto& search_path =
    *duckdb::ClientData::Get(*context).catalog_search_path;
  const auto database =
    duckdb::DatabaseManager::TryGetDefaultDatabase(*context);
  if (absl::c_none_of(search_path.GetResolvedSetPaths(),
                      [](const duckdb::CatalogSearchEntry& entry) {
                        return entry.GetSchema() ==
                               irs::StaticStrings::kPgCatalogSchema;
                      })) {
    add(irs::StaticStrings::kPgCatalogSchema);
    session.retriever->SetSearchPath({duckdb::CatalogSearchEntry{
      database, duckdb::Identifier{irs::StaticStrings::kPgCatalogSchema}}});
  }
  for (const auto& entry : search_path.Get()) {
    const auto& catalog = entry.GetCatalog();
    if (catalog.empty() || catalog == database.GetIdentifierName()) {
      add(entry.GetSchema().GetIdentifierName());
    }
  }
  return session;
}

}  // namespace sdb::pg
