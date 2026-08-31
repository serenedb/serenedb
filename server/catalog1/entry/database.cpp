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

#include "catalog1/entry/database.h"

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <utility>

namespace sdb::catalog {

duckdb::unique_ptr<duckdb::CreateInfo> CreateDatabaseInfo::Copy() const {
  auto result = duckdb::make_uniq<CreateDatabaseInfo>();
  CopyProperties(*result);
  result->public_schema_id = public_schema_id;
  return std::move(result);
}

std::string CreateDatabaseInfo::ToString() const {
  return absl::StrCat("CREATE DATABASE ",
                      duckdb::KeywordHelper::WriteOptionallyQuoted(
                        qualified_name.Name().GetIdentifierName()),
                      ";");
}

DatabaseCatalogEntry::DatabaseCatalogEntry(duckdb::Catalog& catalog,
                                           CreateDatabaseInfo& info)
  : duckdb::InCatalogEntry{duckdb::CatalogType::DATABASE_ENTRY, catalog,
                           info.GetQualifiedName().Name()},
    _public_schema_id{info.public_schema_id} {
  comment = info.comment;
  tags = info.tags;
}

duckdb::unique_ptr<duckdb::CreateInfo> DatabaseCatalogEntry::GetInfo() const {
  auto info = duckdb::make_uniq<CreateDatabaseInfo>();
  info->SetName(name);
  info->public_schema_id = _public_schema_id;
  info->comment = comment;
  info->tags = tags;
  return std::move(info);
}

duckdb::unique_ptr<duckdb::CatalogEntry> DatabaseCatalogEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  return duckdb::make_uniq<DatabaseCatalogEntry>(
    catalog, info->Cast<CreateDatabaseInfo>());
}

std::string DatabaseCatalogEntry::ToSQL() const {
  return GetInfo()->ToString();
}

}  // namespace sdb::catalog
