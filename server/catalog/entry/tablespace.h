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

#pragma once

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/parser/parsed_data/create_tablespace_info.hpp>
#include <memory>
#include <string>
#include <string_view>

#include "catalog/database_directory.h"

namespace sdb::catalog {

inline constexpr std::string_view kDefaultTablespaceSetting =
  "default_tablespace";

void RequireUnreservedTablespaceName(std::string_view name);

class TablespaceCatalogEntry final : public duckdb::InCatalogEntry {
 public:
  static constexpr duckdb::CatalogType Type =
    duckdb::CatalogType::TABLESPACE_ENTRY;
  static constexpr const char* Name = "tablespace";

  TablespaceCatalogEntry(duckdb::Catalog& catalog,
                         duckdb::CreateTablespaceInfo& info,
                         std::shared_ptr<TablespaceRoot> root);

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const final;
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const final;
  std::string ToSQL() const final { return GetInfo()->ToString(); }
  void Rollback(duckdb::CatalogEntry& prev_entry) final;
  void OnDrop() final;

  const std::string& Location() const noexcept { return _location; }
  const std::shared_ptr<TablespaceRoot>& Root() const noexcept { return _root; }

 private:
  std::string _location;
  std::shared_ptr<TablespaceRoot> _root;
};

}  // namespace sdb::catalog
