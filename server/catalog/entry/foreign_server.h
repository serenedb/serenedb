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
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/parser/parsed_data/create_foreign_server_info.hpp>
#include <string>

namespace sdb::catalog {

using ServerOptions = duckdb::case_insensitive_map_t<std::string>;

class ForeignServerCatalogEntry final : public duckdb::InCatalogEntry {
 public:
  static constexpr duckdb::CatalogType Type =
    duckdb::CatalogType::FOREIGN_SERVER_ENTRY;
  static constexpr const char* Name = "foreign server";

  ForeignServerCatalogEntry(duckdb::Catalog& catalog,
                            duckdb::CreateForeignServerInfo& info);

  const std::string& ServerType() const noexcept { return _server_type; }
  const std::string& Version() const noexcept { return _version; }
  const std::string& FdwName() const noexcept { return _fdw_name; }
  const ServerOptions& Options() const noexcept { return _options; }

  duckdb::unique_ptr<duckdb::CatalogEntry> Copy(
    duckdb::ClientContext& context) const override;
  duckdb::unique_ptr<duckdb::CreateInfo> GetInfo() const override;
  std::string ToSQL() const override;

 private:
  std::string _server_type;
  std::string _version;
  std::string _fdw_name;
  ServerOptions _options;
};

}  // namespace sdb::catalog
