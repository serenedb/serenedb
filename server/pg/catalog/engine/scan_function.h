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

#include <duckdb/function/table_function.hpp>

namespace duckdb {

class DatabaseInstance;
class DuckCatalog;

}  // namespace duckdb
namespace sdb::catalog {

class SystemTableEntry;

}  // namespace sdb::catalog
namespace sdb::pg {

class SystemTable;

}  // namespace sdb::pg
namespace sdb::connector {

struct SystemTableBindData final : duckdb::FunctionData {
  SystemTableBindData(const pg::SystemTable& table,
                      duckdb::DuckCatalog& catalog,
                      catalog::SystemTableEntry* entry)
    : table{table}, catalog{catalog}, entry{entry} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<SystemTableBindData>(*this);
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    const auto& data = other.Cast<SystemTableBindData>();
    return &table == &data.table && &catalog == &data.catalog &&
           entry == data.entry;
  }

  const pg::SystemTable& table;
  duckdb::DuckCatalog& catalog;
  catalog::SystemTableEntry* entry;
};

duckdb::TableFunction BindSystemTableScan(
  catalog::SystemTableEntry& entry,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data);

void RegisterSystemTableScanFunction(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
