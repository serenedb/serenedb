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

#include <duckdb.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>

#include "connector/duckdb_table_entry.h"

namespace sdb::connector {

class SereneDBSchemaEntry final : public duckdb::SchemaCatalogEntry {
 public:
  SereneDBSchemaEntry(duckdb::Catalog& catalog, duckdb::CreateSchemaInfo& info);

  ObjectId GetDatabaseId() const;

  void Scan(
    duckdb::ClientContext& context, duckdb::CatalogType type,
    const std::function<void(duckdb::CatalogEntry&)>& callback) override;
  void Scan(
    duckdb::CatalogType type,
    const std::function<void(duckdb::CatalogEntry&)>& callback) override;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateIndex(
    duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo& info,
    duckdb::TableCatalogEntry& table) override;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateFunction(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateFunctionInfo& info) override;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateTable(
    duckdb::CatalogTransaction transaction,
    duckdb::BoundCreateTableInfo& info) override;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateView(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateViewInfo& info) override;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateSequence(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateSequenceInfo& info) override;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateTableFunction(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateTableFunctionInfo& info) override;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateCopyFunction(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateCopyFunctionInfo& info) override;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreatePragmaFunction(
    duckdb::CatalogTransaction transaction,
    duckdb::CreatePragmaFunctionInfo& info) override;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateCollation(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateCollationInfo& info) override;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateType(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateTypeInfo& info) override;

  duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntry(
    duckdb::CatalogTransaction transaction,
    const duckdb::EntryLookupInfo& lookup_info) override;

  void DropEntry(duckdb::ClientContext& context,
                 duckdb::DropInfo& info) override;
  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::AlterInfo& info) override;

 private:
  // Keep table entries alive -- DuckDB returns raw pointers from LookupEntry
  duckdb::case_insensitive_map_t<duckdb::unique_ptr<SereneDBTableEntry>>
    _table_entries;
  duckdb::case_insensitive_map_t<duckdb::unique_ptr<duckdb::CreateTableInfo>>
    _table_infos;
};

}  // namespace sdb::connector
