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

#include <absl/functional/function_ref.h>

#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/standard_entry.hpp>
#include <duckdb/common/enums/on_create_conflict.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <functional>
#include <memory>

#include "catalog/entry_set.h"

namespace sdb::catalog {

struct SchemaSets {
  SchemaSets(bool case_sensitive, OidIndex& oids);

  EntrySet tables;
  EntrySet indexes;
  EntrySet table_functions;
  EntrySet copy_functions;
  EntrySet pragma_functions;
  EntrySet functions;
  EntrySet sequences;
  EntrySet collations;
  EntrySet coordinate_systems;
  EntrySet types;
  EntrySet tokenizers;
  EntrySet jobs;
  EntrySet schemas;
};

class Schema final : public duckdb::SchemaCatalogEntry {
 public:
  Schema(duckdb::Catalog& catalog, duckdb::CreateSchemaInfo& info,
         duckdb::optional_ptr<duckdb::SchemaCatalogEntry> parent = nullptr,
         std::shared_ptr<SchemaSets> sets = {},
         duckdb::shared_ptr<duckdb::SchemaInfo> schema_info = nullptr);

  duckdb::unique_ptr<duckdb::CatalogEntry> AlterEntry(
    duckdb::CatalogTransaction transaction, duckdb::AlterInfo& info) final;

  void Scan(duckdb::ClientContext& context, duckdb::CatalogType type,
            const std::function<void(duckdb::CatalogEntry&)>& callback) final;
  void Scan(duckdb::CatalogTransaction transaction, duckdb::CatalogType type,
            const std::function<void(duckdb::CatalogEntry&)>& callback) final;
  void Scan(duckdb::CatalogType type,
            const std::function<void(duckdb::CatalogEntry&)>& callback) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateIndex(
    duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo& info,
    duckdb::TableCatalogEntry& table) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateIndex(
    duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo& info,
    duckdb::CatalogEntry& relation) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateFunction(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateFunctionInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateTable(
    duckdb::CatalogTransaction transaction,
    duckdb::BoundCreateTableInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateView(
    duckdb::CatalogTransaction transaction, duckdb::CreateViewInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateSequence(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateSequenceInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateTableFunction(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateTableFunctionInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateCopyFunction(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateCopyFunctionInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreatePragmaFunction(
    duckdb::CatalogTransaction transaction,
    duckdb::CreatePragmaFunctionInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateCollation(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateCollationInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateCoordinateSystem(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateCoordinateSystemInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateType(
    duckdb::CatalogTransaction transaction, duckdb::CreateTypeInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateTokenizer(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateTokenizerInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateJob(
    duckdb::CatalogTransaction transaction, duckdb::CreateJobInfo& info) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> LookupEntry(
    duckdb::CatalogTransaction transaction,
    const duckdb::EntryLookupInfo& lookup_info) final;

  void DropEntry(duckdb::ClientContext& context, duckdb::DropInfo& info) final;

  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::AlterInfo& info) final;

  EntrySet& Set(duckdb::CatalogType type);

  duckdb::optional_ptr<duckdb::CatalogEntry> Add(
    duckdb::CatalogTransaction transaction,
    duckdb::unique_ptr<duckdb::CatalogEntry> entry,
    duckdb::OnCreateConflict on_conflict);

  void Drop(duckdb::CatalogTransaction transaction,
            duckdb::CatalogEntry& entry);

  void Clear(duckdb::CatalogTransaction transaction);

  void CreateDefaults(duckdb::CatalogTransaction transaction);

  void Inherit(Schema& builtins, bool types_only) {
    _inherited.push_back({&builtins, types_only});
  }

  void ScanMembers(duckdb::CatalogTransaction transaction,
                   absl::FunctionRef<void(duckdb::CatalogEntry&)> callback);

  void ScanTree(duckdb::CatalogTransaction transaction,
                absl::FunctionRef<void(duckdb::CatalogEntry&)> callback);

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateSchema(
    duckdb::CatalogTransaction transaction, duckdb::CreateSchemaInfo& info);

 private:
  struct Inherited {
    Schema* schema;
    bool types_only;
  };

  bool Inherits(const Inherited& inherited, duckdb::CatalogType type) const;

  void Own(duckdb::CatalogTransaction transaction, duckdb::StandardEntry& owned,
           const duckdb::ChangeOwnershipInfo& ownership);

  bool AlterExisting(duckdb::CatalogTransaction transaction,
                     duckdb::CreateInfo& info, const duckdb::Identifier& name);

  duckdb::optional_ptr<duckdb::CatalogEntry> SiblingEntry(
    duckdb::CatalogTransaction transaction, duckdb::CatalogType type,
    const duckdb::Identifier& name, bool types);

  duckdb::optional_ptr<duckdb::CatalogEntry> NamespaceEntry(
    duckdb::CatalogTransaction transaction, duckdb::CatalogType type,
    const duckdb::Identifier& name);

  void CheckRelationName(duckdb::CatalogTransaction transaction,
                         duckdb::CatalogType type,
                         const duckdb::Identifier& name);

  void Move(duckdb::CatalogTransaction transaction, EntrySet& set,
            duckdb::CatalogType type, duckdb::CatalogEntry& entry,
            Schema& target, const duckdb::AlterInfo& info);

  std::shared_ptr<SchemaSets> _sets;
  std::vector<Inherited> _inherited;
};

}  // namespace sdb::catalog
