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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/main/attached_database.hpp>
#include <functional>
#include <string>

#include "catalog/entry_set.h"
#include "catalog/schema.h"

namespace sdb::catalog {

void DeclareModified(duckdb::CatalogTransaction transaction,
                     duckdb::Catalog& catalog,
                     duckdb::DatabaseModificationType type =
                       duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);

duckdb::optional_ptr<duckdb::CatalogEntry> AlterEntry(
  duckdb::CatalogTransaction transaction, EntrySet& set,
  duckdb::CatalogEntry& entry, duckdb::AlterInfo& info);

[[noreturn]] void ThrowEntryExists(duckdb::CatalogType type,
                                   const duckdb::Identifier& name);

[[noreturn]] void ThrowMissingEntry(duckdb::CatalogType type,
                                    const duckdb::Identifier& name);

std::shared_ptr<DependentIndex> SharedDependents(duckdb::AttachedDatabase& db);

class BaseCatalog : public duckdb::Catalog {
 public:
  BaseCatalog(duckdb::AttachedDatabase& db, bool case_sensitive)
    : duckdb::Catalog{db},
      _oids{db, SharedDependents(db)},
      _schemas{case_sensitive, _oids} {}

  using duckdb::Catalog::CreateSchema;
  using duckdb::Catalog::Initialize;

  bool CrossCatalogDependencies() const final {
    return !GetAttached().IsSystem();
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateSchema(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateSchemaInfo& info) override;

  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> LookupSchema(
    duckdb::CatalogTransaction transaction,
    const duckdb::EntryLookupInfo& schema_lookup,
    duckdb::OnEntryNotFound if_not_found) override;

  void ScanSchemas(
    duckdb::ClientContext& context,
    std::function<void(duckdb::SchemaCatalogEntry&)> callback) final;

  void ScanSchemas(absl::FunctionRef<void(Schema&)> callback);

  void DropSchema(duckdb::ClientContext& context, duckdb::DropInfo& info) final;

  void AlterSchemaEntry(duckdb::CatalogTransaction transaction,
                        duckdb::SchemaCatalogEntry& schema,
                        duckdb::AlterInfo& info) final;

  void AlterSchema(duckdb::CatalogTransaction transaction,
                   duckdb::SchemaCatalogEntry& schema,
                   duckdb::AlterSchemaInfo& info) final;

  duckdb::optional_idx GetCatalogVersion(duckdb::ClientContext& context) final;

  duckdb::PhysicalOperator& PlanCreateTableAs(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalCreateTable& op, duckdb::PhysicalOperator& plan) override;
  duckdb::PhysicalOperator& PlanInsert(
    duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
    duckdb::LogicalInsert& op,
    duckdb::optional_ptr<duckdb::PhysicalOperator> plan) override;
  duckdb::PhysicalOperator& PlanDelete(duckdb::ClientContext& context,
                                       duckdb::PhysicalPlanGenerator& planner,
                                       duckdb::LogicalDelete& op,
                                       duckdb::PhysicalOperator& plan) override;
  duckdb::PhysicalOperator& PlanUpdate(duckdb::ClientContext& context,
                                       duckdb::PhysicalPlanGenerator& planner,
                                       duckdb::LogicalUpdate& op,
                                       duckdb::PhysicalOperator& plan) override;

  duckdb::DatabaseSize GetDatabaseSize(duckdb::ClientContext& context) override;

  void ScanSubjects(
    duckdb::CatalogTransaction transaction, duckdb::CatalogEntry& entry,
    const std::function<void(duckdb::CatalogEntry&)>& callback) final;

  duckdb::catalog_entry_vector_t OwnedEntries(
    duckdb::CatalogTransaction transaction,
    const duckdb::CatalogEntry& owner) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> OwnerOf(
    duckdb::CatalogTransaction transaction, duckdb::CatalogEntry& owned) final;

  virtual duckdb::unique_ptr<duckdb::CatalogEntry> MakeTable(
    duckdb::CatalogTransaction transaction, Schema& schema,
    duckdb::BoundCreateTableInfo& info);

  virtual duckdb::unique_ptr<duckdb::CatalogEntry> MakeIndex(
    duckdb::CatalogTransaction transaction, Schema& schema,
    duckdb::CreateIndexInfo& info, duckdb::CatalogEntry& relation);

  virtual bool Durable() const { return false; }

  virtual duckdb::idx_t RecordId() const { return GetAttached().oid; }

  virtual void OnCommit() {}

  virtual void ReplayCreate(duckdb::ClientContext& context,
                            duckdb::unique_ptr<duckdb::CreateInfo> info);

  virtual void ReplayDrop(duckdb::ClientContext& context,
                          duckdb::CatalogEntry& entry);

  duckdb::optional_ptr<duckdb::CatalogEntry> FindRelation(
    duckdb::CatalogTransaction transaction, duckdb::idx_t oid);

  virtual EntrySet& Set(duckdb::CatalogType type);

  EntrySet& SchemaSet(duckdb::CatalogTransaction transaction,
                      const duckdb::SchemaCatalogEntry& schema);

  OidIndex& Oids() noexcept { return _oids; }

  duckdb::optional_ptr<duckdb::CatalogEntry> LookupOid(
    duckdb::CatalogTransaction transaction, duckdb::idx_t oid);

  duckdb::optional_ptr<duckdb::CatalogEntry> LookupOwner(
    duckdb::CatalogTransaction transaction, duckdb::idx_t oid);

  template<typename T>
  duckdb::optional_ptr<T> FindIn(duckdb::CatalogTransaction transaction,
                                 duckdb::idx_t oid) {
    auto entry = LookupOid(transaction, oid);
    return entry && entry->type == T::Type ? &entry->template Cast<T>()
                                           : nullptr;
  }

 protected:
  duckdb::optional_ptr<duckdb::CatalogEntry> AddTo(
    EntrySet& set, duckdb::CatalogTransaction transaction,
    duckdb::unique_ptr<duckdb::CatalogEntry> entry,
    duckdb::OnCreateConflict on_conflict);

  bool DropFrom(EntrySet& set, duckdb::CatalogTransaction transaction,
                duckdb::CatalogType type, duckdb::DropInfo& info);

  OidIndex _oids;
  EntrySet _schemas;
};

}  // namespace sdb::catalog
