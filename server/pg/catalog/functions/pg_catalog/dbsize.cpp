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

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/database_size.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/index_entry.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>

#include "catalog/catalog.h"
#include "catalog/entry/search_table.h"
#include "connector/inverted_store_index.h"
#include "connector/pg_logical_types.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/lookup.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"

namespace sdb::pg {
namespace {

struct RelationStorageSize {
  int64_t bytes = 0;
  int64_t persistent_blocks = 0;
};

RelationStorageSize StoreTableDataSize(duckdb::ClientContext& context,
                                       const duckdb::TableCatalogEntry& table) {
  RelationStorageSize result;
  if (!table.IsDuckTable()) {
    return result;
  }
  auto& storage = const_cast<duckdb::TableCatalogEntry&>(table).GetStorage();
  duckdb::QueryContext query_context(context);
  irs::containers::FlatHashSet<duckdb::block_id_t> blocks;
  int64_t transient_bytes = 0;
  for (const auto& info : storage.GetColumnSegmentInfo(query_context)) {
    if (info.persistent) {
      blocks.insert(info.block_id);
      blocks.insert(info.additional_blocks.begin(),
                    info.additional_blocks.end());
    } else {
      transient_bytes += static_cast<int64_t>(info.segment_size);
    }
  }
  const auto block_size =
    storage.GetTableIOManager().GetBlockManagerForRowData().GetBlockSize();
  result.persistent_blocks = static_cast<int64_t>(blocks.size());
  result.bytes = result.persistent_blocks * static_cast<int64_t>(block_size) +
                 transient_bytes;
  return result;
}

int64_t BoundIndexBytes(duckdb::ClientContext& context,
                        duckdb::DataTableInfo& info, const auto& match) {
  info.BindIndexes(context);
  int64_t total = 0;
  for (auto entry : info.GetIndexes().IndexEntries()) {
    if (entry->GetBindState() == duckdb::IndexBindState::BOUND) {
      const auto index = entry->GetReadHandle<duckdb::BoundIndex>();
      if (match(index)) {
        total += static_cast<int64_t>(index->GetAllocationSize());
      }
    }
  }
  return total;
}

int64_t StoreTableIndexBytes(duckdb::ClientContext& context,
                             const duckdb::TableCatalogEntry& table) {
  if (!table.IsDuckTable()) {
    return 0;
  }
  return BoundIndexBytes(context,
                         *const_cast<duckdb::TableCatalogEntry&>(table)
                            .GetStorage()
                            .GetDataTableInfo(),
                         [](const auto&) { return true; });
}

int64_t SearchTableBytes(const catalog::SearchTableEntry& table) {
  // Introspection must not open a store as a side effect: report only what
  // is already open.
  const auto& data = table.Storage();
  if (!data) {
    return 0;
  }
  auto reader = data->GetDirectoryReader();
  if (!reader) {
    return 0;
  }
  int64_t total = 0;
  for (const auto& segment : reader.Meta().index_meta.segments) {
    total += static_cast<int64_t>(segment.meta.byte_size);
  }
  return total;
}

// Every entry of one kind across the catalog's schemas, which is duckdb's own
// two-level scan rather than anything of ours.
void VisitEntries(duckdb::ClientContext& context, duckdb::Catalog& catalog,
                  duckdb::CatalogType type,
                  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  std::vector<duckdb::reference<duckdb::CatalogEntry>> entries;
  for (auto& schema : catalog.GetSchemas(context)) {
    schema.get().Scan(context, type, [&](duckdb::CatalogEntry& entry) {
      entries.emplace_back(entry);
    });
  }
  for (auto& entry : entries) {
    callback(entry.get());
  }
}

int64_t RelationDataBytes(duckdb::ClientContext& context,
                          const duckdb::TableCatalogEntry& table) {
  const auto* search = dynamic_cast<const catalog::SearchTableEntry*>(&table);
  return search ? SearchTableBytes(*search)
                : StoreTableDataSize(context, table).bytes;
}

int64_t IndexEntryBytes(duckdb::ClientContext& context,
                        const duckdb::DuckIndexEntry& index) {
  if (const auto* inverted =
        dynamic_cast<const catalog::InvertedIndexEntry*>(&index)) {
    const auto& storage = inverted->Storage();
    if (!storage) {
      return 0;
    }
    return static_cast<int64_t>(storage->GetStats().indexSize);
  }
  // A secondary index is an ART on the store table, so its size is the
  // allocation the store table reports for it.
  return BoundIndexBytes(
    context, index.GetDataTableInfo(),
    [&](const auto& bound) { return bound->GetIndexName() == index.name; });
}

int64_t KeyIndexBytes(duckdb::ClientContext& context,
                      duckdb::TableCatalogEntry& table,
                      const duckdb::UniqueConstraint& key) {
  if (!table.IsDuckTable()) {
    return 0;
  }
  const auto& columns = table.GetColumns();
  std::vector<duckdb::column_t> ids;
  for (const auto logical : key.GetLogicalIndexes(columns)) {
    ids.emplace_back(columns.LogicalToPhysical(logical).index);
  }
  return BoundIndexBytes(
    context, *table.GetStorage().GetDataTableInfo(), [&](const auto& index) {
      return index->GetConstraintType() == key.GetIndexConstraintType() &&
             absl::c_equal(index->GetColumnIds(), ids);
    });
}

int64_t TableIndexesTotalBytes(duckdb::ClientContext& context,
                               duckdb::TableCatalogEntry& table) {
  int64_t total = dynamic_cast<const catalog::SearchTableEntry*>(&table)
                    ? 0
                    : StoreTableIndexBytes(context, table);
  table.ParentSchema(context).Scan(
    context, duckdb::CatalogType::INDEX_ENTRY,
    [&](duckdb::CatalogEntry& entry) {
      auto& index = entry.Cast<duckdb::DuckIndexEntry>();
      if (index.table_oid == table.oid && connector::IsInvertedIndex(index)) {
        total += IndexEntryBytes(context, index);
      }
    });
  return total;
}

duckdb::DatabaseSize DatabaseStorageSize(duckdb::ClientContext& context,
                                         duckdb::Catalog& catalog,
                                         std::string_view only_schema) {
  duckdb::DatabaseSize result;
  auto& attached = catalog.GetAttached();
  if (attached.HasStorageManager()) {
    result.block_size =
      attached.GetStorageManager().GetDatabaseSize().block_size;
  }
  const auto in_scope = [&](const duckdb::CatalogEntry& entry) {
    return only_schema.empty() ||
           entry.ParentSchema(context).name == only_schema;
  };
  int64_t bytes = 0;
  int64_t blocks = 0;
  VisitEntries(context, catalog, duckdb::CatalogType::TABLE_ENTRY,
               [&](duckdb::CatalogEntry& entry) {
                 if (!in_scope(entry)) {
                   return;
                 }
                 // Views and the index-name-as-table wrappers share this set
                 // and own no rows of their own; the cast is the filter.
                 const auto* table =
                   dynamic_cast<const duckdb::TableCatalogEntry*>(&entry);
                 if (!table) {
                   return;
                 }
                 if (const auto* search =
                       dynamic_cast<const catalog::SearchTableEntry*>(table)) {
                   bytes += SearchTableBytes(*search);
                   return;
                 }
                 const auto data = StoreTableDataSize(context, *table);
                 bytes += data.bytes + StoreTableIndexBytes(context, *table);
                 blocks += data.persistent_blocks;
               });
  VisitEntries(context, catalog, duckdb::CatalogType::INDEX_ENTRY,
               [&](duckdb::CatalogEntry& entry) {
                 if (!in_scope(entry)) {
                   return;
                 }
                 auto* index = dynamic_cast<duckdb::DuckIndexEntry*>(&entry);
                 if (index && connector::IsInvertedIndex(*index)) {
                   bytes += IndexEntryBytes(context, *index);
                 }
               });
  result.bytes = static_cast<duckdb::idx_t>(bytes);
  result.total_blocks = static_cast<duckdb::idx_t>(blocks);
  result.used_blocks = result.total_blocks;
  return result;
}

std::optional<int64_t> RelationBytes(const pg::Session& session, uint64_t oid,
                                     bool data, bool indexes) {
  auto& context = *session.context;
  if (auto entry = pg::EntryByOid(session, oid)) {
    if (auto* table = dynamic_cast<duckdb::TableCatalogEntry*>(entry.get())) {
      return (data ? RelationDataBytes(context, *table) : 0) +
             (indexes ? TableIndexesTotalBytes(context, *table) : 0);
    }
    if (auto* index = dynamic_cast<duckdb::DuckIndexEntry*>(entry.get())) {
      return data ? IndexEntryBytes(context, *index) : 0;
    }
    if (entry->type == duckdb::CatalogType::VIEW_ENTRY ||
        entry->type == duckdb::CatalogType::SEQUENCE_ENTRY) {
      return 0;
    }
    return std::nullopt;
  }
  if (const auto key = pg::FindKeyIndex(session, oid); key.table) {
    return data ? KeyIndexBytes(context, *key.table, *key.key_index) : 0;
  }
  if (pg::FindSystemTable(oid) || pg::FindSystemView(oid)) {
    return 0;
  }
  return std::nullopt;
}

template<bool Data, bool Indexes>
void RelationBytesFunction(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  const auto oids = args.data[0].Values<int64_t>();
  auto writer = duckdb::FlatVector::Writer<int64_t>(result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto oid = oids[row];
    const auto bytes =
      oid.IsValid()
        ? RelationBytes(session, static_cast<uint64_t>(oid.GetValue()), Data,
                        Indexes)
        : std::nullopt;
    if (bytes) {
      writer.WriteValue(*bytes);
    } else {
      writer.WriteNull();
    }
  }
}

void RelationForkBytesFunction(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  const auto oids = args.data[0].Values<int64_t>();
  const auto forks = args.data[1].Values<duckdb::string_t>();
  auto writer = duckdb::FlatVector::Writer<int64_t>(result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto oid = oids[row];
    const auto fork = forks[row];
    const auto bytes =
      oid.IsValid() && fork.IsValid()
        ? RelationBytes(session, static_cast<uint64_t>(oid.GetValue()),
                        fork.GetValue().GetString() == "main", false)
        : std::nullopt;
    if (bytes) {
      writer.WriteValue(*bytes);
    } else {
      writer.WriteNull();
    }
  }
}

// pg_database_size(name) -> bigint
void PgDatabaseSizeNameFunction(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto& context = state.GetContext();

  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t input) -> int64_t {
      std::string_view db_name{input.GetData(), input.GetSize()};
      auto database =
        duckdb::Catalog::GetCatalogEntry(context, duckdb::Identifier{db_name});
      if (!database) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                        ERR_MSG("database \"", db_name, "\" does not exist"));
      }
      return static_cast<int64_t>(
        DatabaseStorageSize(context, *database, {}).bytes);
    });
}

// pg_database_size(oid) -> bigint
void PgDatabaseSizeOidFunction(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  auto& context = state.GetContext();

  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
      const auto entry = FindDatabaseById(context, static_cast<uint64_t>(oid));
      duckdb::optional_ptr<duckdb::Catalog> database =
        entry ? duckdb::Catalog::GetCatalogEntry(context, entry->name)
              : nullptr;
      if (!database) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                        ERR_MSG("database with OID ", oid, " does not exist"));
      }
      return static_cast<int64_t>(
        DatabaseStorageSize(context, *database, {}).bytes);
    });
}

// pg_schema_size(name) -> bigint -- non-standard, included for SereneDB tests.
void PgSchemaSizeNameFunction(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  auto& context = state.GetContext();
  auto database = SessionCatalog(context);
  SDB_ASSERT(database);

  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(), [&](duckdb::string_t input) -> int64_t {
      std::string_view schema_name{input.GetData(), input.GetSize()};
      auto schema =
        database->GetSchema(context, duckdb::Identifier{schema_name},
                            duckdb::OnEntryNotFound::RETURN_NULL);
      if (!schema) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                        ERR_MSG("schema \"", schema_name, "\" does not exist"));
      }
      return static_cast<int64_t>(
        DatabaseStorageSize(context, *database, schema_name).bytes);
    });
}

// pg_schema_size(oid) -> bigint
void PgSchemaSizeOidFunction(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  auto& context = state.GetContext();
  // A schema oid resolves in the session's own database, as it does in pg.
  auto database = SessionCatalog(context);
  SDB_ASSERT(database);
  const auto transaction = database->GetCatalogTransaction(context);

  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(), [&](int64_t oid) -> int64_t {
      auto schema =
        database->FindSchemaById(transaction, static_cast<duckdb::idx_t>(oid));
      if (!schema) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                        ERR_MSG("schema with OID ", oid, " does not exist"));
      }
      return static_cast<int64_t>(
        DatabaseStorageSize(context, *database,
                            schema->name.GetIdentifierName())
          .bytes);
    });
}

}  // namespace

void RegisterDbsizeFunctions(duckdb::ExtensionLoader& loader) {
  // pg_relation_size(regclass)
  loader.RegisterFunction(
    duckdb::ScalarFunction{"pg_relation_size",
                           {pg::REGCLASS()},
                           duckdb::LogicalType::BIGINT,
                           RelationBytesFunction<true, false>});

  // pg_relation_size(regclass, text)
  loader.RegisterFunction(
    duckdb::ScalarFunction{"pg_relation_size",
                           {pg::REGCLASS(), duckdb::LogicalType::VARCHAR},
                           duckdb::LogicalType::BIGINT,
                           RelationForkBytesFunction});

  // pg_table_size(regclass)
  loader.RegisterFunction(
    duckdb::ScalarFunction{"pg_table_size",
                           {pg::REGCLASS()},
                           duckdb::LogicalType::BIGINT,
                           RelationBytesFunction<true, false>});

  // pg_total_relation_size(regclass)
  loader.RegisterFunction(
    duckdb::ScalarFunction{"pg_total_relation_size",
                           {pg::REGCLASS()},
                           duckdb::LogicalType::BIGINT,
                           RelationBytesFunction<true, true>});

  // pg_indexes_size(regclass)
  loader.RegisterFunction(
    duckdb::ScalarFunction{"pg_indexes_size",
                           {pg::REGCLASS()},
                           duckdb::LogicalType::BIGINT,
                           RelationBytesFunction<false, true>});

  // pg_database_size(text) and pg_database_size(bigint/oid)
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_database_size",
                                                 {duckdb::LogicalType::VARCHAR},
                                                 duckdb::LogicalType::BIGINT,
                                                 PgDatabaseSizeNameFunction});
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_database_size",
                                                 {duckdb::LogicalType::BIGINT},
                                                 duckdb::LogicalType::BIGINT,
                                                 PgDatabaseSizeOidFunction});

  // pg_schema_size(text) and pg_schema_size(oid) -- non-standard helper.
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_schema_size",
                                                 {duckdb::LogicalType::VARCHAR},
                                                 duckdb::LogicalType::BIGINT,
                                                 PgSchemaSizeNameFunction});
  loader.RegisterFunction(duckdb::ScalarFunction{"pg_schema_size",
                                                 {duckdb::LogicalType::BIGINT},
                                                 duckdb::LogicalType::BIGINT,
                                                 PgSchemaSizeOidFunction});
}

}  // namespace sdb::pg
