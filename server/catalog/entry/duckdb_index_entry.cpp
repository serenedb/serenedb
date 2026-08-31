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

#include "catalog/entry/duckdb_index_entry.h"

#include <algorithm>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/storage/data_table.hpp>

#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "catalog/ddl/catalog.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/entry/duckdb_schema_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/entry/duckdb_view_entry.h"
#include "catalog/log/store.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/table.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {

SereneDBIndexEntry::SereneDBIndexEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  const catalog::CreateIndexInfo& info,
  duckdb::shared_ptr<duckdb::IndexDataTableInfo> storage_info,
  std::string table_name, bool relation_is_table,
  std::vector<size_t> relation_col_positions,
  std::shared_ptr<search::InvertedIndexStorage> inverted_data)
  : duckdb::DuckIndexEntry{catalog, schema,
                           const_cast<catalog::CreateIndexInfo&>(info),
                           std::move(storage_info)},
    _sdb_index{info.GetIndex()},
    _key_columns{info.IsInverted() ? std::vector<ColumnId>{}
                                   : info.GetColumns()},
    _referenced_columns{info.IsInverted() ? std::vector<ColumnId>{}
                                          : info.GetReferencedColumns()},
    _inverted_data{std::move(inverted_data)},
    _table_name{std::move(table_name)},
    _relation_id{info.GetRelationId()} {
  // An ART on the store table or an iresearch directory: the storage is not a
  // duckdb index block list, and the definition lives in the serenedb log.
  // An index carries no owner and no ACL: postgres gives it none, so every
  // privilege decision reads the relation it is built on.
  catalog::AdoptEntryIdentity(*this, info.GetId());
  if (relation_is_table) {
    catalog.Cast<SereneDBCatalog>().SetIndexColumns(
      _relation_id, info.GetId(), std::move(relation_col_positions));
  }
}

namespace {

// Every column the index reads, by the ids the record states -- both kinds
// state them. A key's own expression cannot answer this: `(s.a + 1)` names a
// struct field, and the column the index reads is `s`.
std::vector<size_t> IndexedPositions(const duckdb::CreateTableInfo& table,
                                     const catalog::CreateIndexInfo& index) {
  containers::FlatHashSet<size_t> positions;
  for (auto col_id : index.GetReferencedColumns()) {
    if (const auto* column = catalog::ColumnById(table, col_id)) {
      positions.insert(column->Logical().index);
    }
  }
  std::vector<size_t> out(positions.begin(), positions.end());
  std::sort(out.begin(), out.end());
  return out;
}

}  // namespace

duckdb::unique_ptr<duckdb::CreateInfo> SereneDBIndexEntry::GetInfo() const {
  if (_sdb_index) {
    auto info = duckdb::make_uniq<catalog::CreateIndexInfo>(_sdb_index);
    // The edges are the entry's, the way duckdb keeps them: what an index key
    // resolved to when it was built is not restated by the definition. So is
    // the relation it hangs off, by name: the index names it by id, and what
    // renders the record -- duckdb_indexes().sql -- reads the name.
    info->dependencies = dependencies;
    info->SetSchema(ParentSchema().name);
    info->table = GetTableName();
    return std::move(info);
  }
  // A plain ART: the base rebuilds duckdb's half from its own fields, and the
  // record adds the identity the catalog files it under.
  auto duck = duckdb::IndexCatalogEntry::GetInfo();
  auto& keys = duck->Cast<duckdb::CreateIndexInfo>();
  auto info = duckdb::make_uniq<catalog::CreateIndexInfo>(
    ObjectId{ParentSchema().oid}, ObjectId{oid}, _relation_id,
    name.GetIdentifierName(),
    index_constraint_type == duckdb::IndexConstraintType::UNIQUE, _key_columns,
    _referenced_columns, std::move(keys.parsed_expressions));
  keys.CopyProperties(*info);
  info->table = keys.table;
  info->names = std::move(keys.names);
  info->column_ids = std::move(keys.column_ids);
  info->options = std::move(keys.options);
  return std::move(info);
}

void SereneDBIndexEntry::Rollback(duckdb::CatalogEntry& prev_entry) {
  duckdb::DuckIndexEntry::Rollback(prev_entry);
  if (!prev_entry.deleted) {
    return;
  }
  const auto record = GetInfo();
  catalog::DropIndexArtifacts(
    nullptr, ParentSchema().Cast<SereneDBSchemaEntry>().GetDatabaseId(),
    record->Cast<catalog::CreateIndexInfo>(), _inverted_data);
}

duckdb::unique_ptr<duckdb::CatalogEntry> SereneDBIndexEntry::AlterEntry(
  duckdb::ClientContext& context, duckdb::AlterInfo& info) {
  // The grammar shares one RENAME alter across the relation kinds, so an index
  // rename arrives as an ALTER TABLE -- the same conversion sequences make.
  if (info.type != duckdb::AlterType::ALTER_TABLE ||
      info.Cast<duckdb::AlterTableInfo>().alter_table_type !=
        duckdb::AlterTableType::RENAME_TABLE) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("only RENAME is supported for ALTER INDEX"));
  }
  const auto new_name =
    info.Cast<duckdb::RenameTableInfo>().new_table_name.GetIdentifierName();
  const auto record = GetInfo();
  const auto& index = record->Cast<catalog::CreateIndexInfo>();
  auto& schema = ParentSchema().Cast<SereneDBSchemaEntry>();
  if (const auto* relation = catalog::Find<SereneDBTableEntry>(
        &context, index.GetSchemaId(), _relation_id)) {
    if (MakeStoreIndexInfo(*relation->Definition(), index)) {
      StoreRenameIndex(&context, schema.GetDatabaseId(), _relation_id,
                       index.GetName(), new_name);
    }
  }
  const auto renamed = RenamedIndexRecord(index, new_name);
  auto built = Make(ParentCatalog(), schema,
                    renamed->Cast<catalog::CreateIndexInfo>(), &context);
  built->permissions = permissions;
  return built;
}

duckdb::unique_ptr<duckdb::CatalogEntry> SereneDBIndexEntry::Make(
  duckdb::Catalog& catalog, SereneDBSchemaEntry& schema,
  const catalog::CreateIndexInfo& index, duckdb::ClientContext* context) {
  // The relation an index covers is a table or a view, and both are entries in
  // the catalog this one is being built for -- placed ahead of the indexes that
  // project them.
  // The open directory this index already has, if it has one: the version being
  // superseded is still the one filed under the id here, and the handle is the
  // object's rather than the definition's.
  std::shared_ptr<search::InvertedIndexStorage> inverted_data;
  if (const auto* previous =
        catalog::FindIn<SereneDBIndexEntry>(context, catalog, index.GetId())) {
    inverted_data = previous->GetInvertedData();
  }
  auto found = LookupEntryIn(context, catalog, index.GetRelationId());
  std::string_view relation_name;
  bool relation_is_table = false;
  std::vector<size_t> positions;
  duckdb::optional_ptr<duckdb::DataTable> rows;
  if (auto* view = dynamic_cast<const SereneDBViewEntry*>(found.get())) {
    relation_name = view->name.GetIdentifierName();
  } else if (auto* relation = dynamic_cast<SereneDBTableEntry*>(found.get())) {
    relation_name = relation->name.GetIdentifierName();
    relation_is_table = true;
    positions = IndexedPositions(*relation->Definition(), index);
    rows = relation->TryGetStorage();
  }
  // The relation this version hangs off, named the way duckdb's own entry
  // wants it: the record names it by id, the entry by name.
  auto owned =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, catalog::CreateIndexInfo>(
      index.Copy());
  owned->SetSchema(schema.name);
  owned->table = duckdb::Identifier{relation_name};
  auto table_name = owned->table.GetIdentifierName();
  duckdb::shared_ptr<duckdb::IndexDataTableInfo> storage_info;
  if (rows) {
    storage_info = duckdb::make_shared_ptr<duckdb::IndexDataTableInfo>(
      rows->GetDataTableInfo(), owned->GetIndexName());
  }
  return duckdb::make_uniq<SereneDBIndexEntry>(
    catalog, schema, *owned, std::move(storage_info), std::move(table_name),
    relation_is_table, std::move(positions), std::move(inverted_data));
}

}  // namespace sdb::catalog
