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

#include "connector/duckdb_entry_cache.h"

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/statement/create_statement.hpp>

#include "basics/down_cast.h"
#include "basics/static_strings.h"
#include "catalog/function.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/secondary_index.h"
#include "catalog/sequence.h"
#include "catalog/user_type.h"
#include "catalog/view.h"
#include "connector/duckdb_index_entry.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_system_table_entry.h"
#include "connector/duckdb_table_entry.h"
#include "pg/system_catalog.h"

namespace sdb::connector {
namespace {

bool IsSystemSchema(std::string_view schema_name) {
  return schema_name == StaticStrings::kPgCatalogSchema ||
         schema_name == StaticStrings::kInformationSchema;
}

const catalog::PgSqlView* FindView(bool system, bool info_schema,
                                   ObjectId db_id, std::string_view schema_name,
                                   std::string_view name,
                                   const catalog::Snapshot& snapshot) {
  if (system) {
    auto view = info_schema ? pg::GetInfoSchemaView(name) : pg::GetView(name);
    return view.get();
  }
  auto relation = snapshot.GetRelation(db_id, schema_name, name);
  if (relation && relation->GetType() == catalog::ObjectType::PgSqlView) {
    return &basics::downCast<const catalog::PgSqlView>(*relation);
  }
  return nullptr;
}

duckdb::unique_ptr<duckdb::CatalogEntry> MakeViewEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  std::string_view schema_name, const catalog::PgSqlView& view) {
  auto info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      view.GetInfo().Copy());
  info->schema = schema_name;
  info->temporary = false;
  info->internal = false;
  return duckdb::make_uniq<duckdb::ViewCatalogEntry>(catalog, schema, *info);
}

std::shared_ptr<catalog::PgSqlFunction> FindFunctionByType(
  ObjectId database, std::string_view schema, std::string_view name,
  const catalog::Snapshot& snapshot, duckdb::CatalogType expected_type) {
  std::shared_ptr<catalog::PgSqlFunction> f;
  if (schema == StaticStrings::kPgCatalogSchema) {
    f = pg::GetPgCatalogFunction(name);
  } else if (schema == StaticStrings::kInformationSchema) {
    f = pg::GetInfoSchemaFunction(name);
    if (!f) {
      f = pg::GetPgCatalogFunction(name);
    }
  } else {
    f = pg::GetPgCatalogFunction(name);
    if (!f) {
      f = snapshot.GetFunction(database, schema, name);
    }
  }
  if (f && f->GetInfo().type == expected_type) {
    return f;
  }
  return nullptr;
}

std::shared_ptr<catalog::PgSqlFunction> FindScalarFunction(
  ObjectId database, std::string_view schema, std::string_view name,
  const catalog::Snapshot& snapshot) {
  return FindFunctionByType(database, schema, name, snapshot,
                            duckdb::CatalogType::MACRO_ENTRY);
}

std::shared_ptr<catalog::PgSqlFunction> FindTableFunction(
  ObjectId database, std::string_view schema, std::string_view name,
  const catalog::Snapshot& snapshot) {
  return FindFunctionByType(database, schema, name, snapshot,
                            duckdb::CatalogType::TABLE_MACRO_ENTRY);
}

duckdb::unique_ptr<duckdb::CatalogEntry> MakeMacroEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema,
  std::string_view schema_name, std::string_view name, bool internal,
  const catalog::PgSqlFunction& func) {
  auto info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      func.GetInfo().Copy());
  info->schema = schema_name;
  info->name = name;
  info->temporary = false;
  info->internal = internal;
  if (info->type == duckdb::CatalogType::TABLE_MACRO_ENTRY) {
    return duckdb::make_uniq_base<duckdb::CatalogEntry,
                                  duckdb::TableMacroCatalogEntry>(
      catalog, schema, *info);
  } else {
    return duckdb::make_uniq_base<duckdb::CatalogEntry,
                                  duckdb::ScalarMacroCatalogEntry>(
      catalog, schema, *info);
  }
}

}  // namespace

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> DuckDBEntryCache::EnsureSchema(
  duckdb::Catalog& catalog, ObjectId db_id, std::string_view schema_name,
  const catalog::Snapshot& snapshot) {
  {
    std::shared_lock lock{_lock};
    auto db_it = _databases.find(db_id);
    if (db_it != _databases.end()) {
      auto schema_it = db_it->second.schemas.find(schema_name);
      if (schema_it != db_it->second.schemas.end()) {
        return &schema_it->second.entry;
      }
    }
  }

  if (!IsSystemSchema(schema_name)) {
    if (!snapshot.GetSchema(db_id, schema_name)) {
      return nullptr;
    }
  }

  std::unique_lock lock{_lock};
  auto& schemas = _databases[db_id].schemas;
  auto [it, _] = schemas.try_emplace(schema_name, catalog, schema_name);
  return &it->second.entry;
}

void DuckDBEntryCache::ScanSchemas(
  duckdb::Catalog& catalog, ObjectId db_id,
  const std::function<void(duckdb::SchemaCatalogEntry&)>& callback,
  const catalog::Snapshot& snapshot) {
  if (!snapshot.GetDatabase(db_id)) {
    return;
  }
  auto schemas = snapshot.GetSchemas(db_id);
  for (auto& schema : schemas) {
    auto entry = EnsureSchema(catalog, db_id, schema->GetName(), snapshot);
    if (entry) {
      callback(*entry);
    }
  }
}

namespace {

// Builds a CreateTableInfo (column defs + PK / NOT NULL / CHECK constraints)
// and the indexed column indices for the given catalog::Table. Shared
// between BuildTableEntry (regular table) and BuildIndexScanEntry
// (index-name-as-table).
struct TableInfoAndIndices {
  duckdb::unique_ptr<duckdb::CreateTableInfo> info;
  std::vector<size_t> indexed_col_indices;
};

TableInfoAndIndices BuildTableInfoAndIndices(
  std::string_view name, std::string_view schema_name,
  const catalog::Table& table, const catalog::Snapshot& snapshot) {
  TableInfoAndIndices out;
  out.info = duckdb::make_uniq<duckdb::CreateTableInfo>();
  out.info->table = name;
  out.info->schema = schema_name;

  for (const auto& col : table.Columns()) {
    // Skip internal generated PK column -- it's not part of the user-visible
    // schema and must not show up via `*` expansion or column-count checks.
    // Kept in bind_data separately only if needed for row identification.
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    auto cd = duckdb::ColumnDefinition(std::string{col.GetName()}, col.type);
    if (col.IsGenerated() && col.expr && col.expr->HasExpr()) {
      cd.SetGeneratedExpression(
        col.expr->GetExpr().Copy(),
        col.generated_type == catalog::Column::GeneratedType::kStored
          ? duckdb::TableColumnType::GENERATED_STORED
          : duckdb::TableColumnType::GENERATED_VIRTUAL);
    } else if (col.expr && col.expr->HasExpr()) {
      cd.SetDefaultValue(col.expr->GetExpr().Copy());
    }
    out.info->columns.AddColumn(std::move(cd));
  }

  // PK constraint
  const auto& pk_col_ids = table.PKColumns();
  if (!pk_col_ids.empty()) {
    duckdb::vector<duckdb::string> pk_names;
    for (auto pk_id : pk_col_ids) {
      for (const auto& col : table.Columns()) {
        if (col.GetId() == pk_id) {
          pk_names.emplace_back(col.GetName());
          break;
        }
      }
    }
    out.info->constraints.push_back(
      duckdb::make_uniq<duckdb::UniqueConstraint>(std::move(pk_names), true));
  }

  // CHECK and NOT NULL constraints.
  for (const auto& check : table.CheckConstraints()) {
    if (auto idx = check.IsNotNull(table.Columns())) {
      out.info->constraints.push_back(
        duckdb::make_uniq<duckdb::NotNullConstraint>(
          duckdb::LogicalIndex(*idx)));
    } else if (check.expr && check.expr->HasExpr()) {
      out.info->constraints.push_back(
        duckdb::make_uniq<duckdb::CheckConstraint>(
          check.expr->GetExpr().Copy()));
    }
  }

  // Indexed columns
  auto indexes = snapshot.GetIndexesByRelation(table.GetId());
  const auto& cols = table.Columns();
  containers::FlatHashSet<size_t> idx_set;
  for (auto& index : indexes) {
    for (auto col_id : index->GetReferencedColumnIds()) {
      for (size_t i = 0; i < cols.size(); ++i) {
        if (cols[i].GetId() == col_id) {
          idx_set.insert(i);
          break;
        }
      }
    }
  }
  out.indexed_col_indices.assign(idx_set.begin(), idx_set.end());
  std::sort(out.indexed_col_indices.begin(), out.indexed_col_indices.end());
  return out;
}

}  // namespace

duckdb::unique_ptr<duckdb::CatalogEntry> DuckDBEntryCache::BuildTableEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema, ObjectId db_id,
  std::string_view schema_name, std::string_view table_name,
  const catalog::Snapshot& snapshot) {
  auto table = snapshot.GetTable(db_id, schema_name, table_name);
  if (!table) {
    return nullptr;
  }
  auto built =
    BuildTableInfoAndIndices(table_name, schema.name, *table, snapshot);
  return duckdb::make_uniq<SereneDBTableEntry>(
    catalog, schema, *built.info, std::move(table),
    std::move(built.indexed_col_indices));
}

duckdb::unique_ptr<duckdb::CatalogEntry> DuckDBEntryCache::BuildIndexScanEntry(
  duckdb::Catalog& catalog, duckdb::SchemaCatalogEntry& schema, ObjectId db_id,
  std::string_view schema_name, std::string_view name,
  const catalog::Index& index, const catalog::Snapshot& snapshot) {
  auto relation_obj = snapshot.GetObject(index.GetRelationId());
  if (!relation_obj) {
    return nullptr;
  }
  if (relation_obj->GetType() == catalog::ObjectType::PgSqlView) {
    auto view =
      std::static_pointer_cast<const catalog::PgSqlView>(relation_obj);
    const auto& vinfo = view->GetInfo();
    auto info = duckdb::make_uniq<duckdb::CreateTableInfo>();
    info->table = std::string{name};
    info->schema = std::string{schema.name};
    for (size_t i = 0; i < vinfo.names.size(); ++i) {
      info->columns.AddColumn(
        duckdb::ColumnDefinition(vinfo.names[i], vinfo.types[i]));
    }
    auto col_ids = index.GetColumnIds();
    std::vector<size_t> indexed_col_indices(col_ids.begin(), col_ids.end());
    if (index.GetType() == catalog::ObjectType::InvertedIndex) {
      auto inverted_index_ptr =
        snapshot.GetObject<catalog::InvertedIndex>(index.GetId());
      if (!inverted_index_ptr) {
        return nullptr;
      }
      return duckdb::make_uniq<ViewInvertedIndexScanEntry>(
        catalog, schema, *info, std::move(view), std::move(indexed_col_indices),
        std::move(inverted_index_ptr));
    }
    // Plain (secondary) indexes on views no longer exist; CREATE INDEX
    // rejects them at bind time.
    return nullptr;
  }

  if (relation_obj->GetType() != catalog::ObjectType::Table) {
    return nullptr;
  }
  auto table = std::static_pointer_cast<catalog::Table>(relation_obj);
  auto built = BuildTableInfoAndIndices(name, schema.name, *table, snapshot);

  if (index.GetType() == catalog::ObjectType::InvertedIndex) {
    auto inverted_index_ptr =
      snapshot.GetObject<catalog::InvertedIndex>(index.GetId());
    if (!inverted_index_ptr) {
      return nullptr;
    }
    return duckdb::make_uniq<TableInvertedIndexScanEntry>(
      catalog, schema, *built.info, std::move(table),
      std::move(built.indexed_col_indices), std::move(inverted_index_ptr));
  }

  // Secondary index: find the shard for scanning.
  const auto& sec_index =
    basics::downCast<const catalog::SecondaryIndex>(index);
  auto shard = snapshot.GetIndexShard(index.GetId());
  if (!shard) {
    return nullptr;
  }
  auto sk_shard_id = shard->GetId();
  return duckdb::make_uniq<TableSecondaryIndexScanEntry>(
    catalog, schema, *built.info, std::move(table),
    std::move(built.indexed_col_indices), sk_shard_id, sec_index.IsUnique());
}

duckdb::optional_ptr<duckdb::CatalogEntry> DuckDBEntryCache::EnsureEntry(
  duckdb::CatalogType type, duckdb::Catalog& catalog,
  duckdb::SchemaCatalogEntry& schema, ObjectId db_id,
  std::string_view schema_name, std::string_view name,
  const catalog::Snapshot& snapshot) {
  {
    std::shared_lock lock{_lock};
    auto db_it = _databases.find(db_id);
    if (db_it != _databases.end()) {
      auto schema_it = db_it->second.schemas.find(schema_name);
      if (schema_it != db_it->second.schemas.end()) {
        auto& map = schema_it->second.MapForType(type);
        auto it = map.find(name);
        if (it != map.end()) {
          return it->second.get();
        }
      }
    }
  }

  auto entry =
    BuildEntry(type, catalog, schema, db_id, schema_name, name, snapshot);
  if (!entry) {
    return nullptr;
  }

  std::unique_lock lock{_lock};
  auto& sc = _databases[db_id]
               .schemas.try_emplace(schema_name, catalog, schema_name)
               .first->second;
  auto& map = sc.MapForType(type);
  auto [it, inserted] = map.try_emplace(name, std::move(entry));
  return it->second.get();
}

void DuckDBEntryCache::ScanEntries(
  duckdb::CatalogType type, duckdb::Catalog& catalog,
  duckdb::SchemaCatalogEntry& entry, ObjectId database, std::string_view schema,
  const std::function<void(duckdb::CatalogEntry&)>& callback,
  const catalog::Snapshot& snapshot) {
  // Visit-based scan: the fast path (everything cached) is lock-free apart
  // from a single shared_lock acquire/release, with no allocation. Only
  // missing items are buffered for the unique_lock phase.
  //
  // `visit` is callable as `visit([&](const T& o) { ... })` and iterates the
  // enumeration source (pg static tables, snapshot relations, ...) without
  // materialising a container. `tag` is a `std::type_identity<T>` used only
  // to declare the typed missing vector.
  auto run = [&](auto visit, auto tag) {
    using T = typename decltype(tag)::type;
    std::vector<const T*> missing;
    {
      std::shared_lock lock{_lock};
      const SchemaCache* sc = nullptr;
      auto db_it = _databases.find(database);
      if (db_it != _databases.end()) {
        auto schema_it = db_it->second.schemas.find(schema);
        if (schema_it != db_it->second.schemas.end()) {
          sc = &schema_it->second;
        }
      }
      visit([&](const T& o) {
        if (sc) {
          const auto& map = sc->MapForType(type);
          auto it = map.find(o.GetName());
          if (it != map.end()) {
            callback(*it->second);
            return;
          }
        }
        missing.push_back(&o);
      });
    }
    if (missing.empty()) {
      return;
    }
    std::unique_lock lock{_lock};
    auto& sc = _databases[database]
                 .schemas.try_emplace(schema, catalog, schema)
                 .first->second;
    auto& map = sc.MapForType(type);
    for (const auto* p : missing) {
      auto it = map.try_emplace(p->GetName()).first;
      irs::Finally drop_if_null = [&] noexcept {
        if (!it->second) {
          map.erase(it);
        }
      };
      if (!it->second) {
        it->second = BuildEntry(type, catalog, sc.entry, database, schema,
                                p->GetName(), snapshot);
      }
      if (it->second) {
        callback(*it->second);
      }
    }
  };

  static constexpr std::type_identity<catalog::VirtualTable> kTable{};
  static constexpr std::type_identity<catalog::PgSqlView> kView{};
  static constexpr std::type_identity<catalog::PgSqlFunction> kFunc{};
  static constexpr std::type_identity<catalog::Object> kRelation{};
  static constexpr std::type_identity<catalog::Index> kIndex{};
  static constexpr std::type_identity<catalog::PgSqlType> kType{};

  using enum duckdb::CatalogType;
  if (schema == StaticStrings::kPgCatalogSchema) {
    switch (type) {
      case TABLE_ENTRY:
        run([](auto v) { pg::VisitPgCatalogTables(v); }, kTable);
        run([](auto v) { pg::VisitPgCatalogViews(v); }, kView);
        break;
      case VIEW_ENTRY:
        run([](auto v) { pg::VisitPgCatalogViews(v); }, kView);
        break;
      case SCALAR_FUNCTION_ENTRY:
      case MACRO_ENTRY:
      case TABLE_FUNCTION_ENTRY:
      case TABLE_MACRO_ENTRY:
        run([](auto v) { pg::VisitPgCatalogFunctions(v); }, kFunc);
        break;
      default:
        break;
    }
  } else if (schema == StaticStrings::kInformationSchema) {
    switch (type) {
      case TABLE_ENTRY:
        run([](auto v) { pg::VisitInfoSchemaTables(v); }, kTable);
        run([](auto v) { pg::VisitInfoSchemaViews(v); }, kView);
        break;
      case VIEW_ENTRY:
        run([](auto v) { pg::VisitInfoSchemaViews(v); }, kView);
        break;
      case SCALAR_FUNCTION_ENTRY:
      case MACRO_ENTRY:
      case TABLE_FUNCTION_ENTRY:
      case TABLE_MACRO_ENTRY:
        run([](auto v) { pg::VisitInfoSchemaFunctions(v); }, kFunc);
        break;
      default:
        break;
    }
  } else {
    switch (type) {
      case TABLE_ENTRY:
        run([&](auto v) { snapshot.VisitRelations(database, schema, v); },
            kRelation);
        break;
      case VIEW_ENTRY:
        run([&](auto v) { snapshot.VisitViews(database, schema, v); }, kView);
        break;
      case INDEX_ENTRY:
        run([&](auto v) { snapshot.VisitIndexes(database, schema, v); },
            kIndex);
        break;
      case SEQUENCE_ENTRY:
        run(
          [&](auto v) {
            snapshot.VisitRelations(
              database, schema, [&](const catalog::Object& o) {
                if (o.GetType() == catalog::ObjectType::Sequence) {
                  v(o);
                }
              });
          },
          kRelation);
        break;
      case SCALAR_FUNCTION_ENTRY:
      case MACRO_ENTRY:
      case TABLE_FUNCTION_ENTRY:
      case TABLE_MACRO_ENTRY:
        run([&](auto v) { snapshot.VisitFunctions(database, schema, v); },
            kFunc);
        break;
      case TYPE_ENTRY:
        run(
          [&](auto v) {
            for (const auto& o : snapshot.GetTypes(database, schema)) {
              v(*o);
            }
          },
          kType);
        break;
      default:
        break;
    }
  }
}

duckdb::unique_ptr<duckdb::CatalogEntry> DuckDBEntryCache::BuildEntry(
  duckdb::CatalogType type, duckdb::Catalog& catalog,
  duckdb::SchemaCatalogEntry& entry, ObjectId database, std::string_view schema,
  std::string_view name, const catalog::Snapshot& snapshot) {
  bool system = IsSystemSchema(schema);
  bool info_schema = schema == StaticStrings::kInformationSchema;

  switch (type) {
    using enum duckdb::CatalogType;
    case TABLE_ENTRY:
    case VIEW_ENTRY:
    case INDEX_ENTRY: {
      if (system) {
        // System tables (pg_class, pg_type, etc.)
        if (type == TABLE_ENTRY || type == VIEW_ENTRY) {
          auto* vtable = pg::GetSystemTable(schema, name);
          if (vtable) {
            auto info = duckdb::make_uniq<duckdb::CreateTableInfo>();
            info->table = name;
            info->schema = schema;
            for (auto& [col_name, col_type] :
                 duckdb::StructType::GetChildTypes(vtable->RowType())) {
              info->columns.AddColumn(
                duckdb::ColumnDefinition(col_name, col_type));
            }
            return duckdb::make_uniq<SystemTableEntry>(catalog, entry, *info,
                                                       *vtable);
          }
        }
        // System views (pg_tables, pg_views, etc.)
        if (type == TABLE_ENTRY || type == VIEW_ENTRY) {
          auto* view =
            FindView(system, info_schema, database, schema, name, snapshot);
          if (view) {
            return MakeViewEntry(catalog, entry, schema, *view);
          }
        }
        return nullptr;
      }
      // Single snapshot lookup for tables, views, and indexes.
      auto relation = snapshot.GetRelation(database, schema, name);
      if (!relation) {
        // GetRelation doesn't find regular tables -- use GetTable.
        if (type == TABLE_ENTRY || type == VIEW_ENTRY) {
          return BuildTableEntry(catalog, entry, database, schema, name,
                                 snapshot);
        }
        return nullptr;
      }
      switch (relation->GetType()) {
        case catalog::ObjectType::Table:
          if (type == TABLE_ENTRY || type == VIEW_ENTRY) {
            return BuildTableEntry(catalog, entry, database, schema, name,
                                   snapshot);
          }
          return nullptr;
        case catalog::ObjectType::PgSqlView:
          if (type == TABLE_ENTRY || type == VIEW_ENTRY) {
            return MakeViewEntry(
              catalog, entry, schema,
              basics::downCast<const catalog::PgSqlView>(*relation));
          }
          return nullptr;
        case catalog::ObjectType::SecondaryIndex:
        case catalog::ObjectType::InvertedIndex: {
          if (type == TABLE_ENTRY || type == VIEW_ENTRY) {
            // Index-as-table (SELECT * FROM index_name)
            const auto& index =
              basics::downCast<const catalog::Index>(*relation);
            return BuildIndexScanEntry(catalog, entry, database, schema, name,
                                       index, snapshot);
          }
          if (type != INDEX_ENTRY) {
            return nullptr;
          }
          // Index entry for DROP INDEX / duckdb_indexes() introspection.
          // We only need enough metadata for DropObject to route by name;
          // the actual storage cleanup happens in catalog.DropIndex.
          auto& index = basics::downCast<const catalog::Index>(*relation);
          auto table =
            snapshot.GetObject<catalog::Table>(index.GetRelationId());
          auto info = duckdb::make_uniq<duckdb::CreateIndexInfo>();
          info->schema = schema;
          info->table = table ? table->GetName() : std::string{};
          info->index_name = name;
          info->index_type =
            relation->GetType() == catalog::ObjectType::InvertedIndex
              ? "inverted"
              : "secondary";
          bool is_unique =
            relation->GetType() == catalog::ObjectType::SecondaryIndex &&
            basics::downCast<const catalog::SecondaryIndex>(index).IsUnique();
          info->constraint_type = is_unique
                                    ? duckdb::IndexConstraintType::UNIQUE
                                    : duckdb::IndexConstraintType::NONE;
          return duckdb::make_uniq<SereneDBIndexEntry>(catalog, entry, *info,
                                                       info->table);
        }
        default:
          return nullptr;
      }
    } break;
    case MACRO_ENTRY:
    case SCALAR_FUNCTION_ENTRY: {
      if (auto f = FindScalarFunction(database, schema, name, snapshot)) {
        return MakeMacroEntry(catalog, entry, schema, name, system, *f);
      }
    } break;
    case TABLE_MACRO_ENTRY:
    case TABLE_FUNCTION_ENTRY: {
      if (auto f = FindTableFunction(database, schema, name, snapshot)) {
        return MakeMacroEntry(catalog, entry, schema, name, system, *f);
      }
    } break;
    case TYPE_ENTRY: {
      if (!system) {
        auto sdb_type = snapshot.GetType(database, schema, name);
        if (sdb_type) {
          auto type_info =
            duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTypeInfo>(
              sdb_type->GetInfo().Copy());
          type_info->schema = schema;
          type_info->type = sdb_type->GetLogicalType();
          return duckdb::make_uniq<duckdb::TypeCatalogEntry>(catalog, entry,
                                                             *type_info);
        }
      }
    } break;
    case SEQUENCE_ENTRY: {
      if (system) {
        return nullptr;
      }
      auto schema_obj = snapshot.GetSchema(database, schema);
      if (!schema_obj) {
        return nullptr;
      }
      auto seq = snapshot.GetSequence(database, schema_obj->GetId(), name);
      if (!seq) {
        return nullptr;
      }
      duckdb::CreateSequenceInfo info;
      info.schema = schema;
      info.name = name;
      info.start_value = seq->Options().start_value;
      info.increment = seq->Options().increment;
      info.min_value = seq->Options().min_value;
      info.max_value = seq->Options().max_value;
      info.cycle = seq->Options().cycle;
      info.usage_count = 0;
      return duckdb::make_uniq<duckdb::SequenceCatalogEntry>(catalog, entry,
                                                             info);
    }
    default:
      return nullptr;
  }
  return nullptr;
}

}  // namespace sdb::connector
