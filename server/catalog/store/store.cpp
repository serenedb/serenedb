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

#include "catalog/store/store.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

#include <algorithm>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/exception/binder_exception.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/column_definition.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <exception>
#include <filesystem>
#include <initializer_list>
#include <utility>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/exceptions.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "catalog/database.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/schema.h"
#include "catalog/secondary_index.h"
#include "catalog/table.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kStoreAlias = kStoreDatabaseName;
constexpr std::string_view kStoreFile = "store.db";
constexpr std::string_view kCatalogTable = R"("__sdb_store".main.sdb_catalog)";
constexpr std::string_view kSequenceTable = R"("__sdb_store".main.sdb_seq)";

void ExecOrFatal(duckdb::Connection& conn, const std::string& sql) {
  auto res = conn.Query(sql);
  if (res->HasError()) {
    SDB_FATAL(STARTUP, "catalog store: '", sql, "' failed: ", res->GetError());
  }
}

duckdb::unique_ptr<duckdb::PreparedStatement> PrepareOrFatal(
  duckdb::Connection& conn, std::string_view sql) {
  auto stmt = conn.Prepare(std::string{sql});
  if (stmt->HasError()) {
    SDB_FATAL(STARTUP, "catalog store: preparing '", sql,
              "' failed: ", stmt->GetError());
  }
  return stmt;
}

absl::Status Exec(duckdb::PreparedStatement& stmt,
                  duckdb::vector<duckdb::Value> values) {
  auto res = stmt.Execute(values, /*allow_stream_result=*/false);
  if (res->HasError()) {
    return absl::InternalError(res->GetError());
  }
  return absl::OkStatus();
}

duckdb::Value IdValue(ObjectId id) { return duckdb::Value::UBIGINT(id.id()); }

duckdb::Value TypeValue(ObjectType type) {
  return duckdb::Value::UTINYINT(static_cast<uint8_t>(type));
}

duckdb::Value DefValue(std::string_view def) {
  return duckdb::Value::BLOB(duckdb::const_data_ptr_cast(def.data()),
                             def.size());
}

std::string QuotedIdent(const std::string& name) {
  return duckdb::KeywordHelper::WriteQuoted(name, '"');
}

template<typename Fn>
absl::Status RunInTransaction(duckdb::Connection& conn, Fn&& fn) {
  try {
    conn.BeginTransaction();
  } catch (const std::exception& e) {
    return absl::InternalError(e.what());
  }
  auto r = [&]() noexcept { return fn(); }();
  try {
    if (r.ok()) {
      conn.Commit();
    } else {
      conn.Rollback();
    }
  } catch (const std::exception& e) {
    if (r.ok()) {
      return absl::InternalError(e.what());
    }
  }
  return r;
}

}  // namespace

std::string StoreTableName(std::string_view database, std::string_view schema,
                           std::string_view table) {
  return absl::StrCat(database, ".", schema, ".", table);
}

std::string DroppedStoreTableName(ObjectId table_id) {
  return absl::StrCat("sdb_dropped$", table_id.id());
}

std::optional<StoreIndexDef> MakeStoreIndexDef(std::string_view database,
                                               std::string_view schema,
                                               const Table& table,
                                               const Index& index) {
  if (index.GetType() != ObjectType::InvertedIndex &&
      index.GetType() != ObjectType::SecondaryIndex) {
    return std::nullopt;
  }
  if (table.GetEngine() != TableEngine::Transactional || table.Tombstoned()) {
    return std::nullopt;
  }
  StoreIndexDef def;
  def.table_id = table.GetId();
  def.index_id = index.GetId();

  // Catalog-named types (enums, composites, JSON) cannot be re-parsed by the
  // store connection during the ART build, and ART cannot index nested types.
  // Keys of such types stay unenforced (the index is not mirrored).
  auto art_indexable = [](const duckdb::LogicalType& type) {
    return !type.HasAlias() && type.id() != duckdb::LogicalTypeId::ENUM &&
           !type.IsNested();
  };

  if (index.GetType() == ObjectType::SecondaryIndex) {
    def.kind = StoreIndexDef::Kind::Plain;
    const auto& secondary = basics::downCast<const SecondaryIndex>(index);
    def.unique = secondary.IsUnique();
    auto push_key = [&](std::string rendered) {
      if (!absl::c_contains(def.keys, rendered)) {
        def.keys.push_back(std::move(rendered));
      }
    };
    // Walk the positional key list in source order; a sentinel column slot is
    // an expression key whose payload is the next unconsumed expression. Order
    // (and column/expression interleaving) is the ART key order, so it must be
    // reconstructed verbatim.
    const auto& key_expressions = secondary.Expressions();
    size_t expr_idx = 0;
    for (auto column : secondary.Columns()) {
      if (column == Column::kInvalidId) {  // expression-key slot
        // duckdb's ART builds and maintains expression keys natively; render
        // the parsed expression back to SQL for the store CREATE INDEX. The
        // store table mirrors facade column names, so the text re-binds as-is.
        const auto& expr = key_expressions[expr_idx++];
        if (!art_indexable(expr.return_type)) {
          return std::nullopt;
        }
        push_key(absl::StrCat("(", expr.pretty_printed, ")"));
        continue;
      }
      const auto* col = table.ColumnById(column);
      if (!col || !art_indexable(col->type)) {
        return std::nullopt;
      }
      push_key(QuotedIdent(col->GetName()));
    }
    if (def.keys.empty()) {
      return std::nullopt;
    }
    def.table = StoreTableName(database, schema, table.GetName());
    return def;
  }

  // Inverted index: the store-side BoundIndex feeds iresearch, so it only needs
  // the raw column names (catalog-named types are fine there).
  auto add_column = [&](Column::Id col_id) -> bool {
    const auto* col = table.ColumnById(col_id);
    if (!col) {
      return false;
    }
    // GetReferencedColumns() is already de-duped, so each name is appended at
    // most once -- no name-level dedup needed (that would be O(#cols^2)).
    def.columns.emplace_back(col->GetName());
    return true;
  };
  // Indexed columns plus indexed-expression dependencies must all be in the
  // index's column set so duckdb initializes their chunk vectors for the
  // BoundIndex appends.
  for (auto col_id : index.GetReferencedColumns()) {
    if (!add_column(col_id)) {
      return std::nullopt;
    }
  }
  if (def.columns.empty()) {
    return std::nullopt;
  }
  def.table = StoreTableName(database, schema, table.GetName());
  return def;
}

std::string StoreIndexName(ObjectId index_id) {
  return absl::StrCat("sdb_idx_", index_id.id());
}

StoreTableDef MakeStoreTableDef(std::string_view database,
                                std::string_view schema, const Table& table) {
  StoreTableDef def;
  def.name = StoreTableName(database, schema, table.GetName());
  const auto& cols = table.Columns();
  std::vector<size_t> mirror_pos(cols.size(), SIZE_MAX);
  def.columns.reserve(cols.size());
  for (size_t i = 0; i < cols.size(); ++i) {
    const auto& col = cols[i];
    if (col.GetId() == Column::kGeneratedPKId) {
      continue;
    }
    mirror_pos[i] = def.columns.size();
    def.columns.push_back({std::string{col.GetName()}, col.type});
  }
  for (const auto& constraint : table.CheckConstraints()) {
    if (auto idx = constraint.IsNotNull(cols)) {
      if (mirror_pos[*idx] != SIZE_MAX) {
        def.not_null.push_back(mirror_pos[*idx]);
      }
    } else if (constraint.expr && constraint.expr->HasExpr()) {
      // Function calls bind against the catalog visible to the store
      // connection -- session/catalog-dependent functions (sequences,
      // user macros) capture the wrong context there. Those checks stay
      // facade-side; the facade passes them to inserts itself.
      bool has_function = false;
      auto scan = [&](this auto& self,
                      const duckdb::ParsedExpression& e) -> void {
        if (e.GetExpressionClass() == duckdb::ExpressionClass::FUNCTION) {
          has_function = true;
          return;
        }
        duckdb::ParsedExpressionIterator::EnumerateChildren(
          e, [&](const duckdb::ParsedExpression& child) { self(child); });
      };
      scan(constraint.expr->GetExpr());
      if (!has_function) {
        def.checks.push_back(constraint.expr->GetExpr().Copy());
      }
    }
  }
  auto names_for = [&](std::span<const Column::Id> ids,
                       std::vector<std::string>& out) {
    for (auto col_id : ids) {
      const auto* col = table.ColumnById(col_id);
      SDB_ASSERT(col);
      if (col->type.IsNested()) {
        // ART cannot index nested types; such keys stay unenforced in the
        // store table until the encoded-key indexes land.
        out.clear();
        return;
      }
      out.emplace_back(col->GetName());
    }
  };
  names_for(table.PKColumns(), def.pk_columns);
  for (const auto& unique : table.UniqueConstraints()) {
    std::vector<std::string> names;
    names_for(unique.columns, names);
    if (!names.empty()) {
      def.unique_constraints.push_back(std::move(names));
    }
  }
  return def;
}

void CatalogStore::WriteContext::PutDefinition(ObjectId parent_id,
                                               ObjectType type, ObjectId id,
                                               std::string_view def) {
  _entries.push_back({
    .op = Op::PutDefinition,
    .key =
      {
        parent_id,
        type,
        id,
      },
    .def = std::string{def},
  });
}

void CatalogStore::WriteContext::PutSequence(ObjectId sequence_id,
                                             uint64_t value) {
  _entries.push_back({
    .op = Op::PutSequence,
    .key =
      {
        .id = sequence_id,
      },
    .sequence_value = value,
  });
}

void CatalogStore::WriteContext::DropDefinition(ObjectId parent_id,
                                                ObjectType type, ObjectId id) {
  _entries.push_back({
    .op = Op::DropDefinition,
    .key =
      {
        .parent_id = parent_id,
        .type = type,
        .id = id,
      },
  });
}

void CatalogStore::WriteContext::DropSequence(ObjectId sequence_id) {
  _entries.push_back({
    .op = Op::DropSequence,
    .key =
      {
        .id = sequence_id,
      },
  });
}

void CatalogStore::WriteContext::CreateStoreTable(StoreTableDef def) {
  _entries.push_back({
    .op = Op::CreateStoreTable,
    .store_table = std::move(def),
  });
}

void CatalogStore::WriteContext::DropStoreTable(std::string name) {
  _entries.push_back({
    .op = Op::DropStoreTable,
    .store_table =
      {
        .name = std::move(name),
      },
  });
}

void CatalogStore::WriteContext::RenameStoreTable(std::string name,
                                                  std::string new_name) {
  _entries.push_back({
    .op = Op::RenameStoreTable,
    .store_table =
      {
        .name = std::move(name),
      },
    .name_a = std::move(new_name),
  });
}

void CatalogStore::WriteContext::RenameStoreColumn(std::string table,
                                                   std::string name,
                                                   std::string new_name) {
  _entries.push_back({
    .op = Op::RenameStoreColumn,
    .store_table =
      {
        .name = std::move(table),
      },
    .name_a = std::move(name),
    .name_b = std::move(new_name),
  });
}

void CatalogStore::WriteContext::DropStoreColumn(std::string table,
                                                 std::string name) {
  _entries.push_back({
    .op = Op::DropStoreColumn,
    .store_table =
      {
        .name = std::move(table),
      },
    .name_a = std::move(name),
  });
}

void CatalogStore::WriteContext::AddStoreColumn(std::string table,
                                                std::string name,
                                                std::string type_sql,
                                                std::string default_sql) {
  _entries.push_back({
    .op = Op::AddStoreColumn,
    .def = std::move(default_sql),
    .store_table =
      {
        .name = std::move(table),
      },
    .name_a = std::move(name),
    .name_b = std::move(type_sql),
  });
}

void CatalogStore::WriteContext::ChangeStoreColumnType(std::string table,
                                                       std::string name,
                                                       std::string type_sql,
                                                       std::string using_sql) {
  _entries.push_back({
    .op = Op::ChangeStoreColumnType,
    .def = std::move(using_sql),
    .store_table =
      {
        .name = std::move(table),
      },
    .name_a = std::move(name),
    .name_b = std::move(type_sql),
  });
}

void CatalogStore::WriteContext::DropStoreForeignKey(std::string table,
                                                     std::string other) {
  _entries.push_back({
    .op = Op::DropStoreForeignKey,
    .store_table =
      {
        .name = std::move(table),
      },
    .name_a = std::move(other),
  });
}

void CatalogStore::WriteContext::DropStoreCheck(std::string table,
                                                std::string expr) {
  _entries.push_back({
    .op = Op::DropStoreCheck,
    .store_table =
      {
        .name = std::move(table),
      },
    .name_a = std::move(expr),
  });
}

void CatalogStore::WriteContext::DropStoreNotNull(std::string table,
                                                  std::string column) {
  _entries.push_back({
    .op = Op::DropStoreNotNull,
    .store_table =
      {
        .name = std::move(table),
      },
    .name_a = std::move(column),
  });
}

void CatalogStore::WriteContext::AddStoreNotNull(std::string table,
                                                 std::string column) {
  _entries.push_back({
    .op = Op::AddStoreNotNull,
    .store_table =
      {
        .name = std::move(table),
      },
    .name_a = std::move(column),
  });
}

void CatalogStore::WriteContext::AddStoreCheck(std::string table,
                                               std::string expr) {
  _entries.push_back({
    .op = Op::AddStoreCheck,
    .store_table =
      {
        .name = std::move(table),
      },
    .name_a = std::move(expr),
  });
}

void CatalogStore::WriteContext::AddStorePrimaryKey(
  std::string table, std::vector<std::string> columns) {
  _entries.push_back({
    .op = Op::AddStorePrimaryKey,
    .store_table =
      {
        .name = std::move(table),
        .pk_columns = std::move(columns),
      },
  });
}

void CatalogStore::WriteContext::AddStoreUnique(
  std::string table, std::vector<std::string> columns) {
  _entries.push_back({
    .op = Op::AddStoreUnique,
    .store_table =
      {
        .name = std::move(table),
        .unique_constraints =
          {
            std::move(columns),
          },
      },
  });
}

void CatalogStore::WriteContext::CreateStoreIndex(StoreIndexDef def) {
  _entries.push_back({
    .op = Op::CreateStoreIndex,
    .store_index = std::move(def),
  });
}

void CatalogStore::WriteContext::DropStoreIndex(ObjectId index_id) {
  _entries.push_back({
    .op = Op::DropStoreIndex,
    .store_index =
      {
        .index_id = index_id,
      },
  });
}

void CatalogStore::WriteContext::WriteTombstone(ObjectId parent_id,
                                                ObjectId id) {
  PutDefinition(parent_id, ObjectType::Tombstone, id, {});
}

CatalogStore::CatalogStore() {
  SDB_ASSERT(gInstance == nullptr);
  gInstance = this;
}

CatalogStore::~CatalogStore() { gInstance = nullptr; }

void CatalogStore::Initialize(std::string_view database_directory) {
  namespace fs = std::filesystem;
  const auto dir =
    fs::path{database_directory} / StaticStrings::kCatalogStoreRoot;
  std::error_code ec;
  fs::create_directories(dir, ec);
  if (ec) {
    SDB_FATAL(STARTUP, "catalog store: cannot create directory '", dir.string(),
              "': ", ec.message());
  }
  const auto file = (dir / kStoreFile).string();

  {
    absl::MutexLock lock{&_mutex};
    absl::MutexLock seq_lock{&_seq_mutex};
    _conn = DuckDBEngine::Instance().CreateConnection();
    _seq_conn = DuckDBEngine::Instance().CreateConnection();

    ExecOrFatal(
      *_conn, absl::StrCat("ATTACH '", absl::StrReplaceAll(file, {{"'", "''"}}),
                           "' AS \"", kStoreAlias, "\""));
    ExecOrFatal(*_conn,
                absl::StrCat("CREATE TABLE IF NOT EXISTS ", kCatalogTable,
                             " (parent_id UBIGINT, type UTINYINT, id UBIGINT, "
                             "def BLOB)"));
    ExecOrFatal(*_conn,
                absl::StrCat("CREATE TABLE IF NOT EXISTS ", kSequenceTable,
                             " (id UBIGINT, counter UBIGINT)"));

    _delete_definition = PrepareOrFatal(
      *_conn, absl::StrCat("DELETE FROM ", kCatalogTable,
                           " WHERE parent_id = $1 AND type = $2 AND id = $3"));
    _insert_definition = PrepareOrFatal(
      *_conn,
      absl::StrCat("INSERT INTO ", kCatalogTable, " VALUES ($1, $2, $3, $4)"));
    _delete_by_parent_type = PrepareOrFatal(
      *_conn, absl::StrCat("DELETE FROM ", kCatalogTable,
                           " WHERE parent_id = $1 AND type = $2"));
    _delete_by_parent = PrepareOrFatal(
      *_conn,
      absl::StrCat("DELETE FROM ", kCatalogTable, " WHERE parent_id = $1"));
    _select_definitions = PrepareOrFatal(
      *_conn, absl::StrCat("SELECT id, def FROM ", kCatalogTable,
                           " WHERE parent_id = $1 AND type = $2 ORDER BY id"));
    _delete_sequence_batch = PrepareOrFatal(
      *_conn, absl::StrCat("DELETE FROM ", kSequenceTable, " WHERE id = $1"));
    _insert_sequence_batch = PrepareOrFatal(
      *_conn, absl::StrCat("INSERT INTO ", kSequenceTable, " VALUES ($1, $2)"));

    _select_sequence = PrepareOrFatal(
      *_seq_conn,
      absl::StrCat("SELECT counter FROM ", kSequenceTable, " WHERE id = $1"));
    _delete_sequence = PrepareOrFatal(
      *_seq_conn,
      absl::StrCat("DELETE FROM ", kSequenceTable, " WHERE id = $1"));
    _insert_sequence = PrepareOrFatal(
      *_seq_conn,
      absl::StrCat("INSERT INTO ", kSequenceTable, " VALUES ($1, $2)"));
  }

  EnsureSystemDatabase();
}

void CatalogStore::Shutdown() {
  {
    absl::MutexLock lock{&_mutex};
    absl::MutexLock seq_lock{&_seq_mutex};
    _delete_definition.reset();
    _insert_definition.reset();
    _delete_by_parent_type.reset();
    _delete_by_parent.reset();
    _select_definitions.reset();
    _delete_sequence_batch.reset();
    _insert_sequence_batch.reset();
    _select_sequence.reset();
    _delete_sequence.reset();
    _insert_sequence.reset();
    _conn.reset();
    _seq_conn.reset();
  }
  auto conn = DuckDBEngine::Instance().CreateConnection();
  auto res = conn->Query(absl::StrCat("DETACH \"", kStoreAlias, "\""));
  if (res->HasError()) {
    SDB_WARN(STARTUP, "catalog store: detach failed: ", res->GetError());
  }
}

absl::Status CatalogStore::ExecuteEntries(
  std::vector<WriteContext::Entry>& entries) {
  absl::MutexLock lock{&_mutex};
  return RunInTransaction(*_conn, [&]() -> absl::Status {
    for (const auto& entry : entries) {
      switch (entry.op) {
        case WriteContext::Op::PutDefinition: {
          if (auto r = Exec(*_delete_definition,
                            {IdValue(entry.key.parent_id),
                             TypeValue(entry.key.type), IdValue(entry.key.id)});
              !r.ok()) {
            return r;
          }
          if (auto r =
                Exec(*_insert_definition,
                     {IdValue(entry.key.parent_id), TypeValue(entry.key.type),
                      IdValue(entry.key.id), DefValue(entry.def)});
              !r.ok()) {
            return r;
          }
          break;
        }
        case WriteContext::Op::DropDefinition: {
          if (auto r = Exec(*_delete_definition,
                            {IdValue(entry.key.parent_id),
                             TypeValue(entry.key.type), IdValue(entry.key.id)});
              !r.ok()) {
            return r;
          }
          break;
        }
        case WriteContext::Op::PutSequence: {
          if (auto r = Exec(*_delete_sequence_batch, {IdValue(entry.key.id)});
              !r.ok()) {
            return r;
          }
          if (auto r = Exec(*_insert_sequence_batch,
                            {IdValue(entry.key.id),
                             duckdb::Value::UBIGINT(entry.sequence_value)});
              !r.ok()) {
            return r;
          }
          break;
        }
        case WriteContext::Op::DropSequence: {
          if (auto r = Exec(*_delete_sequence_batch, {IdValue(entry.key.id)});
              !r.ok()) {
            return r;
          }
          break;
        }
        case WriteContext::Op::CreateStoreTable: {
          if (auto r = ExecuteCreateStoreTable(entry.store_table); !r.ok()) {
            return r;
          }
          break;
        }
        case WriteContext::Op::DropStoreTable: {
          auto res = _conn->Query(
            absl::StrCat("DROP TABLE IF EXISTS \"", kStoreAlias, "\".main.",
                         QuotedIdent(entry.store_table.name)));
          if (res->HasError()) {
            return absl::InternalError(res->GetError());
          }
          break;
        }
        case WriteContext::Op::RenameStoreTable: {
          auto res = _conn->Query(
            absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                         QuotedIdent(entry.store_table.name), " RENAME TO ",
                         QuotedIdent(entry.name_a)));
          if (res->HasError()) {
            return absl::InternalError(res->GetError());
          }
          break;
        }
        case WriteContext::Op::RenameStoreColumn: {
          auto res = _conn->Query(absl::StrCat(
            "ALTER TABLE \"", kStoreAlias, "\".main.",
            QuotedIdent(entry.store_table.name), " RENAME COLUMN ",
            QuotedIdent(entry.name_a), " TO ", QuotedIdent(entry.name_b)));
          if (res->HasError()) {
            return absl::InternalError(res->GetError());
          }
          break;
        }
        case WriteContext::Op::DropStoreCheck: {
          try {
            auto& context = *_conn->context;
            duckdb::AlterEntryData data{
              duckdb::QualifiedName{duckdb::Identifier{kStoreAlias},
                                    duckdb::Identifier{"main"},
                                    duckdb::Identifier{entry.store_table.name}},
              duckdb::OnEntryNotFound::RETURN_NULL};
            duckdb::DropConstraintInfo info{std::move(data), entry.name_a, true,
                                            false};
            auto& catalog = duckdb::Catalog::GetCatalog(
              context, duckdb::Identifier{kStoreAlias});
            catalog.Alter(context, info);
          } catch (const std::exception& e) {
            return absl::InternalError(e.what());
          }
          break;
        }
        case WriteContext::Op::DropStoreNotNull: {
          auto res = _conn->Query(
            absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                         QuotedIdent(entry.store_table.name), " ALTER COLUMN ",
                         QuotedIdent(entry.name_a), " DROP NOT NULL"));
          if (res->HasError()) {
            return absl::InternalError(res->GetError());
          }
          break;
        }
        case WriteContext::Op::AddStoreNotNull: {
          auto res = _conn->Query(
            absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                         QuotedIdent(entry.store_table.name), " ALTER COLUMN ",
                         QuotedIdent(entry.name_a), " SET NOT NULL"));
          if (res->HasError()) {
            return absl::InternalError(res->GetError());
          }
          break;
        }
        case WriteContext::Op::AddStoreCheck: {
          auto res =
            _conn->Query(absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                                      QuotedIdent(entry.store_table.name),
                                      " ADD CHECK (", entry.name_a, ")"));
          if (res->HasError()) {
            return absl::InternalError(res->GetError());
          }
          break;
        }
        case WriteContext::Op::AddStorePrimaryKey: {
          std::string cols;
          for (const auto& c : entry.store_table.pk_columns) {
            if (!cols.empty()) {
              cols += ", ";
            }
            cols += QuotedIdent(c);
          }
          auto res =
            _conn->Query(absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                                      QuotedIdent(entry.store_table.name),
                                      " ADD PRIMARY KEY (", cols, ")"));
          if (res->HasError()) {
            return absl::InternalError(res->GetError());
          }
          break;
        }
        case WriteContext::Op::AddStoreUnique: {
          std::string cols;
          for (const auto& c : entry.store_table.unique_constraints.front()) {
            if (!cols.empty()) {
              cols += ", ";
            }
            cols += QuotedIdent(c);
          }
          auto res = _conn->Query(absl::StrCat(
            "ALTER TABLE \"", kStoreAlias, "\".main.",
            QuotedIdent(entry.store_table.name), " ADD UNIQUE (", cols, ")"));
          if (res->HasError()) {
            return absl::InternalError(res->GetError());
          }
          break;
        }
        case WriteContext::Op::CreateStoreIndex: {
          const auto& def = entry.store_index;
          std::string sql;
          if (def.kind == StoreIndexDef::Kind::Inverted) {
            std::string cols;
            for (const auto& name : def.columns) {
              if (!cols.empty()) {
                cols += ", ";
              }
              cols += QuotedIdent(name);
            }
            sql = absl::StrCat("CREATE INDEX ",
                               QuotedIdent(StoreIndexName(def.index_id)),
                               " ON \"", kStoreAlias, "\".main.",
                               QuotedIdent(def.table), " USING inverted(", cols,
                               ") WITH (sdb_table_id=", def.table_id.id(),
                               ", sdb_index_id=", def.index_id.id(), ")");
          } else {
            // def.keys already holds per-key SQL in order (quoted column
            // identifiers or parenthesized expressions); join verbatim.
            std::string cols;
            for (const auto& key : def.keys) {
              if (!cols.empty()) {
                cols += ", ";
              }
              cols += key;
            }
            sql = absl::StrCat("CREATE ", def.unique ? "UNIQUE " : "", "INDEX ",
                               QuotedIdent(StoreIndexName(def.index_id)),
                               " ON \"", kStoreAlias, "\".main.",
                               QuotedIdent(def.table), " (", cols, ")");
          }
          auto res = _conn->Query(sql);
          if (res->HasError()) {
            return absl::InternalError(res->GetError());
          }
          break;
        }
        case WriteContext::Op::DropStoreIndex: {
          auto res = _conn->Query(absl::StrCat(
            "DROP INDEX IF EXISTS \"", kStoreAlias, "\".main.",
            QuotedIdent(StoreIndexName(entry.store_index.index_id))));
          if (res->HasError()) {
            return absl::InternalError(res->GetError());
          }
          break;
        }
        case WriteContext::Op::DropStoreForeignKey: {
          try {
            auto& context = *_conn->context;
            duckdb::AlterEntryData data{
              duckdb::QualifiedName{duckdb::Identifier{kStoreAlias},
                                    duckdb::Identifier{"main"},
                                    duckdb::Identifier{entry.store_table.name}},
              duckdb::OnEntryNotFound::RETURN_NULL};
            duckdb::AlterForeignKeyInfo info{
              std::move(data),
              duckdb::Identifier{entry.name_a},
              {},
              {},
              {},
              {},
              duckdb::AlterForeignKeyType::AFT_DELETE};
            auto& catalog = duckdb::Catalog::GetCatalog(
              context, duckdb::Identifier{kStoreAlias});
            catalog.Alter(context, info);
          } catch (const std::exception& e) {
            return absl::InternalError(e.what());
          }
          break;
        }
        case WriteContext::Op::DropStoreColumn: {
          auto res = _conn->Query(
            absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                         QuotedIdent(entry.store_table.name), " DROP COLUMN ",
                         QuotedIdent(entry.name_a)));
          if (res->HasError()) {
            return absl::InternalError(res->GetError());
          }
          break;
        }
        case WriteContext::Op::AddStoreColumn: {
          std::string base =
            absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                         QuotedIdent(entry.store_table.name), " ADD COLUMN ",
                         QuotedIdent(entry.name_a), " ", entry.name_b);
          std::string sql = base;
          if (!entry.def.empty()) {
            absl::StrAppend(&sql, " DEFAULT ", entry.def);
          }
          auto res = _conn->Query(sql);
          if (res->HasError() && !entry.def.empty()) {
            // The DEFAULT may call a function the store connection can't
            // resolve (sequences, user macros bind facade-side). Add the
            // column without it; existing rows get NULL and the facade fills
            // the default for future inserts.
            SDB_WARN(GENERAL, "store table \"", entry.store_table.name,
                     "\": ADD COLUMN \"", entry.name_a,
                     "\" DEFAULT not mirrored: ", res->GetError());
            res = _conn->Query(base);
          }
          if (res->HasError()) {
            return absl::InternalError(res->GetError());
          }
          break;
        }
        case WriteContext::Op::ChangeStoreColumnType: {
          std::string sql =
            absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                         QuotedIdent(entry.store_table.name), " ALTER COLUMN ",
                         QuotedIdent(entry.name_a), " TYPE ", entry.name_b);
          if (!entry.def.empty()) {
            absl::StrAppend(&sql, " USING ", entry.def);
          }
          auto res = _conn->Query(sql);
          if (res->HasError()) {
            return absl::InternalError(res->GetError());
          }
          break;
        }
      }
    }
    return absl::OkStatus();
  });
}

absl::Status CatalogStore::ExecuteCreateStoreTable(const StoreTableDef& def) {
  auto r = ExecuteCreateStoreTableImpl(def, /*with_checks=*/true);
  if (!r.ok() && !def.checks.empty()) {
    // CHECK expressions may reference facade-only types or functions the store
    // catalog cannot bind. The store table omits them; the facade-bound CHECK
    // is carried into every write plan instead (RetargetStoreConstraints), so
    // it is still enforced on INSERT/UPDATE/upsert.
    auto retry = ExecuteCreateStoreTableImpl(def, /*with_checks=*/false);
    if (retry.ok()) {
      SDB_TRACE(GENERAL, "store table \"", def.name,
                "\": CHECK constraints kept facade-side: ", r.message());
      return retry;
    }
  }
  return r;
}

absl::Status CatalogStore::ExecuteCreateStoreTableImpl(const StoreTableDef& def,
                                                       bool with_checks) try {
  auto info = duckdb::make_uniq<duckdb::CreateTableInfo>(duckdb::QualifiedName{
    duckdb::Identifier{kStoreAlias}, duckdb::Identifier{"main"},
    duckdb::Identifier{def.name}});
  for (const auto& col : def.columns) {
    info->columns.AddColumn(
      duckdb::ColumnDefinition{duckdb::Identifier{col.name}, col.type});
  }
  for (auto idx : def.not_null) {
    info->constraints.push_back(
      duckdb::make_uniq<duckdb::NotNullConstraint>(duckdb::LogicalIndex{idx}));
  }
  if (!def.pk_columns.empty()) {
    duckdb::vector<duckdb::Identifier> pk;
    pk.reserve(def.pk_columns.size());
    for (const auto& name : def.pk_columns) {
      pk.emplace_back(name);
    }
    info->constraints.push_back(
      duckdb::make_uniq<duckdb::UniqueConstraint>(std::move(pk), true));
  }
  for (const auto& unique : def.unique_constraints) {
    duckdb::vector<duckdb::Identifier> names;
    names.reserve(unique.size());
    for (const auto& name : unique) {
      names.emplace_back(name);
    }
    info->constraints.push_back(
      duckdb::make_uniq<duckdb::UniqueConstraint>(std::move(names), false));
  }
  for (const auto& fk : def.foreign_keys) {
    duckdb::vector<duckdb::Identifier> fk_cols;
    duckdb::vector<duckdb::Identifier> pk_cols;
    for (const auto& name : fk.columns) {
      fk_cols.emplace_back(name);
    }
    for (const auto& name : fk.referenced_columns) {
      pk_cols.emplace_back(name);
    }
    duckdb::ForeignKeyInfo fk_info;
    fk_info.type = duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
    fk_info.table = duckdb::Identifier{fk.referenced_table};
    info->constraints.push_back(duckdb::make_uniq<duckdb::ForeignKeyConstraint>(
      std::move(pk_cols), std::move(fk_cols), std::move(fk_info)));
  }
  if (with_checks) {
    for (const auto& check : def.checks) {
      info->constraints.push_back(
        duckdb::make_uniq<duckdb::CheckConstraint>(check->Copy()));
    }
  }
  auto& context = *_conn->context;
  auto& catalog =
    duckdb::Catalog::GetCatalog(context, duckdb::Identifier{kStoreAlias});
  // Acquire the store's (shared) checkpoint lock before creating the table.
  // The direct catalog.CreateTable() call bypasses the statement-execution
  // path that normally registers the modification and takes this lock (which
  // serenedb's DROP/ALTER do go through, via _conn->Query). Without it, a
  // store table can be created concurrently with a store checkpoint; the new
  // table is then not in the checkpoint's snapshot, so MergeCheckpointDeltas
  // never merges its index's added_data delta -> the entry is stranded -> a
  // later delete fails "0 out of 1" -> "Failed to rollback transaction".
  duckdb::MetaTransaction::Get(context).ModifyDatabase(
    catalog.GetAttached(),
    duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);
  SDB_WAIT_ON_FAILURE("pause_store_create_table");
  catalog.CreateTable(context, std::move(info));
  return absl::OkStatus();
} catch (const std::exception& e) {
  return absl::InternalError(e.what());
}

void CatalogStore::ValidateStoreTable(const StoreTableDef& def) {
#ifdef SDB_DEV
  absl::MutexLock lock{&_mutex};
  try {
    _conn->BeginTransaction();
  } catch (const std::exception& e) {
    SDB_ASSERT(false, "store table validation: ", e.what());
    return;
  }
  try {
    auto& context = *_conn->context;
    duckdb::EntryLookupInfo lookup{
      duckdb::CatalogType::TABLE_ENTRY,
      duckdb::QualifiedName{duckdb::Identifier{kStoreAlias},
                            duckdb::Identifier{"main"},
                            duckdb::Identifier{def.name}}};
    auto entry = duckdb::Catalog::GetEntry(
      context, lookup, duckdb::OnEntryNotFound::RETURN_NULL);
    SDB_ASSERT(entry, "store table missing for ", def.name);
    if (entry) {
      const auto& columns =
        entry->Cast<duckdb::TableCatalogEntry>().GetColumns();
      SDB_ASSERT(columns.LogicalColumnCount() == def.columns.size(),
                 "store table column count mismatch for ", def.name);
      duckdb::idx_t i = 0;
      for (const auto& col : columns.Logical()) {
        if (i >= def.columns.size()) {
          break;
        }
        SDB_ASSERT(col.Name().GetIdentifierName() == def.columns[i].name,
                   "store table column name mismatch for ", def.name, ": ",
                   col.Name().GetIdentifierName(), " vs ", def.columns[i].name);
        SDB_ASSERT(col.Type() == def.columns[i].type,
                   "store table column type mismatch for ", def.name, ".",
                   def.columns[i].name);
        ++i;
      }
    }
  } catch (const std::exception& e) {
    SDB_ASSERT(false, "store table validation: ", e.what());
  }
  try {
    _conn->Rollback();
  } catch (const std::exception&) {
  }
#endif
}

void CatalogStore::CreateDefinition(ObjectId parent_id, ObjectType type,
                                    ObjectId id, std::string_view def) {
  WriteContext ctx;
  ctx.PutDefinition(parent_id, type, id, def);
  if (auto r = ExecuteEntries(ctx._entries); !r.ok()) {
    SDB_THROW(ERROR_INTERNAL, r.message());
  }
}

void CatalogStore::Write(absl::FunctionRef<void(WriteContext&)> fill) {
  WriteContext ctx;
  fill(ctx);
  if (auto r = ExecuteEntries(ctx._entries); !r.ok()) {
    SDB_THROW(ERROR_INTERNAL, r.message());
  }
}

void CatalogStore::DropDefinition(ObjectId parent_id, ObjectType type,
                                  ObjectId id) {
  WriteContext ctx;
  ctx.DropDefinition(parent_id, type, id);
  if (auto r = ExecuteEntries(ctx._entries); !r.ok()) {
    SDB_THROW(ERROR_INTERNAL, r.message());
  }
}

void CatalogStore::DropSequence(ObjectId sequence_id) {
  WriteContext ctx;
  ctx.DropSequence(sequence_id);
  if (auto r = ExecuteEntries(ctx._entries); !r.ok()) {
    SDB_THROW(ERROR_INTERNAL, r.message());
  }
}

void CatalogStore::DropEntry(ObjectId parent_id, ObjectType type) {
  absl::MutexLock lock{&_mutex};
  auto r = RunInTransaction(*_conn, [&]() -> absl::Status {
    return Exec(*_delete_by_parent_type, {IdValue(parent_id), TypeValue(type)});
  });
  if (!r.ok()) {
    SDB_THROW(ERROR_INTERNAL, r.message());
  }
}

void CatalogStore::DropEntry(ObjectId parent_id) {
  absl::MutexLock lock{&_mutex};
  auto r = RunInTransaction(*_conn, [&]() -> absl::Status {
    return Exec(*_delete_by_parent, {IdValue(parent_id)});
  });
  if (!r.ok()) {
    SDB_THROW(ERROR_INTERNAL, r.message());
  }
}

void CatalogStore::WriteTombstone(ObjectId parent_id, ObjectId id) {
  CreateDefinition(parent_id, ObjectType::Tombstone, id, {});
}

void CatalogStore::VisitDefinitions(
  ObjectId parent_id, ObjectType type,
  absl::FunctionRef<bool(Key, std::string_view)> visitor) {
  duckdb::unique_ptr<duckdb::QueryResult> res;
  {
    absl::MutexLock lock{&_mutex};
    duckdb::vector<duckdb::Value> params{IdValue(parent_id), TypeValue(type)};
    res = _select_definitions->Execute(params, /*allow_stream_result=*/false);
  }
  if (res->HasError()) {
    SDB_THROW(ERROR_INTERNAL, res->GetError());
  }
  while (auto chunk = res->Fetch()) {
    chunk->Flatten();
    const auto count = chunk->size();
    const auto* ids = duckdb::FlatVector::GetData<uint64_t>(chunk->data[0]);
    const auto* defs =
      duckdb::FlatVector::GetData<duckdb::string_t>(chunk->data[1]);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      if (!visitor(Key{parent_id, type, ObjectId{ids[i]}},
                   std::string_view{defs[i].GetData(), defs[i].GetSize()})) {
        return;
      }
    }
  }
}

void CatalogStore::BootConsumeCatalog(duckdb::DataChunk& input) {
  SDB_ENSURE(_boot_loading.load(std::memory_order_acquire), ERROR_FORBIDDEN,
             "sdb_init_catalog is internal to server startup");
  duckdb::UnifiedVectorFormat parents;
  duckdb::UnifiedVectorFormat types;
  duckdb::UnifiedVectorFormat ids;
  duckdb::UnifiedVectorFormat defs;
  input.data[0].ToUnifiedFormat(parents);
  input.data[1].ToUnifiedFormat(types);
  input.data[2].ToUnifiedFormat(ids);
  input.data[3].ToUnifiedFormat(defs);
  const auto* parent_data =
    duckdb::UnifiedVectorFormat::GetData<uint64_t>(parents);
  const auto* type_data = duckdb::UnifiedVectorFormat::GetData<uint8_t>(types);
  const auto* id_data = duckdb::UnifiedVectorFormat::GetData<uint64_t>(ids);
  const auto* def_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(defs);
  const auto count = input.size();
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto parent = parent_data[parents.sel->get_index(i)];
    const auto type = type_data[types.sel->get_index(i)];
    const auto id = id_data[ids.sel->get_index(i)];
    const auto& def = def_data[defs.sel->get_index(i)];
    _boot_defs[{parent, type}].push_back(
      {.id = ObjectId{id}, .def = std::string{def.GetData(), def.GetSize()}});
  }
}

void CatalogStore::BootConsumeSequences(duckdb::DataChunk& input) {
  SDB_ENSURE(_boot_loading.load(std::memory_order_acquire), ERROR_FORBIDDEN,
             "sdb_init_sequences is internal to server startup");
  duckdb::UnifiedVectorFormat ids;
  duckdb::UnifiedVectorFormat counters;
  input.data[0].ToUnifiedFormat(ids);
  input.data[1].ToUnifiedFormat(counters);
  const auto* id_data = duckdb::UnifiedVectorFormat::GetData<uint64_t>(ids);
  const auto* counter_data =
    duckdb::UnifiedVectorFormat::GetData<uint64_t>(counters);
  const auto count = input.size();
  for (duckdb::idx_t i = 0; i < count; ++i) {
    _boot_sequences[id_data[ids.sel->get_index(i)]] =
      counter_data[counters.sel->get_index(i)];
  }
}

void CatalogStore::LoadBootState() {
  _boot_defs.clear();
  _boot_sequences.clear();
  _boot_loading.store(true, std::memory_order_release);
  auto status = absl::OkStatus();
  {
    absl::MutexLock lock{&_mutex};
    auto res = _conn->Query(
      absl::StrCat("SELECT * FROM sdb_init_catalog((SELECT parent_id, type, "
                   "id, def FROM ",
                   kCatalogTable, "))"));
    if (res->HasError()) {
      status = absl::InternalError(res->GetError());
    } else {
      res = _conn->Query(absl::StrCat(
        "SELECT * FROM sdb_init_sequences((SELECT id, counter FROM ",
        kSequenceTable, "))"));
      if (res->HasError()) {
        status = absl::InternalError(res->GetError());
      }
    }
  }
  _boot_loading.store(false, std::memory_order_release);
  if (!status.ok()) {
    ReleaseBootState();
    SDB_THROW(ERROR_INTERNAL, status.message());
  }
  for (auto& [key, defs] : _boot_defs) {
    std::sort(defs.begin(), defs.end(),
              [](const BootDef& lhs, const BootDef& rhs) {
                return lhs.id.id() < rhs.id.id();
              });
  }
}

void CatalogStore::VisitBoot(
  ObjectId parent_id, ObjectType type,
  absl::FunctionRef<bool(Key, std::string_view)> visitor) const {
  const auto it = _boot_defs.find({parent_id.id(), static_cast<uint8_t>(type)});
  if (it == _boot_defs.end()) {
    return;
  }
  for (const auto& def : it->second) {
    if (!visitor(Key{parent_id, type, def.id}, def.def)) {
      return;
    }
  }
}

bool CatalogStore::TryGetBootSequenceValue(ObjectId sequence_id,
                                           uint64_t& value) const {
  const auto it = _boot_sequences.find(sequence_id.id());
  if (it == _boot_sequences.end()) {
    return false;
  }
  value = it->second;
  return true;
}

void CatalogStore::ReleaseBootState() {
  _boot_defs.clear();
  _boot_sequences.clear();
}

uint64_t CatalogStore::BootDefsLoaded() const {
  uint64_t total = 0;
  for (const auto& [key, defs] : _boot_defs) {
    total += defs.size();
  }
  return total;
}

uint64_t CatalogStore::BootSequencesLoaded() const {
  return _boot_sequences.size();
}

void CatalogStore::PutSequenceValue(ObjectId sequence_id, uint64_t value) {
  absl::MutexLock lock{&_seq_mutex};
  auto r = RunInTransaction(*_seq_conn, [&]() -> absl::Status {
    if (auto r = Exec(*_delete_sequence, {IdValue(sequence_id)}); !r.ok()) {
      return r;
    }
    return Exec(*_insert_sequence,
                {IdValue(sequence_id), duckdb::Value::UBIGINT(value)});
  });
  if (!r.ok()) {
    SDB_THROW(ERROR_INTERNAL, r.message());
  }
}

void CatalogStore::GetSequenceValue(ObjectId sequence_id, uint64_t& value) {
  value = 0;
  duckdb::unique_ptr<duckdb::QueryResult> res;
  {
    absl::MutexLock lock{&_seq_mutex};
    duckdb::vector<duckdb::Value> params{IdValue(sequence_id)};
    res = _select_sequence->Execute(params, /*allow_stream_result=*/false);
  }
  if (res->HasError()) {
    SDB_THROW(ERROR_INTERNAL, res->GetError());
  }
  if (auto chunk = res->Fetch(); chunk && chunk->size() > 0) {
    SDB_ASSERT(chunk->size() == 1);
    chunk->Flatten();
    value = duckdb::FlatVector::GetData<uint64_t>(chunk->data[0])[0];
  }
}

void CatalogStore::EnsureSystemDatabase() {
  bool has_system = false;
  VisitDefinitions(id::kInstance, ObjectType::Database,
                   [&](Key key, std::string_view) {
                     if (key.id == id::kSystemDB) {
                       has_system = true;
                       return false;  // found, stop
                     }
                     return true;  // keep scanning
                   });

  if (has_system) {
    SDB_TRACE(STARTUP, "Found system database");
    return;
  }

  Database database{catalog::Permissions{id::kRootUser}, id::kSystemDB,
                    StaticStrings::kDefaultDatabase};
  duckdb::MemoryStream stream;
  auto database_bytes = SerializeObject(database, stream);
  CreateDefinition(id::kInstance, ObjectType::Database, id::kSystemDB,
                   database_bytes);

  const auto schema_id = NextId();
  Schema schema{catalog::Permissions{id::kRootUser}, id::kSystemDB, schema_id,
                StaticStrings::kPublic};
  auto schema_bytes = SerializeObject(schema, stream);
  CreateDefinition(id::kSystemDB, ObjectType::Schema, schema_id, schema_bytes);
}

CatalogStore& GetCatalogStore() { return CatalogStore::instance(); }

namespace {

void CheckInitInput(const duckdb::vector<duckdb::LogicalType>& types,
                    std::initializer_list<duckdb::LogicalTypeId> expected,
                    std::string_view fn) {
  if (types.size() != expected.size() ||
      !std::equal(expected.begin(), expected.end(), types.begin(),
                  [](duckdb::LogicalTypeId id, const duckdb::LogicalType& t) {
                    return t.id() == id;
                  })) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG(fn, ": unexpected input table shape"));
  }
}

duckdb::unique_ptr<duckdb::FunctionData> BindInitCatalog(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  CheckInitInput(
    input.input_table_types,
    {duckdb::LogicalTypeId::UBIGINT, duckdb::LogicalTypeId::UTINYINT,
     duckdb::LogicalTypeId::UBIGINT, duckdb::LogicalTypeId::BLOB},
    "sdb_init_catalog");
  return_types.emplace_back(duckdb::LogicalType::UBIGINT);
  names.emplace_back("loaded");
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

duckdb::unique_ptr<duckdb::FunctionData> BindInitSequences(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  CheckInitInput(
    input.input_table_types,
    {duckdb::LogicalTypeId::UBIGINT, duckdb::LogicalTypeId::UBIGINT},
    "sdb_init_sequences");
  return_types.emplace_back(duckdb::LogicalType::UBIGINT);
  names.emplace_back("loaded");
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitBootGlobal(
  duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
  // Default MaxThreads() == 1 caps the pipeline at one thread: the boot
  // maps are built lock-free.
  return duckdb::make_uniq<duckdb::GlobalTableFunctionState>();
}

duckdb::OperatorResultType InitCatalogExec(duckdb::ExecutionContext&,
                                           duckdb::TableFunctionInput&,
                                           duckdb::DataChunk& input,
                                           duckdb::DataChunk& output) {
  CatalogStore::instance().BootConsumeCatalog(input);
  output.SetCardinality(0);
  return duckdb::OperatorResultType::NEED_MORE_INPUT;
}

duckdb::OperatorResultType InitSequencesExec(duckdb::ExecutionContext&,
                                             duckdb::TableFunctionInput&,
                                             duckdb::DataChunk& input,
                                             duckdb::DataChunk& output) {
  CatalogStore::instance().BootConsumeSequences(input);
  output.SetCardinality(0);
  return duckdb::OperatorResultType::NEED_MORE_INPUT;
}

duckdb::OperatorFinalizeResultType InitCatalogFinal(duckdb::ExecutionContext&,
                                                    duckdb::TableFunctionInput&,
                                                    duckdb::DataChunk& output) {
  output.SetCardinality(1);
  output.SetValue(
    0, 0, duckdb::Value::UBIGINT(CatalogStore::instance().BootDefsLoaded()));
  return duckdb::OperatorFinalizeResultType::FINISHED;
}

duckdb::OperatorFinalizeResultType InitSequencesFinal(
  duckdb::ExecutionContext&, duckdb::TableFunctionInput&,
  duckdb::DataChunk& output) {
  output.SetCardinality(1);
  output.SetValue(
    0, 0,
    duckdb::Value::UBIGINT(CatalogStore::instance().BootSequencesLoaded()));
  return duckdb::OperatorFinalizeResultType::FINISHED;
}

}  // namespace

void RegisterCatalogStoreFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  {
    duckdb::TableFunction fn{"sdb_init_catalog",
                             {duckdb::LogicalType::TABLE},
                             nullptr,
                             BindInitCatalog};
    fn.init_global = InitBootGlobal;
    fn.in_out_function = InitCatalogExec;
    fn.in_out_function_final = InitCatalogFinal;
    loader.RegisterFunction(fn);
  }
  {
    duckdb::TableFunction fn{"sdb_init_sequences",
                             {duckdb::LogicalType::TABLE},
                             nullptr,
                             BindInitSequences};
    fn.init_global = InitBootGlobal;
    fn.in_out_function = InitSequencesExec;
    fn.in_out_function_final = InitSequencesFinal;
    loader.RegisterFunction(fn);
  }
}

}  // namespace sdb::catalog
