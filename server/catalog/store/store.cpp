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

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_map.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cstring>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/execution/index/art/art.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_context_state.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/column_definition.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <exception>
#include <filesystem>
#include <ranges>
#include <utility>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/file_utils.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/deferred_writes.h"
#include "catalog/duckdb_catalog.h"
#include "catalog/duckdb_catalog_sets.h"
#include "catalog/duckdb_object_entry.h"
#include "catalog/duckdb_table_entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/secondary_index.h"
#include "catalog/store/data_store.h"
#include "catalog/table.h"
#include "catalog/tokenizer.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {
namespace {

// Compact once dead records dominate and the file is worth rewriting.
constexpr uint64_t kCompactMinBytes = 1U << 20U;

// Ids are handed out in order, so this is also creation order -- what makes a
// checkpoint's records reproducible from one run to the next.
void SortById(auto& objects) {
  std::ranges::sort(objects, [](const auto& lhs, const auto& rhs) {
    return lhs->GetId().id() < rhs->GetId().id();
  });
}

// Every index of `database_id` in id order, for the checkpoint's per-relation
// walk. Through the committing transaction, like every other kind here.
std::vector<IndexInfoRef> DatabaseIndexes(duckdb::ClientContext* context,
                                          ObjectId database_id) {
  std::vector<IndexInfoRef> indexes;
  catalog::VisitIndexes(context, database_id, [&](const IndexInfoRef& index) {
    indexes.push_back(index);
  });
  SortById(indexes);
  return indexes;
}

}  // namespace

duckdb::optional_ptr<duckdb::TableCatalogEntry> HostTableEntry(
  duckdb::AttachedDatabase& db, uint64_t catalog_id) {
  if (TryGetCatalog() == nullptr) {
    return nullptr;
  }
  // Off this catalog's own sets, not through the database manager: an attach
  // reads its own tables back before the attachment is in it.
  auto& catalog = db.GetCatalog().Cast<catalog::SereneDBCatalog>();
  return catalog.LookupTableById(catalog.CommittedRead(), catalog_id);
}

ObjectId StoreDatabaseId(duckdb::AttachedDatabase& db) {
  auto& catalog = db.GetCatalog();
  if (catalog.GetCatalogType() != catalog::kSereneDBCatalogType) {
    return {};
  }
  return catalog.Cast<catalog::SereneDBCatalog>().GetDatabaseId();
}

bool IsStoreDatabase(duckdb::AttachedDatabase& db) {
  return StoreDatabaseId(db).isSet();
}

duckdb::optional_ptr<duckdb::AttachedDatabase> TryStoreDatabase(
  duckdb::ClientContext& context, ObjectId database_id) {
  // Through the name rather than a scan of the database manager: the
  // attachment alias is the database name, and every read path that resolves a
  // store table goes through here.
  auto& manager = duckdb::DatabaseManager::Get(context);
  auto database = catalog::FindDatabase(&context, database_id);
  if (!database) {
    return nullptr;
  }
  return manager.GetDatabase(context, duckdb::Identifier{database.Name()});
}

duckdb::shared_ptr<duckdb::AttachedDatabase> TryStoreDatabase(
  ObjectId database_id) {
  auto database = catalog::FindDatabase(nullptr, database_id);
  if (!database) {
    return nullptr;
  }
  return duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance())
    .GetDatabase(duckdb::Identifier{database.Name()});
}

duckdb::unique_ptr<duckdb::CreateIndexInfo> MakeStoreIndexInfo(
  const duckdb::CreateTableInfo& table, const CreateIndexInfoBase& index) {
  if (catalog::TableEngineOf(table) != TableEngine::Transactional) {
    return nullptr;
  }
  auto info = duckdb::make_uniq<duckdb::CreateIndexInfo>();
  info->oid = index.GetId().id();
  info->SetIndexName(duckdb::Identifier{index.GetName()});
  info->index_type = index.index_type;

  // Catalog-named types (enums, composites, JSON) cannot be re-parsed by the
  // store connection during the ART build, and ART cannot index nested types.
  // Keys of such types stay unenforced (the index is not mirrored).
  auto art_indexable = [](const duckdb::LogicalType& type) {
    return !type.HasAlias() && type.id() != duckdb::LogicalTypeId::ENUM &&
           !type.IsNested();
  };

  if (!index.IsInverted()) {
    info->index_type = duckdb::ART::TYPE_NAME;
    const auto& secondary =
      basics::downCast<const CreateSecondaryIndexInfo>(index);
    info->constraint_type = secondary.IsUnique()
                              ? duckdb::IndexConstraintType::UNIQUE
                              : duckdb::IndexConstraintType::NONE;
    containers::FlatHashSet<std::string> seen;
    // Into parsed_expressions: that is the half duckdb persists, and the
    // executor fills the binder's own list from it, live and replayed alike.
    auto push_key = [&](duckdb::unique_ptr<duckdb::ParsedExpression> key) {
      if (seen.emplace(key->ToString()).second) {
        info->parsed_expressions.push_back(std::move(key));
      }
    };
    // Walk the positional key list in source order; a sentinel column slot is
    // an expression key whose payload is the next unconsumed expression. Order
    // (and column/expression interleaving) is the ART key order, so it must be
    // reconstructed verbatim.
    const auto& key_expressions = secondary.Expressions();
    size_t expr_idx = 0;
    for (auto column : secondary.Columns()) {
      if (column == kInvalidColumnId) {  // expression-key slot
        // duckdb's ART builds and maintains expression keys natively, and the
        // stored text is the one form the catalog keeps an expression key in --
        // a rename re-renders it, so it names the table's own columns.
        const auto& expr = key_expressions[expr_idx++];
        if (!art_indexable(expr.return_type)) {
          return nullptr;
        }
        auto parsed = duckdb::Parser::ParseExpressionList(expr.pretty_printed);
        SDB_ENSURE(parsed.size() == 1, "index key \"", expr.pretty_printed,
                   "\" is not a single expression");
        push_key(std::move(parsed.front()));
        continue;
      }
      const auto* col = catalog::ColumnById(table, column);
      if (!col || !art_indexable(col->Type())) {
        return nullptr;
      }
      push_key(duckdb::make_uniq<duckdb::ColumnRefExpression>(col->Name()));
    }
    if (info->parsed_expressions.empty()) {
      return nullptr;
    }
    return info;
  }

  // Inverted index: injected as a bound index built straight from the
  // catalog objects, so the info only names the target; the referenced
  // columns just have to exist.
  if (index.GetReferencedColumns().empty()) {
    return nullptr;
  }
  for (auto col_id : index.GetReferencedColumns()) {
    if (!catalog::ColumnById(table, col_id)) {
      return nullptr;
    }
  }
  return info;
}

bool IsPlainStoreIndex(const duckdb::CreateIndexInfo& info) noexcept {
  return info.index_type == duckdb::ART::TYPE_NAME;
}

duckdb::AlterEntryData StoreTarget(duckdb::OnEntryNotFound if_not_found) {
  return duckdb::AlterEntryData{duckdb::QualifiedName{}, if_not_found};
}

duckdb::optional_ptr<duckdb::TableCatalogEntry> GetStoreTableEntry(
  duckdb::ClientContext& context, duckdb::Catalog& catalog, ObjectId table_id,
  duckdb::OnEntryNotFound if_not_found) {
  auto& sdb_catalog = catalog.Cast<catalog::SereneDBCatalog>();
  auto entry = sdb_catalog.LookupTableById(
    sdb_catalog.GetCatalogTransaction(context), table_id.id());
  if (!entry) {
    SDB_ENSURE(if_not_found == duckdb::OnEntryNotFound::RETURN_NULL,
               "relation ", table_id.id(), " does not exist");
    return nullptr;
  }
  return entry;
}

duckdb::optional_ptr<duckdb::TableCatalogEntry> GetStoreTableEntry(
  duckdb::ClientContext& context, ObjectId database_id, ObjectId table_id,
  duckdb::OnEntryNotFound if_not_found) {
  auto db = TryStoreDatabase(context, database_id);
  if (!db) {
    SDB_ENSURE(if_not_found == duckdb::OnEntryNotFound::RETURN_NULL,
               "database ", database_id.id(), " is not attached");
    return nullptr;
  }
  return GetStoreTableEntry(context, db->GetCatalog(), table_id, if_not_found);
}

namespace {}  // namespace

void CatalogStore::WriteContext::Catalog::PutTable(
  const duckdb::CreateTableInfo& table, wal::PutMode mode, Permissions perm,
  std::vector<wal::OwnedSequence> sequences) {
  const auto schema_id = catalog::ParentIdOf(table);
  SDB_ASSERT(schema_id.isSet());
  // A later version of a table names no sequence, so the owned ones are
  // performed under the table's own mode.
  SDB_ASSERT(sequences.empty() || mode == wal::PutMode::Create);
  _entries.emplace_back(wal::PutTable{.schema_id = schema_id,
                                      .id = catalog::IdOf(table),
                                      .mode = mode,
                                      .info = catalog::Clone(table),
                                      .perm = std::move(perm),
                                      .sequences = std::move(sequences)});
}

void CatalogStore::WriteContext::Catalog::PutEntry(
  ObjectId parent_id, duckdb::CatalogType type, ObjectId id, wal::PutMode mode,
  std::shared_ptr<const duckdb::CreateInfo> info, Permissions perm) {
  // Which of the two index kinds comes off the definition, exactly as duckdb
  // tells one index from another.
  const bool inverted =
    type == duckdb::CatalogType::INDEX_ENTRY &&
    basics::downCast<const CreateIndexInfoBase>(*info).IsInverted();
  _entries.emplace_back(wal::PutEntry{.parent_id = parent_id,
                                      .type = type,
                                      .inverted = inverted,
                                      .id = id,
                                      .mode = mode,
                                      .info = std::move(info),
                                      .perm = std::move(perm)});
}

void CatalogStore::WriteContext::Catalog::DropObject(ObjectId parent_id,
                                                     duckdb::CatalogType type,
                                                     ObjectId id) {
  _entries.emplace_back(
    wal::DropObject{.parent_id = parent_id, .type = type, .id = id});
}

void CatalogStore::WriteContext::Catalog::SetSequence(ObjectId sequence_id,
                                                      uint64_t value) {
  _entries.emplace_back(wal::SetSequence{.id = sequence_id, .value = value});
}

void CatalogStore::WriteContext::Catalog::DropSequence(ObjectId sequence_id) {
  _entries.emplace_back(wal::DropSequence{.id = sequence_id});
}

void CatalogStore::WriteContext::Store::CreateTable(ObjectId database_id,
                                                    ObjectId table) {
  _ops.emplace_back(database_id, table);
}

void CatalogStore::WriteContext::Store::DropTable(ObjectId database_id,
                                                  ObjectId table) {
  auto info = duckdb::make_uniq<duckdb::DropInfo>();
  info->type = duckdb::CatalogType::TABLE_ENTRY;
  _ops.emplace_back(database_id, table, std::move(info));
}

void CatalogStore::WriteContext::Store::Alter(
  ObjectId database_id, ObjectId relation,
  duckdb::unique_ptr<duckdb::AlterInfo> info) {
  info->oid = relation.id();
  _ops.emplace_back(database_id, relation, std::move(info));
}

void CatalogStore::WriteContext::Store::CreateIndex(
  ObjectId database_id, duckdb::unique_ptr<duckdb::CreateIndexInfo> info,
  TableInfoRef table, IndexInfoRef index) {
  const auto relation = index->GetRelationId();
  _ops.emplace_back(database_id, relation, std::move(info), std::move(table),
                    std::move(index));
}

void CatalogStore::WriteContext::Store::DropIndex(ObjectId database_id,
                                                  ObjectId relation_id,
                                                  std::string_view name) {
  auto info = duckdb::make_uniq<duckdb::DropInfo>();
  info->type = duckdb::CatalogType::INDEX_ENTRY;
  info->SetName(duckdb::Identifier{name});
  _ops.emplace_back(database_id, relation_id, std::move(info));
}

void CatalogStore::WriteContext::Store::RenameIndex(ObjectId database_id,
                                                    ObjectId relation_id,
                                                    std::string_view from,
                                                    std::string_view to) {
  duckdb::AlterEntryData target{
    duckdb::QualifiedName{duckdb::Identifier{}, duckdb::Identifier{},
                          duckdb::Identifier{from}},
    duckdb::OnEntryNotFound::RETURN_NULL};
  _ops.emplace_back(
    database_id, relation_id,
    duckdb::make_uniq<duckdb::RenameTableInfo>(target, duckdb::Identifier{to}));
}

void CatalogStore::WriteContext::Store::ReshapeTable(
  ObjectId database_id, ObjectId table, const duckdb::CreateTableInfo& before,
  const duckdb::CreateTableInfo& after) {
  const auto had = [&](const duckdb::Constraint& constraint) {
    return absl::c_any_of(before.constraints, [&](const auto& previous) {
      return previous->oid == constraint.oid;
    });
  };
  const auto survives = [&](const duckdb::Constraint& constraint) {
    return absl::c_any_of(after.constraints, [&](const auto& next) {
      return next->oid == constraint.oid;
    });
  };
  // A key over a nested column has no store-side equivalent, and a key naming
  // a column the store does not have is not one either.
  const auto key_names = [](const duckdb::UniqueConstraint& unique,
                            const duckdb::CreateTableInfo& info) {
    duckdb::vector<duckdb::Identifier> names;
    for (const auto& key : unique.GetColumnNames()) {
      const auto* column = catalog::ColumnByName(info, key.GetIdentifierName());
      if (column == nullptr || column->Type().IsNested()) {
        return duckdb::vector<duckdb::Identifier>{};
      }
      names.emplace_back(column->Name());
    }
    return names;
  };
  const auto alter = [&](duckdb::unique_ptr<duckdb::AlterInfo> info) {
    Alter(database_id, table, std::move(info));
  };

  // A constant DEFAULT backfills existing rows; the store handler retries
  // without it when the expression calls a function the store connection
  // cannot bind.
  for (const auto& column : after.columns.Logical()) {
    const auto* previous =
      catalog::ColumnById(before, ObjectId{column.CatalogOid()});
    if (previous != nullptr) {
      if (previous->Name().GetIdentifierName() !=
          column.Name().GetIdentifierName()) {
        alter(duckdb::make_uniq<duckdb::RenameColumnInfo>(
          StoreTarget(), previous->Name(), column.Name()));
      }
      continue;
    }
    duckdb::ColumnDefinition definition{column.Name(), column.Type()};
    definition.SetCompressionType(column.CompressionType());
    if (!column.Generated() && column.HasDefaultValue()) {
      definition.SetDefaultValue(column.DefaultValue().Copy());
    }
    alter(duckdb::make_uniq<duckdb::AddColumnInfo>(
      StoreTarget(), std::move(definition), /*if_column_not_exists=*/false));
  }
  // Constraints the ALTER removed.
  for (const auto& constraint : before.constraints) {
    if (survives(*constraint)) {
      continue;
    }
    switch (constraint->type) {
      case duckdb::ConstraintType::NOT_NULL: {
        const auto index = constraint->Cast<duckdb::NotNullConstraint>().index;
        if (index.index < before.columns.LogicalColumnCount()) {
          alter(duckdb::make_uniq<duckdb::DropNotNullInfo>(
            StoreTarget(), before.columns.GetColumn(index).Name()));
        }
      } break;
      case duckdb::ConstraintType::CHECK:
        // No SQL spells "the CHECK with this body", so the expression text is
        // what names it on the way out as well as on the way in.
        alter(duckdb::make_uniq<duckdb::DropConstraintInfo>(
          StoreTarget(duckdb::OnEntryNotFound::RETURN_NULL),
          constraint->Cast<duckdb::CheckConstraint>().expression->ToString(),
          /*if_constraint_not_found=*/true, /*cascade=*/false));
        break;
      default:
        break;
    }
  }
  // And the ones it added. A primary key recreates the store table's storage
  // and validates the rows already there; the implied NOT NULLs come through
  // the same loop.
  for (const auto& constraint : after.constraints) {
    if (had(*constraint)) {
      continue;
    }
    switch (constraint->type) {
      case duckdb::ConstraintType::NOT_NULL: {
        const auto index = constraint->Cast<duckdb::NotNullConstraint>().index;
        if (index.index < after.columns.LogicalColumnCount()) {
          alter(duckdb::make_uniq<duckdb::SetNotNullInfo>(
            StoreTarget(), after.columns.GetColumn(index).Name()));
        }
      } break;
      case duckdb::ConstraintType::UNIQUE: {
        const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
        auto names = key_names(unique, after);
        if (names.empty()) {
          break;
        }
        auto key = duckdb::make_uniq<duckdb::UniqueConstraint>(
          std::move(names), unique.IsPrimaryKey());
        // The name the ART answers to is the constraint's own, which is what
        // RebuildMissingIndexes looks for and what an error message shows.
        key->constraint_name =
          unique.GetName(after.GetTableName()).GetIdentifierName();
        alter(duckdb::make_uniq<duckdb::AddConstraintInfo>(StoreTarget(),
                                                           std::move(key)));
      } break;
      case duckdb::ConstraintType::CHECK: {
        // Function calls bind against the store connection's catalog, so a
        // check that makes one is not mirrored; plain checks are, and the
        // store verifies them against existing rows.
        const auto& check = constraint->Cast<duckdb::CheckConstraint>();
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
        scan(*check.expression);
        if (!has_function) {
          alter(duckdb::make_uniq<duckdb::AddConstraintInfo>(
            StoreTarget(), duckdb::make_uniq<duckdb::CheckConstraint>(
                             check.expression->Copy())));
        }
      } break;
      default:
        break;
    }
  }
}

void CatalogStore::WriteContext::Catalog::DropPrepare(wal::DropPrepare drop) {
  _entries.emplace_back(std::move(drop));
}

void CatalogStore::WriteContext::Catalog::DropChildren(ObjectId parent_id) {
  _entries.emplace_back(wal::DropChildren{.parent_id = parent_id});
}

void CatalogStore::WriteContext::Catalog::PrepareCommit(ObjectId id) {
  _entries.emplace_back(wal::PrepareCommit{.id = id});
}

CatalogStore::CatalogStore() {
  SDB_ASSERT(gInstance == nullptr);
  gInstance = this;
  // Past the band the fixed system ids are carved out of, before anything a
  // catalog record can name is allocated. duckdb's own boot entries sit below
  // it and are never written down, so they cost nothing here.
  RestoreId(id::kMaxSystem.id());
  IdAllocator().SetOidReservationSink(&CatalogStore::ReserveOids);
}

CatalogStore::~CatalogStore() {
  IdAllocator().SetOidReservationSink(nullptr);
  gInstance = nullptr;
}

std::string CatalogStore::DatabaseFilePath(ObjectId database_id) {
  return basics::file_utils::BuildFilename(
    std::string{GetCatalogStore().DataDirectory()},
    absl::StrCat(database_id.id(), ".db"));
}

std::vector<ObjectId> CatalogStore::DatabaseFileIds() {
  namespace fs = std::filesystem;
  std::vector<ObjectId> ids;
  std::error_code ec;
  for (const auto& entry :
       fs::directory_iterator{GetCatalogStore().DataDirectory(), ec}) {
    if (!entry.is_regular_file(ec)) {
      continue;
    }
    const auto name = entry.path().filename().string();
    if (!name.ends_with(".db")) {
      continue;
    }
    uint64_t id = 0;
    if (absl::SimpleAtoi(std::string_view{name}.substr(0, name.size() - 3),
                         &id)) {
      ids.emplace_back(id);
    }
  }
  return ids;
}

void CatalogStore::Initialize(std::string_view database_directory) {
  _directory = basics::file_utils::BuildFilename(
    std::string{database_directory}, std::string{StaticStrings::kCatalogRoot});
  _data_directory = basics::file_utils::BuildFilename(
    std::string{database_directory},
    std::string{StaticStrings::kDataStoreRoot});
  {
    std::error_code ec;
    std::filesystem::create_directories(_data_directory, ec);
    if (ec) {
      SDB_FATAL(STARTUP, "catalog: cannot create directory '", _data_directory,
                "': ", ec.message());
    }
  }
}

void CatalogStore::Replay(
  absl::FunctionRef<void(std::span<const wal::Entry>)> apply) {
  uint64_t records = 0;
  // The catalog mutex is taken inside the store's everywhere else (a commit
  // appends and publishes under it), so the store's is released before each
  // frame's definitions go in. Boot is single-threaded, but the ordering has to
  // hold for the deadlock detector as much as for a second thread.
  _wal.Open(_directory, [&](std::span<const uint8_t> frame) {
    auto parsed = wal::ParseEntries(frame);
    std::vector<wal::Entry> entries;
    IdAllocator().RestoreOidReservation(parsed.header.oid_horizon);
    {
      absl::MutexLock lock{&_mutex};
      // A snapshot frame carries the position its state is in step with rather
      // than advancing the log; a log frame is the batch that landed at it.
      _position = std::max(_position, parsed.header.position);
      if (parsed.entries.empty()) {
        return;
      }
      // Every frame in the file is a decision that was reached: the catalog
      // commits first, so a frame is present exactly when its batch was
      // acknowledged, and replay is a plain re-apply.
      entries.reserve(parsed.entries.size());
      for (auto& t : parsed.entries) {
        entries.push_back(std::move(t.entry));
      }
      ApplyEntries(entries,
                   parsed.header.snapshot ? 0 : parsed.header.position);
    }
    // In the order the records were decided -- which is an order every parent
    // is already in, because nothing can be created under something that does
    // not exist yet.
    apply(entries);
    records += entries.size();
  });
  // Every id the last run could have named is spent, whether or not the
  // catalog ever recorded what was named after it: reissuing one would collide
  // with a store table, an iresearch directory or a WAL shard already on disk.
  // Done here, before anything can allocate.
  RestoreId(IdAllocator().OidReservation());
  for (const auto open_id : AllOpenDrops()) {
    RestoreId(open_id.id());
  }
  {
    absl::MutexLock lock{&_mutex};
    // An upper bound on what a checkpoint would write, and on the live bytes:
    // the file may hold a long dead tail, and the record rule is what folds
    // that. Once a checkpoint has been written both are the state's real cost.
    _checkpoint_records = records;
    _records_since_checkpoint.store(0, std::memory_order_relaxed);
    _live_bytes = _wal.GetStats().size_on_disk;
  }
}

void CatalogStore::Shutdown() { _wal.Close(); }

void CatalogStore::ApplyEntries(std::span<const wal::Entry> entries,
                                uint64_t position) {
  // The lambda runs only under this function's lock; clang's analysis does not
  // carry the requirement across a lambda boundary.
  const auto set_sequence = [&](ObjectId id, uint64_t value,
                                bool max_merge) ABSL_NO_THREAD_SAFETY_ANALYSIS {
    absl::MutexLock seq_lock{&_seq_mutex};
    auto [it, inserted] = _sequences.try_emplace(id.id(), value);
    if (!inserted) {
      // Horizon bumps append outside the sequence lock, so entries for one
      // sequence can land out of order; max-merge makes any order replay to
      // the highest covered horizon. setval stays an ordered assign -- a
      // concurrent bump racing it is PG's "unspecified interleaving".
      it->second = max_merge ? std::max(it->second, value) : value;
    }
  };

  containers::FlatHashMap<uint64_t, std::vector<store_op::Targeted>> frame_ops;
  for (const auto& entry : entries) {
    std::visit(
      [&](const auto& e) ABSL_NO_THREAD_SAFETY_ANALYSIS {
        using T = std::decay_t<decltype(e)>;
        if constexpr (std::is_same_v<T, wal::PutTable>) {
          for (const auto& seq : e.sequences) {
            // The seed is a floor, never a rewind: the create's entry is
            // appended when its transaction commits, so a nextval issued
            // earlier in that transaction has already bumped the horizon past
            // it, and assigning would hand the same values out twice.
            set_sequence(seq.id, seq.seed, true);
          }
        } else if constexpr (std::is_same_v<T, wal::PutEntry> ||
                             std::is_same_v<T, wal::DropChildren> ||
                             std::is_same_v<T, wal::DropObject>) {
          // Definitions are not state here: replay puts them straight into the
          // catalog, which is the only place one lives.
        } else if constexpr (std::is_same_v<T, wal::DropPrepare>) {
          // Applies on arrival: the object stops being visible here, and the
          // drop stays open until its commit reclaims the subtree.
          _open.insert_or_assign(e.id.id(), e);
        } else if constexpr (std::is_same_v<T, wal::PrepareCommit>) {
          FinishOpen(e.id);
        } else if constexpr (std::is_same_v<T, wal::SetSequence>) {
          set_sequence(e.id, e.value, false);
        } else if constexpr (std::is_same_v<T, wal::BumpSequence>) {
          set_sequence(e.id, e.value, true);
        } else if constexpr (std::is_same_v<T, store_op::Targeted>) {
          // Log-only: it is not state, it is what a database behind this
          // position has to run to catch up. Held until that database's data
          // half is known durable, which is what compaction waits for. The
          // frame's ops are gathered into one batch below: a frame lands
          // atomically, so it is the unit a database catches up in.
          if (position != 0) {
            frame_ops[e.database_id.id()].push_back(e);
          }
        } else if constexpr (std::is_same_v<T, wal::DropSequence>) {
          absl::MutexLock seq_lock{&_seq_mutex};
          _sequences.erase(e.id.id());
        } else {
          static_assert(false, "entry type is not applied");
        }
      },
      entry);
  }
  for (auto& [database_id, ops] : frame_ops) {
    _unacked[database_id].push_back(PendingBatch{
      .position = position,
      .ops = std::make_shared<const std::vector<store_op::Targeted>>(
        std::move(ops))});
  }
}

void CatalogStore::FinishOpen(ObjectId id) { _open.erase(id.id()); }

uint64_t CatalogStore::AppendFrame(std::span<const wal::Entry> entries) {
  SDB_ASSERT(!entries.empty());
  const auto position = ++_position;
  _frame_scratch.Rewind();
  wal::SerializeEntries(
    {.position = position, .oid_horizon = IdAllocator().OidReservation()},
    entries, _frame_scratch);
  const std::span bytes{_frame_scratch.GetData(), _frame_scratch.GetPosition()};
  _wal.Append(bytes);
  _records_since_checkpoint.fetch_add(entries.size(),
                                      std::memory_order_relaxed);
  return position;
}

uint64_t CatalogStore::Commit(std::span<const wal::Entry> entries) {
  const auto position = AppendFrame(entries);
  ApplyEntries(entries, position);
  return position;
}

uint64_t CatalogStore::CommitFrames(
  duckdb::ClientContext* context,
  std::span<const std::vector<wal::Entry>> frames,
  absl::FunctionRef<void()> publish) {
  if (frames.empty()) {
    // Nothing durable to add, but the publish still runs under this lock: what
    // a rewrite must never see is a half of the pair.
    absl::MutexLock lock{&_mutex};
    publish();
    return 0;
  }
  uint64_t position = 0;
  // The failure this ordering exists for: once the log is a consensus log, an
  // append refused by a lost leadership or a partition is routine, and the
  // answer is an aborted transaction and an ordinary error -- not a fatal.
  SDB_IF_FAILURE("catalog_append_fails") {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("catalog log: could not append the transaction"));
  }
  {
    absl::MutexLock lock{&_mutex};
    for (const auto& entries : frames) {
      position = Commit(entries);
    }
    publish();
    // A checkpoint written at the very instant the batch became visible: the
    // catalog already answers for every frame in the log, so the rewrite is
    // whole. Holding both locks across the pair is what makes that true.
    SDB_IF_FAILURE("compact_at_catalog_commit") {
      if (_unacked.empty()) {
        Compact(context);
      }
    }
    MaybeCompact(context);
  }
  // The catalog has decided and the data has not committed yet: the one window
  // a crash can hit, and exactly the gap the position closes at boot.
  SDB_IF_FAILURE("crash_after_catalog_before_data") { SDB_IMMEDIATE_ABORT(); }
  SDB_IF_FAILURE("crash_catalog_before_store_drop") { SDB_IMMEDIATE_ABORT(); }
  SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
  return position;
}

void CatalogStore::MaybeCompact(duckdb::ClientContext* context) {
  // Compaction replaces the log with the state it produced, so a batch whose
  // data half is still outstanding would lose the very record its replay needs.
  if (!_unacked.empty()) {
    return;
  }
  const auto size = _wal.GetStats().size_on_disk;
  if (size < kCompactMinBytes) {
    return;
  }
  const auto appended =
    _records_since_checkpoint.load(std::memory_order_relaxed);
  const auto grown = _live_bytes != 0 && size >= _live_bytes * kLiveBytesGrowth;
  if (appended <= _checkpoint_records && !grown) {
    return;
  }
  Compact(context);
}

void CatalogStore::TryCompact() {
  auto* catalog = TryGetCatalog();
  if (catalog == nullptr) {
    return;
  }
  // A rewrite reads the catalog, and a catalog commit writes both it and the
  // log; the catalog mutex is what separates the two. Try, never wait: the
  // caller is on a commit or an append path and every one of them attempts this
  // again, while a mutation already holding the mutex is about to attempt it
  // itself.
  catalog->TryExcludingMutations([this] {
    absl::MutexLock lock{&_mutex};
    MaybeCompact(nullptr);
  });
}

void CatalogStore::CompactNow() {
  absl::MutexLock lock{&_mutex};
  if (!_unacked.empty()) {
    return;
  }
  Compact(nullptr);
}

std::vector<wal::Entry> CatalogStore::CheckpointDefinitions(
  duckdb::ClientContext* context) {
  // Through the same builder every durable batch uses: a checkpoint is the
  // catalog written out, not a second way of writing it -- and it is replayed
  // like any other run of records, so it walks the tree top down. A definition
  // arriving before its parent has nowhere to hang, and the kinds inside a
  // schema are ordered the same way a statement could have created them: what a
  // body names before the body, an owned sequence after the table that owns it.
  // Every record a checkpoint writes is a create: the file it produces is
  // replayed into an empty catalog, so nothing it names is there yet.
  WriteContext::Catalog out;
  std::vector<IndexInfoRef> indexes;
  const auto put_indexes = [&](ObjectId relation_id) {
    for (const auto& index : indexes) {
      if (index->GetRelationId() != relation_id) {
        continue;
      }
      out.PutEntry(index->GetParentId(), duckdb::CatalogType::INDEX_ENTRY,
                   index->GetId(), wal::PutMode::Create, index, Permissions{});
    }
  };
  // Through the committing transaction when there is one, and only then. A
  // checkpoint fired from inside a commit runs before duckdb has made that
  // transaction's entries visible to anybody else, so a committed read would
  // rewrite the log without the very records this commit just appended and
  // drop them. A transaction that wrote no role of its own has nothing to add
  // and must not start a transaction on the cluster-global attachment to say
  // so.
  {
    auto* role_reader =
      context != nullptr && catalog::HasUncommittedRoles(*context) ? context
                                                                   : nullptr;
    std::vector<std::shared_ptr<const CreateRoleInfo>> roles;
    catalog::VisitRoles(role_reader, [&](const CreateRoleInfo& role) {
      roles.push_back(role.CloneRole());
    });
    std::ranges::sort(roles, {},
                      [](const auto& role) { return role->GetId(); });
    for (auto& role : roles) {
      const auto id = role->GetId();
      out.PutEntry(id::kInstance, duckdb::CatalogType::ROLE_ENTRY, id,
                   wal::PutMode::Create, std::move(role));
    }
  }
  // VisitDatabases already routes a transaction that wrote none to the shared
  // cache, so the context can go straight in.
  std::vector<catalog::DatabaseRef> databases;
  catalog::VisitDatabases(
    context, [&](const catalog::DatabaseRef& db) { databases.push_back(db); });
  std::ranges::sort(databases, {},
                    [](const catalog::DatabaseRef& db) { return db.Id(); });
  for (auto& database : databases) {
    const auto db_id = database.Id();
    indexes = DatabaseIndexes(context, db_id);
    out.PutEntry(id::kInstance, duckdb::CatalogType::DATABASE_ENTRY, db_id,
                 wal::PutMode::Create, database.info, database.perm);
    // Foreign servers are database children, so they come out of the catalog's
    // own set rather than a schema's.
    auto servers = catalog::DatabaseForeignServers(context, db_id);
    std::ranges::sort(servers, {}, [](const HeldForeignServer& server) {
      return server.first->GetId();
    });
    for (auto& [server, perm] : servers) {
      out.PutEntry(db_id, duckdb::CatalogType::FOREIGN_SERVER_ENTRY,
                   server->GetId(), wal::PutMode::Create, server, perm);
    }
    auto schemas = catalog::DatabaseSchemas(context, db_id);
    std::ranges::sort(schemas, {}, [](const HeldSchema& schema) {
      return IdOf(*schema.first).id();
    });
    // Once for the whole database, because the walk is per database and not
    // per schema.
    auto tokenizers = catalog::DatabaseTokenizers(context, db_id);
    std::ranges::sort(tokenizers, {}, [](const HeldTokenizer& tokenizer) {
      return tokenizer.first->GetId();
    });
    // And the types and functions, for the same reason.
    auto types = catalog::DatabaseTypes(context, db_id);
    std::ranges::sort(types, {}, [](const duckdb::TypeCatalogEntry* type) {
      return type->oid;
    });
    std::vector<const duckdb::MacroCatalogEntry*> functions;
    catalog::VisitFunctions(context, db_id,
                            [&](const duckdb::MacroCatalogEntry& function) {
                              functions.push_back(&function);
                            });
    std::ranges::sort(
      functions, {},
      [](const duckdb::MacroCatalogEntry* function) { return function->oid; });
    std::vector<HeldTable> tables;
    catalog::VisitTables(
      context, db_id, [&](const TableInfoRef& table, const Permissions& perm) {
        tables.emplace_back(table, perm);
      });
    std::ranges::sort(tables, {}, [](const HeldTable& table) {
      return catalog::IdOf(*table.first).id();
    });
    std::vector<const duckdb::ViewCatalogEntry*> views;
    catalog::VisitViews(
      context, db_id,
      [&](const duckdb::ViewCatalogEntry& view) { views.push_back(&view); });
    std::ranges::sort(views, {}, [](const duckdb::ViewCatalogEntry* view) {
      return view->oid;
    });
    // And the sequences, whose entry is the object too. A sequence a table owns
    // is filed under that table rather than the schema, so the walk below picks
    // them apart by the owning-table id the definition carries.
    auto sequences = catalog::DatabaseSequences(context, db_id);
    std::ranges::sort(
      sequences, {},
      [](const catalog::SereneDBSequenceEntry* seq) { return seq->oid; });
    // `owner` is the table a sequence is owned by, unset for the free-standing
    // ones. A sequence's name is always in its schema's relation namespace, so
    // the record's parent is the schema either way -- what the owning table
    // decides is only where in the walk it comes out.
    const auto put_sequences = [&](ObjectId schema_id, ObjectId owner) {
      for (const auto* sequence : sequences) {
        if (ObjectId{sequence->ParentSchema().oid} != schema_id ||
            sequence->GetOwnerTableId() != owner) {
          continue;
        }
        out.PutEntry(schema_id, duckdb::CatalogType::SEQUENCE_ENTRY,
                     ObjectId{sequence->oid}, wal::PutMode::Create,
                     sequence->Definition(), sequence->permissions);
      }
    };
    for (auto& [schema, schema_perm] : schemas) {
      const auto schema_id = IdOf(*schema);
      out.PutEntry(db_id, duckdb::CatalogType::SCHEMA_ENTRY, schema_id,
                   wal::PutMode::Create, schema, schema_perm);
      for (auto& [tokenizer, perm] : tokenizers) {
        if (tokenizer->GetParentId() != schema_id) {
          continue;
        }
        out.PutEntry(schema_id, duckdb::CatalogType::TOKENIZER_ENTRY,
                     tokenizer->GetId(), wal::PutMode::Create, tokenizer, perm);
      }
      for (const auto* type : types) {
        if (ObjectId{type->ParentSchema().oid} != schema_id) {
          continue;
        }
        out.PutEntry(
          schema_id, duckdb::CatalogType::TYPE_ENTRY, ObjectId{type->oid},
          wal::PutMode::Create,
          std::shared_ptr<const duckdb::CreateInfo>{type->GetInfo().release()},
          type->permissions);
      }
      // Free-standing sequences before anything that can name one in a DEFAULT;
      // the ones a table owns are written after that table, below.
      put_sequences(schema_id, ObjectId{});
      for (const auto* function : functions) {
        if (ObjectId{function->ParentSchema().oid} != schema_id) {
          continue;
        }
        out.PutEntry(schema_id, duckdb::CatalogType::MACRO_ENTRY,
                     ObjectId{function->oid}, wal::PutMode::Create,
                     std::shared_ptr<const duckdb::CreateInfo>{
                       function->GetInfo().release()},
                     function->permissions);
      }
      for (const auto& [table, perm] : tables) {
        if (catalog::ParentIdOf(*table) != schema_id) {
          continue;
        }
        const auto table_id = catalog::IdOf(*table);
        // Owned sequences are definitions of their own in the catalog, so they
        // come back as their own entries rather than riding this one.
        out.PutTable(*table, wal::PutMode::Create, perm);
        put_sequences(schema_id, table_id);
        put_indexes(table_id);
      }
      for (const auto* view : views) {
        if (ObjectId{view->ParentSchema().oid} != schema_id) {
          continue;
        }
        out.PutEntry(
          schema_id, duckdb::CatalogType::VIEW_ENTRY, ObjectId{view->oid},
          wal::PutMode::Create,
          std::shared_ptr<const duckdb::CreateInfo>{view->GetInfo().release()},
          view->permissions);
        put_indexes(ObjectId{view->oid});
      }
    }
  }
  return std::move(out._entries);
}

void CatalogStore::Compact(duckdb::ClientContext* context) {
  SDB_ASSERT(_unacked.empty());
  // The catalog is where a definition lives, so the checkpoint is read out of
  // it -- and it answers for every frame in the log this rewrite replaces,
  // because the caller holds the catalog mutex and a catalog commit appends and
  // publishes under it. `context` is the transaction that commit belongs to, so
  // the kinds whose entry is the object are read where its own writes are.
  auto entries = CheckpointDefinitions(context);

  absl::MutexLock seq_lock{&_seq_mutex};
  std::vector<uint64_t> seq_ids;
  seq_ids.reserve(_sequences.size());
  for (const auto& [id, value] : _sequences) {
    seq_ids.push_back(id);
  }
  std::sort(seq_ids.begin(), seq_ids.end());
  for (const auto id : seq_ids) {
    entries.emplace_back(
      wal::SetSequence{.id = ObjectId{id}, .value = _sequences[id]});
  }
  const auto records = entries.size() + _open.size();

  _wal.Compact([&](CatalogWal::FrameSink sink) {
    _mutex.AssertHeld();
    _seq_mutex.AssertHeld();
    // Snapshot frames: the state as of the current log position, carrying it
    // rather than advancing it, so a reopened file starts counting where the
    // one it replaces stopped.
    const wal::FrameHeader header{.position = _position,
                                  .oid_horizon = IdAllocator().OidReservation(),
                                  .snapshot = true};
    duckdb::MemoryStream stream;
    // Written even with nothing in it: the horizon rides the header, and a
    // rewrite that dropped the last frame would let the next boot reissue
    // every id the file had spent.
    wal::SerializeEntries(header, entries, stream);
    sink({stream.GetData(), stream.GetPosition()});
    // Drops still open stay open, each in its own frame: their reclamation has
    // not landed, so the rewritten file has to leave the DropPrepare in place
    // for boot to redo it.
    std::vector<uint64_t> open_ids;
    open_ids.reserve(_open.size());
    for (const auto& [drop_id, drop] : _open) {
      open_ids.push_back(drop_id);
    }
    std::sort(open_ids.begin(), open_ids.end());
    for (const auto drop_id : open_ids) {
      duckdb::MemoryStream drop_stream;
      const wal::Entry drop{_open.at(drop_id)};
      wal::SerializeEntries(header, {&drop, 1}, drop_stream);
      sink({drop_stream.GetData(), drop_stream.GetPosition()});
    }
  });
  _checkpoint_records = records;
  _records_since_checkpoint.store(0, std::memory_order_relaxed);
  _live_bytes = _wal.GetStats().size_on_disk;
}

void CatalogStore::Write(
  duckdb::ClientContext* context, absl::FunctionRef<void(WriteContext&)> fill,
  absl::FunctionRef<void(std::span<const wal::Entry>)> performed) {
  WriteContext ctx;
  fill(ctx);
  if (ctx._catalog._entries.empty() && ctx._store._ops.empty()) {
    return;
  }

  auto entries = std::move(ctx._catalog._entries);
  auto store_ops = std::move(ctx._store._ops);
  // One direction, one frame. The catalog is the decision point, so a batch's
  // records and the store half that reconstructs them land together, and the
  // position that frame lands at is what the data commit records. There is no
  // removals/updates split left to make: a cascade's removals and the new shape
  // of the table that survives them are one decision, and the position is what
  // tells boot whether the data caught up with it.
  const bool deferrable =
    context != nullptr && context->transaction.HasActiveTransaction();
  if (store_ops.empty()) {
    if (deferrable) {
      performed(QueueDeferredFrame(*context, std::move(entries)));
      return;
    }
    // Nothing to defer to: boot, background drop tasks and teardown. The batch
    // is decided the moment it is applied, and the catalog answers for it
    // either already -- a drop whose tombstone committed long ago -- or at the
    // publish the same mutation makes under the catalog mutex, which is held
    // across the pair and which a rewrite has to take.
    {
      absl::MutexLock lock{&_mutex};
      Commit(entries);
    }
    performed(entries);
    TryCompact();
    return;
  }

  // A batch's store ops all name one database: a transaction writes exactly one
  // attached database, and cross-database writes are refused outright.
  const auto database_id = store_ops.front().database_id;
  auto ops = std::make_shared<const std::vector<store_op::Targeted>>(
    std::move(store_ops));
  for (const auto& op : *ops) {
    entries.emplace_back(op);
  }
  // The data work takes duckdb's WAL and table locks, and DDL operators call in
  // here while holding table locks -- so it must never run under _mutex
  // (lock-order inversion against committing writers). Store-op batches
  // serialize on _store_mutex instead: it owns the data connection, while
  // _mutex is only taken around each append.
  // The records as the caller performs them, taken while the frame is settled
  // and read once the store lock is back down -- the catalog's own work belongs
  // outside it.
  std::span<const wal::Entry> settled;
  {
    absl::MutexLock store_lock{&_store_mutex};

    if (deferrable) {
      // The data work runs now, uncommitted, on the statement's own
      // transaction: the rest of the statement has to see it. Only the records
      // wait, and they go out from the commit itself, ahead of the data commit
      // that carries the position they landed at.
      RunStoreOps(context, *ops, 0);
      // The store work is done and uncommitted: a crash here must leave no
      // trace of it, and none of the records that describe it.
      SDB_IF_FAILURE("crash_ddl_after_store_work") { SDB_IMMEDIATE_ABORT(); }
      // A checkpoint is the catalog written out, and a transaction's entries
      // are not in the catalog until it commits -- so one running inside the
      // window has to leave them undecided, exactly as the file it replaces
      // did. The mutation holds the catalog mutex, so this is already
      // exclusive.
      SDB_IF_FAILURE("compact_inside_ddl") { CompactNow(); }
      settled = QueueDeferredFrame(*context, std::move(entries));
    } else {
      // No transaction to carry the position: boot, background drop tasks and
      // teardown. Same direction -- the catalog decides first -- with the store
      // connection's own transaction recording the position as it commits, so
      // an inline batch that dies between the two is caught by the same boot
      // gap.
      uint64_t position = 0;
      {
        absl::MutexLock lock{&_mutex};
        position = Commit(entries);
      }
      SDB_IF_FAILURE("crash_catalog_before_store_drop") {
        SDB_IMMEDIATE_ABORT();
      }
      SDB_IF_FAILURE("crash_on_drop") { SDB_IMMEDIATE_ABORT(); }
      RunStoreOps(context, *ops, position);
      // The store connection committed the data and the position together, so
      // the batch is in step and the log is free to fold it. A failure above
      // throws instead, leaving the batch outstanding for the next boot to
      // replay.
      AckDatabasePosition(database_id, position);
      settled = entries;
    }
  }
  performed(settled);
  if (!deferrable) {
    TryCompact();
  }
}

namespace {

// The relation a batch of store ops reshapes, as the user named it. Every op
// carries the store table it targets; the first one is the batch's subject,
// which is what an error about the batch has to name. Off the transaction's own
// view, because the batch that lost the race is often the one whose target the
// winner dropped, and the committed catalog no longer holds it.
std::string StoreOpsSubject(duckdb::ClientContext* context,
                            std::span<const store_op::Targeted> ops) {
  for (const auto& targeted : ops) {
    const auto table_id = targeted.relation_id;
    if (!table_id.isSet()) {
      continue;
    }
    if (context == nullptr) {
      continue;
    }
    if (const auto* table = catalog::FindSessionTable(*context, table_id)) {
      return std::string{table->name.GetIdentifierName()};
    }
  }
  return {};
}

}  // namespace

void CatalogStore::RunStoreOps(duckdb::ClientContext* context,
                               std::span<const store_op::Targeted> store_ops,
                               uint64_t position) {
  auto r = GetDataStore().ApplyStoreOps(context, store_ops, position);
  if (r.ok()) {
    return;
  }
  if (absl::IsAborted(r)) {
    // Another transaction reshaped the same rows. Retryable, and the same
    // 40001 a concurrently dropped target raises -- not duckdb's internal
    // conflict text, which names the store table rather than the user's.
    const auto name = StoreOpsSubject(context, store_ops);
    if (name.empty()) {
      // Nothing to name: the batch reshapes no relation the catalog still
      // holds. Same answer the entry write gives when it cannot name one.
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
                      ERR_MSG("could not serialize access due to concurrent "
                              "DDL on the same object"));
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_T_R_SERIALIZATION_FAILURE),
      ERR_MSG("could not serialize access due to concurrent update of table \"",
              name, "\""));
  }
  THROW_SQL_ERROR(ERR_MSG(r.message()));
}

uint64_t CatalogStore::LogPosition() const {
  absl::MutexLock lock{&_mutex};
  return _position;
}

void CatalogStore::AckDatabasePosition(ObjectId database_id,
                                       uint64_t position) {
  absl::MutexLock lock{&_mutex};
  const auto it = _unacked.find(database_id.id());
  if (it == _unacked.end()) {
    return;
  }
  auto& pending = it->second;
  std::erase_if(pending, [&](const PendingBatch& batch) {
    return batch.position <= position;
  });
  if (pending.empty()) {
    _unacked.erase(it);
  }
}

void CatalogStore::ForgetUnackedExcept(std::span<const ObjectId> live) {
  absl::MutexLock lock{&_mutex};
  absl::erase_if(_unacked, [&](const auto& entry) {
    return !absl::c_linear_search(live, ObjectId{entry.first});
  });
}

std::vector<CatalogStore::PendingBatch> CatalogStore::PendingFor(
  ObjectId database_id, uint64_t committed_position) const {
  absl::MutexLock lock{&_mutex};
  std::vector<PendingBatch> out;
  const auto it = _unacked.find(database_id.id());
  if (it == _unacked.end()) {
    return out;
  }
  for (const auto& batch : it->second) {
    if (batch.position > committed_position) {
      out.push_back(batch);
    }
  }
  std::ranges::sort(out, {}, &PendingBatch::position);
  return out;
}

void CatalogStore::WriteFrame(std::span<const wal::Entry> entries) {
  if (entries.empty()) {
    return;
  }
  {
    absl::MutexLock lock{&_mutex};
    Commit(entries);
  }
  TryCompact();
}

void CatalogStore::DropObject(ObjectId parent_id, duckdb::CatalogType type,
                              ObjectId id) {
  Write(
    [&](WriteContext& ctx) { ctx.catalog().DropObject(parent_id, type, id); });
}

void CatalogStore::DropSequence(ObjectId sequence_id) {
  Write([&](WriteContext& ctx) { ctx.catalog().DropSequence(sequence_id); });
}

void CatalogStore::DropPrepare(wal::DropPrepare drop) {
  Write([&](WriteContext& ctx) { ctx.catalog().DropPrepare(std::move(drop)); });
}

void CatalogStore::PrepareCommit(ObjectId id) {
  Write([&](WriteContext& ctx) { ctx.catalog().PrepareCommit(id); });
}

std::vector<ObjectId> CatalogStore::AllOpenDrops() const {
  absl::MutexLock lock{&_mutex};
  std::vector<ObjectId> ids;
  ids.reserve(_open.size());
  for (const auto& [open_id, drop] : _open) {
    ids.emplace_back(open_id);
  }
  std::sort(ids.begin(), ids.end(),
            [](ObjectId lhs, ObjectId rhs) { return lhs.id() < rhs.id(); });
  return ids;
}

std::optional<wal::DropPrepare> CatalogStore::OpenDrop(ObjectId id) const {
  absl::MutexLock lock{&_mutex};
  const auto it = _open.find(id.id());
  if (it == _open.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<uint64_t> CatalogStore::TryGetBootSequenceValue(
  ObjectId sequence_id) const {
  absl::MutexLock lock{&_seq_mutex};
  const auto it = _sequences.find(sequence_id.id());
  if (it == _sequences.end()) {
    return std::nullopt;
  }
  return it->second;
}

namespace {

// The hottest appends (every sequence horizon bump, every id reservation): one
// entry, encoded by the shared serializer into a stack buffer rather than by a
// second hand-written copy of the frame layout. Sized for the largest of them,
// so the stream never has to grow.
constexpr size_t kSmallFrameSize =
  sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint64_t) +
  sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint64_t);

// Position zero: a counter bump carries no data half, so no database can be
// out of step with it and it costs nothing to leave out of the log's ordering.
// That is also what keeps this path off _mutex, where the position lives.
template<typename Entry>
void AppendSmallFrame(CatalogWal& wal, Entry entry) {
  uint8_t bytes[kSmallFrameSize];
  duckdb::MemoryStream stream{bytes, sizeof(bytes)};
  const wal::Entry one{entry};
  wal::SerializeEntries({}, {&one, 1}, stream);
  SDB_ASSERT(stream.GetPosition() <= sizeof(bytes));
  wal.Append({bytes, stream.GetPosition()});
}

}  // namespace

void CatalogStore::PutSequenceValue(ObjectId sequence_id, uint64_t value) {
  {
    absl::MutexLock seq_lock{&_seq_mutex};
    _sequences.insert_or_assign(sequence_id.id(), value);
  }
  AppendSmallFrame(_wal, wal::SetSequence{.id = sequence_id, .value = value});
  NoteSequenceAppend();
}

void CatalogStore::ReserveOids(uint64_t horizon) {
  if (!Available()) {
    return;
  }
  auto& store = GetCatalogStore();
  // Replay allocates ids of its own -- deserializing a table default-constructs
  // its columns -- while the file is still being read. Nothing named after one
  // of those ever reaches disk, and Initialize raises the counter past the
  // horizon it just read, so skipping the append is exactly right.
  if (!store._wal.Writable()) {
    return;
  }
  // A frame with no records: the horizon is a header field, so a bump has no
  // data half and no position, and needs neither _mutex nor an ordering.
  uint8_t bytes[kSmallFrameSize];
  duckdb::MemoryStream stream{bytes, sizeof(bytes)};
  wal::SerializeEntries({.oid_horizon = horizon}, {}, stream);
  SDB_ASSERT(stream.GetPosition() <= sizeof(bytes));
  store._wal.Append({bytes, stream.GetPosition()});
  // Counted with the sequence appends: same shape, same reason -- each leaves
  // the previous horizon dead, and the check needs _mutex.
  store.NoteSequenceAppend();
}

void CatalogStore::AdvanceSequenceValue(ObjectId sequence_id, uint64_t value) {
  {
    absl::MutexLock seq_lock{&_seq_mutex};
    auto [it, inserted] = _sequences.try_emplace(sequence_id.id(), value);
    if (!inserted) {
      it->second = std::max(it->second, value);
    }
  }
  AppendSmallFrame(_wal, wal::BumpSequence{.id = sequence_id, .value = value});
  NoteSequenceAppend();
}

// A sequence horizon bump and an id reservation append off _mutex, so they
// count themselves rather than going through AppendFrame. Compaction needs both
// the catalog mutex and _mutex, so it is only attempted every
// kSequenceCompactCheck appends: a nextval must not serialize on the DDL mutex.
void CatalogStore::NoteSequenceAppend() {
  const auto n =
    _records_since_checkpoint.fetch_add(1, std::memory_order_relaxed) + 1;
  if (n % kSequenceCompactCheck != 0) {
    return;
  }
  TryCompact();
}

uint64_t CatalogStore::GetSequenceValue(ObjectId sequence_id) {
  return TryGetBootSequenceValue(sequence_id).value_or(0);
}

wal::ParsedFrame CatalogStore::ParseFrame(std::span<const uint8_t> frame) {
  return wal::ParseEntries(frame);
}

void CatalogStore::VisitSnapshot(
  absl::FunctionRef<void(Key, std::shared_ptr<const duckdb::CreateInfo>)>
    info_visitor,
  absl::FunctionRef<void(ObjectId, uint64_t)> sequence_visitor) {
  // The definitions a checkpoint would write, off the catalog and in the same
  // order -- so the view and the file it describes cannot drift apart.
  for (const auto& entry : CheckpointDefinitions(nullptr)) {
    std::visit(
      [&](const auto& e) {
        using T = std::decay_t<decltype(e)>;
        if constexpr (std::is_same_v<T, wal::PutTable>) {
          info_visitor(Key{e.schema_id, duckdb::CatalogType::TABLE_ENTRY, e.id},
                       e.info);
        } else if constexpr (std::is_same_v<T, wal::PutEntry>) {
          info_visitor(Key{e.parent_id, e.type, e.id}, e.info);
        }
      },
      entry);
  }

  absl::MutexLock lock{&_mutex};
  // Open drops have no definition to show, but hiding them would make the view
  // claim a clean catalog while a drop is still in flight.
  std::vector<ObjectId> in_flight;
  in_flight.reserve(_open.size());
  for (const auto& [open_id, drop] : _open) {
    in_flight.emplace_back(open_id);
  }
  std::sort(in_flight.begin(), in_flight.end(),
            [](ObjectId lhs, ObjectId rhs) { return lhs.id() < rhs.id(); });
  for (const auto drop_id : in_flight) {
    const auto& drop = _open.at(drop_id.id());
    info_visitor(
      Key{drop.parent_id, duckdb::CatalogType::DELETED_ENTRY, drop_id},
      nullptr);
  }
  absl::MutexLock seq_lock{&_seq_mutex};
  std::vector<uint64_t> seq_ids;
  seq_ids.reserve(_sequences.size());
  for (const auto& [id, value] : _sequences) {
    seq_ids.push_back(id);
  }
  std::sort(seq_ids.begin(), seq_ids.end());
  for (const auto id : seq_ids) {
    sequence_visitor(ObjectId{id}, _sequences.at(id));
  }
}

CatalogStore& GetCatalogStore() { return *CatalogStore::gInstance; }

void RecordCatalogPositionOnCommit(duckdb::ClientContext& context,
                                   ObjectId database_id, uint64_t position) {
  if (position == 0) {
    return;
  }
  auto db = TryStoreDatabase(context, database_id);
  if (!db) {
    return;
  }
  // Through ModifyDatabase, not GetTransaction: the position is a write to
  // this database's WAL, so the transaction has to be read-write and hold the
  // single-writable-database slot. A batch whose store half changed nothing
  // duckdb tracks -- dropping an injected inverted index is the case -- would
  // otherwise commit a WAL record on a read-only transaction.
  auto& meta = duckdb::MetaTransaction::Get(context);
  meta.ModifyDatabase(*db, duckdb::DatabaseModificationType::ALTER_TABLE);
  auto& transaction = meta.GetTransaction(*db);
  transaction.Cast<duckdb::DuckTransaction>().SetCatalogPosition(position);
}

}  // namespace sdb::catalog
