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

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <fast_float/fast_float.h>

#include <algorithm>
#include <cstring>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/parser.hpp>
#include <exception>
#include <utility>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/file_utils.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "basics/system-compiler.h"
#include "catalog/database.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/schema.h"
#include "catalog/secondary_index.h"
#include "catalog/store/data_store.h"
#include "catalog/table.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {
namespace {

constexpr uint8_t kRecordVersion = 1;

// Compact once dead records dominate and the file is worth rewriting.
constexpr uint64_t kCompactMinBytes = 1U << 20U;

const CatalogStore::Key kPendingAlterKey{id::kInstance,
                                         ObjectType::PendingAlter,
                                         id::kInstance};

}  // namespace

std::string StoreTableName(ObjectId table_id) {
  return absl::StrCat("t", table_id.id());
}

std::string StoreColumnName(ObjectId column_id) {
  return absl::StrCat("c", column_id.id());
}

std::string StoreIndexName(ObjectId index_id) {
  return absl::StrCat("i", index_id.id());
}

std::optional<ObjectId> ParseStoreId(char prefix, std::string_view name) {
  if (name.size() < 2 || name[0] != prefix) {
    return std::nullopt;
  }
  uint64_t value = 0;
  const auto* begin = name.data() + 1;
  const auto* end = name.data() + name.size();
  const auto [ptr, ec] = fast_float::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) {
    return std::nullopt;
  }
  return ObjectId{value};
}

namespace {

// Store-side SQL (CHECKs, index expression keys, USING casts) is rendered
// from facade expressions; column references must resolve against the
// store table's c<id> columns. `ToStore` false maps the other way, for
// store-side error texts shown to users.
template<bool ToStore>
void RewriteRefs(duckdb::unique_ptr<duckdb::ParsedExpression>& expr,
                 const Table& table) {
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::COLUMN_REF) {
    auto& ref = expr->Cast<duckdb::ColumnRefExpression>();
    const auto& name = ref.GetColumnName().GetIdentifierName();
    if constexpr (ToStore) {
      for (const auto& col : table.Columns()) {
        if (absl::EqualsIgnoreCase(col.GetName(), name)) {
          auto& names = ref.ColumnNamesMutable();
          names.clear();
          names.emplace_back(StoreColumnName(col.GetId()));
          break;
        }
      }
    } else {
      if (auto id = ParseStoreId('c', name)) {
        if (const auto* col = table.ColumnById(*id)) {
          auto& names = ref.ColumnNamesMutable();
          names.clear();
          names.emplace_back(std::string{col->GetName()});
        }
      }
    }
    return;
  }
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::ParsedExpression>& child) {
      RewriteRefs<ToStore>(child, table);
    });
}

template<bool ToStore>
std::optional<std::string> RewriteExpr(std::string_view sql_expr,
                                       const Table& table) try {
  auto parsed = duckdb::Parser::ParseExpressionList(std::string{sql_expr});
  if (parsed.size() != 1) {
    return std::nullopt;
  }
  RewriteRefs<ToStore>(parsed[0], table);
  return parsed[0]->ToString();
} catch (const std::exception&) {
  return std::nullopt;
}

}  // namespace

std::optional<std::string> RewriteExprToStoreNames(std::string_view sql_expr,
                                                   const Table& table) {
  return RewriteExpr<true>(sql_expr, table);
}

std::optional<std::string> RewriteExprToFacadeNames(std::string_view sql_expr,
                                                    const Table& table) {
  return RewriteExpr<false>(sql_expr, table);
}

std::optional<StoreIndexDef> MakeStoreIndexDef(const Table& table,
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
        // the parsed expression back to SQL for the store CREATE INDEX, with
        // column references mapped to the store table's c<id> columns.
        const auto& expr = key_expressions[expr_idx++];
        if (!art_indexable(expr.return_type)) {
          return std::nullopt;
        }
        auto rewritten = RewriteExprToStoreNames(expr.pretty_printed, table);
        if (!rewritten) {
          return std::nullopt;
        }
        push_key(absl::StrCat("(", *rewritten, ")"));
        continue;
      }
      const auto* col = table.ColumnById(column);
      if (!col || !art_indexable(col->type)) {
        return std::nullopt;
      }
      push_key(StoreColumnName(column));
    }
    if (def.keys.empty()) {
      return std::nullopt;
    }
    def.table = StoreTableName(table.GetId());
    return def;
  }

  // Inverted index: injected as a bound index built straight from the
  // catalog objects, so the def only names the target; the referenced
  // columns just have to exist.
  if (index.GetReferencedColumns().empty()) {
    return std::nullopt;
  }
  for (auto col_id : index.GetReferencedColumns()) {
    if (!table.ColumnById(col_id)) {
      return std::nullopt;
    }
  }
  def.table = StoreTableName(table.GetId());
  return def;
}

duckdb::optional_ptr<duckdb::TableCatalogEntry> GetStoreTableEntry(
  duckdb::ClientContext& context, ObjectId table_id,
  duckdb::OnEntryNotFound if_not_found) {
  const duckdb::EntryLookupInfo lookup(
    duckdb::CatalogType::TABLE_ENTRY,
    duckdb::QualifiedName(duckdb::Identifier{kStoreDatabaseName},
                          duckdb::Identifier{"main"},
                          duckdb::Identifier{StoreTableName(table_id)}));
  auto entry = duckdb::Catalog::GetEntry(context, lookup, if_not_found);
  if (!entry) {
    return nullptr;
  }
  return &entry->Cast<duckdb::TableCatalogEntry>();
}

StoreTableDef MakeStoreTableDef(const Table& table) {
  StoreTableDef def;
  def.name = StoreTableName(table.GetId());
  def.table_id = table.GetId();
  const auto& cols = table.Columns();
  std::vector<size_t> mirror_pos(cols.size(), SIZE_MAX);
  def.columns.reserve(cols.size());
  for (size_t i = 0; i < cols.size(); ++i) {
    const auto& col = cols[i];
    if (col.GetId() == Column::kGeneratedPKId) {
      continue;
    }
    mirror_pos[i] = def.columns.size();
    def.columns.push_back({StoreColumnName(col.GetId()), col.type});
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
        auto copy = constraint.expr->GetExpr().Copy();
        RewriteRefs<true>(copy, table);
        def.checks.push_back(std::move(copy));
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
      out.push_back(StoreColumnName(col_id));
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

void CatalogStore::WriteContext::CreateStoreIndex(
  StoreIndexDef def, std::shared_ptr<const Table> table,
  std::shared_ptr<const Index> index) {
  _entries.push_back({
    .op = Op::CreateStoreIndex,
    .store_index = std::move(def),
    .table_obj = std::move(table),
    .index_obj = std::move(index),
  });
}

void CatalogStore::WriteContext::DropStoreIndex(ObjectId index_id,
                                                ObjectId relation_id) {
  _entries.push_back({
    .op = Op::DropStoreIndex,
    .store_index =
      {
        .table = StoreTableName(relation_id),
        .table_id = relation_id,
        .index_id = index_id,
      },
  });
}

void CatalogStore::WriteContext::WriteTombstone(ObjectId parent_id,
                                                ObjectId id) {
  PutDefinition(parent_id, ObjectType::Tombstone, id, {});
}

namespace {

bool IsCatalogRecord(CatalogStore::Op op) {
  switch (op) {
    case CatalogStore::Op::PutDefinition:
    case CatalogStore::Op::DropDefinition:
    case CatalogStore::Op::PutSequence:
    case CatalogStore::Op::AdvanceSequence:
    case CatalogStore::Op::DropSequence:
    case CatalogStore::Op::DropByParentType:
    case CatalogStore::Op::DropByParent:
      return true;
    default:
      return false;
  }
}

bool IsConstructiveStoreOp(CatalogStore::Op op) {
  switch (op) {
    case CatalogStore::Op::CreateStoreTable:
    case CatalogStore::Op::AddStoreColumn:
    case CatalogStore::Op::AddStoreNotNull:
    case CatalogStore::Op::AddStoreCheck:
    case CatalogStore::Op::AddStorePrimaryKey:
    case CatalogStore::Op::AddStoreUnique:
    case CatalogStore::Op::CreateStoreIndex:
      return true;
    default:
      return false;
  }
}

void SerializeRecords(std::span<const CatalogStore::Entry> records,
                      duckdb::MemoryStream& stream) {
  stream.Write<uint8_t>(kRecordVersion);
  stream.Write<uint32_t>(static_cast<uint32_t>(records.size()));
  for (const auto& record : records) {
    stream.Write<uint8_t>(static_cast<uint8_t>(record.op));
    switch (record.op) {
      case CatalogStore::Op::PutDefinition:
        stream.Write<uint64_t>(record.key.parent_id.id());
        stream.Write<uint8_t>(static_cast<uint8_t>(record.key.type));
        stream.Write<uint64_t>(record.key.id.id());
        stream.Write<uint32_t>(static_cast<uint32_t>(record.def.size()));
        stream.WriteData(
          reinterpret_cast<const duckdb::data_t*>(record.def.data()),
          record.def.size());
        break;
      case CatalogStore::Op::DropDefinition:
        stream.Write<uint64_t>(record.key.parent_id.id());
        stream.Write<uint8_t>(static_cast<uint8_t>(record.key.type));
        stream.Write<uint64_t>(record.key.id.id());
        break;
      case CatalogStore::Op::PutSequence:
      case CatalogStore::Op::AdvanceSequence:
        stream.Write<uint64_t>(record.key.id.id());
        stream.Write<uint64_t>(record.sequence_value);
        break;
      case CatalogStore::Op::DropSequence:
        stream.Write<uint64_t>(record.key.id.id());
        break;
      case CatalogStore::Op::DropByParentType:
        stream.Write<uint64_t>(record.key.parent_id.id());
        stream.Write<uint8_t>(static_cast<uint8_t>(record.key.type));
        break;
      case CatalogStore::Op::DropByParent:
        stream.Write<uint64_t>(record.key.parent_id.id());
        break;
      default:
        SDB_ASSERT(false, "store op in a catalog frame");
        break;
    }
  }
}

// Forward cursor over a checksummed record payload; bounds enforced with
// SDB_ENSURE because a corrupted-but-checksum-valid frame is a fatal
// invariant break, not a truncation.
struct RecordCursor {
  const uint8_t* p;
  const uint8_t* end;

  explicit RecordCursor(std::span<const uint8_t> buf)
    : p{buf.data()}, end{buf.data() + buf.size()} {}

  template<typename T>
  T Read() {
    SDB_ENSURE(p + sizeof(T) <= end, "catalog wal: truncated record");
    T v;
    std::memcpy(&v, p, sizeof(T));
    p += sizeof(T);
    return v;
  }

  std::string_view ReadBytes(size_t n) {
    SDB_ENSURE(p + n <= end, "catalog wal: truncated record payload");
    std::string_view v{reinterpret_cast<const char*>(p), n};
    p += n;
    return v;
  }
};

std::vector<CatalogStore::Entry> ParseRecords(std::span<const uint8_t> frame) {
  RecordCursor cursor{frame};
  const auto version = cursor.Read<uint8_t>();
  SDB_ENSURE(version == kRecordVersion, "catalog wal: unknown record version ",
             version);
  const auto count = cursor.Read<uint32_t>();
  std::vector<CatalogStore::Entry> records;
  records.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    CatalogStore::Entry record;
    record.op = static_cast<CatalogStore::Op>(cursor.Read<uint8_t>());
    switch (record.op) {
      case CatalogStore::Op::PutDefinition: {
        record.key.parent_id = ObjectId{cursor.Read<uint64_t>()};
        record.key.type = static_cast<ObjectType>(cursor.Read<uint8_t>());
        record.key.id = ObjectId{cursor.Read<uint64_t>()};
        const auto len = cursor.Read<uint32_t>();
        record.def = std::string{cursor.ReadBytes(len)};
        break;
      }
      case CatalogStore::Op::DropDefinition:
        record.key.parent_id = ObjectId{cursor.Read<uint64_t>()};
        record.key.type = static_cast<ObjectType>(cursor.Read<uint8_t>());
        record.key.id = ObjectId{cursor.Read<uint64_t>()};
        break;
      case CatalogStore::Op::PutSequence:
      case CatalogStore::Op::AdvanceSequence:
        record.key.id = ObjectId{cursor.Read<uint64_t>()};
        record.sequence_value = cursor.Read<uint64_t>();
        break;
      case CatalogStore::Op::DropSequence:
        record.key.id = ObjectId{cursor.Read<uint64_t>()};
        break;
      case CatalogStore::Op::DropByParentType:
        record.key.parent_id = ObjectId{cursor.Read<uint64_t>()};
        record.key.type = static_cast<ObjectType>(cursor.Read<uint8_t>());
        break;
      case CatalogStore::Op::DropByParent:
        record.key.parent_id = ObjectId{cursor.Read<uint64_t>()};
        break;
      default:
        SDB_FATAL(STARTUP, "catalog wal: unknown record kind ",
                  static_cast<int>(record.op));
        break;
    }
    records.push_back(std::move(record));
  }
  SDB_ENSURE(cursor.p == cursor.end, "catalog wal: trailing record bytes");
  return records;
}

}  // namespace

CatalogStore::CatalogStore() {
  SDB_ASSERT(gInstance == nullptr);
  gInstance = this;
}

CatalogStore::~CatalogStore() { gInstance = nullptr; }

void CatalogStore::Initialize(std::string_view database_directory) {
  _directory = basics::file_utils::BuildFilename(
    std::string{database_directory}, std::string{StaticStrings::kCatalogRoot});
  const auto& dir = _directory;
  {
    absl::MutexLock lock{&_mutex};
    _wal.Open(dir, [&](std::span<const uint8_t> frame) {
      _mutex.AssertHeld();
      ApplyRecords(ParseRecords(frame));
    });
  }
  EnsureSystemDatabase();
}

void CatalogStore::Shutdown() { _wal.Close(); }

void CatalogStore::ApplyRecords(std::span<const Entry> records) {
  for (const auto& record : records) {
    switch (record.op) {
      case Op::PutDefinition: {
        auto& defs =
          _defs[{record.key.parent_id.id(),
                 static_cast<uint8_t>(record.key.type)}];
        auto it = std::lower_bound(defs.begin(), defs.end(), record.key.id,
                                   [](const BootDef& lhs, ObjectId id) {
                                     return lhs.id.id() < id.id();
                                   });
        if (it != defs.end() && it->id == record.key.id) {
          it->def = record.def;
          ++_dead_records;
        } else {
          defs.insert(it, BootDef{record.key.id, record.def});
          ++_live_records;
        }
        break;
      }
      case Op::DropDefinition: {
        auto it = _defs.find(
          {record.key.parent_id.id(), static_cast<uint8_t>(record.key.type)});
        if (it == _defs.end()) {
          ++_dead_records;
          break;
        }
        auto& defs = it->second;
        auto pos = std::lower_bound(defs.begin(), defs.end(), record.key.id,
                                    [](const BootDef& lhs, ObjectId id) {
                                      return lhs.id.id() < id.id();
                                    });
        if (pos != defs.end() && pos->id == record.key.id) {
          defs.erase(pos);
          --_live_records;
          _dead_records += 2;
        } else {
          ++_dead_records;
        }
        break;
      }
      case Op::PutSequence: {
        absl::MutexLock seq_lock{&_seq_mutex};
        auto [it, inserted] =
          _sequences.insert_or_assign(record.key.id.id(),
                                      record.sequence_value);
        if (inserted) {
          ++_live_records;
        } else {
          ++_dead_records;
        }
        break;
      }
      case Op::AdvanceSequence: {
        // Horizon bumps append outside the sequence lock, so records of one
        // sequence can land out of order; max-merge makes any order replay
        // to the highest covered horizon. setval stays an ordered assign
        // (PutSequence) -- a concurrent advance racing a setval is PG's
        // "unspecified interleaving".
        absl::MutexLock seq_lock{&_seq_mutex};
        auto [it, inserted] =
          _sequences.try_emplace(record.key.id.id(), record.sequence_value);
        if (inserted) {
          ++_live_records;
        } else {
          it->second = std::max(it->second, record.sequence_value);
          ++_dead_records;
        }
        break;
      }
      case Op::DropSequence: {
        absl::MutexLock seq_lock{&_seq_mutex};
        if (_sequences.erase(record.key.id.id()) > 0) {
          --_live_records;
          _dead_records += 2;
        } else {
          ++_dead_records;
        }
        break;
      }
      case Op::DropByParentType: {
        auto it = _defs.find(
          {record.key.parent_id.id(), static_cast<uint8_t>(record.key.type)});
        if (it != _defs.end()) {
          _live_records -= it->second.size();
          _dead_records += it->second.size() + 1;
          _defs.erase(it);
        } else {
          ++_dead_records;
        }
        break;
      }
      case Op::DropByParent: {
        ++_dead_records;
        for (auto it = _defs.begin(); it != _defs.end();) {
          if (it->first.first == record.key.parent_id.id()) {
            _live_records -= it->second.size();
            _dead_records += it->second.size();
            _defs.erase(it++);
          } else {
            ++it;
          }
        }
        break;
      }
      default:
        SDB_ASSERT(false, "store op applied to catalog maps");
        break;
    }
  }
}

void CatalogStore::AppendBatch(std::span<const Entry> records) {
  SDB_ASSERT(!records.empty());
  duckdb::MemoryStream stream;
  SerializeRecords(records, stream);
  _wal.Append({stream.GetData(), stream.GetPosition()});
  ApplyRecords(records);
}

void CatalogStore::MaybeCompact() {
  if (_dead_records <= _live_records ||
      _wal.GetStats().size_on_disk < kCompactMinBytes) {
    return;
  }
  absl::MutexLock seq_lock{&_seq_mutex};
  std::vector<std::pair<uint64_t, uint8_t>> keys;
  keys.reserve(_defs.size());
  for (const auto& [key, defs] : _defs) {
    keys.push_back(key);
  }
  std::sort(keys.begin(), keys.end());
  std::vector<uint64_t> seq_ids;
  seq_ids.reserve(_sequences.size());
  for (const auto& [id, value] : _sequences) {
    seq_ids.push_back(id);
  }
  std::sort(seq_ids.begin(), seq_ids.end());

  _wal.Compact([&](CatalogWal::FrameSink sink) {
    _mutex.AssertHeld();
    _seq_mutex.AssertHeld();
    duckdb::MemoryStream stream;
    std::vector<Entry> records;
    for (const auto& key : keys) {
      for (const auto& def : _defs[key]) {
        Entry record;
        record.op = Op::PutDefinition;
        record.key = {ObjectId{key.first}, static_cast<ObjectType>(key.second),
                      def.id};
        record.def = def.def;
        records.push_back(std::move(record));
      }
    }
    for (const auto id : seq_ids) {
      Entry record;
      record.op = Op::PutSequence;
      record.key = {.id = ObjectId{id}};
      record.sequence_value = _sequences[id];
      records.push_back(std::move(record));
    }
    if (records.empty()) {
      return;
    }
    SerializeRecords(records, stream);
    sink({stream.GetData(), stream.GetPosition()});
  });
  _dead_records = 0;
}

void CatalogStore::Write(absl::FunctionRef<void(WriteContext&)> fill) {
  WriteContext ctx;
  fill(ctx);
  if (ctx._entries.empty()) {
    return;
  }

  std::vector<Entry> records;
  std::vector<Entry> store_ops;
  bool has_retype = false;
  bool has_destructive = false;
  for (auto& entry : ctx._entries) {
    if (IsCatalogRecord(entry.op)) {
      records.push_back(std::move(entry));
      continue;
    }
    has_retype |= entry.op == Op::ChangeStoreColumnType;
    has_destructive |= !IsConstructiveStoreOp(entry.op) &&
                       entry.op != Op::ChangeStoreColumnType;
    store_ops.push_back(std::move(entry));
  }

  absl::MutexLock lock{&_mutex};
  if (store_ops.empty()) {
    AppendBatch(records);
    MaybeCompact();
    return;
  }

  auto& data = GetDataStore();
  if (has_retype) {
    // Order-sensitive bracket: flag first, data commit, then the batch. The
    // reconciler resolves the flag by comparing store shapes against its
    // payload if we crash in between.
    duckdb::MemoryStream payload;
    SerializeRecords(records, payload);
    Entry flag;
    flag.op = Op::PutDefinition;
    flag.key = kPendingAlterKey;
    flag.def = std::string{reinterpret_cast<const char*>(payload.GetData()),
                           payload.GetPosition()};
    AppendBatch({&flag, 1});
    SDB_IF_FAILURE("crash_catalog_after_flag") { SDB_IMMEDIATE_ABORT(); }
    if (auto r = data.ApplyStoreOps(store_ops); !r.ok()) {
      Entry drop;
      drop.op = Op::DropDefinition;
      drop.key = kPendingAlterKey;
      AppendBatch({&drop, 1});
      THROW_SQL_ERROR(ERR_MSG(r.message()));
    }
    SDB_IF_FAILURE("crash_catalog_after_retype_data") {
      SDB_IMMEDIATE_ABORT();
    }
    Entry drop;
    drop.op = Op::DropDefinition;
    drop.key = kPendingAlterKey;
    records.push_back(std::move(drop));
    AppendBatch(records);
  } else if (!has_destructive) {
    // Constructive: validate + commit on the data DB first; a crash before
    // the append leaves orphans the reconciler drops.
    if (auto r = data.ApplyStoreOps(store_ops); !r.ok()) {
      THROW_SQL_ERROR(ERR_MSG(r.message()));
    }
    SDB_IF_FAILURE("crash_catalog_after_store_create") {
      SDB_IMMEDIATE_ABORT();
    }
    if (!records.empty()) {
      AppendBatch(records);
    }
  } else {
    // Destructive (or mixed, all derivable from defs): the append is the ack
    // point; a crash before the data commit leaves leftovers the reconciler
    // finishes.
    if (!records.empty()) {
      AppendBatch(records);
    }
    SDB_IF_FAILURE("crash_catalog_before_store_drop") {
      SDB_IMMEDIATE_ABORT();
    }
    if (auto r = data.ApplyStoreOps(store_ops); !r.ok()) {
      THROW_SQL_ERROR(ERR_MSG(r.message()));
    }
  }
  MaybeCompact();
}

void CatalogStore::CreateDefinition(ObjectId parent_id, ObjectType type,
                                    ObjectId id, std::string_view def) {
  Write([&](WriteContext& ctx) { ctx.PutDefinition(parent_id, type, id, def); });
}

void CatalogStore::DropDefinition(ObjectId parent_id, ObjectType type,
                                  ObjectId id) {
  Write([&](WriteContext& ctx) { ctx.DropDefinition(parent_id, type, id); });
}

void CatalogStore::DropSequence(ObjectId sequence_id) {
  Write([&](WriteContext& ctx) { ctx.DropSequence(sequence_id); });
}

void CatalogStore::WriteContext::DropEntry(ObjectId parent_id,
                                           ObjectType type) {
  _entries.push_back({
    .op = Op::DropByParentType,
    .key = {.parent_id = parent_id, .type = type},
  });
}

void CatalogStore::WriteContext::DropEntry(ObjectId parent_id) {
  _entries.push_back({
    .op = Op::DropByParent,
    .key = {.parent_id = parent_id},
  });
}

void CatalogStore::DropEntry(ObjectId parent_id, ObjectType type) {
  Write([&](WriteContext& ctx) { ctx.DropEntry(parent_id, type); });
}

void CatalogStore::DropEntry(ObjectId parent_id) {
  Write([&](WriteContext& ctx) { ctx.DropEntry(parent_id); });
}

void CatalogStore::WriteTombstone(ObjectId parent_id, ObjectId id) {
  Write([&](WriteContext& ctx) { ctx.WriteTombstone(parent_id, id); });
}

void CatalogStore::VisitDefinitions(
  ObjectId parent_id, ObjectType type,
  absl::FunctionRef<bool(Key, std::string_view)> visitor) {
  VisitBoot(parent_id, type, visitor);
}

void CatalogStore::VisitBoot(
  ObjectId parent_id, ObjectType type,
  absl::FunctionRef<bool(Key, std::string_view)> visitor) const {
  std::vector<BootDef> defs;
  {
    absl::MutexLock lock{&_mutex};
    const auto it = _defs.find({parent_id.id(), static_cast<uint8_t>(type)});
    if (it == _defs.end()) {
      return;
    }
    defs = it->second;
  }
  for (const auto& def : defs) {
    if (!visitor(Key{parent_id, type, def.id}, def.def)) {
      return;
    }
  }
}

bool CatalogStore::TryGetBootSequenceValue(ObjectId sequence_id,
                                           uint64_t& value) const {
  absl::MutexLock lock{&_seq_mutex};
  const auto it = _sequences.find(sequence_id.id());
  if (it == _sequences.end()) {
    return false;
  }
  value = it->second;
  return true;
}

namespace {

// The hottest appends (every horizon bump): a fixed-shape single sequence
// record, serialized into a stack buffer.
struct SequenceFrame {
  uint8_t bytes[sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint8_t) +
                sizeof(uint64_t) + sizeof(uint64_t)];

  SequenceFrame(CatalogStore::Op op, ObjectId sequence_id, uint64_t value) {
    uint8_t* p = bytes;
    const auto put = [&p](const auto v) {
      std::memcpy(p, &v, sizeof(v));
      p += sizeof(v);
    };
    put(kRecordVersion);
    put(uint32_t{1});
    put(static_cast<uint8_t>(op));
    put(sequence_id.id());
    put(value);
    SDB_ASSERT(p == bytes + sizeof(bytes));
  }
};

}  // namespace

void CatalogStore::PutSequenceValue(ObjectId sequence_id, uint64_t value) {
  // Off the DDL mutex so concurrent sequence persists group-commit in the
  // WAL. Map before append: a compaction snapshotting concurrently then
  // already contains the value, and the caller only hands values out after
  // the append returns durable.
  {
    absl::MutexLock seq_lock{&_seq_mutex};
    _sequences.insert_or_assign(sequence_id.id(), value);
  }
  const SequenceFrame frame{Op::PutSequence, sequence_id, value};
  _wal.Append({frame.bytes, sizeof(frame.bytes)});
}

void CatalogStore::AdvanceSequenceValue(ObjectId sequence_id, uint64_t value) {
  {
    absl::MutexLock seq_lock{&_seq_mutex};
    auto [it, inserted] = _sequences.try_emplace(sequence_id.id(), value);
    if (!inserted) {
      it->second = std::max(it->second, value);
    }
  }
  const SequenceFrame frame{Op::AdvanceSequence, sequence_id, value};
  _wal.Append({frame.bytes, sizeof(frame.bytes)});
}

void CatalogStore::GetSequenceValue(ObjectId sequence_id, uint64_t& value) {
  if (!TryGetBootSequenceValue(sequence_id, value)) {
    value = 0;
  }
}

std::optional<std::vector<CatalogStore::Entry>>
CatalogStore::TakePendingAlter() {
  std::string payload;
  {
    absl::MutexLock lock{&_mutex};
    const auto it =
      _defs.find({kPendingAlterKey.parent_id.id(),
                  static_cast<uint8_t>(kPendingAlterKey.type)});
    if (it == _defs.end() || it->second.empty()) {
      return std::nullopt;
    }
    SDB_ASSERT(it->second.size() == 1);
    payload = it->second.front().def;
  }
  return ParseRecords(
    {reinterpret_cast<const uint8_t*>(payload.data()), payload.size()});
}

void CatalogStore::CommitPendingAlter(std::vector<Entry> records) {
  Entry drop;
  drop.op = Op::DropDefinition;
  drop.key = kPendingAlterKey;
  records.push_back(std::move(drop));
  absl::MutexLock lock{&_mutex};
  AppendBatch(records);
  MaybeCompact();
}

void CatalogStore::AbortPendingAlter() {
  Entry drop;
  drop.op = Op::DropDefinition;
  drop.key = kPendingAlterKey;
  absl::MutexLock lock{&_mutex};
  AppendBatch({&drop, 1});
}

std::vector<CatalogStore::Entry> CatalogStore::ParseFrame(
  std::span<const uint8_t> frame) {
  return ParseRecords(frame);
}

void CatalogStore::VisitSnapshot(
  absl::FunctionRef<void(Key, std::string_view)> def_visitor,
  absl::FunctionRef<void(ObjectId, uint64_t)> sequence_visitor) {
  absl::MutexLock lock{&_mutex};
  std::vector<std::pair<uint64_t, uint8_t>> keys;
  keys.reserve(_defs.size());
  for (const auto& [key, defs] : _defs) {
    keys.push_back(key);
  }
  std::sort(keys.begin(), keys.end());
  for (const auto& key : keys) {
    for (const auto& def : _defs[key]) {
      def_visitor(
        Key{ObjectId{key.first}, static_cast<ObjectType>(key.second), def.id},
        def.def);
    }
  }
  absl::MutexLock seq_lock{&_seq_mutex};
  std::vector<uint64_t> seq_ids;
  seq_ids.reserve(_sequences.size());
  for (const auto& [id, value] : _sequences) {
    seq_ids.push_back(id);
  }
  std::sort(seq_ids.begin(), seq_ids.end());
  for (const auto id : seq_ids) {
    sequence_visitor(ObjectId{id}, _sequences[id]);
  }
}

void CatalogStore::ValidateStoreTable(const StoreTableDef& def) {
#ifdef SDB_DEV
  if (DataStore::IsReady()) {
    GetDataStore().ValidateStoreTable(def);
  }
#endif
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

}  // namespace sdb::catalog
