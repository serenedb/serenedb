////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <duckdb/common/types/value.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "catalog/entry.h"
#include "catalog/persistence/index.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::search {

class InvertedIndexStorage;

}  // namespace sdb::search
namespace sdb {
namespace catalog {

// Which of the two index kinds an info is, as duckdb's own `index_type` states
// it: one home for the fact, read by upstream machinery and by us alike.
inline constexpr std::string_view kInvertedIndexType = "inverted";
inline constexpr std::string_view kSecondaryIndexType = "secondary";

inline constexpr std::string_view kIncludedKind = "included";
inline constexpr std::string_view kIVFKind = "ivf";

using persistence::ExpressionData;
using persistence::InvertedIndexOptions;
using persistence::PkColumnKind;

// The base column an index key names when it is a plain column reference:
// the stable id the postings are filed under and the type they are tokenized
// as. By value rather than a pointer into the relation's column list, which a
// concurrent rewrite of that relation would move.
struct IndexedColumnRef {
  ColumnId id;
  duckdb::LogicalType type;
};

struct CreateIndexColumn {
  std::string_view name;
  std::optional<IndexedColumnRef> column;
  std::optional<ExpressionData> indexed_expr;
  std::string opclass;
  std::optional<duckdb::case_insensitive_map_t<duckdb::Value>> opclass_options;

  bool IsIndexedExpression() const noexcept {
    SDB_ASSERT(column.has_value() != indexed_expr.has_value());
    return !column;
  }

  bool HasParentheses() const noexcept { return opclass_options.has_value(); }

  bool IsBuiltin(std::string_view name) const noexcept {
    return HasParentheses() && opclass == name;
  }

  const ExpressionData& GetIndexedExpression() const noexcept {
    SDB_ASSERT(IsIndexedExpression());
    return *indexed_expr;
  }

  const IndexedColumnRef& GetColumn() const noexcept {
    SDB_ASSERT(!IsIndexedExpression());
    return *column;
  }
};

// The iresearch runtime of one inverted index -- writer, reader, refresh state.
// It is not part of the definition and it is not per-version: a rename or an
// ALTER INDEX SET writes a new definition and the storage behind it is the
// same directory (keyed by ids, not by name). Held behind a shared_ptr so
// binding it works through a const definition -- the SequenceCounter
// arrangement -- and so every version of the definition reaches the same
// storage.
//
// Never serialized: what the catalog log holds is the definition.
class InvertedIndexRuntime {
 public:
  const std::shared_ptr<search::InvertedIndexStorage>& Get() const noexcept {
    return _storage;
  }
  void Set(std::shared_ptr<search::InvertedIndexStorage> storage) noexcept {
    _storage = std::move(storage);
  }

 private:
  std::shared_ptr<search::InvertedIndexStorage> _storage;
};

// One index, in the form a catalog entry is built from. duckdb's own
// CreateIndexInfo has no stable ids, no catalog column ids and no comment, so
// this extends it rather than replacing it: what upstream understands stays
// where upstream looks for it, and the persisted payload adds what it lacks.
//
// Owner and ACL are not here, and are nowhere: postgres gives an index no ACL
// of its own, so every privilege decision reads the relation it is built on
// (RequireRelationOwner).
//
// An index is the one object with two ancestors -- its name lives in the
// schema's relation namespace, its rows belong to a relation -- so both ids
// ride the info, and the record that carries it names only the schema.
class CreateIndexInfoBase : public duckdb::CreateIndexInfo {
 public:
  ObjectId GetId() const noexcept { return ObjectId{oid}; }
  ObjectId GetSchemaId() const noexcept { return ObjectId{parent_oid}; }
  // The schema is where the name lives; the relation the rows belong to is the
  // other ancestor, which no other kind has.
  ObjectId GetParentId() const noexcept { return GetSchemaId(); }
  ObjectId GetRelationId() const noexcept { return _relation_id; }

  std::string_view GetName() const noexcept {
    return GetIndexName().GetIdentifierName();
  }

  // The de-duped key columns, and every column the index reads -- keys plus
  // expression and predicate dependencies. The store mirror declares the second
  // set, so duckdb populates their chunk vectors on DML.
  const std::vector<ColumnId>& GetColumns() const noexcept { return _columns; }
  const std::vector<ColumnId>& GetReferencedColumns() const noexcept {
    return _referenced_columns;
  }

  bool ReferencesColumn(ColumnId id) const noexcept {
    return _referenced_columns_set.contains(id);
  }

  virtual containers::FlatHashSet<ObjectId> GetTokenizers() const { return {}; }

  // CreateInfo::comment is the one home: COMMENT ON reaches it there, and
  // duckdb's own serialization carries it.
  std::string_view Comment() const noexcept {
    return comment.IsNull()
             ? std::string_view{}
             : std::string_view{duckdb::StringValue::Get(comment)};
  }

  // Which of the two index kinds this is -- the same fact duckdb's own
  // `index_type` string carries, which is how it tells one index apart from
  // another.
  bool IsInverted() const noexcept { return index_type == kInvertedIndexType; }

  // The iresearch runtime behind an inverted index. Null for a secondary index,
  // whose rows are an ART on the store table.
  //
  // It rides the info because the info is what the catalog record carries: an
  // applier rebuilding the object from a record has nowhere else to find the
  // storage the statement bound, and a rewrite (rename, comment, ALTER INDEX
  // SET) is the same index behind the same directory, so every version shares
  // one holder. AdoptRuntime is how a rewrite hands its own over.
  const std::shared_ptr<InvertedIndexRuntime>& Runtime() const noexcept {
    return _runtime;
  }
  void AdoptRuntime(std::shared_ptr<InvertedIndexRuntime> runtime) noexcept {
    SDB_ASSERT(IsInverted() == (runtime != nullptr));
    _runtime = std::move(runtime);
  }

  const std::shared_ptr<search::InvertedIndexStorage>& GetData()
    const noexcept {
    return _runtime->Get();
  }
  void SetData(std::shared_ptr<search::InvertedIndexStorage> storage) const {
    _runtime->Set(std::move(storage));
  }

  virtual void WriteJson(basics::JsonSink& sink) const = 0;

 protected:
  struct DerivedColumnIds {
    std::vector<ColumnId> columns;
    std::vector<ColumnId> referenced_columns;
    containers::FlatHashSet<ColumnId> referenced_columns_set;
  };

  CreateIndexInfoBase(ObjectId schema_id, ObjectId id, ObjectId relation_id,
                      std::string_view name, std::string comment,
                      DerivedColumnIds derived, bool inverted);

  static std::pair<std::vector<ColumnId>, containers::FlatHashSet<ColumnId>>
  DedupColumns(std::span<const ColumnId> columns);

  // `extra_deps` are columns referenced by the index beyond its keys and
  // expression dependencies (e.g. a partial-index predicate's columns); folded
  // into the referenced set in the same dedup pass.
  template<typename Expressions>
  static DerivedColumnIds DeriveIds(std::span<const ColumnId> columns,
                                    Expressions&& expressions,
                                    std::span<const ColumnId> extra_deps) {
    auto [column_ids, seen] = DedupColumns(columns);
    auto referenced = column_ids;
    auto add_dep = [&](ColumnId dep) {
      if (seen.emplace(dep).second) {  // reuse the column dedup set
        referenced.push_back(dep);
      }
    };
    for (const auto& expression : expressions) {
      for (const auto dep : expression.dependent_columns) {
        add_dep(dep);
      }
    }
    for (const auto dep : extra_deps) {
      add_dep(dep);
    }
    return {std::move(column_ids), std::move(referenced), std::move(seen)};
  }

  std::vector<ColumnId> _columns;
  std::vector<ColumnId> _referenced_columns;
  containers::FlatHashSet<ColumnId> _referenced_columns_set;
  ObjectId _relation_id;

 private:
  std::shared_ptr<InvertedIndexRuntime> _runtime;
};

// An index is its own CreateIndexInfoBase -- everything is the info's,
// including the iresearch storage holder and what its expression keys resolved
// to (CreateInfo::dependencies).
//
// An index has no owner and no ACL of its own: postgres gives an index none, so
// every privilege decision reads the relation it is built on
// (RequireRelationOwner).
using IndexInfoRef = std::shared_ptr<const CreateIndexInfoBase>;

IndexInfoRef NewSecondaryIndex(ObjectId schema_id, ObjectId id,
                               ObjectId relation_id, std::string name,
                               std::vector<catalog::CreateIndexColumn> columns,
                               bool unique);

IndexInfoRef NewInvertedIndex(duckdb::ClientContext& context,
                              ObjectId database_id,
                              std::string_view schema_name, ObjectId schema_id,
                              ObjectId id, ObjectId relation_id,
                              std::string name,
                              std::vector<catalog::CreateIndexColumn> columns,
                              InvertedIndexOptions options,
                              ExpressionData predicate);

// The inverted index `id` names in `database_id`, off the entries that hold it.
// For the background paths, which have no statement to read through and no
// database to start from but the one the storage was opened for.
IndexInfoRef FindInvertedIndex(ObjectId database_id, ObjectId id);

// Rename, SET COMMENT and ALTER INDEX SET rewrite the info: it is const and
// shared, and it is what a catalog entry is built from. The storage holder
// carries over -- it is the same index behind the same directory.
IndexInfoRef RenamedIndex(const CreateIndexInfoBase& index,
                          std::string_view name);
IndexInfoRef CommentedIndex(const CreateIndexInfoBase& index,
                            std::string_view comment);
IndexInfoRef ReoptionedIndex(const CreateIndexInfoBase& index,
                             InvertedIndexOptions options);

// The versions of `indexes` an ALTER TABLE that renamed columns leaves behind.
// An expression key's stored `pretty_printed` bakes the column names in for
// display and re-rendering, so a rename has to refresh it; the bound
// expression is column-id keyed and unaffected. Only the indexes whose text
// actually changed are returned -- the others are already right.
std::vector<IndexInfoRef> RerenderedIndexes(
  std::span<const IndexInfoRef> indexes, const duckdb::CreateTableInfo& before,
  const duckdb::CreateTableInfo& after);

}  // namespace catalog
}  // namespace sdb
