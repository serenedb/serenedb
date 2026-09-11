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

#include <absl/functional/function_ref.h>

#include <duckdb/common/types/value.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <iresearch/index/column_info.hpp>
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
namespace sdb::basics {

class JsonSink;

}  // namespace sdb::basics
namespace sdb {
namespace catalog {

// Which of the two index kinds a record is, as duckdb's own `index_type` states
// it: one home for the fact, read by upstream machinery and by us alike. The
// store's mirror of a plain one says duckdb's own name for the ART instead.
inline constexpr std::string_view kInvertedIndexType = "inverted";
inline constexpr std::string_view kSecondaryIndexType = "secondary";

inline constexpr std::string_view kIncludedKind = "included";
inline constexpr std::string_view kIVFKind = "ivf";
inline constexpr std::string_view kHNSWKind = "hnsw";

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

// One index, as the catalog holds it. No owner or ACL anywhere: postgres gives
// an index none, every privilege decision reads the relation it is built on
// (RequireIndexOwner). The one kind with two ancestors -- name in the
// schema's relation namespace, rows in a relation -- so both ids are its own;
// the record that carries it names only the schema.
class Index {
 public:
  Index(const Index&) = delete;
  Index& operator=(const Index&) = delete;
  virtual ~Index() = default;

  ObjectId GetId() const noexcept { return _id; }
  ObjectId GetSchemaId() const noexcept { return _schema_id; }
  ObjectId GetParentId() const noexcept { return GetSchemaId(); }
  ObjectId GetRelationId() const noexcept { return _relation_id; }

  std::string_view GetName() const noexcept { return _name; }

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

  virtual containers::FlatHashSet<ObjectId> GetTokenizers() const = 0;

  std::string_view Comment() const noexcept { return _comment; }

  virtual void WriteJson(basics::JsonSink& sink) const = 0;

  // What the kind reads back on its own, inside the record's payload.
  virtual void SerializePayload(duckdb::Serializer& sink) const = 0;

 protected:
  struct DerivedColumnIds {
    std::vector<ColumnId> columns;
    std::vector<ColumnId> referenced_columns;
    containers::FlatHashSet<ColumnId> referenced_columns_set;
  };

  Index(ObjectId schema_id, ObjectId id, ObjectId relation_id,
        std::string_view name, std::string comment, DerivedColumnIds derived);

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
  std::string _name;
  std::string _comment;
  ObjectId _id;
  ObjectId _schema_id;
  ObjectId _relation_id;
};

// An index on its way into the catalog or into the log, which is the only place
// this shape is used: the entry holds the index itself. What duckdb's own
// CreateIndexInfo has nowhere to put -- the relation the rows belong to --
// rides beside the payload the kind reads back on its own, inside duckdb's own
// CreateInfo serialization.
class CreateIndexInfo final : public duckdb::CreateIndexInfo {
 public:
  // A plain ART, which duckdb's own fields describe in full: it is the index
  // duckdb builds and maintains, and serenedb adds only the identity the
  // catalog files it under.
  CreateIndexInfo(
    ObjectId schema_id, ObjectId id, ObjectId relation_id,
    std::string_view name, bool unique, std::vector<ColumnId> key_columns,
    std::vector<ColumnId> referenced_columns,
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> keys);
  // An inverted index, which serenedb's own object describes.
  explicit CreateIndexInfo(std::shared_ptr<const Index> index);

  ObjectId GetId() const noexcept { return ObjectId{oid}; }
  ObjectId GetSchemaId() const noexcept { return ObjectId{parent_oid}; }
  ObjectId GetRelationId() const noexcept { return _relation_id; }
  std::string_view GetName() const noexcept {
    return GetIndexName().GetIdentifierName();
  }

  bool IsInverted() const noexcept { return _index != nullptr; }

  // Null for a plain ART: that index is duckdb's, and duckdb's own fields here
  // -- the keys, their types, the UNIQUE constraint -- are the whole of it.
  const std::shared_ptr<const Index>& GetIndex() const noexcept {
    return _index;
  }

  // The columns the index is filed under, by the ids that outlive a rename:
  // the keys, and every column it reads (keys plus expression and predicate
  // dependencies). A kInvalidColumnId key slot is an expression.
  const std::vector<ColumnId>& GetColumns() const noexcept;
  const std::vector<ColumnId>& GetReferencedColumns() const noexcept;
  bool ReferencesColumn(ColumnId id) const noexcept;
  bool IsUnique() const noexcept {
    return constraint_type == duckdb::IndexConstraintType::UNIQUE;
  }

  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;
  void Serialize(duckdb::Serializer& sink) const final;

 private:
  std::shared_ptr<const Index> _index;
  std::vector<ColumnId> _key_columns;
  std::vector<ColumnId> _referenced_columns;
  ObjectId _relation_id;
};

duckdb::unique_ptr<duckdb::CreateInfo> DeserializeIndexInfo(
  duckdb::Deserializer& src);

duckdb::unique_ptr<Index> NewInvertedIndex(
  duckdb::ClientContext& context, ObjectId database_id,
  std::string_view schema_name, ObjectId schema_id, ObjectId id,
  ObjectId relation_id, std::string name,
  std::vector<catalog::CreateIndexColumn> columns, InvertedIndexOptions options,
  ExpressionData predicate, bool search_engine);

// The inverted index `id` names in `database_id`, off the entries that hold it.
// For the background paths, which have no statement to read through and no
// database to start from but the one the storage was opened for.
std::shared_ptr<const Index> FindInvertedIndex(ObjectId database_id,
                                               ObjectId id);

// The versions of `indexes` an ALTER TABLE leaves behind: every index of the
// relation, republished so the index-as-table wrappers are rebuilt from the
// altered definition. A plain ART's keys name the table's columns, so a rename
// re-renders them; the ids it is filed under do not move.
std::vector<duckdb::unique_ptr<CreateIndexInfo>> RelationIndexVersions(
  std::span<const duckdb::unique_ptr<CreateIndexInfo>> indexes,
  const duckdb::CreateTableInfo& before, const duckdb::CreateTableInfo& after);

// The same index under a new name, and under a new comment: an inverted one is
// rebuilt, a plain ART is duckdb's own record with the field changed.
duckdb::unique_ptr<duckdb::CreateInfo> RenamedIndexRecord(
  const CreateIndexInfo& index, std::string_view name);
duckdb::unique_ptr<duckdb::CreateInfo> RecommentedIndexRecord(
  const CreateIndexInfo& index, std::string_view comment);
duckdb::unique_ptr<duckdb::CreateInfo> ReoptionedIndexRecord(
  const CreateIndexInfo& index, InvertedIndexOptions options);

}  // namespace catalog
}  // namespace sdb
