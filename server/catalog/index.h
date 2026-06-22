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
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_set.h"
#include "catalog/object.h"
#include "catalog/persistence/index.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"

namespace sdb {
namespace catalog {

inline constexpr std::string_view kIncludedKind = "included";
inline constexpr std::string_view kHNSWKind = "hnsw";

class SecondaryIndex;
class InvertedIndex;

using persistence::ExpressionData;
using persistence::InvertedIndexOptions;

struct CreateIndexColumn {
  std::string_view name;
  const catalog::Column* catalog_column = nullptr;
  std::optional<ExpressionData> indexed_expr;
  std::string opclass;
  // nullopt = no parentheses in source SQL; an (empty or non-empty) map means
  // parens were present, distinguishing `col opclass` from `col opclass ()`.
  std::optional<duckdb::case_insensitive_map_t<duckdb::Value>> opclass_options;

  bool IsIndexedExpression() const noexcept {
    SDB_ASSERT(!catalog_column != !indexed_expr);
    return !catalog_column;
  }

  bool HasParentheses() const noexcept { return opclass_options.has_value(); }

  bool IsBuiltin(std::string_view name) const noexcept {
    return HasParentheses() && opclass == name;
  }

  const ExpressionData& GetIndexedExpression() const noexcept {
    SDB_ASSERT(IsIndexedExpression());
    return *indexed_expr;
  }

  const catalog::Column& GetCatalogColumn() const noexcept {
    SDB_ASSERT(!IsIndexedExpression());
    return *catalog_column;
  }
};

class Index : public Object {
 public:
  ObjectId GetDatabaseId() const noexcept { return _database_id; }
  auto GetRelationId() const noexcept { return _relation_id; }

  // Plain-column key ids, de-duped in first-seen order (expression keys
  // excluded). Returns a reference; the subclass computes it once at
  // construction, no per-call allocation.
  const std::vector<Column::Id>& GetColumnIds() const noexcept {
    return _column_ids;
  }
  // GetColumnIds() plus each expression key's dependent columns (de-duped).
  const std::vector<Column::Id>& GetReferencedColumnIds() const noexcept {
    return _referenced_column_ids;
  }

  // O(1) membership: is `id` a plain-column key of this index? (Backed by a
  // set built at construction -- use this instead of scanning GetColumnIds().)
  bool HasColumn(Column::Id id) const noexcept {
    return _column_id_set.contains(id);
  }

  virtual containers::FlatHashSet<ObjectId> GetTokenizers() const { return {}; }

  virtual ~Index() = default;

 protected:
  // The base's common query surface, derived once by the subclass from its key
  // storage. `column_ids` = de-duped plain-column key ids (first-seen order);
  // `referenced` = `column_ids` followed by each expression's dependent
  // columns, de-duped.
  struct DerivedColumnIds {
    std::vector<Column::Id> column_ids;
    std::vector<Column::Id> referenced;
  };

  // The subclass owns its key storage (secondary: ordered sentinel list;
  // inverted: column ids + expression keys) and hands the base the derived id
  // surface (computed once via DeriveIds/the subclass equivalent).
  Index(ObjectId database_id, ObjectId schema_id, ObjectId id,
        ObjectId relation_id, std::string name, DerivedColumnIds derived,
        ObjectType type);

  // De-duped plain-column ids in first-seen order (Column::kInvalidId
  // expression sentinels skipped), AND the membership set used to de-dup them.
  // Returning the set lets the caller extend it with expression dependent
  // columns without building a second hash set.
  static std::pair<std::vector<Column::Id>, containers::FlatHashSet<Column::Id>>
  DedupColumns(std::span<const Column::Id> columns);
  // One pass (single hash set): de-dup `columns` and append each expression's
  // dependent columns.
  static DerivedColumnIds DeriveIds(
    std::span<const Column::Id> columns,
    std::span<const ExpressionData> expressions);

  ObjectId _database_id;
  ObjectId _relation_id;
  std::vector<Column::Id> _column_ids;
  std::vector<Column::Id> _referenced_column_ids;
  containers::FlatHashSet<Column::Id> _column_id_set;
};

ResultOr<std::shared_ptr<SecondaryIndex>> CreateSecondaryIndex(
  ObjectId database_id, ObjectId schema_id, ObjectId id, ObjectId relation_id,
  std::string name, std::vector<catalog::CreateIndexColumn> columns,
  bool unique);

ResultOr<std::shared_ptr<InvertedIndex>> CreateInvertedIndex(
  duckdb::ClientContext& context, ObjectId database_id,
  std::string_view schema_name, ObjectId schema_id, ObjectId id,
  ObjectId relation_id, std::string name,
  std::vector<catalog::CreateIndexColumn> columns,
  const std::shared_ptr<const Snapshot>& snapshot,
  InvertedIndexOptions options);

}  // namespace catalog
}  // namespace sdb
