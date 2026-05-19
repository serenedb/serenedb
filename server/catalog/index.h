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
#include <optional>
#include <string>
#include <vector>

#include "catalog/object.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"
#include "catalog/types.h"

namespace sdb {

class IndexShard;

namespace catalog {

class SecondaryIndex;
class InvertedIndex;

struct InvertedIndexOptions {
  uint32_t row_group_size = 0;
  uint32_t norm_row_group_size = 0;
  uint32_t commit_interval_ms = 0;
  uint32_t consolidation_interval_ms = 0;
  uint32_t cleanup_interval_step = 0;
  std::optional<ScorerOptions> topk_scorer;
};

// Carried on a `CreateIndexColumn` whose source SQL was a parenthesised
// expression instead of a bare column reference. The expression has already
// been normalised and serialised by the IndexBinder; `dependent_columns`
// is the set of base-table columns the expression reads from, used by the
// sink to project the chunk and by DML to decide which indexes a write
// must touch. `pretty_printed` is captured at CREATE INDEX time for
// diagnostics so we don't need a ClientContext to display the expression.
struct IndexedExpressionData {
  std::string serialized;
  std::string pretty_printed;
  std::vector<Column::Id> dependent_columns;
  duckdb::LogicalType return_type;
};

// Aggregated info about column for index creation.
// Filled on different levels during creaton to gather all
// necessary info for building and validating new index.
//
// `indexed_expr` is set iff the source was `(expr)` rather than a column
// reference; `catalog_column`/`name`/`json_pointer` are unused in that case
// and `IsIndexedExpression()` returns true. The two arms are mutually
// exclusive.
struct CreateIndexColumn {
  const catalog::Column* catalog_column{nullptr};
  std::string_view name;
  std::string opclass;
  std::string json_pointer;
  std::optional<IndexedExpressionData> indexed_expr;
  // nullopt = no parentheses in source SQL; an (empty or non-empty) map means
  // parens were present, distinguishing `col opclass` from `col opclass ()`.
  std::optional<duckdb::case_insensitive_map_t<duckdb::Value>> opclass_options;

  bool IsIndexedExpression() const noexcept { return indexed_expr.has_value(); }

  const IndexedExpressionData& GetIndexedExpression() const {
    SDB_ASSERT(IsIndexedExpression());
    return *indexed_expr;
  }

  const catalog::Column* GetCatalogColumn() const noexcept {
    SDB_ASSERT(!IsIndexedExpression());
    return catalog_column;
  }

  void SetCatalogColumn(const catalog::Column* col) noexcept {
    SDB_ASSERT(!IsIndexedExpression());
    catalog_column = col;
  }

  std::string_view ColumnName() const noexcept {
    SDB_ASSERT(!IsIndexedExpression());
    return name;
  }
};

class Index : public Object {
 public:
  ObjectId GetDatabaseId() const noexcept { return _database_id; }
  auto GetRelationId() const noexcept { return _relation_id; }
  std::span<const Column::Id> GetColumnIds() const noexcept {
    return _column_ids;
  }

  // Returns _column_ids by default; InvertedIndex overrides to also include
  // base-table columns referenced only by indexed expressions, so that DML
  // routing and chunk projection see the full set of columns the index
  // depends on.
  virtual std::vector<Column::Id> GetReferencedColumnIds() const {
    return std::vector<Column::Id>(_column_ids.begin(), _column_ids.end());
  }

  virtual containers::FlatHashSet<ObjectId> GetTokenizers() const { return {}; }

  virtual ResultOr<std::shared_ptr<IndexShard>> CreateIndexShard(
    bool is_new, ObjectId id) const = 0;

  virtual ~Index() = default;

 protected:
  Index(ObjectId database_id, ObjectId schema_id, ObjectId id,
        ObjectId relation_id, std::string name,
        std::vector<Column::Id> column_ids, ObjectType type);

  ObjectId _database_id;
  ObjectId _relation_id;
  std::vector<Column::Id> _column_ids;
};

ResultOr<std::shared_ptr<SecondaryIndex>> CreateSecondaryIndex(
  ObjectId database_id, ObjectId schema_id, ObjectId id, ObjectId relation_id,
  std::string name, std::vector<catalog::CreateIndexColumn> columns,
  bool unique);

ResultOr<std::shared_ptr<InvertedIndex>> CreateInvertedIndex(
  ObjectId database_id, std::string_view schema_name, ObjectId schema_id,
  ObjectId id, ObjectId relation_id, std::string name,
  std::vector<catalog::CreateIndexColumn> columns,
  const std::shared_ptr<const Snapshot>& snapshot,
  InvertedIndexOptions options);

}  // namespace catalog
}  // namespace sdb
