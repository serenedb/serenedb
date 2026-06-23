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
#include <string>
#include <vector>

#include "basics/containers/flat_hash_set.h"
#include "catalog/object.h"
#include "catalog/persistence/index.h"
#include "catalog/scorer_options.h"
#include "catalog/table_options.h"

namespace sdb {
namespace catalog {

inline constexpr std::string_view kIncludedKind = "included";
inline constexpr std::string_view kIVFKind = "ivf";

class SecondaryIndex;
class InvertedIndex;

using persistence::ExpressionData;
using persistence::InvertedIndexOptions;

struct CreateIndexColumn {
  const catalog::Column* catalog_column = nullptr;
  std::string_view name;
  std::string opclass;
  std::optional<ExpressionData> indexed_expr;
  // nullopt = no parentheses in source SQL; an (empty or non-empty) map means
  // parens were present, distinguishing `col opclass` from `col opclass ()`.
  std::optional<duckdb::case_insensitive_map_t<duckdb::Value>> opclass_options;

  bool IsIndexedExpression() const noexcept { return indexed_expr.has_value(); }

  bool HasParentheses() const noexcept { return opclass_options.has_value(); }

  bool IsBuiltin(std::string_view name) const noexcept {
    return HasParentheses() && opclass == name;
  }

  const ExpressionData& GetIndexedExpression() const {
    SDB_ASSERT(IsIndexedExpression());
    return *indexed_expr;
  }

  const catalog::Column* GetCatalogColumn() const noexcept {
    SDB_ASSERT(!IsIndexedExpression());
    return catalog_column;
  }
};

class Index : public Object {
 public:
  ObjectId GetDatabaseId() const noexcept { return _database_id; }
  auto GetRelationId() const noexcept { return _relation_id; }
  std::span<const Column::Id> GetColumnIds() const noexcept {
    return _column_ids;
  }

  virtual std::vector<Column::Id> GetReferencedColumnIds() const = 0;

  virtual containers::FlatHashSet<ObjectId> GetTokenizers() const { return {}; }

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
  duckdb::ClientContext& context, ObjectId database_id,
  std::string_view schema_name, ObjectId schema_id, ObjectId id,
  ObjectId relation_id, std::string name,
  std::vector<catalog::CreateIndexColumn> columns,
  const std::shared_ptr<const Snapshot>& snapshot,
  InvertedIndexOptions options);

}  // namespace catalog
}  // namespace sdb
