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

// Aggregated info about column for index creation.
// Filled on different levels during creaton to gather all
// necessary info for building and validating new index.
struct CreateIndexColumn {
  const catalog::Column* catalog_column{nullptr};
  std::string_view name;
  std::string opclass;
  std::string json_pointer;
  // nullopt = no parentheses in source SQL; an (empty or non-empty) map means
  // parens were present, distinguishing `col opclass` from `col opclass ()`.
  std::optional<duckdb::case_insensitive_map_t<duckdb::Value>> opclass_options;
};

class Index : public SchemaObject {
 public:
  auto GetRelationId() const noexcept { return _relation_id; }
  std::span<const Column::Id> GetColumnIds() const noexcept {
    return _column_ids;
  }

  virtual containers::FlatHashSet<ObjectId> GetTokenizers() const { return {}; }

  virtual ResultOr<std::shared_ptr<IndexShard>> CreateIndexShard(
    bool is_new, ObjectId id) const = 0;

  virtual ~Index() = default;

 protected:
  Index(ObjectId database_id, ObjectId schema_id, ObjectId id,
        ObjectId relation_id, std::string name,
        std::vector<Column::Id> column_ids, ObjectType type);

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
