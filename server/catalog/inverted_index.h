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

#pragma once

#include <iresearch/index/column_info.hpp>
#include <iresearch/index/index_features.hpp>
#include <optional>
#include <string>
#include <vector>

#include "basics/containers/node_hash_set.h"
#include "catalog/index.h"
#include "catalog/scorer_options.h"
#include "catalog/search_analyzer_impl.h"
#include "catalog/tokenizer.h"
#include "storage_engine/index_shard.h"

namespace sdb::catalog {

struct HNSWColumnConfig {
  int d = 0;
  int m = 32;
  int ef_construction = 40;
  irs::HNSWMetric metric = irs::HNSWMetric::L2Sqr;
};

struct ExpressionInfo {
  std::string serialized_expr;
  std::vector<Column::Id> dependent_columns;
  ObjectId text_dictionary = ObjectId::none();
  search::Features features;

  struct HasherBySerialized {
    using is_transparent = void;

    size_t operator()(std::string_view sv) const { return absl::HashOf(sv); }
    size_t operator()(const ExpressionInfo& info) const {
      return (*this)(info.serialized_expr);
    }

    bool operator()(const ExpressionInfo& a, std::string_view b) const {
      return a.serialized_expr == b;
    }
    bool operator()(const ExpressionInfo& a, const ExpressionInfo& b) const {
      return a.serialized_expr == b.serialized_expr;
    }
  };
};

struct InvertedIndexColumnInfo {
  ObjectId text_dictionary = ObjectId::none();
  bool store_values = false;
  search::Features features;
  std::optional<HNSWColumnConfig> hnsw_config;
};

struct FieldTokenizer {
  Tokenizer::TokenizerWrapper analyzer;
  irs::IndexFeatures features = irs::IndexFeatures::None;
};

class InvertedIndex final : public Index {
 public:
  using ColumnOptions =
    containers::FlatHashMap<Column::Id, InvertedIndexColumnInfo>;
  using ExpressionOptions =
    containers::NodeHashSet<ExpressionInfo, ExpressionInfo::HasherBySerialized,
                            ExpressionInfo::HasherBySerialized>;

  InvertedIndex(ObjectId database_id, ObjectId schema_id, ObjectId id,
                ObjectId relation_id, std::string name,
                std::vector<Column::Id> column_ids, ColumnOptions columns,
                ExpressionOptions expressions,
                std::optional<ScorerOptions> wand_scorer = std::nullopt)
    : Index{database_id,
            schema_id,
            id,
            relation_id,
            std::move(name),
            std::move(column_ids),
            ObjectType::InvertedIndex},
      _columns{std::move(columns)},
      _expressions{std::move(expressions)},
      _wand_scorer{std::move(wand_scorer)} {}

  static std::shared_ptr<InvertedIndex> ReadInternal(vpack::Slice slice,
                                                     ReadContext ctx);
  void WriteInternal(vpack::Builder& builder) const final;
  std::shared_ptr<Object> Clone() const final;
  ResultOr<std::shared_ptr<IndexShard>> CreateIndexShard(
    bool is_new, ObjectId id, IndexShardOptions&) const final;

  const InvertedIndexColumnInfo* FindColumnInfo(
    catalog::Column::Id column_id) const noexcept;

  const ExpressionOptions& GetExpressions() const noexcept {
    return _expressions;
  }

  FieldTokenizer GetColumnTokenizer(
    const std::shared_ptr<const Snapshot>& snapshot,
    catalog::Column::Id column_id) const;

  FieldTokenizer GetExprTokenizer(
    const std::shared_ptr<const Snapshot>& snapshot,
    std::string_view serialized_expr) const;

  std::optional<irs::HNSWInfo> GetColumnHNSWInfo(
    catalog::Column::Id column_id) const;

  const std::optional<ScorerOptions>& GetWandScorer() const noexcept {
    return _wand_scorer;
  }

  containers::FlatHashSet<ObjectId> GetTokenizers() const final;

 private:
  ColumnOptions _columns;
  ExpressionOptions _expressions;
  std::optional<ScorerOptions> _wand_scorer;
};

}  // namespace sdb::catalog
