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
#include <span>
#include <string>
#include <vector>

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

// One configured JSON path inside a JSON-typed column of an inverted index.
struct JsonPathInfo {
  std::vector<std::string> path;
  ObjectId text_dictionary = ObjectId::none();
  search::Features features;
};

struct InvertedIndexColumnInfo {
  ObjectId text_dictionary = ObjectId::none();
  bool store_values = false;
  search::Features features;
  std::optional<HNSWColumnConfig> hnsw_config;
  std::vector<JsonPathInfo> json_paths;
};

struct ColumnTokenizer {
  Tokenizer::TokenizerWrapper analyzer;
  irs::IndexFeatures features = irs::IndexFeatures::None;
};

class InvertedIndex final : public Index {
 public:
  using ColumnOptions =
    containers::FlatHashMap<Column::Id, InvertedIndexColumnInfo>;

  InvertedIndex(ObjectId database_id, ObjectId schema_id, ObjectId id,
                ObjectId relation_id, std::string name,
                std::vector<Column::Id> column_ids, ColumnOptions columns,
                std::optional<ScorerOptions> wand_scorer = std::nullopt)
    : Index{database_id,
            schema_id,
            id,
            relation_id,
            std::move(name),
            std::move(column_ids),
            ObjectType::InvertedIndex},
      _columns{std::move(columns)},
      _wand_scorer{std::move(wand_scorer)} {}

  static std::shared_ptr<InvertedIndex> ReadInternal(vpack::Slice slice,
                                                     ReadContext ctx);
  void WriteInternal(vpack::Builder& builder) const final;
  std::shared_ptr<Object> Clone() const final;
  ResultOr<std::shared_ptr<IndexShard>> CreateIndexShard(
    bool is_new, ObjectId id, IndexShardOptions&) const final;

  const InvertedIndexColumnInfo* FindColumnInfo(
    catalog::Column::Id column_id) const noexcept;

  ColumnTokenizer GetColumnTokenizer(
    const std::shared_ptr<const Snapshot>& snapshot,
    catalog::Column::Id columnd_id) const;

  std::optional<ColumnTokenizer> GetJsonPathTokenizer(
    const std::shared_ptr<const Snapshot>& snapshot,
    catalog::Column::Id column_id, std::span<const std::string> path) const;

  std::optional<irs::HNSWInfo> GetColumnHNSWInfo(
    catalog::Column::Id column_id) const;

  const std::optional<ScorerOptions>& GetWandScorer() const noexcept {
    return _wand_scorer;
  }

  containers::FlatHashSet<ObjectId> GetTokenizers() const final;

 private:
  ColumnOptions _columns;
  std::optional<ScorerOptions> _wand_scorer;
};

}  // namespace sdb::catalog
