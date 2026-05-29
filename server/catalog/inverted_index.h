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

#include <duckdb/common/enums/compression_type.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/index_features.hpp>
#include <optional>
#include <string>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/node_hash_map.h"
#include "catalog/index.h"
#include "catalog/scorer_options.h"
#include "catalog/search_analyzer_impl.h"
#include "catalog/tokenizer.h"
#include "storage_engine/index_shard.h"

namespace sdb::catalog {

// Numeric/temporal DuckDB types the inverted-index sink indexes via a
// signed-int slice. Usable in both constexpr and runtime contexts.
constexpr bool IsNumericSliceKind(duckdb::LogicalTypeId kind) noexcept {
  switch (kind) {
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::DATE:
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return true;
    default:
      return false;
  }
}

struct HNSWColumnConfig {
  int d = 0;
  int m = 32;
  int ef_construction = 40;
  irs::HNSWMetric metric = irs::HNSWMetric::L2Sqr;
};

struct InvertedIndexEntryInfo {
  ObjectId text_dictionary = ObjectId::none();
  search::Features features;
  std::optional<irs::field_id> synthetic_column;
  uint32_t norm_row_group_size = 0;
  bool store_values = false;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  std::optional<HNSWColumnConfig> hnsw_config;
  uint32_t row_group_size = 0;

  std::optional<ExpressionData> expression;

  bool IsExpression() const noexcept { return expression.has_value(); }
  bool IsColumn() const noexcept { return !expression.has_value(); }
  const ExpressionData* GetExpressionData() const noexcept {
    return expression ? &*expression : nullptr;
  }
};

struct ColumnTokenizer {
  Tokenizer::TokenizerWrapper analyzer;
  irs::IndexFeatures features = irs::IndexFeatures::None;
  std::optional<irs::field_id> tokenizer_column;
};

class InvertedIndex final : public Index {
 public:
  using Entries =
    containers::NodeHashMap<irs::field_id, InvertedIndexEntryInfo>;

  InvertedIndex(ObjectId database_id, ObjectId schema_id, ObjectId id,
                ObjectId relation_id, std::string name,
                std::vector<Column::Id> column_ids, Entries entries,
                InvertedIndexOptions options)
    : Index{database_id,
            schema_id,
            id,
            relation_id,
            std::move(name),
            std::move(column_ids),
            ObjectType::InvertedIndex},
      _entries{std::move(entries)},
      _options{std::move(options)} {
    BuildSerializedExprIndex();
    BuildSyntheticFeaturesIndex();
    BumpTickServerForEntryIds();
  }

  static std::shared_ptr<InvertedIndex> ReadInternal(vpack::Slice slice,
                                                     ReadContext ctx);
  void WriteInternal(vpack::Builder& builder) const final;
  std::shared_ptr<Object> Clone() const final;
  ResultOr<std::shared_ptr<IndexShard>> CreateIndexShard(
    bool is_new, ObjectId id) const final;

  const InvertedIndexEntryInfo* FindEntry(irs::field_id id) const noexcept;
  // Convenience: returns the entry only if it is a plain column (not an
  // indexed expression). Use when the caller knows column id semantics.
  const InvertedIndexEntryInfo* FindColumnInfo(
    catalog::Column::Id column_id) const noexcept;
  const search::Features* FindSyntheticFeatures(
    irs::field_id synthetic_id) const noexcept;

  std::vector<Column::Id> GetReferencedColumnIds() const final;

  const Entries& GetEntries() const noexcept { return _entries; }

  ColumnTokenizer GetColumnTokenizer(
    const std::shared_ptr<const Snapshot>& snapshot,
    catalog::Column::Id columnd_id) const;

  ColumnTokenizer GetExprTokenizer(
    const std::shared_ptr<const Snapshot>& snapshot,
    std::string_view serialized_expr) const;

  ColumnTokenizer GetExprTokenizerByFieldId(
    const std::shared_ptr<const Snapshot>& snapshot,
    irs::field_id field_id) const;

  std::optional<irs::field_id> FindFieldIdBySerialized(
    std::string_view serialized_expr) const noexcept;

  std::optional<irs::HNSWInfo> GetHNSWInfo(irs::field_id field_id) const;

  const InvertedIndexOptions& GetOptions() const noexcept { return _options; }

  const std::optional<ScorerOptions>& GetTopKScorer() const noexcept {
    return _options.topk_scorer;
  }

  containers::FlatHashSet<ObjectId> GetTokenizers() const final;

 private:
  void BuildSerializedExprIndex();
  void BuildSyntheticFeaturesIndex();
  void BumpTickServerForEntryIds();

  Entries _entries;
  // Reverse map: serialized expression -> field_id.
  // Views point into the durable storage in entries
  containers::FlatHashMap<std::string_view, irs::field_id> _expr_to_field;
  // Reverse map: synthetic field_id -> owner entry's features.
  containers::FlatHashMap<irs::field_id, search::Features>
    _synthetic_to_features;
  InvertedIndexOptions _options;
};

}  // namespace sdb::catalog
