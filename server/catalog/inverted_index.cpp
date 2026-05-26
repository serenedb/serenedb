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

#include "catalog/inverted_index.h"

#include <faiss/MetricType.h>

#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/tokenizers.hpp>

#include "absl/algorithm/container.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/containers/node_hash_map.h"
#include "basics/containers/node_hash_set.h"
#include "basics/down_cast.h"
#include "basics/serializer.h"
#include "catalog/catalog.h"
#include "database/ticks.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/index_shard.h"

namespace sdb::catalog {
namespace {

ResultOr<ColumnTokenizer> BuildColumnTokenizer(
  const std::shared_ptr<const Snapshot>& snapshot, ObjectId text_dictionary,
  search::Features features) {
  if (!text_dictionary.isSet()) {
    auto analyzer = std::make_unique<irs::StringTokenizer>();
    return ColumnTokenizer{.analyzer = Tokenizer::TokenizerWrapper{
                             analyzer.release(), Tokenizer::Deleter{nullptr}}};
  }
  auto dict = snapshot->GetObject<Tokenizer>(text_dictionary);
  if (!dict) {
    return std::unexpected<Result>{std::in_place, ERROR_INTERNAL,
                                   "Dictionary for inverted index does not "
                                   "exists"};
  }
  auto tokenizer = dict->GetTokenizer();
  if (!tokenizer) {
    return std::unexpected<Result>{std::move(tokenizer.error())};
  }
  return ColumnTokenizer{.analyzer = *std::move(tokenizer),
                         .features = features.GetIndexFeatures()};
}

// Persistent on-disk catalog format.
struct ColumnSerialized {
  ObjectId text_dictionary = ObjectId::none();
  bool store_values = false;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  search::Features features;
  std::optional<HNSWColumnConfig> hnsw_config;
  std::optional<irs::field_id> synthetic_column;
  uint32_t row_group_size = 0;
  uint32_t norm_row_group_size = 0;
};

// Persistent on-disk catalog format.
struct ExpressionSerialized {
  std::string serialized_expr;
  std::vector<Column::Id> dependent_columns;
  duckdb::LogicalType return_type;
  ObjectId text_dictionary = ObjectId::none();
  search::Features features;
  irs::field_id field_id = 0;
  std::optional<irs::field_id> synthetic_column;
  uint32_t norm_row_group_size = 0;
  std::string pretty_printed;

  struct HasherBySerialized {
    using is_transparent = void;

    size_t operator()(std::string_view sv) const { return absl::HashOf(sv); }
    size_t operator()(const ExpressionSerialized& info) const {
      return (*this)(info.serialized_expr);
    }

    bool operator()(const ExpressionSerialized& a, std::string_view b) const {
      return a.serialized_expr == b;
    }
    bool operator()(const ExpressionSerialized& a,
                    const ExpressionSerialized& b) const {
      return a.serialized_expr == b.serialized_expr;
    }
  };
};

using ColumnSerializedMap =
  containers::NodeHashMap<Column::Id, ColumnSerialized>;
using ExpressionSerializedSet =
  containers::NodeHashSet<ExpressionSerialized,
                          ExpressionSerialized::HasherBySerialized,
                          ExpressionSerialized::HasherBySerialized>;

}  // namespace

ResultOr<std::shared_ptr<IndexShard>> InvertedIndex::CreateIndexShard(
  bool is_new, ObjectId id) const {
  return search::InvertedIndexShard::Create(id, *this, is_new);
}

namespace {

// Persistent on-disk catalog format.
struct InvertedIndexData {
  std::string name;
  std::vector<Column::Id> column_ids;
  ColumnSerializedMap columns;
  std::vector<ExpressionSerialized> expressions;
  InvertedIndexOptions options;
};

InvertedIndexData PackEntries(std::string_view name,
                              std::span<const Column::Id> column_ids,
                              const InvertedIndex::Entries& entries,
                              const InvertedIndexOptions& options) {
  InvertedIndexData data;
  data.name = std::string{name};
  data.column_ids.assign(column_ids.begin(), column_ids.end());
  data.options = options;
  for (const auto& [field_id, entry] : entries) {
    if (const auto* expr = entry.GetExpressionData()) {
      data.expressions.push_back(ExpressionSerialized{
        .serialized_expr = expr->serialized_expr,
        .dependent_columns = expr->dependent_columns,
        .return_type = expr->return_type,
        .text_dictionary = entry.text_dictionary,
        .features = entry.features,
        .field_id = field_id,
        .synthetic_column = entry.synthetic_column,
        .norm_row_group_size = entry.norm_row_group_size,
        .pretty_printed = expr->pretty_printed,
      });
    } else {
      data.columns.emplace(static_cast<Column::Id>(field_id),
                           ColumnSerialized{
                             .text_dictionary = entry.text_dictionary,
                             .store_values = entry.store_values,
                             .compression = entry.compression,
                             .features = entry.features,
                             .hnsw_config = entry.hnsw_config,
                             .synthetic_column = entry.synthetic_column,
                             .row_group_size = entry.row_group_size,
                             .norm_row_group_size = entry.norm_row_group_size,
                           });
    }
  }
  return data;
}

std::shared_ptr<InvertedIndex> UnpackEntries(InvertedIndexData data,
                                             ReadContext ctx) {
  InvertedIndex::Entries entries;
  entries.reserve(data.columns.size() + data.expressions.size());
  for (auto& [col_id, col] : data.columns) {
    entries.emplace(static_cast<irs::field_id>(col_id),
                    InvertedIndexEntryInfo{
                      .text_dictionary = col.text_dictionary,
                      .features = col.features,
                      .synthetic_column = col.synthetic_column,
                      .norm_row_group_size = col.norm_row_group_size,
                      .store_values = col.store_values,
                      .compression = col.compression,
                      .hnsw_config = std::move(col.hnsw_config),
                      .row_group_size = col.row_group_size,
                    });
  }
  for (auto& expr : data.expressions) {
    entries.emplace(
      expr.field_id,
      InvertedIndexEntryInfo{
        .text_dictionary = expr.text_dictionary,
        .features = expr.features,
        .synthetic_column = expr.synthetic_column,
        .norm_row_group_size = expr.norm_row_group_size,
        .expression =
          ExpressionData{
            .serialized_expr = std::move(expr.serialized_expr),
            .dependent_columns = std::move(expr.dependent_columns),
            .return_type = std::move(expr.return_type),
            .pretty_printed = std::move(expr.pretty_printed),
          },
      });
  }
  return std::make_shared<InvertedIndex>(
    ctx.database_id, ctx.schema_id, ctx.id, ctx.relation_id,
    std::move(data.name), std::move(data.column_ids), std::move(entries),
    std::move(data.options));
}

}  // namespace

std::shared_ptr<InvertedIndex> InvertedIndex::Deserialize(
  duckdb::Deserializer& src, ReadContext ctx) {
  InvertedIndexData data;
  basics::ReadTuple(src, data);
  return UnpackEntries(std::move(data), ctx);
}

void InvertedIndex::Serialize(duckdb::Serializer& sink) const {
  auto data = PackEntries(GetName(), _column_ids, _entries, _options);
  basics::WriteTuple(sink, data);
}

void InvertedIndex::BuildSerializedExprIndex() {
  _expr_to_field.clear();
  _expr_to_field.reserve(_entries.size());
  for (const auto& [field_id, entry] : _entries) {
    if (const auto* expr = entry.GetExpressionData()) {
      _expr_to_field.emplace(expr->serialized_expr, field_id);
    }
  }
}

void InvertedIndex::BumpTickServerForEntryIds() {
  for (const auto& [field_id, entry] : _entries) {
    if (entry.IsExpression()) {
      UpdateTickServer(field_id);
    }
    if (entry.synthetic_column) {
      UpdateTickServer(*entry.synthetic_column);
    }
  }
}

const InvertedIndexEntryInfo* InvertedIndex::FindEntry(
  irs::field_id id) const noexcept {
  auto it = _entries.find(id);
  return it == _entries.end() ? nullptr : &it->second;
}

const InvertedIndexEntryInfo* InvertedIndex::FindColumnInfo(
  catalog::Column::Id column_id) const noexcept {
  const auto* entry = FindEntry(static_cast<irs::field_id>(column_id));
  return entry != nullptr && entry->IsColumn() ? entry : nullptr;
}

std::vector<Column::Id> InvertedIndex::GetReferencedColumnIds() const {
  std::vector<Column::Id> ids(_column_ids.begin(), _column_ids.end());
  containers::FlatHashSet<Column::Id> seen(ids.begin(), ids.end());
  for (const auto& [_, entry] : _entries) {
    if (const auto* expr = entry.GetExpressionData()) {
      for (auto id : expr->dependent_columns) {
        if (seen.insert(id).second) {
          ids.push_back(id);
        }
      }
    }
  }
  return ids;
}

void InvertedIndex::BuildSyntheticFeaturesIndex() {
  _synthetic_to_features.clear();
  for (const auto& [field_id, entry] : _entries) {
    if (entry.synthetic_column) {
      _synthetic_to_features.emplace(*entry.synthetic_column, entry.features);
    }
  }
}

const search::Features* InvertedIndex::FindSyntheticFeatures(
  irs::field_id synthetic_id) const noexcept {
  auto it = _synthetic_to_features.find(synthetic_id);
  const search::Features* result =
    it == _synthetic_to_features.end() ? nullptr : &it->second;
#ifdef SDB_DEV
  const search::Features* fallback = nullptr;
  for (const auto& [_, entry] : _entries) {
    if (entry.synthetic_column == synthetic_id) {
      fallback = &entry.features;
      break;
    }
  }
  SDB_ASSERT(static_cast<bool>(result) == static_cast<bool>(fallback));
  SDB_ASSERT(!result ||
             result->GetIndexFeatures() == fallback->GetIndexFeatures());
#endif
  return result;
}

ColumnTokenizer InvertedIndex::GetColumnTokenizer(
  const std::shared_ptr<const Snapshot>& snapshot,
  catalog::Column::Id column_id) const {
  const auto* info = FindColumnInfo(column_id);
  if (!info) {
    SDB_THROW(ERROR_INTERNAL, "Column id ", column_id,
              " not found in the index definition");
  }
  auto tokenizer =
    BuildColumnTokenizer(snapshot, info->text_dictionary, info->features);
  SDB_ENSURE(tokenizer, ERROR_INTERNAL, tokenizer.error().errorMessage());
  if (!info->features.HasFeatures(irs::IndexFeatures::Norm)) {
    tokenizer->tokenizer_column = info->synthetic_column;
  }
  return *std::move(tokenizer);
}

ColumnTokenizer InvertedIndex::GetExprTokenizer(
  const std::shared_ptr<const Snapshot>& snapshot,
  std::string_view serialized_expr) const {
  auto it = _expr_to_field.find(serialized_expr);
  if (it == _expr_to_field.end()) {
    SDB_THROW(ERROR_INTERNAL,
              "Indexed expression not found in the index definition");
  }
  return GetExprTokenizerByFieldId(snapshot, it->second);
}

ColumnTokenizer InvertedIndex::GetExprTokenizerByFieldId(
  const std::shared_ptr<const Snapshot>& snapshot,
  irs::field_id field_id) const {
  const auto* entry = FindEntry(field_id);
  if (entry == nullptr || !entry->IsExpression()) {
    SDB_THROW(ERROR_INTERNAL, "Indexed expression with field_id ", field_id,
              " not found in the index definition");
  }
  auto tokenizer =
    BuildColumnTokenizer(snapshot, entry->text_dictionary, entry->features);
  SDB_ENSURE(tokenizer, ERROR_INTERNAL, tokenizer.error().errorMessage());
  if (!entry->features.HasFeatures(irs::IndexFeatures::Norm)) {
    tokenizer->tokenizer_column = entry->synthetic_column;
  }
  return *std::move(tokenizer);
}

std::optional<irs::field_id> InvertedIndex::FindFieldIdBySerialized(
  std::string_view serialized_expr) const noexcept {
  auto it = _expr_to_field.find(serialized_expr);
  if (it == _expr_to_field.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<irs::HNSWInfo> InvertedIndex::GetHNSWInfo(
  irs::field_id field_id) const {
  const auto* entry = FindEntry(field_id);
  if (!entry || !entry->hnsw_config) {
    return std::nullopt;
  }
  const auto& cfg = *entry->hnsw_config;
  return irs::HNSWInfo{
    .max_doc = 0,
    .d = cfg.d,
    .m = cfg.m,
    .metric = cfg.metric,
    .ef_construction = cfg.ef_construction,
  };
}

containers::FlatHashSet<ObjectId> InvertedIndex::GetTokenizers() const {
  containers::FlatHashSet<ObjectId> res;
  for (const auto& [_, entry] : _entries) {
    if (entry.text_dictionary.isSet()) {
      res.insert(entry.text_dictionary);
    }
  }
  return res;
}

std::shared_ptr<Object> InvertedIndex::Clone() const {
  duckdb::MemoryStream stream;
  return DeserializeObject<InvertedIndex>(SerializeObject(*this, stream),
                                          {
                                            .id = GetId(),
                                            .database_id = GetDatabaseId(),
                                            .schema_id = GetParentId(),
                                            .relation_id = GetRelationId(),
                                          });
}

}  // namespace sdb::catalog
