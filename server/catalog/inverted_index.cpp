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
#include "basics/down_cast.h"
#include "basics/serializer.h"
#include "catalog/catalog.h"
#include "catalog/persistence/inverted_index.h"
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

using persistence::ColumnSerialized;
using persistence::ColumnSerializedMap;
using persistence::ExpressionSerialized;
using persistence::InvertedIndexData;

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
        .pretty_printed = expr->pretty_printed,
        .dependent_columns = expr->dependent_columns,
        .return_type = expr->return_type,
        .synthetic_column = entry.synthetic_column,
        .text_dictionary = entry.text_dictionary,
        .field_id = field_id,
        .norm_row_group_size = entry.norm_row_group_size,
        .features = entry.features,
        .null_field_id = entry.null_field_id,
        .bool_field_id = entry.bool_field_id,
        .numeric_field_id = entry.numeric_field_id,
      });
    } else {
      data.columns.emplace(static_cast<Column::Id>(field_id),
                           ColumnSerialized{
                             .text_dictionary = entry.text_dictionary,
                             .store_values = entry.store_values,
                             .indexed_term_dict = entry.indexed_term_dict,
                             .distinct_count = entry.distinct_count,
                             .compression = entry.compression,
                             .features = entry.features,
                             .hnsw_config = entry.hnsw_config,
                             .synthetic_column = entry.synthetic_column,
                             .row_group_size = entry.row_group_size,
                             .norm_row_group_size = entry.norm_row_group_size,
                             .null_field_id = entry.null_field_id,
                             .bool_field_id = entry.bool_field_id,
                             .numeric_field_id = entry.numeric_field_id,
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
                      .indexed_term_dict = col.indexed_term_dict,
                      .distinct_count = col.distinct_count,
                      .compression = col.compression,
                      .hnsw_config = std::move(col.hnsw_config),
                      .row_group_size = col.row_group_size,
                      .null_field_id = col.null_field_id,
                      .bool_field_id = col.bool_field_id,
                      .numeric_field_id = col.numeric_field_id,
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
        .null_field_id = expr.null_field_id,
        .bool_field_id = expr.bool_field_id,
        .numeric_field_id = expr.numeric_field_id,
      });
  }
  return std::make_shared<InvertedIndex>(
    ctx.database_id, ctx.schema_id, ctx.id, ctx.relation_id,
    std::move(data.name), std::move(data.column_ids), std::move(entries),
    std::move(data.options));
}

}  // namespace

ResultOr<std::shared_ptr<IndexShard>> InvertedIndex::CreateIndexShard(
  bool is_new, ObjectId id) const {
  return search::InvertedIndexShard::Create(id, *this, is_new);
}

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
    if (irs::field_limits::valid(entry.synthetic_column)) {
      UpdateTickServer(entry.synthetic_column);
    }
    if (irs::field_limits::valid(entry.null_field_id)) {
      UpdateTickServer(entry.null_field_id);
    }
    if (irs::field_limits::valid(entry.bool_field_id)) {
      UpdateTickServer(entry.bool_field_id);
    }
    if (irs::field_limits::valid(entry.numeric_field_id)) {
      UpdateTickServer(entry.numeric_field_id);
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

void InvertedIndex::AppendKindSuffix(std::string& out,
                                     const duckdb::LogicalType& type) {
  const auto k = term_dict::Classify(type.id());
  switch (k) {
    case term_dict::Kind::String:
      out += "(string)";
      break;
    case term_dict::Kind::Bool:
      out += "(bool)";
      break;
    case term_dict::Kind::Null:
      out += "(null)";
      break;
    default:
      if (term_dict::IsNumeric(k)) {
        out += "(numeric)";
      }
      break;
  }
}

namespace term_dict {

Result Validate(std::string_view label, const duckdb::LogicalType& type,
                std::string_view opclass) {
  const auto kind = type.id();
  const auto unsupported = [&] {
    return Result{
      ERROR_BAD_PARAMETER,       "Column '",      label,
      "' has unsupported type ", type.ToString(), " and can not be indexed"};
  };

  if (kind == duckdb::LogicalTypeId::LIST ||
      kind == duckdb::LogicalTypeId::ARRAY) {
    const auto child = (kind == duckdb::LogicalTypeId::LIST
                          ? duckdb::ListType::GetChildType(type)
                          : duckdb::ArrayType::GetChildType(type))
                         .id();
    if (child == duckdb::LogicalTypeId::GEOMETRY ||
        !IsSupported(Classify(child))) {
      return unsupported();
    }
    return {};
  }

  if (!IsSupported(Classify(kind))) {
    return unsupported();
  }
  if (kind == duckdb::LogicalTypeId::GEOMETRY && opclass.empty()) {
    return unsupported();
  }
  return {};
}

}  // namespace term_dict
namespace included {

Result Validate(std::string_view label, const duckdb::LogicalType& type) {
  using enum duckdb::LogicalTypeId;
  switch (type.id()) {
    case SQLNULL:
    case BOOLEAN:
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case UTINYINT:
    case USMALLINT:
    case UINTEGER:
    case UBIGINT:
    case HUGEINT:
    case UHUGEINT:
    case FLOAT:
    case DOUBLE:
    case DECIMAL:
    case BIGNUM:
    case DATE:
    case TIME:
    case TIME_NS:
    case TIME_TZ:
    case TIMESTAMP_SEC:
    case TIMESTAMP_MS:
    case TIMESTAMP:
    case TIMESTAMP_NS:
    case TIMESTAMP_TZ:
    case TIMESTAMP_TZ_NS:
    case INTERVAL:
    case VARCHAR:
    case CHAR:
    case BLOB:
    case GEOMETRY:
    case UUID:
    case BIT:
    case ENUM:
    case LIST:
    case ARRAY:
    case STRUCT:
    case MAP:
    case VARIANT:
      return {};
    default:
      return {ERROR_BAD_PARAMETER,
              "Column '",
              label,
              "' has type ",
              type.ToString(),
              " which is not supported in INCLUDE"};
  }
}

}  // namespace included
namespace hnsw {

uint32_t Dimension(const duckdb::LogicalType& type) noexcept {
  if (type.id() != duckdb::LogicalTypeId::ARRAY) {
    return 0;
  }
  if (duckdb::ArrayType::GetChildType(type).id() !=
      duckdb::LogicalTypeId::FLOAT) {
    return 0;
  }
  return static_cast<uint32_t>(duckdb::ArrayType::GetSize(type));
}

Result Validate(std::string_view label, const duckdb::LogicalType& type) {
  if (Dimension(type) == 0) {
    return {ERROR_BAD_PARAMETER, "Column '", label,
            "' must be ARRAY(FLOAT, N) to use the 'hnsw' opclass, not ",
            type.ToString()};
  }
  return {};
}

}  // namespace hnsw

InvertedIndex::FieldLookup InvertedIndex::LookupField(
  irs::field_id id) const noexcept {
  auto it = _field_lookup.find(id);
  if (it == _field_lookup.end()) {
    return {};
  }
  return it->second;
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
    if (irs::field_limits::valid(entry.synthetic_column)) {
      auto [it, ok] =
        _synthetic_to_features.emplace(entry.synthetic_column, entry.features);
      SDB_ENSURE(ok, ERROR_INTERNAL,
                 "synthetic_column collision in inverted index: id ",
                 entry.synthetic_column);
    }
  }
}

void InvertedIndex::BuildFieldLookupIndex() {
  _field_lookup.clear();
  const auto insert = [&](irs::field_id id, const InvertedIndexEntryInfo* entry,
                          irs::field_id entry_field_id) {
    if (irs::field_limits::valid(id)) {
      auto [it, ok] = _field_lookup.emplace(
        id, FieldLookup{.entry = entry, .entry_field_id = entry_field_id});
      SDB_ENSURE(ok, ERROR_INTERNAL,
                 "field_id collision in inverted index lookup: id ", id);
    }
  };
  for (const auto& [entry_fid, entry] : _entries) {
    insert(entry_fid, &entry, entry_fid);
    insert(entry.null_field_id, &entry, entry_fid);
    insert(entry.bool_field_id, &entry, entry_fid);
    insert(entry.numeric_field_id, &entry, entry_fid);
    insert(entry.synthetic_column, &entry, entry_fid);
  }
  insert(term_dict::kPKFieldId, nullptr, term_dict::kPKFieldId);
}

const search::Features* InvertedIndex::FindSyntheticFeatures(
  irs::field_id synthetic_id) const noexcept {
  auto it = _synthetic_to_features.find(synthetic_id);
  return it == _synthetic_to_features.end() ? nullptr : &it->second;
}

ColumnTokenizer InvertedIndex::GetTokenizer(
  const std::shared_ptr<const Snapshot>& snapshot,
  irs::field_id field_id) const {
  const auto* entry = FindEntry(field_id);
  if (entry == nullptr) {
    SDB_THROW(ERROR_INTERNAL, "Field id ", field_id,
              " not found in the index definition");
  }
  auto tokenizer =
    BuildColumnTokenizer(snapshot, entry->text_dictionary, entry->features);
  SDB_ENSURE(tokenizer, ERROR_INTERNAL, tokenizer.error().errorMessage());
  if (!entry->features.HasFeatures(irs::IndexFeatures::Norm) &&
      irs::field_limits::valid(entry->synthetic_column)) {
    tokenizer->tokenizer_column = entry->synthetic_column;
  }
  return *std::move(tokenizer);
}

irs::field_id InvertedIndex::FindFieldIdBySerialized(
  std::string_view serialized_expr) const noexcept {
  auto it = _expr_to_field.find(serialized_expr);
  if (it == _expr_to_field.end()) {
    return irs::field_limits::invalid();
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
