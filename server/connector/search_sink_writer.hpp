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

#include <absl/container/flat_hash_map.h>
#include <simdjson.h>

#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/columnstore/column_writer.hpp>
#include <iresearch/columnstore/format.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/index_writer.hpp>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "catalog/inverted_index.h"
#include "catalog/search_analyzer_impl.h"
#include "connector/index_expression.hpp"
#include "primary_key.hpp"
#include "search/inverted_index_shard.h"
#include "search_remove_filter.hpp"
#include "sink_writer_base.hpp"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

class SearchRemoveFilterBase;

using TokenizerProvider =
  absl::AnyInvocable<catalog::ColumnTokenizer(catalog::Column::Id)>;

using ExpressionTokenizerProvider =
  absl::AnyInvocable<catalog::ColumnTokenizer(irs::field_id)>;

inline TokenizerProvider MakeTokenizerProvider(
  const std::shared_ptr<const catalog::Snapshot>& snapshot,
  const catalog::InvertedIndex& index) {
  return [snapshot, &index](catalog::Column::Id column_id) {
    return index.GetColumnTokenizer(snapshot, column_id);
  };
}

inline ExpressionTokenizerProvider MakeExpressionTokenizerProvider(
  std::shared_ptr<const catalog::Snapshot> snapshot,
  const catalog::InvertedIndex& index) {
  return [snapshot = std::move(snapshot),
          &index](irs::field_id field_id) -> catalog::ColumnTokenizer {
    return index.GetExprTokenizerByFieldId(snapshot, field_id);
  };
}

inline std::vector<IndexedExpression> MakeIndexedExpressions(
  const catalog::InvertedIndex& index, duckdb::ClientContext& client_context) {
  std::vector<IndexedExpression> entries;
  entries.reserve(index.GetEntries().size());
  for (const auto& [field_id, entry] : index.GetEntries()) {
    const auto* expr = entry.GetExpressionData();
    if (!expr) {
      continue;
    }
    SDB_ASSERT(!expr->serialized_expr.empty());
    SDB_ASSERT(!expr->dependent_columns.empty());
    SDB_ASSERT(field_id != 0);
    auto bound =
      DeserializeBoundExpression(expr->serialized_expr, client_context);
    const bool is_geojson =
      expr->return_type.IsJSONType() && entry.synthetic_column.has_value();
    entries.emplace_back(std::move(bound), expr->serialized_expr,
                         expr->dependent_columns, field_id, is_geojson);
  }
  return entries;
}

using StoreValuesProvider = std::function<bool(irs::field_id)>;

inline StoreValuesProvider MakeStoreValuesProvider(
  const catalog::InvertedIndex& index) {
  return [&index](irs::field_id field_id) {
    const auto* entry = index.FindEntry(field_id);
    return entry != nullptr && entry->store_values;
  };
}

inline StoreValuesProvider NoStoreValues() {
  return [](irs::field_id) { return false; };
}

// Lets the sink skip the per-row tokenize+postings path for INCLUDE-only
// entries.
using IsTextIndexedProvider = std::function<bool(irs::field_id)>;

inline IsTextIndexedProvider MakeIsTextIndexedProvider(
  const catalog::InvertedIndex& index) {
  return [&index](irs::field_id field_id) {
    const auto* entry = index.FindEntry(field_id);
    return entry != nullptr && entry->text_dictionary.isSet();
  };
}

// Conservative fallback: assume every column is text-indexed. Preserves the
// pre-INCLUDE behaviour for code paths and tests that don't yet thread a
// real provider through.
inline IsTextIndexedProvider AllTextIndexed() {
  return [](irs::field_id) { return true; };
}

// Default for SearchTableSinkWriter: no column is text-indexed (search
// tables have no InvertedIndex in the initial cut -- M2 PR 2.3 rejects
// CREATE INDEX). Flipped per-column when that feature lands.
inline IsTextIndexedProvider NeverTextIndexed() {
  return [](irs::field_id) { return false; };
}

// Per-entry HNSW config (columns and expressions share the field_id namespace).
using HNSWInfoProvider =
  std::function<std::optional<irs::HNSWInfo>(irs::field_id)>;

inline HNSWInfoProvider MakeHNSWInfoProvider(
  const catalog::InvertedIndex& index) {
  return
    [&index](irs::field_id field_id) { return index.GetHNSWInfo(field_id); };
}

inline HNSWInfoProvider NoHNSW() {
  return
    [](irs::field_id) -> std::optional<irs::HNSWInfo> { return std::nullopt; };
}

class SearchSinkInsertBaseImpl : public ColumnSinkWriterImplBase {
 public:
  SearchSinkInsertBaseImpl(
    irs::IndexWriter::Transaction& trx, TokenizerProvider&& tokenizer_provider,
    StoreValuesProvider&& store_values_provider,
    IsTextIndexedProvider&& is_text_indexed_provider,
    HNSWInfoProvider&& hnsw_info_provider,
    std::span<const catalog::Column::Id> columns,
    ExpressionTokenizerProvider&& expr_tokenizer_provider = {},
    std::vector<IndexedExpression>&& indexed_exprs = {});

  void InitImpl(size_t batch_size);

  void WriteImpl(std::span<const rocksdb::Slice> cell_slices,
                 std::string_view full_key);

  // Switches the active column for subsequent per-cell WriteImpl calls
  // and, when `vec`/`count` are provided, routes the typed batch into
  // irs::columnstore::ColumnWriter for columns registered with
  // store_values=true. Per-cell Write still runs separately for the
  // inverted-index side; the batch path only feeds the typed columnstore.
  bool SwitchColumnImpl(const ColumnDescriptor& col, const duckdb::Vector& vec,
                        duckdb::idx_t count);

  bool SwitchExpressionImpl(const ExpressionDescriptor& expr_desc,
                            const duckdb::Vector& vec, duckdb::idx_t count);

  void AppendCsContinuation(const duckdb::Vector& vec, duckdb::idx_t count,
                            duckdb::idx_t row_offset_from_first_doc);

  void FinishImpl();

  std::span<const IndexedExpression> IndexedExpressionImpl() const noexcept {
    return _indexed_expressions;
  }

  void AbortImpl() {
    _columnstore_writers.clear();
    _active_columnstore_writer = nullptr;
    // We don't own the transaction so Abort should be called outside.
    _document.reset();
  }

 protected:
  struct Field {
    std::string_view Name() const noexcept {
      SDB_ASSERT(!irs::IsNull(name));
      return name;
    }

    irs::IndexFeatures GetIndexFeatures() const noexcept {
      return index_features;
    }

    irs::Tokenizer& GetTokens() const noexcept {
      SDB_ASSERT(analyzer || string_analyzer);
      SDB_ASSERT(!analyzer || !string_analyzer);
      return analyzer ? *analyzer : *string_analyzer;
    }

    bool Write(irs::DataOutput& out) const {
      if (store_attr && !irs::IsNull(store_attr->value)) {
        out.WriteBytes(store_attr->value.data(), store_attr->value.size());
      }
      return true;
    }

    void PrepareForVerbatimStringValue();
    void PrepareForStringValue(catalog::ColumnTokenizer&& column_analyzer);
    void SetStringValue(std::string_view value);

    void PrepareForNumericValue();
    template<typename T>
    void SetNumericValue(T value);

    void PrepareForBooleanValue();
    void SetBooleanValue(bool value);

    void PrepareForNullValue();
    void SetNullValue();

    search::AnalyzerImpl::CacheType::ptr analyzer;
    catalog::Tokenizer::TokenizerWrapper string_analyzer;
    std::string_view name;
    irs::IndexFeatures index_features;
    // For paths that don't receive a StoreAttr from an analyzer
    // (HNSW vector columns, PK). Ignored when store_attr points elsewhere.
    irs::StoreAttr own_store;
    // Source of stored bytes for Write(). Either points at the analyzer's
    // StoreAttr (string columns with store-capable analyzer), or at own_store,
    // or is nullptr (column does not store values).
    const irs::StoreAttr* store_attr = nullptr;
  };

  using Writer = std::function<void(
    std::string_view full_key, std::span<const rocksdb::Slice> cell_slices)>;

  template<typename WriteFunc>
  Writer MakeIndexWriter(WriteFunc&& write_func);

  template<typename WriteFunc>
  Writer MakeStoreAttrWriter(irs::field_id tokenizer_column_id, bool have_nulls,
                             WriteFunc&& write_func);

  // Actual value processors. It is set to write executor (see MakeIndexWriter)
  // as a template. This methods are responsible for extracting value from
  // rocksdb slices and setting it to Field structure accordingly.
  static Field& WriteStringValue(std::string_view full_key,
                                 std::span<const rocksdb::Slice> cell_slices,
                                 Field& field);
  static Field& WriteBooleanValue(std::string_view full_key,
                                  std::span<const rocksdb::Slice> cell_slices,
                                  Field& field);

  template<typename T>
  static Field& WriteNumericValue(std::string_view full_key,
                                  std::span<const rocksdb::Slice> cell_slices,
                                  Field& field);

  template<duckdb::LogicalTypeId Kind>
  void SetupColumnWriter(catalog::Column::Id column_id, bool have_nulls);

  template<duckdb::LogicalTypeId ChildKind>
  void SetupListExpressionWriter(irs::field_id field_id, bool have_nulls,
                                 duckdb::idx_t array_size);

  bool SetupListExpressionWriterForChild(irs::field_id field_id,
                                         bool have_nulls,
                                         duckdb::LogicalTypeId child_kind,
                                         duckdb::idx_t array_size);

  template<duckdb::LogicalTypeId Kind>
  void WriteListElementValue(const duckdb::UnifiedVectorFormat& child_fmt,
                             duckdb::idx_t flat_idx, bool have_nulls);

  void InsertNullValue();

  irs::columnstore::ColumnWriter* EnsurePerRowBlobWriter(
    irs::field_id field_id);
  void AppendPerRowBlob(irs::field_id field_id, irs::bytes_view bytes);
  void AppendPerRowBlobNull(irs::field_id field_id);

  // Opens-once and bulk-appends `vec` to the typed columnstore writer for
  // `field_id`.
  void BulkAppendToColumnstore(irs::field_id field_id,
                               const duckdb::LogicalType& type,
                               const duckdb::Vector& vec, duckdb::idx_t count);

  void AppendPerRowPrimaryKey(std::string_view row_key);

  struct JsonExpressionFields {
    std::string string_name;
    std::string numeric_name;
    std::string bool_name;
    std::string null_name;
    Field string_field;
    Field numeric_field;
    Field bool_field;
    Field null_field;
    std::optional<irs::field_id> tokenizer_column;

    void InitForExpression(irs::field_id field_id,
                           catalog::ColumnTokenizer string_analyzer);
  };

  void SetupJsonExpressionWriter(irs::field_id field_id,
                                 catalog::ColumnTokenizer string_analyzer);

  TokenizerProvider _tokenizer_provider;
  StoreValuesProvider _store_values_provider;
  IsTextIndexedProvider _is_text_indexed_provider;
  HNSWInfoProvider _hnsw_info_provider;
  ExpressionTokenizerProvider _subexpr_tokenizer_provider;
  std::vector<IndexedExpression> _indexed_expressions;
  Field _field;
  Field _pk_field;
  Field _null_field;
  std::string _name_buffer;
  std::string _null_name_buffer;
  irs::IndexWriter::Transaction& _trx;
  std::optional<irs::IndexWriter::Document> _document;

  Writer _current_writer;
  bool _emit_pk = true;

  containers::FlatHashMap<irs::field_id, irs::columnstore::ColumnWriter*>
    _columnstore_writers;
  irs::columnstore::ColumnWriter* _active_columnstore_writer = nullptr;
  catalog::Column::Id _active_column_id{};
  duckdb::LogicalType _active_column_type;

  containers::FlatHashMap<irs::field_id, irs::columnstore::ColumnWriter*>
    _per_row_blob_writers;
  duckdb::Vector _row_buffer{duckdb::LogicalType::BLOB, 1,
                             duckdb::VectorDataInitialization::UNINITIALIZED};

  std::vector<JsonExpressionFields> _json_fields;
  simdjson::ondemand::parser _json_parser;
  std::string _json_buffer;

  duckdb::RecursiveUnifiedVectorFormat _list_fmt;
};

class SearchSinkDeleteBaseImpl {
 public:
  SearchSinkDeleteBaseImpl(irs::IndexWriter::Transaction& trx);

  void InitImpl(size_t batch_size);

  void FinishImpl();

  void DeleteRowImpl(std::string_view row_key);

  void AbortImpl() { _remove_filter.reset(); }

 protected:
  irs::IndexWriter::Transaction& _trx;
  std::shared_ptr<SearchRemoveFilterBase> _remove_filter;
};

// SearchSinkInsertBaseImpl stores a reference to the transaction, so the
// transaction object must exist before it is constructed.
class SearchSinkBackfillTrxHolder {
 protected:
  SearchSinkBackfillTrxHolder(irs::IndexWriter::Transaction trx)
    : _trx_storage{std::move(trx)} {}
  irs::IndexWriter::Transaction _trx_storage;
};

}  // namespace sdb::connector
