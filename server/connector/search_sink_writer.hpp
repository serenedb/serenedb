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
#include <absl/functional/any_invocable.h>
#include <simdjson.h>

#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <functional>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_writer.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/index_writer.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "catalog/inverted_index.h"
#include "catalog/search_analyzer_impl.h"
#include "connector/duckdb_primary_key.h"
#include "connector/index_expression.hpp"
#include "primary_key.hpp"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"
#include "search_remove_filter.hpp"
#include "sink_writer_base.hpp"

namespace duckdb {

class DataChunk;

}  // namespace duckdb
namespace sdb::connector {

class SearchRemoveFilterBase;

using TokenizerProvider =
  absl::AnyInvocable<catalog::ColumnTokenizer(irs::field_id)>;

inline TokenizerProvider MakeTokenizerProvider(
  std::shared_ptr<const catalog::Snapshot> snapshot,
  const catalog::InvertedIndex& index) {
  return [snapshot = std::move(snapshot),
          &index](irs::field_id field_id) -> catalog::ColumnTokenizer {
    return index.GetTokenizer(snapshot, field_id);
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
    SDB_ASSERT(irs::field_limits::valid(field_id));
    auto bound =
      DeserializeBoundExpression(expr->serialized_expr, client_context);
    const bool is_geojson = expr->return_type.IsJSONType() &&
                            irs::field_limits::valid(entry.synthetic_column);
    entries.emplace_back(std::move(bound), expr->serialized_expr,
                         expr->dependent_columns, field_id, is_geojson);
  }
  return entries;
}

using EntryInfoProvider =
  absl::AnyInvocable<const catalog::InvertedIndexEntryInfo*(irs::field_id)>;

inline EntryInfoProvider MakeEntryInfoProvider(
  const catalog::InvertedIndex& index) {
  return [&index](irs::field_id field_id) { return index.FindEntry(field_id); };
}

inline EntryInfoProvider NoEntryInfoProvider() {
  return [](irs::field_id) -> const catalog::InvertedIndexEntryInfo* {
    return nullptr;
  };
}

// Entry-info for the search-table model: every column is a plain stored
// columnstore column (no term dict / HNSW / JSON), only the PK is indexed as a
// term. Until CREATE INDEX on search tables lands and supplies a real provider.
inline EntryInfoProvider AllStoredEntryInfoProvider() {
  static const catalog::InvertedIndexEntryInfo kStored = [] {
    catalog::InvertedIndexEntryInfo e;
    e.store_values = true;  // IsStored() == true, IsTermDict() == false
    return e;
  }();
  return [](irs::field_id) { return &kStored; };
}

class SearchSinkInsertBaseImpl : public ColumnSinkWriterImplBase {
 public:
  SearchSinkInsertBaseImpl(irs::IndexWriter::Transaction& trx,
                           TokenizerProvider&& tokenizer_provider,
                           EntryInfoProvider&& entry_info_provider,
                           std::span<const catalog::Column::Id> columns,
                           std::vector<IndexedExpression>&& indexed_exprs = {},
                           bool store_pk_blob = true);

  void InitImpl(size_t batch_size);

  void SwitchFieldImpl(irs::field_id field_id, const duckdb::LogicalType& type,
                       const duckdb::Vector& vec,
                       std::span<const std::string_view> row_keys,
                       duckdb::idx_t count);

  void FinishImpl();

  std::span<const IndexedExpression> IndexedExpressionImpl() const noexcept {
    return _indexed_expressions;
  }

  void AbortImpl() {
    _column_writers.clear();
    // We don't own the transaction so Abort should be called outside.
    _document.reset();
  }

 protected:
  struct Field {
    irs::field_id Id() const noexcept { return id; }

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
        out.WriteData(store_attr->value.data(), store_attr->value.size());
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
    irs::field_id id{irs::field_limits::invalid()};
    irs::IndexFeatures index_features;
    // For paths that don't receive a StoreAttr from an analyzer
    // (HNSW vector columns, PK). Ignored when store_attr points elsewhere.
    irs::StoreAttr own_store;
    // Source of stored bytes for Write(). Either points at the analyzer's
    // StoreAttr (string columns with store-capable analyzer), or at own_store,
    // or is nullptr (column does not store values).
    const irs::StoreAttr* store_attr = nullptr;
  };

  template<duckdb::LogicalTypeId Kind>
  void SetFieldValueFromVector(Field& field,
                               const duckdb::UnifiedVectorFormat& fmt,
                               duckdb::idx_t idx);

  void EmitField(Field* field_to_insert, std::string_view full_row_key);

  template<duckdb::LogicalTypeId Kind>
  void WriteScalarBatch(std::span<const std::string_view> row_keys,
                        duckdb::idx_t count, irs::field_id tokenizer_column);

  template<duckdb::LogicalTypeId ChildKind>
  void WriteListBatch(std::span<const std::string_view> row_keys,
                      duckdb::idx_t count, duckdb::idx_t array_size);

  bool DispatchScalarBatch(duckdb::LogicalTypeId kind,
                           std::span<const std::string_view> row_keys,
                           duckdb::idx_t count, irs::field_id tokenizer_column);

  bool DispatchListBatch(duckdb::LogicalTypeId child_kind,
                         std::span<const std::string_view> row_keys,
                         duckdb::idx_t count, duckdb::idx_t array_size);

  void WriteJsonBatch(const duckdb::Vector& vec,
                      std::span<const std::string_view> row_keys,
                      duckdb::idx_t count);

  void EmitPkOnlyBatch(std::span<const std::string_view> row_keys,
                       duckdb::idx_t count);

  void InsertNullValue();

  irs::ColumnWriter* EnsurePerRowBlobWriter(irs::field_id field_id);
  void MaybeEmitPk(std::string_view full_row_key);
  void AppendPkBlob(std::string_view row_key);
  void AppendPerRowBlob(irs::field_id field_id, irs::bytes_view bytes);

  void AppendToColumn(irs::field_id field_id, const duckdb::LogicalType& type,
                      const duckdb::Vector& vec, duckdb::idx_t count);

  struct JsonExpressionFields {
    Field string_field;
    Field numeric_field;
    Field bool_field;
    Field null_field;
    irs::field_id tokenizer_column = irs::field_limits::invalid();

    void InitForExpression(irs::field_id entry_field_id,
                           const catalog::InvertedIndexEntryInfo* entry,
                           catalog::ColumnTokenizer string_analyzer);
  };

  TokenizerProvider _tokenizer_provider;
  EntryInfoProvider _entry_info_provider;
  std::vector<IndexedExpression> _indexed_expressions;
  Field _pk_field;
  Field _field;
  Field _null_field;
  irs::IndexWriter::Transaction& _trx;
  std::optional<irs::IndexWriter::Document> _document;

  containers::FlatHashMap<irs::field_id, irs::ColumnWriter*> _column_writers;

  containers::FlatHashMap<irs::field_id, irs::ColumnWriter*>
    _per_row_blob_writers;
  irs::ColumnWriter* _pk_blob_writer = nullptr;
  // Latch so the PK is emitted once per document (cleared after the first
  // column's batch), independent of whether a PK blob column exists.
  bool _emit_pk = false;
  // false for search tables (PK indexed as a term only); true for inverted
  // indexes (PK blob maps a hit back to the row).
  bool _store_pk_blob = true;

  JsonExpressionFields _json_fields;
  simdjson::ondemand::parser _json_parser;
  std::string _json_buffer;

  duckdb::RecursiveUnifiedVectorFormat _vec_fmt;
};

class SearchSinkDeleteBaseImpl {
 public:
  explicit SearchSinkDeleteBaseImpl(irs::IndexWriter::Transaction& trx);

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

// A search-table iresearch insert sink: the inverted-index insert base in
// search-table mode (all columns stored, PK as a term only, no PK blob). Shared
// by the INSERT operator and WAL recovery.
inline std::unique_ptr<SearchSinkInsertBaseImpl> MakeSearchTableInsertSink(
  irs::IndexWriter::Transaction& trx,
  std::span<const catalog::Column::Id> columns) {
  return std::make_unique<SearchSinkInsertBaseImpl>(
    trx, TokenizerProvider{}, AllStoredEntryInfoProvider(), columns,
    std::vector<IndexedExpression>{}, /*store_pk_blob=*/false);
}

// Write one materialised DataChunk into `sink` -- the shared core of both the
// search-table INSERT Sink and WAL recovery replay, so a recovered row's
// PK/encoding is byte-identical to the written one. `pk_base` is the
// generated-PK base for this chunk (0/ignored for explicit-PK, where the key is
// built from `pk_columns`).
void WriteChunkToSearchSink(
  SearchSinkInsertBaseImpl& sink, duckdb::DataChunk& chunk,
  std::span<const catalog::Column::Id> column_ids,
  std::span<const duckdb_primary_key::PKColumn> pk_columns,
  std::string_view table_key, bool uses_generated_pk, uint64_t pk_base);

}  // namespace sdb::connector
