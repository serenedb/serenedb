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
#include <iresearch/analysis/batch/token_batch.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/column/column_writer.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/index_writer.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "basics/containers/flat_hash_set.h"
#include "catalog/inverted_index.h"
#include "connector/duckdb_primary_key.h"
#include "connector/duckdb_sink_writer_base.h"
#include "connector/index_expression.hpp"
#include "connector/inverter_sink.hpp"
#include "search_remove_filter.hpp"

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
  const auto& expression_keys = index.ExpressionKeys();
  std::vector<IndexedExpression> entries;
  entries.reserve(expression_keys.size());
  for (const auto& key : expression_keys) {
    const auto& expr = key.data;
    const auto field_id = key.field_id;
    SDB_ASSERT(!expr.serialized_expr.empty());
    SDB_ASSERT(!expr.dependent_columns.empty());
    SDB_ASSERT(irs::field_limits::valid(field_id));
    auto bound =
      DeserializeBoundExpression(expr.serialized_expr, client_context);
    const auto* entry = index.FindEntry(field_id);
    const bool is_geojson = expr.return_type.IsJSONType() && entry &&
                            irs::field_limits::valid(entry->synthetic_column);
    entries.emplace_back(std::move(bound), expr.serialized_expr,
                         expr.dependent_columns, field_id, is_geojson);
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

inline EntryInfoProvider AllStoredEntryInfoProvider() {
  static const catalog::InvertedIndexEntryInfo kStored = [] {
    catalog::InvertedIndexEntryInfo e;
    e.store_values = true;
    return e;
  }();
  return [](irs::field_id) { return &kStored; };
}

struct PkPolicy {
  bool index_term = true;
  catalog::PkColumnKind column = catalog::PkColumnKind::I64;
};

inline duckdb::LogicalType PkColumnType(catalog::PkColumnKind kind) {
  switch (kind) {
    case catalog::PkColumnKind::I64:
      return duckdb::LogicalType::BIGINT;
    case catalog::PkColumnKind::I64I64:
      static const auto kType =
        duckdb::LogicalType::STRUCT({{"hi", duckdb::LogicalType::BIGINT},
                                     {"lo", duckdb::LogicalType::BIGINT}});
      return kType;
    case catalog::PkColumnKind::None:
    case catalog::PkColumnKind::Unable:
      return duckdb::LogicalType::SQLNULL;
  }
}

class SearchSinkInsertBaseImpl {
 public:
  SearchSinkInsertBaseImpl(irs::IndexWriter::Transaction& trx,
                           TokenizerProvider&& tokenizer_provider,
                           EntryInfoProvider&& entry_info_provider,
                           std::vector<IndexedExpression>&& indexed_exprs = {},
                           PkPolicy pk_policy = {});

  void InitImpl(size_t batch_size, const PkChunk& pk = {});

  void SwitchFieldImpl(irs::field_id field_id, const duckdb::LogicalType& type,
                       const duckdb::Vector& vec, duckdb::idx_t count);

  void FinishImpl();

  std::span<const IndexedExpression> IndexedExpressionImpl() const noexcept {
    return _indexed_expressions;
  }

  void AbortImpl() {
    _column_writers.clear();
    _pk_column_writer = nullptr;
    _document.reset();
  }

  struct KeyScratch {
    std::vector<duckdb::UnifiedVectorFormat> pk_formats;
    std::vector<std::string> row_keys;
    std::vector<std::string_view> key_views;
  };
  KeyScratch& GetKeyScratch() noexcept { return _key_scratch; }

 protected:
  struct Field {
    irs::field_id Id() const noexcept { return id; }

    irs::IndexFeatures GetIndexFeatures() const noexcept {
      return index_features;
    }

    irs::analysis::Tokenizer& GetTokens() const noexcept {
      SDB_ASSERT(string_analyzer);
      return *string_analyzer;
    }

    void PrepareForKeywordStringValue();
    // Binds (does not consume) the analyzer: the sink's tokenizer cache owns
    // the pool lease for its own lifetime.
    void PrepareForStringValue(catalog::ColumnTokenizer& column_analyzer);

    void PrepareForBlockValue();

    irs::analysis::Tokenizer* string_analyzer = nullptr;
    irs::field_id id{irs::field_limits::invalid()};
    irs::IndexFeatures index_features;
    bool keyword = false;
    bool has_store = false;
  };

  template<duckdb::LogicalTypeId Kind>
  void WriteScalarBatch(duckdb::idx_t count, irs::field_id tokenizer_column);

  void WriteKeywordColumn(duckdb::idx_t count, irs::field_id tokenizer_column);

  template<typename Fill>
  void GatherKeywordBlock(Fill&& fill);

  template<duckdb::LogicalTypeId Kind, typename ForEach>
  void GatherNumericBlock(const duckdb::UnifiedVectorFormat& fmt,
                          duckdb::idx_t total, ForEach&& for_each);

  template<duckdb::LogicalTypeId Kind>
  void WriteAnalyzedColumn(duckdb::idx_t count, irs::field_id tokenizer_column);

  template<duckdb::LogicalTypeId Kind>
  void WriteNumericColumn(duckdb::idx_t count);

  void WriteBoolColumn(duckdb::idx_t count);

  void WriteNullColumn(duckdb::idx_t count);

  template<duckdb::LogicalTypeId ChildKind>
  void WriteListBatch(duckdb::idx_t count, duckdb::idx_t array_size);

  bool DispatchScalarBatch(duckdb::LogicalTypeId kind, duckdb::idx_t count,
                           irs::field_id tokenizer_column);

  bool DispatchListBatch(duckdb::LogicalTypeId child_kind, duckdb::idx_t count,
                         duckdb::idx_t array_size);

  void WriteJsonBatch(const duckdb::Vector& vec, duckdb::idx_t count);

  irs::ColumnWriter* EnsureColumnWriter(irs::field_id field_id,
                                        const duckdb::LogicalType& type);
  irs::ColumnWriter* EnsureBlobColumnWriter(irs::field_id field_id);
  void AppendPkColumn(const duckdb::Vector& pk, duckdb::idx_t count);
  void EmitPkTerms(std::span<const std::string_view> keys);
  void AppendBlobAt(irs::ColumnWriter& writer, irs::doc_id_t doc,
                    irs::bytes_view bytes);

  template<typename OnRow>
  void GatherRows(duckdb::idx_t count, OnRow&& on_row);
  template<typename OnValid>
  void GatherValidRows(duckdb::idx_t count,
                       const duckdb::UnifiedVectorFormat& fmt,
                       OnValid&& on_valid);
  void FinishColumnBlocks(const Field& null_field);

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
                           catalog::ColumnTokenizer& string_analyzer);
  };

  // Per-field tokenizer leases resolved once per sink: SwitchFieldImpl runs
  // per column per chunk, and re-resolving costs a catalog lookup plus an
  // analyzer pool round-trip each time.
  catalog::ColumnTokenizer& ResolveTokenizer(irs::field_id field_id);

  TokenizerProvider _tokenizer_provider;
  EntryInfoProvider _entry_info_provider;
  containers::FlatHashMap<irs::field_id, catalog::ColumnTokenizer>
    _tokenizer_cache;
  std::vector<IndexedExpression> _indexed_expressions;
  Field _pk_field;
  Field _field;
  Field _null_field;
  irs::IndexWriter::Transaction& _trx;
  std::optional<irs::IndexWriter::Document> _document;

  containers::FlatHashMap<irs::field_id, irs::ColumnWriter*> _column_writers;
  irs::ColumnWriter* _pk_column_writer = nullptr;
  PkPolicy _pk_policy;

  JsonExpressionFields _json_fields;
  simdjson::ondemand::parser _json_parser;
  std::string _json_buffer;

  class StoreAppender final : public irs::TokenConsumer {
   public:
    void Bind(SearchSinkInsertBaseImpl& impl,
              irs::ColumnWriter& writer) noexcept {
      _impl = &impl;
      _writer = &writer;
    }

    void Consume(irs::TokenBatch&, std::span<const irs::DocRun>) final {
      SDB_ASSERT(false);
    }

    void OnStore(irs::doc_id_t doc, irs::bytes_view store) final {
      _impl->AppendBlobAt(*_writer, doc, store);
    }

   private:
    SearchSinkInsertBaseImpl* _impl = nullptr;
    irs::ColumnWriter* _writer = nullptr;
  };

  duckdb::RecursiveUnifiedVectorFormat _vec_fmt;
  StoreAppender _store_appender;
  const duckdb::Vector* _cur_vec = nullptr;
  bool _flat_column = false;
  KeyScratch _key_scratch;

  InverterSink _inverter_sink;
  // Numeric-trie insertion: term slab scratch decoupled from the token
  // protocol; one value expands to NumericTermCount<T>() strided terms.
  template<typename T>
  void InsertNumericColumn(irs::FieldInverter& slot, std::span<const T> values,
                           std::span<const irs::doc_id_t> docs);

  std::vector<duckdb::string_t> _term_scratch;
  std::vector<uint64_t> _numeric_scratch;
  std::vector<irs::doc_id_t> _doc_scratch;
  std::vector<irs::doc_id_t> _bool_docs;
  std::vector<irs::doc_id_t> _null_docs;
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

class DuckDBSearchSinkInsertWriter final : public DuckDBSinkIndexWriter,
                                           public SearchSinkInsertBaseImpl {
 public:
  DuckDBSearchSinkInsertWriter(
    irs::IndexWriter::Transaction& trx, TokenizerProvider&& tokenizer_provider,
    std::span<const catalog::Column::Id> indexed_columns,
    EntryInfoProvider&& entry_info_provider = NoEntryInfoProvider(),
    std::vector<IndexedExpression>&& indexed_exprs = {},
    PkPolicy pk_policy = {})
    : SearchSinkInsertBaseImpl{trx, std::move(tokenizer_provider),
                               std::move(entry_info_provider),
                               std::move(indexed_exprs), pk_policy},
      _indexed{indexed_columns.begin(), indexed_columns.end()} {}

  void Init(duckdb::idx_t batch_size, const PkChunk& pk) final {
    InitImpl(batch_size, pk);
  }

  bool SwitchColumn(const ColumnDescriptor& col, const duckdb::Vector& vec,
                    duckdb::idx_t count) final {
    if (_indexed.contains(col.id)) {
      SwitchFieldImpl(static_cast<irs::field_id>(col.id), col.type, vec, count);
    }
    return false;
  }

  bool SwitchExpression(const ExpressionDescriptor& expr_desc,
                        const duckdb::Vector& vec, duckdb::idx_t count) final {
    SwitchFieldImpl(expr_desc.field_id, expr_desc.type, vec, count);
    return false;
  }

  std::span<const IndexedExpression> IndexedExpressions() const final {
    return IndexedExpressionImpl();
  }

  void Finish() final { FinishImpl(); }
  void Abort() final { AbortImpl(); }

 private:
  containers::FlatHashSet<catalog::Column::Id> _indexed;
};

class DuckDBSearchSinkDeleteWriter final : public DuckDBSinkIndexWriter,
                                           public SearchSinkDeleteBaseImpl {
 public:
  explicit DuckDBSearchSinkDeleteWriter(irs::IndexWriter::Transaction& trx)
    : SearchSinkDeleteBaseImpl{trx} {}

  void Init(duckdb::idx_t batch_size, const PkChunk& /*pk*/) final {
    InitImpl(batch_size);
  }

  void DeleteRow(std::string_view encoded_pk) final {
    DeleteRowImpl(encoded_pk);
  }

  void Finish() final { FinishImpl(); }
  void Abort() final { AbortImpl(); }
};

inline std::unique_ptr<SearchSinkInsertBaseImpl> MakeSearchTableInsertSink(
  irs::IndexWriter::Transaction& trx) {
  return std::make_unique<SearchSinkInsertBaseImpl>(
    trx, TokenizerProvider{}, AllStoredEntryInfoProvider(),
    std::vector<IndexedExpression>{},
    PkPolicy{.index_term = true, .column = catalog::PkColumnKind::None});
}

void WriteChunkToSearchSink(
  SearchSinkInsertBaseImpl& sink, duckdb::DataChunk& chunk,
  std::span<const catalog::Column::Id> column_ids,
  std::span<const duckdb_primary_key::PKColumn> pk_columns,
  bool uses_generated_pk, uint64_t pk_base);

}  // namespace sdb::connector
