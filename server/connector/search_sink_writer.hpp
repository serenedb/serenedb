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
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/token_batch.hpp>
#include <iresearch/formats/column/column_writer.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/containers/node_hash_map.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "connector/duckdb_sink_writer_base.h"
#include "connector/index_expression.hpp"
#include "connector/inverted_store_index.h"
#include "connector/primary_key.h"
#include "search/inverted_index_storage.h"
#include "search/search_analyzer_impl.h"
#include "search/search_table.h"
#include "search_remove_filter.hpp"
#include "server/utils/primary_key.h"

namespace duckdb {

class DataChunk;

}  // namespace duckdb
namespace sdb::connector {

class SearchRemoveFilter;

using TokenizerProvider =
  absl::AnyInvocable<catalog::ColumnTokenizer(irs::field_id)>;

using EntryInfoProvider =
  absl::AnyInvocable<const catalog::InvertedIndexField*(irs::field_id)>;

inline EntryInfoProvider MakeEntryInfoProvider(
  const catalog::InvertedIndexConfig& config) {
  return
    [&config](irs::field_id field_id) { return config.FindEntry(field_id); };
}

inline std::vector<ColumnId> IndexedColumnIds(
  const catalog::InvertedIndexConfig& config) {
  std::vector<ColumnId> ids;
  for (const auto& [field_id, field] : config.fields) {
    if (field_id < kFirstSyntheticColumnId) {
      ids.emplace_back(field_id);
    }
  }
  return ids;
}

inline EntryInfoProvider NoEntryInfoProvider() {
  return
    [](irs::field_id) -> const catalog::InvertedIndexField* { return nullptr; };
}

inline const catalog::InvertedIndexField* AllStoredEntry() {
  static const catalog::InvertedIndexField kStored = [] {
    catalog::InvertedIndexField e;
    e.store_values = true;
    return e;
  }();
  return &kStored;
}

class SearchSinkInsertBaseImpl {
 public:
  SearchSinkInsertBaseImpl(
    irs::IndexWriter::Transaction& trx, TokenizerProvider&& tokenizer_provider,
    EntryInfoProvider&& entry_info_provider, PkPolicy pk_policy = {},
    std::vector<IndexedExpression>&& indexed_exprs = {},
    std::shared_ptr<const catalog::InvertedIndexConfig> config = {});

  void InitImpl(size_t batch_size, const PkChunk& pk = {},
                irs::CommitOnFlush* commit_on_flush = nullptr);

  void SwitchFieldImpl(irs::field_id field_id, const duckdb::LogicalType& type,
                       const duckdb::Vector& vec, duckdb::idx_t count);

  void AppendToColumn(irs::field_id field_id, const duckdb::LogicalType& type,
                      const duckdb::Vector& vec, duckdb::idx_t count);

  std::vector<irs::field_id> TermFieldsForColumn(ColumnId column) const {
    return _config ? _config->TermFields(column) : std::vector<irs::field_id>{};
  }

  std::span<const IndexedExpression> IndexedExpressions() const noexcept {
    return _indexed_expressions;
  }

  void FinishImpl();

  void AbortImpl() {
    _column_writers.clear();
    _pk_column_writer = nullptr;
    _document.reset();
  }

  struct KeyScratch {
    std::vector<std::string> row_keys;
    std::vector<duckdb::string_t> key_views;
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

    void PrepareForKeywordStringValue(irs::field_id field_id);
    // Binds (does not consume) the analyzer: the sink's tokenizer cache owns
    // the pool lease for its own lifetime.
    void PrepareForStringValue(irs::field_id field_id,
                               catalog::ColumnTokenizer& column_analyzer);

    void PrepareForBlockValue(irs::field_id field_id);

    irs::analysis::Tokenizer* string_analyzer = nullptr;
    irs::field_id id{irs::field_limits::invalid()};
    // Blob column receiving per-value stored bytes; invalid when the field
    // stores nothing.
    irs::field_id store_column{irs::field_limits::invalid()};
    irs::IndexFeatures index_features;
    bool keyword = false;
  };

  template<typename Func>
  void InvertField(const Field& field, Func&& func);

  template<typename Func>
  void InvertTokens(const Field& field, irs::StoreSink* store, Func&& func);

  void WriteKeywordColumn(const Field& field, const Field& null_field,
                          const duckdb::Vector& vec, duckdb::idx_t count);

  void WriteAnalyzedColumn(const Field& field, const Field& null_field,
                           duckdb::idx_t count);

  template<duckdb::LogicalTypeId Kind>
  void WriteNumericColumn(const Field& field, const Field& null_field,
                          duckdb::idx_t count);

  void WriteBoolColumn(const Field& field, const Field& null_field,
                       duckdb::idx_t count);

  void WriteNullColumn(const Field& null_field, duckdb::idx_t count);

  template<duckdb::LogicalTypeId ChildKind>
  void WriteListBatch(const Field& field, const Field& null_field,
                      duckdb::idx_t count, duckdb::idx_t array_size);

  bool DispatchListBatch(duckdb::LogicalTypeId child_kind, const Field& field,
                         const Field& null_field, duckdb::idx_t count,
                         duckdb::idx_t array_size);

  void WriteJsonBatch(const duckdb::Vector& vec, duckdb::idx_t count);

  irs::ColumnWriter* EnsureColumnWriter(irs::field_id field_id,
                                        const duckdb::LogicalType& type);
  irs::ColumnWriter* EnsureBlobColumnWriter(irs::field_id field_id) {
    return EnsureColumnWriter(field_id, duckdb::LogicalType::BLOB);
  }
  void AppendPkColumn(const duckdb::Vector& pk, duckdb::idx_t count);
  void EmitPkTerms(const Field& pk_field,
                   std::span<const duckdb::string_t> keys);
  void AppendBlobAt(irs::ColumnWriter& writer, irs::doc_id_t doc,
                    duckdb::string_t bytes);

  template<typename Insert>
  void WriteColumnBlock(const Field& null_field, duckdb::idx_t count,
                        Insert&& insert);
  void FinishColumnBlocks(const Field& null_field);

  struct JsonExpressionFields {
    Field string_field;
    Field numeric_field;
    Field bool_field;
    Field null_field;

    void InitForExpression(irs::field_id entry_field_id,
                           const catalog::InvertedIndexField* entry,
                           catalog::ColumnTokenizer& string_analyzer);
  };

  // Per-field tokenizer leases resolved once per sink: SwitchFieldImpl runs
  // per column per chunk, and re-resolving costs a catalog lookup plus an
  // analyzer pool round-trip each time.
  catalog::ColumnTokenizer& ResolveTokenizer(irs::field_id field_id);

  TokenizerProvider _tokenizer_provider;
  EntryInfoProvider _entry_info_provider;
  irs::containers::FlatHashMap<irs::field_id, catalog::ColumnTokenizer>
    _tokenizer_cache;
  Field _pk_field;
  Field _field;
  Field _null_field;
  irs::IndexWriter::Transaction* _trx;
  std::optional<irs::IndexWriter::Document> _document;

  irs::containers::FlatHashMap<irs::field_id, irs::ColumnWriter*>
    _column_writers;
  irs::ColumnWriter* _pk_column_writer = nullptr;
  PkPolicy _pk_policy;

  JsonExpressionFields _json_fields;
  simdjson::ondemand::parser _json_parser;
  std::string _json_buffer;

  class StoreAppender final : public irs::StoreSink {
   public:
    void Bind(SearchSinkInsertBaseImpl& impl,
              irs::ColumnWriter& writer) noexcept {
      _impl = &impl;
      _writer = &writer;
    }

    void OnStore(irs::doc_id_t doc, irs::bytes_view store) final {
      _impl->AppendBlobAt(
        *_writer, doc,
        duckdb::string_t{reinterpret_cast<const char*>(store.data()),
                         static_cast<uint32_t>(store.size())});
    }

   private:
    SearchSinkInsertBaseImpl* _impl = nullptr;
    irs::ColumnWriter* _writer = nullptr;
  };

  duckdb::RecursiveUnifiedVectorFormat _vec_fmt;
  StoreAppender _store_appender;
  KeyScratch _key_scratch;
  std::vector<IndexedExpression> _indexed_expressions;
  std::shared_ptr<const catalog::InvertedIndexConfig> _config;

  std::vector<duckdb::string_t> _json_bool_terms;
  std::vector<double> _json_nums;
  std::vector<irs::doc_id_t> _json_num_docs;
  std::vector<irs::doc_id_t> _json_bool_docs;
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
  irs::IndexWriter::Transaction* _trx;
  std::shared_ptr<SearchRemoveFilter> _remove_filter;
};

class DuckDBSearchSinkInsertWriter final : public DuckDBSinkIndexWriter,
                                           public SearchSinkInsertBaseImpl {
 public:
  DuckDBSearchSinkInsertWriter(
    irs::IndexWriter::Transaction& trx, TokenizerProvider&& tokenizer_provider,
    std::span<const ColumnId> indexed_columns,
    EntryInfoProvider&& entry_info_provider = NoEntryInfoProvider(),
    PkPolicy pk_policy = {})
    : SearchSinkInsertBaseImpl{trx, std::move(tokenizer_provider),
                               std::move(entry_info_provider), pk_policy},
      _indexed{indexed_columns.begin(), indexed_columns.end()} {}

  void Init(duckdb::idx_t batch_size, const PkChunk& pk,
            irs::CommitOnFlush* commit_on_flush = nullptr) final {
    InitImpl(batch_size, pk, commit_on_flush);
  }

  bool SwitchColumn(const ColumnDescriptor& col, const duckdb::Vector& vec,
                    duckdb::idx_t count) final {
    if (_indexed.contains(col.id)) {
      SwitchFieldImpl(col.id, col.type, vec, count);
    }
    return false;
  }

  bool SwitchExpression(const ExpressionDescriptor& expr_desc,
                        const duckdb::Vector& vec, duckdb::idx_t count) final {
    SwitchFieldImpl(expr_desc.field_id, expr_desc.type, vec, count);
    return false;
  }

  void Finish() final { FinishImpl(); }
  void Abort() final { AbortImpl(); }

 private:
  irs::containers::FlatHashSet<ColumnId> _indexed;
};

class DuckDBSearchSinkDeleteWriter final : public DuckDBSinkIndexWriter,
                                           public SearchSinkDeleteBaseImpl {
 public:
  explicit DuckDBSearchSinkDeleteWriter(irs::IndexWriter::Transaction& trx)
    : SearchSinkDeleteBaseImpl{trx} {}

  void Init(duckdb::idx_t batch_size, const PkChunk& /*pk*/,
            irs::CommitOnFlush* /*commit_on_flush*/ = nullptr) final {
    InitImpl(batch_size);
  }

  void DeleteRow(std::string_view encoded_pk) final {
    DeleteRowImpl(encoded_pk);
  }

  void Finish() final { FinishImpl(); }
  void Abort() final { AbortImpl(); }
};

std::unique_ptr<SearchSinkInsertBaseImpl> MakeSearchTableInsertSink(
  irs::IndexWriter::Transaction& trx, const search::SearchTable& shard,
  duckdb::Catalog& catalog, duckdb::ClientContext& context);

void WriteChunkToSearchSink(SearchSinkInsertBaseImpl& sink,
                            duckdb::DataChunk& chunk,
                            std::span<const ColumnId> column_ids,
                            uint64_t pk_base, duckdb::idx_t table_id,
                            duckdb::ClientContext& context);

void WriteRebuiltChunkToSearchSink(SearchSinkInsertBaseImpl& sink,
                                   duckdb::DataChunk& chunk,
                                   std::span<const ColumnId> column_ids,
                                   duckdb::idx_t rowid_slot,
                                   duckdb::idx_t table_id,
                                   duckdb::ClientContext& context);

}  // namespace sdb::connector
