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

#include "basics/containers/flat_hash_set.h"
#include "basics/containers/node_hash_map.h"
#include "basics/primary_key.hpp"
#include "catalog/duckdb_primary_key.h"
#include "catalog/inverted_index.h"
#include "connector/duckdb_sink_writer_base.h"
#include "connector/index_expression.hpp"
#include "search/inverted_index_storage.h"
#include "search/search_analyzer_impl.h"
#include "search/search_table.h"
#include "search_remove_filter.hpp"

namespace duckdb {

class DataChunk;

}  // namespace duckdb
namespace sdb::connector {

class SearchRemoveFilter;

using TokenizerProvider =
  absl::AnyInvocable<catalog::ColumnTokenizer(irs::field_id)>;

inline TokenizerProvider MakeTokenizerProvider(
  catalog::TokenizerMap dicts, const catalog::InvertedIndex& index) {
  return [dicts = std::move(dicts),
          &index](irs::field_id field_id) -> catalog::ColumnTokenizer {
    return index.GetTokenizer(dicts, field_id);
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

inline const catalog::InvertedIndexEntryInfo* AllStoredEntry() {
  static const catalog::InvertedIndexEntryInfo kStored = [] {
    catalog::InvertedIndexEntryInfo e;
    e.store_values = true;
    return e;
  }();
  return &kStored;
}

inline EntryInfoProvider AllStoredEntryInfoProvider() {
  return [](irs::field_id) { return AllStoredEntry(); };
}

// Holds one immutable index-config snapshot for the sink's lifetime so entry
// pointers stay valid if a concurrent CREATE/DROP INDEX swaps the config.
inline EntryInfoProvider MakeConfigEntryInfoProvider(
  std::shared_ptr<const catalog::InvertedIndex::Entries> config) {
  return [config = std::move(config)](
           irs::field_id field_id) -> const catalog::InvertedIndexEntryInfo* {
    auto it = config->find(field_id);
    return it != config->end() ? &it->second : AllStoredEntry();
  };
}

inline TokenizerProvider MakeConfigTokenizerProvider(
  std::shared_ptr<const catalog::InvertedIndex::Entries> config,
  catalog::TokenizerMap dicts) {
  return [config = std::move(config), dicts = std::move(dicts)](
           irs::field_id field_id) -> catalog::ColumnTokenizer {
    auto it = config->find(field_id);
    // Not a merged-config field: the all-stored entry names no dictionary, so
    // this resolves to the default string tokenizer.
    const auto& entry = it != config->end() ? it->second : *AllStoredEntry();
    return catalog::TokenizerForEntry(dicts, entry);
  };
}

struct PkPolicy {
  bool index_term = true;
  catalog::PkColumnKind column = catalog::PkColumnKind::Has;
};

class SearchSinkInsertBaseImpl {
 public:
  SearchSinkInsertBaseImpl(
    irs::IndexWriter::Transaction& trx, TokenizerProvider&& tokenizer_provider,
    EntryInfoProvider&& entry_info_provider, PkPolicy pk_policy = {},
    std::vector<IndexedExpression>&& indexed_exprs = {},
    std::shared_ptr<const search::SearchTable::TermsByColumn> terms_by_column =
      {});

  void SetTransaction(irs::IndexWriter::Transaction& trx) noexcept {
    _trx = &trx;
  }

  void InitImpl(size_t batch_size, const PkChunk& pk = {},
                irs::CommitOnFlush* commit_on_flush = nullptr);

  void SwitchFieldImpl(irs::field_id field_id, const duckdb::LogicalType& type,
                       const duckdb::Vector& vec, duckdb::idx_t count);

  void AppendValueColumn(irs::field_id field_id,
                         const duckdb::LogicalType& type,
                         const duckdb::Vector& vec, duckdb::idx_t count);

  std::span<const irs::field_id> TermFieldsForColumn(
    catalog::ColumnId col_id) const noexcept;

  // The indexed expressions declared on this table, evaluated per chunk by
  // WriteChunkToSearchSink.
  std::span<const IndexedExpression> IndexedExpressionImpl() const noexcept {
    return _indexed_expressions;
  }

  void FinishImpl();

  void AbortImpl() {
    _column_writers.clear();
    _per_row_blob_writers.clear();
    _pk_column_writer = nullptr;
    _document.reset();
  }

  struct KeyScratch {
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
    irs::StoreAttr own_store;
    const irs::StoreAttr* store_attr = nullptr;
  };

  template<duckdb::LogicalTypeId Kind>
  void SetFieldValueFromVector(Field& field,
                               const duckdb::UnifiedVectorFormat& fmt,
                               duckdb::idx_t idx);

  void EmitField(Field* field_to_insert);

  template<duckdb::LogicalTypeId Kind>
  void WriteScalarBatch(duckdb::idx_t count, irs::field_id tokenizer_column);

  template<duckdb::LogicalTypeId ChildKind>
  void WriteListBatch(duckdb::idx_t count, duckdb::idx_t array_size);

  bool DispatchScalarBatch(duckdb::LogicalTypeId kind, duckdb::idx_t count,
                           irs::field_id tokenizer_column);

  bool DispatchListBatch(duckdb::LogicalTypeId child_kind, duckdb::idx_t count,
                         duckdb::idx_t array_size);

  void WriteJsonBatch(const duckdb::Vector& vec, duckdb::idx_t count);

  void InsertNullValue();

  irs::ColumnWriter* EnsurePerRowColumnWriter(irs::field_id field_id,
                                              const duckdb::LogicalType& type);
  irs::ColumnWriter* EnsurePerRowBlobWriter(irs::field_id field_id);
  void AppendPkColumn(const duckdb::Vector& pk, duckdb::idx_t count);
  void EmitPkTerms(std::span<const std::string_view> keys);
  void AppendBlobTo(irs::ColumnWriter& writer, irs::bytes_view bytes);

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
  Field _pk_field;
  Field _field;
  Field _null_field;
  irs::IndexWriter::Transaction* _trx;
  std::optional<irs::IndexWriter::Document> _document;

  containers::FlatHashMap<irs::field_id, irs::ColumnWriter*> _column_writers;

  containers::FlatHashMap<irs::field_id, irs::ColumnWriter*>
    _per_row_blob_writers;
  irs::ColumnWriter* _pk_column_writer = nullptr;
  PkPolicy _pk_policy;
  std::vector<IndexedExpression> _indexed_expressions;
  std::shared_ptr<const search::SearchTable::TermsByColumn> _terms_by_column;

  JsonExpressionFields _json_fields;
  simdjson::ondemand::parser _json_parser;
  std::string _json_buffer;

  duckdb::RecursiveUnifiedVectorFormat _vec_fmt;
  KeyScratch _key_scratch;
};

class SearchSinkDeleteBaseImpl {
 public:
  explicit SearchSinkDeleteBaseImpl(irs::IndexWriter::Transaction& trx);

  void SetTransaction(irs::IndexWriter::Transaction& trx) noexcept {
    _trx = &trx;
  }

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
    std::span<const catalog::ColumnId> indexed_columns,
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
      SwitchFieldImpl(static_cast<irs::field_id>(col.id), col.type, vec, count);
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
  containers::FlatHashSet<catalog::ColumnId> _indexed;
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

// Built off the shard's merged index config, so the sink writes each column
// under the term field the owning index allocated and evaluates the indexed
// expressions declared on the table.
std::unique_ptr<SearchSinkInsertBaseImpl> MakeSearchTableInsertSink(
  irs::IndexWriter::Transaction& trx, const search::SearchTable& shard,
  duckdb::ClientContext& context);

// Rows are keyed by the synthetic rowid `pk_base + row`: it is the PK term and
// is also stored under kGeneratedPKId so a scan can materialise it.
void WriteChunkToSearchSink(SearchSinkInsertBaseImpl& sink,
                            duckdb::DataChunk& chunk,
                            std::span<const catalog::ColumnId> column_ids,
                            uint64_t pk_base, ObjectId table_id,
                            duckdb::ClientContext& context);

}  // namespace sdb::connector
