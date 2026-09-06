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
#include "catalog1/entry/inverted_index.h"
#include "connector/column_id.h"
#include "connector/duckdb_sink_writer_base.h"
#include "connector/index_expression.hpp"
#include "connector/inverted_store_index.h"
#include "connector/primary_key.h"
#include "search/inverted_index_storage.h"
#include "search/search_analyzer_impl.h"
#include "search_remove_filter.hpp"

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
      ids.push_back(field_id);
    }
  }
  return ids;
}

inline EntryInfoProvider NoEntryInfoProvider() {
  return
    [](irs::field_id) -> const catalog::InvertedIndexField* { return nullptr; };
}

inline EntryInfoProvider AllStoredEntryInfoProvider() {
  static const catalog::InvertedIndexField kStored = [] {
    catalog::InvertedIndexField e;
    e.store_values = true;
    return e;
  }();
  return [](irs::field_id) { return &kStored; };
}

class SearchSinkInsertBaseImpl {
 public:
  SearchSinkInsertBaseImpl(irs::IndexWriter::Transaction& trx,
                           TokenizerProvider&& tokenizer_provider,
                           EntryInfoProvider&& entry_info_provider,
                           PkPolicy pk_policy = {});

  void SetTransaction(irs::IndexWriter::Transaction& trx) noexcept {
    _trx = &trx;
  }

  void InitImpl(size_t batch_size, const PkChunk& pk = {},
                irs::CommitOnFlush* commit_on_flush = nullptr);

  void SwitchFieldImpl(irs::field_id field_id, const duckdb::LogicalType& type,
                       const duckdb::Vector& vec, duckdb::idx_t count);

  void FinishImpl();

  void AbortImpl() {
    _column_writers.clear();
    _per_row_blob_writers.clear();
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
                           const catalog::InvertedIndexField* entry,
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
  containers::FlatHashSet<ColumnId> _indexed;
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

inline std::unique_ptr<SearchSinkInsertBaseImpl> MakeSearchTableInsertSink(
  irs::IndexWriter::Transaction& trx) {
  return std::make_unique<SearchSinkInsertBaseImpl>(
    trx, TokenizerProvider{}, AllStoredEntryInfoProvider(),
    PkPolicy{.index_term = true, .column = connector::PkColumnKind::None});
}

void WriteChunkToSearchSink(SearchSinkInsertBaseImpl& sink,
                            duckdb::DataChunk& chunk,
                            std::span<const ColumnId> column_ids,
                            std::span<const primary_key::PKColumn> pk_columns,
                            bool uses_generated_pk, uint64_t pk_base);

}  // namespace sdb::connector
