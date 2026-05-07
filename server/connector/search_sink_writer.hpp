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

#include <simdjson.h>

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/index/index_writer.hpp>
#include <span>
#include <string>
#include <vector>

#include "catalog/inverted_index.h"
#include "catalog/search_analyzer_impl.h"
#include "primary_key.hpp"
#include "search/inverted_index_shard.h"
#include "search_remove_filter.hpp"
#include "sink_writer_base.hpp"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

class SearchRemoveFilterBase;

using TokenizerProvider =
  absl::AnyInvocable<catalog::ColumnTokenizer(catalog::Column::Id)>;

// One JSON path's worth of indexing config, resolved against the catalog.
struct JsonPathSinkConfig {
  std::span<const std::string> path;
  catalog::ColumnTokenizer tokenizer;
};

using JsonPathsProvider =
  absl::AnyInvocable<std::vector<JsonPathSinkConfig>(catalog::Column::Id)>;

// A JsonPathsProvider that returns an empty vector for every column. Useful
// for code paths that do not yet support path-based JSON indexing.
inline JsonPathsProvider NoJsonPaths() {
  return [](catalog::Column::Id) { return std::vector<JsonPathSinkConfig>{}; };
}

inline TokenizerProvider MakeTokenizerProvider(
  const std::shared_ptr<const catalog::Snapshot>& snapshot,
  const catalog::InvertedIndex& index) {
  return [snapshot, &index](catalog::Column::Id column_id) {
    return index.GetColumnTokenizer(snapshot, column_id);
  };
}

// Resolves every configured JSON path for `column_id` against the catalog.
// Returns an empty vector for columns without path-based indexing.
inline JsonPathsProvider MakeJsonPathsProvider(
  std::shared_ptr<const catalog::Snapshot> snapshot,
  const catalog::InvertedIndex& index) {
  return [snapshot = std::move(snapshot), &index](
           catalog::Column::Id column_id) -> std::vector<JsonPathSinkConfig> {
    const auto* col = index.FindColumnInfo(column_id);
    if (!col) {
      return {};
    }
    std::vector<JsonPathSinkConfig> out;
    out.reserve(col->json_paths.size());
    for (const auto& p : col->json_paths) {
      auto analyzer = index.GetJsonPathTokenizer(snapshot, column_id, p.path);
      if (!analyzer) {
        continue;
      }
      out.emplace_back(p.path, *std::move(analyzer));
    }
    return out;
  };
}

class SearchSinkInsertBaseImpl : public ColumnSinkWriterImplBase {
 public:
  SearchSinkInsertBaseImpl(irs::IndexWriter::Transaction& trx,
                           TokenizerProvider&& tokenizer_provider,
                           JsonPathsProvider&& json_paths_provider,
                           std::span<const catalog::Column::Id> columns);

  void InitImpl(size_t batch_size);

  void WriteImpl(std::span<const rocksdb::Slice> cell_slices,
                 std::string_view full_key);

  bool SwitchColumnImpl(const duckdb::LogicalType& type, bool have_nulls,
                        catalog::Column::Id column_id);
  void FinishImpl();

  void AbortImpl() {
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

    void PrepareForVectorValue();

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

  // Generic writer. Non-null rows (field.store_attr != nullptr) use
  // NonNullAction; null rows -- nullable_writer_func substitutes _null_field
  // which has no store_attr and a null-stream analyzer -- always get
  // Insert<INDEX> so IS NULL queries still find them, even on paths where
  // NonNullAction is STORE-only (e.g. HNSW). HasStore=false skips the
  // per-row check and just Inserts<INDEX> -- for paths that never store.
  template<bool HasStore, irs::Action NonNullAction, typename WriteFunc>
  Writer MakeWriterImpl(WriteFunc&& write_func);

  // Thin wrappers over MakeWriterImpl:
  //   MakeIndexWriter      -- INDEX only (no columnstore), cheapest path.
  //   MakeIndexStoreWriter -- non-null INDEX|STORE, null INDEX.
  //   MakeStoreWriter      -- non-null STORE, null INDEX (HNSW vectors).
  template<typename WriteFunc>
  Writer MakeIndexWriter(WriteFunc&& write_func) {
    return MakeWriterImpl<false, irs::Action::INDEX>(
      std::forward<WriteFunc>(write_func));
  }
  template<typename WriteFunc>
  Writer MakeIndexStoreWriter(WriteFunc&& write_func) {
    return MakeWriterImpl<true, irs::Action::INDEX | irs::Action::STORE>(
      std::forward<WriteFunc>(write_func));
  }
  template<typename WriteFunc>
  Writer MakeStoreWriter(WriteFunc&& write_func) {
    return MakeWriterImpl<true, irs::Action::STORE>(
      std::forward<WriteFunc>(write_func));
  }

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

  static Field& WriteVectorValue(std::string_view full_key,
                                 std::span<const rocksdb::Slice> cell_slices,
                                 Field& field);

  // Setup column writer according to type kind.
  // Builds actual executor to avoid switch/case on each row whenever possible.
  template<duckdb::LogicalTypeId Kind>
  void SetupColumnWriter(catalog::Column::Id column_id, bool have_nulls);

  // Setup the writer for a JSON column with one or more configured paths.
  // Each path becomes a distinct iresearch field named
  // [8 bytes BE column_id] + "." + key1 + "." + key2 + ... + <MangleString>.
  void SetupJsonColumnWriter(catalog::Column::Id column_id,
                             std::vector<JsonPathSinkConfig> paths);

  struct JsonPathField {
    // Backing storage for each per-type field name; Field::name is a
    // string_view into the corresponding buffer.
    std::string string_name;
    std::string numeric_name;
    std::string bool_name;
    std::string null_name;
    Field string_field;   // user's configured analyzer
    Field numeric_field;  // built-in NumericTokenizer
    Field bool_field;     // built-in BooleanTokenizer
    Field null_field;     // built-in NullTokenizer
    // JSON Pointer view inside one of the name buffers.
    std::string_view pointer;

    void Init(catalog::Column::Id column_id, std::span<const std::string> path,
              catalog::ColumnTokenizer string_analyzer);
  };

  TokenizerProvider _tokenizer_provider;
  JsonPathsProvider _json_paths_provider;
  Field _field;
  Field _pk_field;
  Field _null_field;
  std::string _name_buffer;
  std::string _null_name_buffer;
  irs::IndexWriter::Transaction& _trx;
  std::optional<irs::IndexWriter::Document> _document;

  Writer _current_writer;
  bool _emit_pk{true};

  // State for the currently active JSON column (empty when the column is not
  // path-indexed). Rebuilt on every SwitchColumn.
  std::vector<JsonPathField> _json_fields;
  simdjson::ondemand::parser _json_parser;
  std::string _json_buffer;
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
