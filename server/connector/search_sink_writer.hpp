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

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/index/index_writer.hpp>
#include <span>
#include <string>
#include <vector>

#include "basics/containers/flat_hash_set.h"
#include "catalog/inverted_index.h"
#include "catalog/search_analyzer_impl.h"
#include "connector/json_expression_canonicalizer.hpp"
#include "primary_key.hpp"
#include "search/inverted_index_shard.h"
#include "search_remove_filter.hpp"
#include "sink_writer_base.hpp"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

class SearchRemoveFilterBase;

using TokenizerProvider =
  absl::AnyInvocable<catalog::ColumnTokenizer(catalog::Column::Id)>;

// Keyed tokenizer accessor for JSON-path fields. The writer calls it every
// time SwitchJsonExpression runs because each call moves the analyzer into
// `_field`; a single moveable value would not suffice.
using JsonPathTokenizerProvider = absl::AnyInvocable<catalog::ColumnTokenizer(
  catalog::Column::Id, std::string_view)>;

// Bound JSON-path expression + its canonical serialized form, owned by the
// caller and handed off to the writer. The writer drives ExpressionExecutor
// against the bound expression and uses the serialized form as the iresearch
// field-name suffix (matching the SELECT-side filter exactly).
struct JsonPathBoundEntry {
  duckdb::unique_ptr<duckdb::Expression> bound_expression;
  std::string serialized_expr;
  catalog::Column::Id column_id;
};

inline TokenizerProvider MakeTokenizerProvider(
  const std::shared_ptr<const catalog::Snapshot>& snapshot,
  const catalog::InvertedIndex& index) {
  return [snapshot, &index](catalog::Column::Id column_id) {
    return index.GetColumnTokenizer(snapshot, column_id);
  };
}

// Builds the keyed tokenizer accessor for JSON-path fields. Captures the
// snapshot + index reference + client-context pointer so each call can
// resolve the path-specific dictionary lazily.
inline JsonPathTokenizerProvider MakeJsonPathTokenizerProvider(
  std::shared_ptr<const catalog::Snapshot> snapshot,
  const catalog::InvertedIndex& index, duckdb::ClientContext* client_context) {
  return [snapshot = std::move(snapshot), &index, client_context](
           catalog::Column::Id column_id,
           std::string_view serialized_expr) -> catalog::ColumnTokenizer {
    SDB_ASSERT(client_context,
               "JSON-path tokenizer lookup requires a ClientContext");
    auto tokenizer = index.GetJsonPathTokenizer(
      snapshot, column_id, std::string{serialized_expr}, *client_context);
    SDB_ASSERT(tokenizer, "JSON-path tokenizer not found for serialized expr");
    return *std::move(tokenizer);
  };
}

inline std::vector<JsonPathBoundEntry> MakeJsonPathBoundEntries(
  const catalog::InvertedIndex& index,
  std::span<const catalog::Column::Id> columns,
  duckdb::ClientContext* client_context) {
  std::vector<JsonPathBoundEntry> entries;
  if (!client_context) {
    return entries;
  }
  for (auto col_id : columns) {
    const auto* col = index.FindColumnInfo(col_id);
    if (!col) {
      continue;
    }
    for (const auto& json_subexpr : col->json_paths) {
      SDB_ASSERT(!json_subexpr.serialized_expr.empty(),
                 "Any json expr should be serialized in non-empty string");
      auto bound = DeserializeBoundExpression(json_subexpr.serialized_expr,
                                              *client_context);
      entries.push_back(
        {std::move(bound), json_subexpr.serialized_expr, col_id});
    }
  }
  return entries;
}

class SearchSinkInsertBaseImpl : public ColumnSinkWriterImplBase {
 public:
  SearchSinkInsertBaseImpl(
    irs::IndexWriter::Transaction& trx, TokenizerProvider&& tokenizer_provider,
    JsonPathTokenizerProvider&& json_path_tokenizer_provider,
    std::vector<JsonPathBoundEntry>&& json_path_entries,
    std::span<const catalog::Column::Id> columns);

  void InitImpl(size_t batch_size);

  void WriteImpl(std::span<const rocksdb::Slice> cell_slices,
                 std::string_view full_key);

  bool SwitchColumnImpl(const ColumnDescriptor& col);
  // Like SwitchColumnImpl, but the field-name suffix is the canonical form
  // of a user JSON-extract expression (column_id is the base JSON column).
  // The Vector subsequently passed to Write*() must be the result of
  // evaluating that bound expression -- the sink does no JSON parsing.
  bool SwitchJsonExpressionImpl(const duckdb::LogicalType& return_type,
                                bool have_nulls, catalog::Column::Id column_id,
                                std::string_view canonical_expression);
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
  // `name_suffix`, when non-empty, is appended to the field-name prefix as a
  // JSON-Pointer-escaped segment (used by the JSON-expression path). A
  // non-empty suffix also tells the VARCHAR/BLOB path to source the
  // tokenizer from `_json_path_tokenizer_provider(column_id, name_suffix)`
  // instead of `_tokenizer_provider(column_id)` -- the per-path dictionary
  // (text_dict/verbatim_dict) is what gives phrase queries their positions.
  template<duckdb::LogicalTypeId Kind>
  void SetupColumnWriter(catalog::Column::Id column_id, bool have_nulls,
                         std::string_view name_suffix = {});

  // Sets up `_aux_numeric_field` + `_aux_bool_field` for a JSON-path VARCHAR
  // emission (under the same canonical suffix as `_field`) and wraps
  // `_current_writer` so each cell is parsed once and dispatched to the
  // string field plus any numeric/bool variants whose parse succeeds.
  void SetupJsonAuxTypedFields(catalog::Column::Id column_id,
                               std::string_view name_suffix);

  TokenizerProvider _tokenizer_provider;
  JsonPathTokenizerProvider _json_path_tokenizer_provider;
  Field _field;
  Field _pk_field;
  Field _null_field;
  // Auxiliary fields for JSON-path VARCHAR emissions. The user-visible cast
  // path (e.g. `(content->>'val')::int = 42`) requires the writer to also
  // emit numeric/bool variants of each row so the SELECT-side filter can
  // hit the natural-type-mangled iresearch field. Set up alongside `_field`
  // when SwitchJsonExpressionImpl runs with a per-path tokenizer.
  Field _aux_numeric_field;
  Field _aux_bool_field;
  std::string _name_buffer;
  std::string _null_name_buffer;
  std::string _aux_numeric_name_buffer;
  std::string _aux_bool_name_buffer;
  irs::IndexWriter::Transaction& _trx;
  std::optional<irs::IndexWriter::Document> _document;

  Writer _current_writer;
  bool _emit_pk{true};

  // Bound JSON-extract expressions handed off at writer construction time.
  // Owned here; views into them are exposed via JsonExpressionEvals so the
  // caller can drive ExpressionExecutor against the input chunk.
  std::vector<JsonPathBoundEntry> _json_exprs_owned;
  std::vector<JsonExpressionEval> _json_expr_views;

  containers::FlatHashSet<catalog::Column::Id> _json_columns;

 public:
  std::span<const JsonExpressionEval> JsonExpressionEvalsImpl() const noexcept {
    return _json_expr_views;
  }
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
