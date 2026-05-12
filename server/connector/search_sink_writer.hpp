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

// Factory that builds a fresh tokenizer for a JSON-path field per call. The
// writer needs a fresh instance every time SwitchJsonExpression runs because
// each call moves the analyzer into `_field`; the per-path tokenizer cannot
// be a single moveable value.
using JsonPathTokenizerFactory = absl::AnyInvocable<catalog::ColumnTokenizer()>;

// One JSON path's worth of indexing config, resolved against the catalog.
struct JsonPathSinkConfig {
  duckdb::unique_ptr<duckdb::Expression> bound_expression;
  JsonPathTokenizerFactory make_tokenizer;
  // Structural canonical for this path (e.g. `->>|host`). Used as the
  // iresearch field-name suffix so writer and SELECT-side filter agree on
  // the field identity regardless of binder state.
  std::string path_canonical;
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
// `client_context` is required when entries carry a serialised bound
// expression (DuckDB's deserializer resolves catalog references through it).
// Returns an empty vector for columns without path-based indexing.
inline JsonPathsProvider MakeJsonPathsProvider(
  std::shared_ptr<const catalog::Snapshot> snapshot,
  const catalog::InvertedIndex& index, duckdb::ClientContext* client_context) {
  return [snapshot = std::move(snapshot), &index, client_context](
           catalog::Column::Id column_id) -> std::vector<JsonPathSinkConfig> {
    const auto* col = index.FindColumnInfo(column_id);
    if (!col) {
      return {};
    }
    std::vector<JsonPathSinkConfig> out;
    // GetJsonPathTokenizer needs a ClientContext to deserialise candidate
    // paths' bound expressions. Without one (e.g. early-init paths) we
    // can't resolve anything -- return empty.
    if (!client_context) {
      return out;
    }
    out.reserve(col->json_paths.size());
    for (const auto& json_subexpr : col->json_paths) {
      // Probe once so columns without a usable tokenizer are filtered out.
      auto probe = index.GetJsonPathTokenizer(
        snapshot, column_id, json_subexpr.serialized_bound_expression,
        *client_context);
      if (!probe) {
        continue;
      }
      duckdb::unique_ptr<duckdb::Expression> bound;
      if (!json_subexpr.serialized_bound_expression.empty()) {
        bound = DeserializeBoundExpression(
          json_subexpr.serialized_bound_expression, *client_context);
      }
      // Capture pointer BY VALUE -- the outer closure's parameter goes out
      // of scope once MakeJsonPathsProvider returns, so a `&` capture would
      // dangle. The pointee (ClientContext) outlives the writer.
      auto serialized = json_subexpr.serialized_bound_expression;
      JsonPathTokenizerFactory factory =
        [ctx = client_context, snapshot, &index, column_id,
         ser = std::move(serialized)]() -> catalog::ColumnTokenizer {
        auto t = index.GetJsonPathTokenizer(snapshot, column_id, ser, *ctx);
        SDB_ASSERT(t,
                   "JSON-path tokenizer disappeared between probe and "
                   "writer setup");
        return *std::move(t);
      };
      out.emplace_back(std::move(bound), std::move(factory),
                       json_subexpr.serialized_bound_expression);
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
  // JSON-Pointer-escaped segment (used by the JSON-expression path).
  // When `tokenizer_override` is non-null, the VARCHAR/BLOB path uses it
  // instead of `_tokenizer_provider(column_id)` -- needed for JSON-path
  // fields whose tokenizer (text_dict/verbatim_dict) is per-path, not
  // per-column.
  template<duckdb::LogicalTypeId Kind>
  void SetupColumnWriter(
    catalog::Column::Id column_id, bool have_nulls,
    std::string_view name_suffix = {},
    JsonPathTokenizerFactory* tokenizer_override = nullptr);

  // Sets up `_aux_numeric_field` + `_aux_bool_field` for a JSON-path VARCHAR
  // emission (under the same canonical suffix as `_field`) and wraps
  // `_current_writer` so each cell is parsed once and dispatched to the
  // string field plus any numeric/bool variants whose parse succeeds.
  void SetupJsonAuxTypedFields(catalog::Column::Id column_id,
                               std::string_view name_suffix);

  TokenizerProvider _tokenizer_provider;
  JsonPathsProvider _json_paths_provider;
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

  // Bound JSON-extract expressions resolved at writer construction time.
  // Owned here; views into them are exposed via JsonExpressionEvals so the
  // caller can drive ExpressionExecutor against the input chunk.
  struct OwnedJsonExpr {
    duckdb::unique_ptr<duckdb::Expression> expr;
    std::string serialized;
    catalog::Column::Id column_id;
    // Factory the writer calls per SwitchJsonExpression to mint a fresh
    // tokenizer for this path's iresearch field. Carries the path's
    // dictionary + features (verbatim_dict / text_dict) so phrase queries
    // get the position-bearing analyzer the user configured.
    JsonPathTokenizerFactory make_tokenizer;
  };
  std::vector<OwnedJsonExpr> _owned_json_exprs;
  std::vector<JsonExpressionEval> _json_expr_views;
  // Columns whose JSON expressions are evaluated externally via
  // ExpressionExecutor. SwitchColumnImpl skips them so the simdjson and
  // whole-column paths don't double-write.
  containers::FlatHashSet<catalog::Column::Id> _columns_via_executor;

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
