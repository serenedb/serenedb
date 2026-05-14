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
#include "connector/index_expression.hpp"
#include "primary_key.hpp"
#include "search/inverted_index_shard.h"
#include "search_remove_filter.hpp"
#include "sink_writer_base.hpp"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

class SearchRemoveFilterBase;

using ColumnTokenizerProvider =
  absl::AnyInvocable<catalog::FieldTokenizer(catalog::Column::Id)>;

using ExpressionTokenizerProvider = absl::AnyInvocable<catalog::FieldTokenizer(
  catalog::Column::Id, std::string_view)>;

inline ColumnTokenizerProvider MakeColumnTokenizerProvider(
  const std::shared_ptr<const catalog::Snapshot>& snapshot,
  const catalog::InvertedIndex& index) {
  return [snapshot, &index](catalog::Column::Id column_id) {
    return index.GetColumnTokenizer(snapshot, column_id);
  };
}

inline ExpressionTokenizerProvider MakeExpressionTokenizerProvider(
  std::shared_ptr<const catalog::Snapshot> snapshot,
  const catalog::InvertedIndex& index) {
  return [snapshot = std::move(snapshot), &index](
           catalog::Column::Id column_id,
           std::string_view serialized_expr) -> catalog::FieldTokenizer {
    return index.GetExprTokenizer(snapshot, column_id, serialized_expr);
  };
}

inline std::vector<IndexedExpression> MakeIndexedExpressions(
  const catalog::InvertedIndex& index,
  std::span<const catalog::Column::Id> columns,
  duckdb::ClientContext* client_context) {
  std::vector<IndexedExpression> entries;
  if (!client_context) {
    return entries;
  }
  for (auto col_id : columns) {
    const auto* col = index.FindColumnInfo(col_id);
    if (!col) {
      continue;
    }
    for (const auto& expr_info : col->expressions_infos) {
      SDB_ASSERT(!expr_info.serialized_expr.empty(),
                 "Any expr should be serialized in non-empty string");
      auto bound =
        DeserializeBoundExpression(expr_info.serialized_expr, *client_context);
      entries.push_back({std::move(bound), expr_info.serialized_expr, col_id});
    }
  }
  return entries;
}

class SearchSinkInsertBaseImpl : public ColumnSinkWriterImplBase {
 public:
  SearchSinkInsertBaseImpl(
    irs::IndexWriter::Transaction& trx,
    ColumnTokenizerProvider&& tokenizer_provider,
    std::span<const catalog::Column::Id> columns,
    ExpressionTokenizerProvider&& expr_tokenizer_provider = {},
    std::vector<IndexedExpression>&& indexed_exprs = {});

  void InitImpl(size_t batch_size);

  void WriteImpl(std::span<const rocksdb::Slice> cell_slices,
                 std::string_view full_key);

  bool SwitchColumnImpl(const ColumnDescriptor& col);
  bool SwitchExpressionImpl(const ExpressionDescriptor& expr_desc);
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
    void PrepareForStringValue(catalog::FieldTokenizer&& column_analyzer);
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

  template<duckdb::LogicalTypeId Kind>
  void SetupColumnWriter(catalog::Column::Id column_id, bool have_nulls,
                         std::string_view name_suffix = {});

  // Sets up `_aux_numeric_field` + `_aux_bool_field` for an indexed expression
  void SetupExprAuxTypedFields(catalog::Column::Id column_id,
                               std::string_view name_suffix);

  ColumnTokenizerProvider _tokenizer_provider;
  ExpressionTokenizerProvider _subexpr_tokenizer_provider;
  Field _field;
  Field _pk_field;
  Field _null_field;
  // Auxiliary fields for indexed expression
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

  std::vector<IndexedExpression> _indexed_expressions;
  // Column IDs that are used in indexed expressions.
  // For now only exactly once column ID per expression is allowed.
  containers::FlatHashSet<catalog::Column::Id> _indexed_expressions_columns;

 public:
  std::span<const IndexedExpression> IndexedExpressionImpl() const noexcept {
    return _indexed_expressions;
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
