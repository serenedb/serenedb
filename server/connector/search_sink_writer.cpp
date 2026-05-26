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

#include "search_sink_writer.hpp"

#include <duckdb/common/enum_util.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/analysis/tokenizers.hpp>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/endian.h"
#include "catalog/mangling.h"
#include "catalog/table_options.h"
#include "connector/common.h"
#include "connector/key_utils.hpp"
#include "connector/search_field_name.hpp"
#include "search_remove_filter.hpp"

namespace sdb::connector {
namespace {

constexpr size_t kDefaultPoolSize = 8;  // arbitrary value
irs::UnboundedObjectPool<search::AnalyzerImpl::Builder> gStringStreamPool(
  kDefaultPoolSize);
irs::UnboundedObjectPool<search::AnalyzerImpl::Builder> gNumberStreamPool(
  kDefaultPoolSize);
irs::UnboundedObjectPool<search::AnalyzerImpl::Builder> gBoolStreamPool(
  kDefaultPoolSize);
irs::UnboundedObjectPool<search::AnalyzerImpl::Builder> gNullStreamPool(
  kDefaultPoolSize);

void SetNameToBuffer(std::string& name_buffer, catalog::Column::Id column_id) {
  SDB_ASSERT(name_buffer.size() >= sizeof(column_id));
  absl::big_endian::Store(name_buffer.data(), column_id);
}

// TODO(Dronplane): most likely will need this in filter builder.
//  Move to some shared place if it would be the case.

// Underlying signed-int representation used to index numeric/temporal DuckDB
// types in iresearch. DATE/TIMESTAMP/TIMESTAMP_TZ map to their underlying
// int32/int64 (raw-int compare matches DuckDB's date_t::operator< /
// timestamp_t::operator< exactly -- see date.hpp / timestamp.hpp).
template<duckdb::LogicalTypeId Kind>
struct NumericSliceTypeImpl;

template<>
struct NumericSliceTypeImpl<duckdb::LogicalTypeId::TINYINT> {
  using Type = int8_t;
};
template<>
struct NumericSliceTypeImpl<duckdb::LogicalTypeId::SMALLINT> {
  using Type = int16_t;
};
template<>
struct NumericSliceTypeImpl<duckdb::LogicalTypeId::INTEGER> {
  using Type = int32_t;
};
template<>
struct NumericSliceTypeImpl<duckdb::LogicalTypeId::DATE> {
  using Type = int32_t;
};
template<>
struct NumericSliceTypeImpl<duckdb::LogicalTypeId::BIGINT> {
  using Type = int64_t;
};
template<>
struct NumericSliceTypeImpl<duckdb::LogicalTypeId::TIMESTAMP> {
  using Type = int64_t;
};
template<>
struct NumericSliceTypeImpl<duckdb::LogicalTypeId::TIMESTAMP_TZ> {
  using Type = int64_t;
};
template<>
struct NumericSliceTypeImpl<duckdb::LogicalTypeId::FLOAT> {
  using Type = float;
};
template<>
struct NumericSliceTypeImpl<duckdb::LogicalTypeId::DOUBLE> {
  using Type = double;
};

template<duckdb::LogicalTypeId Kind>
using NumericSliceType = typename NumericSliceTypeImpl<Kind>::Type;

template<duckdb::LogicalTypeId Kind>
inline constexpr bool kIsNumericKind =
  Kind == duckdb::LogicalTypeId::TINYINT ||
  Kind == duckdb::LogicalTypeId::SMALLINT ||
  Kind == duckdb::LogicalTypeId::INTEGER ||
  Kind == duckdb::LogicalTypeId::BIGINT ||
  Kind == duckdb::LogicalTypeId::FLOAT ||
  Kind == duckdb::LogicalTypeId::DOUBLE ||
  Kind == duckdb::LogicalTypeId::DATE ||
  Kind == duckdb::LogicalTypeId::TIMESTAMP ||
  Kind == duckdb::LogicalTypeId::TIMESTAMP_TZ;

}  // namespace

SearchSinkInsertBaseImpl::SearchSinkInsertBaseImpl(
  irs::IndexWriter::Transaction& trx, TokenizerProvider&& tokenizer_provider,
  StoreValuesProvider&& store_values_provider,
  IsTextIndexedProvider&& is_text_indexed_provider,
  HNSWInfoProvider&& hnsw_info_provider,
  std::span<const catalog::Column::Id> columns,
  ExpressionTokenizerProvider&& expr_tokenizer_provider,
  std::vector<IndexedExpression>&& indexed_exprs)
  : ColumnSinkWriterImplBase{columns},
    _tokenizer_provider{std::move(tokenizer_provider)},
    _store_values_provider{std::move(store_values_provider)},
    _is_text_indexed_provider{std::move(is_text_indexed_provider)},
    _hnsw_info_provider{std::move(hnsw_info_provider)},
    _subexpr_tokenizer_provider{std::move(expr_tokenizer_provider)},
    _indexed_expressions{std::move(indexed_exprs)},
    _trx{trx} {
  _pk_field.PrepareForVerbatimStringValue();
  _pk_field.name = kPkFieldName;
}

bool SearchSinkInsertBaseImpl::SwitchColumnImpl(const ColumnDescriptor& col,
                                                const duckdb::Vector& vec,
                                                duckdb::idx_t count) {
  const auto column_id = col.id;
  const auto& type = col.type;
  const auto have_nulls = col.have_nulls;
  _active_column_id = column_id;
  _active_column_type = type;
  _active_columnstore_writer = nullptr;
  std::optional<irs::HNSWInfo> hnsw_info;
  if (_hnsw_info_provider) {
    hnsw_info = _hnsw_info_provider(column_id);
  }
  const bool wants_columnstore =
    (_store_values_provider && _store_values_provider(column_id)) || hnsw_info;
  auto open_typed_batch_cs = [&] {
    BulkAppendToColumnstore(static_cast<irs::field_id>(column_id), type, vec,
                            count);
  };

  const bool text_indexed =
    _is_text_indexed_provider && _is_text_indexed_provider(column_id);

  if (!IsIndexed(column_id)) {
#ifdef SDB_DEV
    _current_writer = nullptr;
#endif
    return false;
  }
  if (wants_columnstore && !text_indexed && !hnsw_info) {
    open_typed_batch_cs();
#ifdef SDB_DEV
    _current_writer = nullptr;
#endif
    return false;
  }
  if (wants_columnstore && hnsw_info) {
    open_typed_batch_cs();
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::SQLNULL:
      SetupColumnWriter<duckdb::LogicalTypeId::SQLNULL>(column_id, false);
      break;
    case duckdb::LogicalTypeId::VARCHAR:
      SetupColumnWriter<duckdb::LogicalTypeId::VARCHAR>(column_id, have_nulls);
      break;
    case duckdb::LogicalTypeId::BLOB:
      SetupColumnWriter<duckdb::LogicalTypeId::BLOB>(column_id, have_nulls);
      break;
    case duckdb::LogicalTypeId::BOOLEAN:
      SetupColumnWriter<duckdb::LogicalTypeId::BOOLEAN>(column_id, have_nulls);
      break;
    case duckdb::LogicalTypeId::TINYINT:
      SetupColumnWriter<duckdb::LogicalTypeId::TINYINT>(column_id, have_nulls);
      break;
    case duckdb::LogicalTypeId::SMALLINT:
      SetupColumnWriter<duckdb::LogicalTypeId::SMALLINT>(column_id, have_nulls);
      break;
    case duckdb::LogicalTypeId::INTEGER:
      SetupColumnWriter<duckdb::LogicalTypeId::INTEGER>(column_id, have_nulls);
      break;
    case duckdb::LogicalTypeId::BIGINT:
      SetupColumnWriter<duckdb::LogicalTypeId::BIGINT>(column_id, have_nulls);
      break;
    case duckdb::LogicalTypeId::FLOAT:
      SetupColumnWriter<duckdb::LogicalTypeId::FLOAT>(column_id, have_nulls);
      break;
    case duckdb::LogicalTypeId::DOUBLE:
      SetupColumnWriter<duckdb::LogicalTypeId::DOUBLE>(column_id, have_nulls);
      break;
    case duckdb::LogicalTypeId::DATE:
      SetupColumnWriter<duckdb::LogicalTypeId::DATE>(column_id, have_nulls);
      break;
    case duckdb::LogicalTypeId::TIMESTAMP:
      SetupColumnWriter<duckdb::LogicalTypeId::TIMESTAMP>(column_id,
                                                          have_nulls);
      break;
    // TODO(Dronplane): other timestamp derived types could be handled same way
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      SetupColumnWriter<duckdb::LogicalTypeId::TIMESTAMP_TZ>(column_id,
                                                             have_nulls);
      break;
    case duckdb::LogicalTypeId::ARRAY: {
      SetupColumnWriter<duckdb::LogicalTypeId::ARRAY>(column_id, have_nulls);
      break;
    }
    case duckdb::LogicalTypeId::GEOMETRY: {
      SetupColumnWriter<duckdb::LogicalTypeId::GEOMETRY>(column_id, have_nulls);
      break;
    }
    default:
      // Unsupported type for inverted index (e.g. INTEGER without opclass).
      // Skip this column rather than crashing in WriteImpl.
      _current_writer = nullptr;
      return false;
  }
  SDB_ASSERT(_document);
  _document->NextFieldBatch();
  if (wants_columnstore && !_active_columnstore_writer) {
    open_typed_batch_cs();
  }
  return true;
}

bool SearchSinkInsertBaseImpl::SwitchExpressionImpl(
  const ExpressionDescriptor& expr_desc, const duckdb::Vector& vec,
  duckdb::idx_t count) {
  if (!_subexpr_tokenizer_provider) {
    _current_writer = nullptr;
    return false;
  }
  const auto have_nulls = expr_desc.have_nulls;
  const auto field_id = expr_desc.field_id;
  const auto kind = expr_desc.type.id();
  SDB_ASSERT(field_id != 0);

  const bool wants_columnstore =
    _store_values_provider && _store_values_provider(field_id);
  const bool text_indexed =
    _is_text_indexed_provider && _is_text_indexed_provider(field_id);
  if (wants_columnstore) {
    BulkAppendToColumnstore(field_id, expr_desc.type, vec, count);
  }
  if (wants_columnstore && !text_indexed) {
    _current_writer = nullptr;
    return false;
  }

  if (expr_desc.type.IsJSONType()) {
    auto tokenizer = _subexpr_tokenizer_provider(field_id);
    if (irs::get<irs::StoreAttr>(*tokenizer.analyzer) != nullptr) {
      MakeFieldName(field_id, _name_buffer);
      search::mangling::MangleString(_name_buffer);
      const auto tokenizer_column = tokenizer.tokenizer_column;
      _field.PrepareForStringValue(std::move(tokenizer));
      SDB_ASSERT(_field.store_attr != nullptr);
      SDB_ASSERT(tokenizer_column);
      _current_writer =
        MakeStoreAttrWriter(*tokenizer_column, have_nulls, &WriteStringValue);
      _field.name = _name_buffer;
      SDB_ASSERT(_document);
      _document->NextFieldBatch();
      return true;
    }
    SetupJsonExpressionWriter(field_id, std::move(tokenizer));
    SDB_ASSERT(_document);
    _document->NextFieldBatch();
    return true;
  }

  MakeFieldName(field_id, _name_buffer);
  if (have_nulls || kind == duckdb::LogicalTypeId::SQLNULL) {
    MakeFieldName(field_id, _null_name_buffer);
    search::mangling::MangleNull(_null_name_buffer);
    _null_field.name = _null_name_buffer;
    if (!_null_field.analyzer) {
      _null_field.PrepareForNullValue();
    }
  }

  auto make_nullable = [&]<typename WriteFunc>(WriteFunc&& write_func) {
    return
      [&, write_func = std::forward<WriteFunc>(write_func)](
        std::string_view full_key, std::span<const rocksdb::Slice> cell_slices,
        Field& field) -> Field& {
        if (cell_slices.empty() ||
            (cell_slices.size() == 1 && cell_slices.front().empty())) {
          _null_field.SetNullValue();
          return _null_field;
        }
        return write_func(full_key, cell_slices, field);
      };
  };

  switch (kind) {
    case duckdb::LogicalTypeId::SQLNULL:
      _current_writer =
        MakeIndexWriter([&](std::string_view, std::span<const rocksdb::Slice>,
                            Field&) -> Field& {
          _null_field.SetNullValue();
          return _null_field;
        });
      break;
    case duckdb::LogicalTypeId::VARCHAR: {
      search::mangling::MangleString(_name_buffer);
      auto tokenizer = _subexpr_tokenizer_provider(field_id);
      const auto tokenizer_column = tokenizer.tokenizer_column;
      _field.PrepareForStringValue(std::move(tokenizer));
      const bool has_store = _field.store_attr != nullptr;
      if (has_store) {
        SDB_ASSERT(tokenizer_column,
                   "expression tokenizer registers StoreAttr but catalog has "
                   "no synthetic column allocated for field_id ",
                   field_id);
        _current_writer =
          MakeStoreAttrWriter(*tokenizer_column, have_nulls, &WriteStringValue);
      } else {
        _current_writer = have_nulls
                            ? MakeIndexWriter(make_nullable(&WriteStringValue))
                            : MakeIndexWriter(&WriteStringValue);
      }
    } break;
    case duckdb::LogicalTypeId::BOOLEAN:
      search::mangling::MangleBool(_name_buffer);
      _field.PrepareForBooleanValue();
      _current_writer = have_nulls
                          ? MakeIndexWriter(make_nullable(&WriteBooleanValue))
                          : MakeIndexWriter(&WriteBooleanValue);
      break;
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::DATE:
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ: {
      search::mangling::MangleNumeric(_name_buffer);
      _field.PrepareForNumericValue();
      auto numeric_writer = [&]() -> Writer {
        switch (kind) {
          case duckdb::LogicalTypeId::TINYINT:
            return have_nulls ? MakeIndexWriter(
                                  make_nullable(&WriteNumericValue<int8_t>))
                              : MakeIndexWriter(&WriteNumericValue<int8_t>);
          case duckdb::LogicalTypeId::SMALLINT:
            return have_nulls ? MakeIndexWriter(
                                  make_nullable(&WriteNumericValue<int16_t>))
                              : MakeIndexWriter(&WriteNumericValue<int16_t>);
          case duckdb::LogicalTypeId::INTEGER:
          case duckdb::LogicalTypeId::DATE:
            return have_nulls ? MakeIndexWriter(
                                  make_nullable(&WriteNumericValue<int32_t>))
                              : MakeIndexWriter(&WriteNumericValue<int32_t>);
          case duckdb::LogicalTypeId::BIGINT:
          case duckdb::LogicalTypeId::TIMESTAMP:
          case duckdb::LogicalTypeId::TIMESTAMP_TZ:
            return have_nulls ? MakeIndexWriter(
                                  make_nullable(&WriteNumericValue<int64_t>))
                              : MakeIndexWriter(&WriteNumericValue<int64_t>);
          case duckdb::LogicalTypeId::FLOAT:
            return have_nulls
                     ? MakeIndexWriter(make_nullable(&WriteNumericValue<float>))
                     : MakeIndexWriter(&WriteNumericValue<float>);
          case duckdb::LogicalTypeId::DOUBLE:
            return have_nulls ? MakeIndexWriter(
                                  make_nullable(&WriteNumericValue<double>))
                              : MakeIndexWriter(&WriteNumericValue<double>);
          default:
            SDB_UNREACHABLE();
        }
      }();
      _current_writer = std::move(numeric_writer);
      break;
    }
    default:
      _current_writer = nullptr;
      return false;
  }
  _field.name = _name_buffer;
  SDB_ASSERT(_document);
  _document->NextFieldBatch();
  return true;
}

void SearchSinkInsertBaseImpl::AppendCsContinuation(
  const duckdb::Vector& vec, duckdb::idx_t count,
  duckdb::idx_t row_offset_from_first_doc) {
  if (!_active_columnstore_writer) {
    return;
  }
  SDB_ASSERT(_document);
  const uint64_t start_row =
    (_document->DocId() - irs::doc_limits::min()) + row_offset_from_first_doc;
  _active_columnstore_writer->Append(start_row, vec, count);
}

void SearchSinkInsertBaseImpl::WriteImpl(
  std::span<const rocksdb::Slice> cell_slices, std::string_view full_key) {
  SDB_ASSERT(_current_writer);
  SDB_ASSERT(_document);
  _current_writer(full_key, cell_slices);
  _document->NextDocument();
}

void SearchSinkInsertBaseImpl::FinishImpl() {
  _columnstore_writers.clear();
  _per_row_blob_writers.clear();
  _active_columnstore_writer = nullptr;
  _document.reset();
}

void SearchSinkInsertBaseImpl::BulkAppendToColumnstore(
  irs::field_id field_id, const duckdb::LogicalType& type,
  const duckdb::Vector& vec, duckdb::idx_t count) {
  if (!_document) {
    return;
  }
  auto* doc_columnstore = _document->Columnstore();
  if (!doc_columnstore) {
    return;
  }
  auto [it, inserted] = _columnstore_writers.try_emplace(field_id, nullptr);
  if (inserted) {
    it->second = &doc_columnstore->OpenColumn(field_id, type);
  }
  _active_columnstore_writer = it->second;
  _document->NextFieldBatch();
  const uint64_t start_row = _document->DocId() - irs::doc_limits::min();
  _active_columnstore_writer->Append(start_row, vec, count);
}

irs::columnstore::ColumnWriter*
SearchSinkInsertBaseImpl::EnsurePerRowBlobWriter(irs::field_id field_id) {
  auto* doc_columnstore = _document ? _document->Columnstore() : nullptr;
  if (!doc_columnstore) {
    return nullptr;
  }
  auto [it, inserted] = _per_row_blob_writers.try_emplace(field_id, nullptr);
  if (!it->second) {
    it->second =
      &doc_columnstore->OpenColumn(field_id, duckdb::LogicalType::BLOB);
  }
  return it->second;
}

void SearchSinkInsertBaseImpl::AppendPerRowBlob(irs::field_id field_id,
                                                irs::bytes_view bytes) {
  auto* writer = EnsurePerRowBlobWriter(field_id);
  if (!writer) {
    return;
  }
  _row_buffer.Initialize(duckdb::VectorDataInitialization::ZERO_INITIALIZE, 1);
  auto* slots =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(_row_buffer);
  slots[0] = duckdb::StringVector::AddStringOrBlob(
    _row_buffer, reinterpret_cast<const char*>(bytes.data()), bytes.size());
  const uint64_t row = _document->DocId() - irs::doc_limits::min();
  writer->Append(row, _row_buffer, 1);
}

void SearchSinkInsertBaseImpl::AppendPerRowBlobNull(irs::field_id field_id) {
  auto it = _per_row_blob_writers.find(field_id);
  if (it == _per_row_blob_writers.end() || !it->second) {
    return;
  }
  _row_buffer.Initialize(duckdb::VectorDataInitialization::ZERO_INITIALIZE, 1);
  const uint64_t row = _document->DocId() - irs::doc_limits::min();
  it->second->Append(row, _row_buffer, 1);
}

void SearchSinkInsertBaseImpl::AppendPerRowPrimaryKey(
  std::string_view row_key) {
  AppendPerRowBlob(static_cast<irs::field_id>(catalog::Column::kGeneratedPKId),
                   irs::ViewCast<irs::byte_type>(row_key));
}

template<duckdb::LogicalTypeId Kind>
void SearchSinkInsertBaseImpl::SetupColumnWriter(catalog::Column::Id column_id,
                                                 bool have_nulls) {
  basics::StrResize(_name_buffer, sizeof(column_id));
  SetNameToBuffer(_name_buffer, column_id);

  if (have_nulls || Kind == duckdb::LogicalTypeId::SQLNULL) {
    basics::StrResize(_null_name_buffer, sizeof(column_id));
    SetNameToBuffer(_null_name_buffer, column_id);
    search::mangling::MangleNull(_null_name_buffer);
    _null_field.name = _null_name_buffer;
    if (!_null_field.analyzer) {
      _null_field.PrepareForNullValue();
    }
  }

  // Generic wrapper for handling nulls in column.
  auto make_nullable_writer_func =
    [&]<typename WriteFunc>(WriteFunc&& write_func) {
      return
        [&, write_func = std::forward<WriteFunc>(write_func)](
          std::string_view full_key,
          std::span<const rocksdb::Slice> cell_slices, Field& field) -> Field& {
          if (cell_slices.empty() ||
              (cell_slices.size() == 1 && cell_slices.front().empty())) {
            _null_field.SetNullValue();
            return _null_field;
          }
          return write_func(full_key, cell_slices, field);
        };
    };

  if constexpr (Kind == duckdb::LogicalTypeId::SQLNULL) {
    _current_writer = MakeIndexWriter(
      [&](std::string_view, std::span<const rocksdb::Slice>, Field&) -> Field& {
        _null_field.SetNullValue();
        return _null_field;
      });
  } else if constexpr (Kind == duckdb::LogicalTypeId::VARCHAR ||
                       Kind == duckdb::LogicalTypeId::BLOB) {
    search::mangling::MangleString(_name_buffer);
    auto column_tokenizer = _tokenizer_provider(column_id);
    const auto tokenizer_column = column_tokenizer.tokenizer_column;
    _field.PrepareForStringValue(std::move(column_tokenizer));
    const bool has_store = _field.store_attr != nullptr;
    if (has_store) {
      SDB_ASSERT(tokenizer_column,
                 "tokenizer registers StoreAttr but catalog has no tokenizer "
                 "column allocated for column ",
                 column_id);
      _current_writer =
        MakeStoreAttrWriter(*tokenizer_column, have_nulls, &WriteStringValue);
    } else {
      _current_writer =
        have_nulls
          ? MakeIndexWriter(make_nullable_writer_func(&WriteStringValue))
          : MakeIndexWriter(&WriteStringValue);
    }
  } else if constexpr (Kind == duckdb::LogicalTypeId::BOOLEAN) {
    search::mangling::MangleBool(_name_buffer);
    _field.PrepareForBooleanValue();
    if (have_nulls) {
      _current_writer =
        MakeIndexWriter(make_nullable_writer_func(&WriteBooleanValue));
    } else {
      _current_writer = MakeIndexWriter(&WriteBooleanValue);
    }
  } else if constexpr (kIsNumericKind<Kind>) {
    search::mangling::MangleNumeric(_name_buffer);
    _field.PrepareForNumericValue();
    using T = NumericSliceType<Kind>;
    if (have_nulls) {
      _current_writer =
        MakeIndexWriter(make_nullable_writer_func(&WriteNumericValue<T>));
    } else {
      _current_writer = MakeIndexWriter(&WriteNumericValue<T>);
    }
  } else if constexpr (Kind == duckdb::LogicalTypeId::ARRAY) {
    _current_writer = [](std::string_view, std::span<const rocksdb::Slice>) {};
  } else if constexpr (Kind == duckdb::LogicalTypeId::GEOMETRY) {
    search::mangling::MangleString(_name_buffer);
    auto column_tokenizer = _tokenizer_provider(column_id);
    const auto tokenizer_column = column_tokenizer.tokenizer_column;
    _field.PrepareForStringValue(std::move(column_tokenizer));
    const bool has_store = _field.store_attr != nullptr;
    auto geo_writer = [](std::string_view,
                         std::span<const rocksdb::Slice> cell_slices,
                         Field& field) -> Field& {
      SDB_ASSERT(!cell_slices.empty());
      const auto& slice = cell_slices.back();
      const irs::bytes_view wkb{
        reinterpret_cast<const irs::byte_type*>(slice.data()), slice.size()};
      auto& geo =
        basics::downCast<irs::analysis::GeoAnalyzer>(*field.string_analyzer);
      // TODO(Dronplane) Should be similar to CSV ignore_errors behavior
      // Same for VARCHAR
      std::ignore = geo.resetWKB(wkb);
      return field;
    };
    if (has_store) {
      SDB_ASSERT(tokenizer_column,
                 "geo tokenizer registers StoreAttr but catalog has no "
                 "tokenizer column allocated for column ",
                 column_id);
      _current_writer =
        MakeStoreAttrWriter(*tokenizer_column, have_nulls, geo_writer);
    } else {
      _current_writer =
        have_nulls ? MakeIndexWriter(make_nullable_writer_func(geo_writer))
                   : MakeIndexWriter(geo_writer);
    }
  } else {
    // Defensive: SwitchColumnImpl only calls this for Kinds handled above.
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "TypeKind ",
              duckdb::EnumUtil::ToString(Kind),
              " is not supported in search index");
  }
  _field.name = _name_buffer;

  if (_emit_pk) {
    // TODO(Dronplane): if pk contains only one column and that column is also
    // indexed we can avoid indexing this column twice. But then we need to get
    // here info about this case and also search source should be ready to
    // handle that.
    _current_writer = [&, data_writer = std::move(_current_writer)](
                        std::string_view full_key,
                        std::span<const rocksdb::Slice> cell_slices) {
      auto row_key = key_utils::ExtractRowKey(full_key);
      _pk_field.SetStringValue(row_key);
      const bool r = _document->Insert(_pk_field);
      if (!r) {
        SDB_THROW(ERROR_INTERNAL,
                  "Failed to insert PK field into IResearch document");
      }
      AppendPerRowPrimaryKey(row_key);
      data_writer(full_key, cell_slices);
    };
    _emit_pk = false;
  }
}

void SearchSinkInsertBaseImpl::JsonExpressionFields::InitForExpression(
  irs::field_id field_id, catalog::ColumnTokenizer string_analyzer) {
  std::string prefix;
  MakeFieldName(field_id, prefix);

  tokenizer_column = string_analyzer.tokenizer_column;
  string_name = prefix;
  search::mangling::MangleString(string_name);
  string_field.PrepareForStringValue(std::move(string_analyzer));
  string_field.name = string_name;

  numeric_name = prefix;
  search::mangling::MangleNumeric(numeric_name);
  numeric_field.PrepareForNumericValue();
  numeric_field.name = numeric_name;

  bool_name = prefix;
  search::mangling::MangleBool(bool_name);
  bool_field.PrepareForBooleanValue();
  bool_field.name = bool_name;

  null_name = std::move(prefix);
  search::mangling::MangleNull(null_name);
  null_field.PrepareForNullValue();
  null_field.name = null_name;
}

void SearchSinkInsertBaseImpl::SetupJsonExpressionWriter(
  irs::field_id field_id, catalog::ColumnTokenizer string_analyzer) {
  _json_fields.clear();
  _json_fields.emplace_back().InitForExpression(field_id,
                                                std::move(string_analyzer));
  for (const auto& jpf : _json_fields) {
    if (jpf.tokenizer_column) {
      EnsurePerRowBlobWriter(*jpf.tokenizer_column);
    }
  }

  _current_writer = [this](std::string_view /*full_key*/,
                           std::span<const rocksdb::Slice> cell_slices) {
    if (cell_slices.size() == 1 && cell_slices.front().empty()) {
      for (const auto& jpf : _json_fields) {
        if (jpf.tokenizer_column) {
          AppendPerRowBlobNull(*jpf.tokenizer_column);
        }
      }
      return;
    }
    std::string_view json_str;
    if (cell_slices.size() == 1) {
      auto s = cell_slices.front();
      if (!s.starts_with(kStringPrefix)) {
        json_str = {s.data(), s.size()};
      } else {
        json_str = {s.data() + 1, s.size() - 1};
      }
    } else {
      SDB_ASSERT(cell_slices.size() == 2);
      json_str = {cell_slices[1].data(), cell_slices[1].size()};
    }
    if (json_str.empty()) {
      return;
    }

    _json_buffer.assign(json_str);
    _json_buffer.append(simdjson::SIMDJSON_PADDING, '\0');
    simdjson::padded_string_view padded_view{
      _json_buffer.data(), json_str.size(), _json_buffer.size()};

    auto insert_field = [this](Field& field) {
      const bool ok = _document->Insert(&field);
      if (!ok) {
        SDB_THROW(ERROR_INTERNAL,
                  "Failed to insert JSON expression field into IResearch "
                  "document");
      }
    };

    auto& jpf = _json_fields.front();
    bool wrote_string_blob = false;

    simdjson::ondemand::document doc;
    auto res = _json_parser.iterate(padded_view).get(doc);
    SDB_ASSERT(res == simdjson::SUCCESS);
    simdjson::ondemand::json_type t{};
    if (doc.type().get(t) == simdjson::SUCCESS) {
      switch (t) {
        case simdjson::ondemand::json_type::string: {
          auto s = doc.get_string();
          if (s.error() == simdjson::SUCCESS) {
            jpf.string_field.SetStringValue(s.value_unsafe());
            insert_field(jpf.string_field);
            if (jpf.tokenizer_column && jpf.string_field.store_attr) {
              AppendPerRowBlob(*jpf.tokenizer_column,
                               jpf.string_field.store_attr->value);
              wrote_string_blob = true;
            }
          }
          break;
        }
        case simdjson::ondemand::json_type::number: {
          double d;
          if (doc.get_double().get(d) == simdjson::SUCCESS) {
            jpf.numeric_field.SetNumericValue(d);
            insert_field(jpf.numeric_field);
          }
          break;
        }
        case simdjson::ondemand::json_type::boolean: {
          bool b;
          if (doc.get_bool().get(b) == simdjson::SUCCESS) {
            jpf.bool_field.SetBooleanValue(b);
            insert_field(jpf.bool_field);
          }
          break;
        }
        case simdjson::ondemand::json_type::null: {
          jpf.null_field.SetNullValue();
          insert_field(jpf.null_field);
          break;
        }
        case simdjson::ondemand::json_type::object:
        case simdjson::ondemand::json_type::array:
          SDB_THROW(ERROR_BAD_PARAMETER,
                    "JSON expression indexed by an inverted index must point "
                    "to a primitive (string/number/boolean/null) leaf; got an "
                    "object or array");
        default:
          break;
      }
    }
    if (jpf.tokenizer_column && !wrote_string_blob) {
      AppendPerRowBlobNull(*jpf.tokenizer_column);
    }
  };
}

template<typename WriteFunc>
SearchSinkInsertBaseImpl::Writer SearchSinkInsertBaseImpl::MakeIndexWriter(
  WriteFunc&& write_func) {
  return
    [&, func = std::forward<WriteFunc>(write_func)](
      std::string_view full_key, std::span<const rocksdb::Slice> cell_slices) {
      auto& field = func(full_key, cell_slices, _field);
      if (!_document->Insert(&field)) {
        SDB_THROW(ERROR_INTERNAL,
                  "Failed to insert field into IResearch document");
      }
    };
}

template<typename WriteFunc>
SearchSinkInsertBaseImpl::Writer SearchSinkInsertBaseImpl::MakeStoreAttrWriter(
  irs::field_id tokenizer_column_id, bool have_nulls, WriteFunc&& write_func) {
  EnsurePerRowBlobWriter(tokenizer_column_id);
  return
    [this, tokenizer_column_id, have_nulls,
     func = std::forward<WriteFunc>(write_func)](
      std::string_view full_key, std::span<const rocksdb::Slice> cell_slices) {
      const bool is_null =
        have_nulls && cell_slices.size() == 1 && cell_slices.front().empty();
      Field* field = is_null ? (_null_field.SetNullValue(), &_null_field)
                             : &func(full_key, cell_slices, _field);
      if (!_document->Insert(field)) {
        SDB_THROW(ERROR_INTERNAL,
                  "Failed to insert field into IResearch document");
      }
      if (is_null || !field->store_attr) {
        AppendPerRowBlobNull(tokenizer_column_id);
      } else {
        AppendPerRowBlob(tokenizer_column_id, field->store_attr->value);
      }
    };
}

void SearchSinkInsertBaseImpl::InitImpl(size_t batch_size) {
  SDB_ASSERT(batch_size > 0);
  if (_document) {
    _document.reset();
  }
  _document.emplace(_trx.Insert(false, batch_size));
  _emit_pk = true;
}

SearchSinkInsertBaseImpl::Field& SearchSinkInsertBaseImpl::WriteStringValue(
  std::string_view, std::span<const rocksdb::Slice> cell_slices,
  SearchSinkInsertBaseImpl::Field& field) {
  SDB_ASSERT(!cell_slices.empty());
  // if string is prefixed during Insert - two slices will be present
  // one is prefix, second is actual string data
  // But if we are re-indexing from existing data (Update operation) - only one
  // slice will be present
  SDB_ASSERT(cell_slices.size() <= 2);
  if (!cell_slices.front().starts_with(kStringPrefix)) {
    field.SetStringValue(
      {cell_slices.front().data(), cell_slices.front().size()});
  } else {
    if (cell_slices.size() == 1) {
      // re-indexing case
      field.SetStringValue(
        {cell_slices.front().data() + 1, cell_slices.front().size() - 1});
    } else {
      field.SetStringValue({cell_slices[1].data(), cell_slices[1].size()});
    }
  }
  return field;
}

template<typename T>
SearchSinkInsertBaseImpl::Field& SearchSinkInsertBaseImpl::WriteNumericValue(
  std::string_view, std::span<const rocksdb::Slice> cell_slices,
  SearchSinkInsertBaseImpl::Field& field) {
  SDB_ASSERT(cell_slices.size() == 1);
  SDB_ASSERT(sizeof(T) == cell_slices[0].size());
  // this is true as long as we match machine ending with storage ending
  static_assert(basics::IsLittleEndian());
  if constexpr (sizeof(T) == 1) {
    static_assert(std::is_integral_v<T>);
    // absl::little_endian has no Load<int8_t>; a single byte has no endianness.
    // NumericTokenizer has no int8 overload, so widen to int32 via T to
    // preserve sign.
    field.SetNumericValue(
      static_cast<int32_t>(static_cast<T>(cell_slices[0].data()[0])));
  } else if constexpr (sizeof(T) == 2) {
    static_assert(std::is_integral_v<T>);
    // NumericTokenizer has no int16 overload, so widen to int32.
    // Load16 returns uint16_t; cast through T to preserve sign.
    field.SetNumericValue(static_cast<int32_t>(
      static_cast<T>(absl::little_endian::Load16(cell_slices[0].data()))));
  } else {
    field.SetNumericValue(absl::little_endian::Load<T>(cell_slices[0].data()));
  }
  return field;
}

SearchSinkInsertBaseImpl::Field& SearchSinkInsertBaseImpl::WriteBooleanValue(
  std::string_view, std::span<const rocksdb::Slice> cell_slices,
  SearchSinkInsertBaseImpl::Field& field) {
  SDB_ASSERT(cell_slices.size() == 1);
  SDB_ASSERT(cell_slices[0].size() == 1);
  field.SetBooleanValue(cell_slices.front() == kTrueValue);
  return field;
}

void SearchSinkInsertBaseImpl::Field::PrepareForVerbatimStringValue() {
  string_analyzer.reset();
  index_features = irs::IndexFeatures::None;
  analyzer = gStringStreamPool.emplace(search::AnalyzerImpl::StringStreamTag{});
  own_store.value = {};
  store_attr = &own_store;
}

void SearchSinkInsertBaseImpl::Field::PrepareForStringValue(
  catalog::ColumnTokenizer&& column_analyzer) {
  index_features = column_analyzer.features;
  SDB_ASSERT(column_analyzer.analyzer);
  analyzer.reset();
  string_analyzer = std::move(column_analyzer.analyzer);
  store_attr = irs::get<irs::StoreAttr>(*string_analyzer);
}

void SearchSinkInsertBaseImpl::Field::SetStringValue(std::string_view value) {
  SDB_ASSERT(analyzer || string_analyzer);
  SDB_ASSERT(!analyzer || !string_analyzer);
  if (analyzer) {
    auto& sstream = basics::downCast<irs::StringTokenizer>(*analyzer);
    sstream.reset(value);
  } else {
    string_analyzer->reset(value);
  }
}

void SearchSinkInsertBaseImpl::Field::PrepareForNumericValue() {
  string_analyzer.reset();
  index_features = irs::IndexFeatures::None;
  analyzer = gNumberStreamPool.emplace(search::AnalyzerImpl::NumberStreamTag{});
}

template<typename T>
void SearchSinkInsertBaseImpl::Field::SetNumericValue(T value) {
  // Only the four types NumericTokenizer::reset() accepts natively reach here.
  // TINYINT/SMALLINT slices are widened to int32_t inside WriteNumericValue;
  // HUGEINT is rejected at SwitchColumnImpl.
  auto& nstream = basics::downCast<irs::NumericTokenizer>(*analyzer);
  if constexpr (std::is_same_v<T, float>) {
#ifdef FLOAT_T_IS_DOUBLE_T
    // On builds where float_t aliases double, NumericTokenizer has no
    // reset(float) overload. Widen so indexed FLOAT columns still work.
    nstream.reset(static_cast<double>(value));
#else
    nstream.reset(value);
#endif
  } else {
    nstream.reset(value);
  }
}

void SearchSinkInsertBaseImpl::Field::PrepareForBooleanValue() {
  string_analyzer.reset();
  index_features = irs::IndexFeatures::None;
  analyzer = gBoolStreamPool.emplace(search::AnalyzerImpl::BoolStreamTag{});
}

void SearchSinkInsertBaseImpl::Field::SetBooleanValue(bool value) {
  auto& bstream = basics::downCast<irs::BooleanTokenizer>(*analyzer);
  bstream.reset(value);
}

void SearchSinkInsertBaseImpl::Field::PrepareForNullValue() {
  string_analyzer.reset();
  index_features = irs::IndexFeatures::None;
  analyzer = gNullStreamPool.emplace(search::AnalyzerImpl::NullStreamTag{});
}

void SearchSinkInsertBaseImpl::Field::SetNullValue() {
  auto& nstream = basics::downCast<irs::NullTokenizer>(*analyzer);
  nstream.reset();
}

SearchSinkDeleteBaseImpl::SearchSinkDeleteBaseImpl(
  irs::IndexWriter::Transaction& trx)
  : _trx{trx} {}

void SearchSinkDeleteBaseImpl::DeleteRowImpl(std::string_view row_key) {
  SDB_ASSERT(_remove_filter);
  _remove_filter->Add(row_key);
}

void SearchSinkDeleteBaseImpl::InitImpl(size_t batch_size) {
  SDB_ASSERT(batch_size > 0);
  FinishImpl();
  SDB_ASSERT(!_remove_filter);
  _remove_filter = std::make_shared<SearchRemoveFilter>(batch_size);
}

void SearchSinkDeleteBaseImpl::FinishImpl() {
  if (_remove_filter && !_remove_filter->Empty()) {
    _trx.Remove(std::move(_remove_filter));
  }
  _remove_filter.reset();
}

}  // namespace sdb::connector
