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

#include <simdjson.h>

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

std::string_view ExtractRawString(std::span<const rocksdb::Slice> cell_slices) {
  SDB_ASSERT(!cell_slices.empty() && cell_slices.size() <= 2);
  const auto& first = cell_slices.front();
  if (!first.starts_with(kStringPrefix)) {
    return {first.data(), first.size()};
  }
  if (cell_slices.size() == 1) {
    return {first.data() + 1, first.size() - 1};
  }
  return {cell_slices[1].data(), cell_slices[1].size()};
}

}  // namespace

SearchSinkInsertBaseImpl::SearchSinkInsertBaseImpl(
  irs::IndexWriter::Transaction& trx,
  ColumnTokenizerProvider&& tokenizer_provider,
  std::span<const catalog::Column::Id> columns,
  ExpressionTokenizerProvider&& subexpr_tokenizer_provider,
  std::vector<IndexedExpression>&& subexprs)
  : ColumnSinkWriterImplBase{columns},
    _tokenizer_provider{std::move(tokenizer_provider)},
    _subexpr_tokenizer_provider{std::move(subexpr_tokenizer_provider)},
    _trx{trx},
    _indexed_expressions{std::move(subexprs)} {
  _pk_field.PrepareForVerbatimStringValue();
  _pk_field.name = kPkFieldName;

  SDB_ASSERT(absl::c_all_of(_indexed_expressions, [](const auto& e) {
    return static_cast<bool>(e.normalized_expr);
  }));
}

bool SearchSinkInsertBaseImpl::SetupTypedWriter(catalog::Column::Id column_id,
                                                const duckdb::LogicalType& type,
                                                bool have_nulls,
                                                std::string_view name_suffix) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::SQLNULL:
      SetupColumnWriter<duckdb::LogicalTypeId::SQLNULL>(column_id, false,
                                                        name_suffix);
      return true;
    case duckdb::LogicalTypeId::VARCHAR:
      SetupColumnWriter<duckdb::LogicalTypeId::VARCHAR>(column_id, have_nulls,
                                                        name_suffix);
      return true;
    case duckdb::LogicalTypeId::BLOB:
      SetupColumnWriter<duckdb::LogicalTypeId::BLOB>(column_id, have_nulls,
                                                     name_suffix);
      return true;
    case duckdb::LogicalTypeId::BOOLEAN:
      SetupColumnWriter<duckdb::LogicalTypeId::BOOLEAN>(column_id, have_nulls,
                                                        name_suffix);
      return true;
    case duckdb::LogicalTypeId::TINYINT:
      SetupColumnWriter<duckdb::LogicalTypeId::TINYINT>(column_id, have_nulls,
                                                        name_suffix);
      return true;
    case duckdb::LogicalTypeId::SMALLINT:
      SetupColumnWriter<duckdb::LogicalTypeId::SMALLINT>(column_id, have_nulls,
                                                         name_suffix);
      return true;
    case duckdb::LogicalTypeId::INTEGER:
      SetupColumnWriter<duckdb::LogicalTypeId::INTEGER>(column_id, have_nulls,
                                                        name_suffix);
      return true;
    case duckdb::LogicalTypeId::BIGINT:
      SetupColumnWriter<duckdb::LogicalTypeId::BIGINT>(column_id, have_nulls,
                                                       name_suffix);
      return true;
    case duckdb::LogicalTypeId::FLOAT:
      SetupColumnWriter<duckdb::LogicalTypeId::FLOAT>(column_id, have_nulls,
                                                      name_suffix);
      return true;
    case duckdb::LogicalTypeId::DOUBLE:
      SetupColumnWriter<duckdb::LogicalTypeId::DOUBLE>(column_id, have_nulls,
                                                       name_suffix);
      return true;
    case duckdb::LogicalTypeId::DATE:
      SetupColumnWriter<duckdb::LogicalTypeId::DATE>(column_id, have_nulls,
                                                     name_suffix);
      return true;
    case duckdb::LogicalTypeId::TIMESTAMP:
      SetupColumnWriter<duckdb::LogicalTypeId::TIMESTAMP>(column_id, have_nulls,
                                                          name_suffix);
      return true;
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      SetupColumnWriter<duckdb::LogicalTypeId::TIMESTAMP_TZ>(
        column_id, have_nulls, name_suffix);
      return true;
    case duckdb::LogicalTypeId::ARRAY:
      SetupColumnWriter<duckdb::LogicalTypeId::ARRAY>(column_id, have_nulls,
                                                      name_suffix);
      return true;
    case duckdb::LogicalTypeId::GEOMETRY:
      SetupColumnWriter<duckdb::LogicalTypeId::GEOMETRY>(column_id, have_nulls,
                                                         name_suffix);
      return true;
    default:
      return false;
  }
}

bool SearchSinkInsertBaseImpl::SwitchColumnImpl(const ColumnDescriptor& col) {
  const auto column_id = col.id;
  if (!IsIndexed(column_id)) {
#ifdef SDB_DEV
    _current_writer = nullptr;
#endif
    return false;
  }
  if (!SetupTypedWriter(column_id, col.type, col.have_nulls, {})) {
    _current_writer = nullptr;
    return false;
  }
  SDB_ASSERT(_document.has_value());
  _document->NextFieldBatch();
  return true;
}

bool SearchSinkInsertBaseImpl::SwitchExpressionImpl(
  const ExpressionDescriptor& expr_desc) {
  const auto& return_type = expr_desc.type;
  const auto have_nulls = expr_desc.have_nulls;
  const auto serialized_expr = expr_desc.serialized_expr;
  // column_id is unused on the expression path: SetupColumnWriter routes
  // through MakeExpressionFieldName + _subexpr_tokenizer_provider whenever
  // name_suffix (serialized_expr) is non-empty.
  constexpr catalog::Column::Id kUnusedColumnId{};
  if (return_type.IsJSONType()) {
    SetupColumnWriter<duckdb::LogicalTypeId::VARCHAR>(
      kUnusedColumnId, have_nulls, serialized_expr);
    SetupExprAuxTypedFields(kUnusedColumnId, serialized_expr);
  } else if (!SetupTypedWriter(kUnusedColumnId, return_type, have_nulls,
                               serialized_expr)) {
    _current_writer = nullptr;
    return false;
  }
  SDB_ASSERT(_document.has_value());
  _document->NextFieldBatch();
  return true;
}

void SearchSinkInsertBaseImpl::SetupExprAuxTypedFields(
  catalog::Column::Id column_id, std::string_view name_suffix) {
  // Build the numeric-mangled field name and analyzer.
  MakeExpressionFieldName(name_suffix, _aux_numeric_name_buffer);
  search::mangling::MangleNumeric(_aux_numeric_name_buffer);
  _aux_numeric_field.PrepareForNumericValue();
  _aux_numeric_field.name = _aux_numeric_name_buffer;
  _aux_numeric_field.store_attr = nullptr;

  // Build the bool-mangled field name and analyzer.
  MakeExpressionFieldName(name_suffix, _aux_bool_name_buffer);
  search::mangling::MangleBool(_aux_bool_name_buffer);
  _aux_bool_field.PrepareForBooleanValue();
  _aux_bool_field.name = _aux_bool_name_buffer;
  _aux_bool_field.store_attr = nullptr;

  _current_writer = [this, string_writer = std::move(_current_writer)](
                      std::string_view full_key,
                      std::span<const rocksdb::Slice> cell_slices) {
    if (cell_slices.empty() ||
        (cell_slices.size() == 1 && cell_slices.front().empty())) {
      string_writer(full_key, cell_slices);
      return;
    }
    auto raw = ExtractRawString(cell_slices);
    if (raw.empty()) {
      string_writer(full_key, cell_slices);
      return;
    }
    // DOM: ondemand mis-classifies top-level scalars from the first byte.
    simdjson::dom::parser dom_parser;
    simdjson::dom::element doc;
    if (dom_parser.parse(raw.data(), raw.size()).get(doc) !=
        simdjson::SUCCESS) {
      SDB_THROW(ERROR_INTERNAL,
                "Invalid JSON leaf for indexed expression: ", raw);
    }
    if (doc.is_number()) {
      _aux_numeric_field.SetNumericValue(double(doc.get_double()));
      const bool ok =
        _document->template Insert<irs::Action::INDEX>(&_aux_numeric_field);
      if (!ok) {
        SDB_THROW(ERROR_INTERNAL,
                  "Failed to insert expression numeric variant into "
                  "IResearch document");
      }
      return;
    }
    if (doc.is_bool()) {
      _aux_bool_field.SetBooleanValue(bool(doc.get_bool()));
      const bool ok =
        _document->template Insert<irs::Action::INDEX>(&_aux_bool_field);
      if (!ok) {
        SDB_THROW(ERROR_INTERNAL,
                  "Failed to insert expression bool variant into "
                  "IResearch document");
      }
      return;
    }
    string_writer(full_key, cell_slices);
  };
}

void SearchSinkInsertBaseImpl::WriteImpl(
  std::span<const rocksdb::Slice> cell_slices, std::string_view full_key) {
  SDB_ASSERT(_current_writer);
  SDB_ASSERT(_document.has_value());
  _current_writer(full_key, cell_slices);
  _document->NextDocument();
}

void SearchSinkInsertBaseImpl::FinishImpl() { _document.reset(); }

template<duckdb::LogicalTypeId Kind>
void SearchSinkInsertBaseImpl::SetupColumnWriter(catalog::Column::Id column_id,
                                                 bool have_nulls,
                                                 std::string_view name_suffix) {
  if (name_suffix.empty()) {
    MakeColumnFieldName(column_id, _name_buffer);
  } else {
    MakeExpressionFieldName(name_suffix, _name_buffer);
  }

  if (have_nulls || Kind == duckdb::LogicalTypeId::SQLNULL) {
    if (name_suffix.empty()) {
      MakeColumnFieldName(column_id, _null_name_buffer);
    } else {
      MakeExpressionFieldName(name_suffix, _null_name_buffer);
    }
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
          // Two shapes mean "null": size 1 with empty front (WriteFlatColumn
          // path) and size 0 (WriteUnifiedColumn / WriteConstantColumn path
          // for null rows).
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
      [&](std::string_view full_key,
          std::span<const rocksdb::Slice> cell_slices, Field&) -> Field& {
        SDB_ASSERT(cell_slices.size() == 1);
        SDB_ASSERT(cell_slices.front().empty());
        _null_field.SetNullValue();
        return _null_field;
      });
  } else if constexpr (Kind == duckdb::LogicalTypeId::VARCHAR ||
                       Kind == duckdb::LogicalTypeId::BLOB) {
    search::mangling::MangleString(_name_buffer);
    // A non-empty `name_suffix` indicates an expression field
    auto tokenizer = name_suffix.empty()
                       ? _tokenizer_provider(column_id)
                       : _subexpr_tokenizer_provider(name_suffix);
    _field.PrepareForStringValue(std::move(tokenizer));
    const bool has_store = _field.store_attr != nullptr;
    if (has_store) {
      _current_writer =
        have_nulls
          ? MakeIndexStoreWriter(make_nullable_writer_func(&WriteStringValue))
          : MakeIndexStoreWriter(&WriteStringValue);
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
    // HNSW vector field: non-null rows STORE raw float32 bytes (no inverted
    // index); null rows fall through to INDEX-only via the null-stream
    // analyzer so IS NULL queries still work.
    // No name mangling: the field name is the raw big-endian column ID,
    // matching the column_info key registered in InvertedIndexShard.
    _field.PrepareForVectorValue();
    if (have_nulls) {
      _current_writer =
        MakeStoreWriter(make_nullable_writer_func(&WriteVectorValue));
    } else {
      _current_writer = MakeStoreWriter(&WriteVectorValue);
    }
  } else if constexpr (Kind == duckdb::LogicalTypeId::GEOMETRY) {
    // GEOMETRY column: WKB bytes arrive in cell_slices and go straight to
    // the geo analyzer via resetWKB. The analyzer parses internally, which
    // lets future LatLng-coding work fuse the WKB read with the encoder
    // write without changing this call site.
    search::mangling::MangleString(_name_buffer);
    _field.PrepareForStringValue(_tokenizer_provider(column_id));
    const bool has_store = _field.store_attr != nullptr;
    auto geo_writer = [](std::string_view,
                         std::span<const rocksdb::Slice> cell_slices,
                         Field& field) -> Field& {
      SDB_ASSERT(!cell_slices.empty());
      // Raw WKB: last slice (row-prefix serialization may prepend other
      // slices, so use back()).
      const auto& slice = cell_slices.back();
      const irs::bytes_view wkb{
        reinterpret_cast<const irs::byte_type*>(slice.data()), slice.size()};
      auto& geo =
        basics::downCast<irs::analysis::GeoAnalyzer>(*field.string_analyzer);
      // Parse failure is treated silently (option A from the port plan): the
      // analyzer keeps whatever state it had, which at worst means this row
      // contributes no new terms. Matches the VARCHAR path behavior on bad
      // input.
      (void)geo.resetWKB(wkb);
      return field;
    };
    if (has_store) {
      _current_writer =
        have_nulls ? MakeIndexStoreWriter(make_nullable_writer_func(geo_writer))
                   : MakeIndexStoreWriter(geo_writer);
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
      _pk_field.own_store.value = irs::ViewCast<irs::byte_type>(row_key);
      _pk_field.SetStringValue(row_key);
      // We need indexed PK for removes
      const bool r =
        _document->template Insert<irs::Action::INDEX | irs::Action::STORE>(
          _pk_field);
      if (!r) {
        SDB_THROW(ERROR_INTERNAL,
                  "Failed to insert PK field into IResearch document");
      }
      data_writer(full_key, cell_slices);
    };
    _emit_pk = false;
  }
}

template<bool HasStore, irs::Action NonNullAction, typename WriteFunc>
SearchSinkInsertBaseImpl::Writer SearchSinkInsertBaseImpl::MakeWriterImpl(
  WriteFunc&& write_func) {
  return
    [&, func = std::forward<WriteFunc>(write_func)](
      std::string_view full_key, std::span<const rocksdb::Slice> cell_slices) {
      auto& field = func(full_key, cell_slices, _field);
      bool r;
      if constexpr (HasStore) {
        // Force INDEX only for NULLs so
        // IS NULL queries find them via the null-stream tokens.
        r = field.store_attr
              ? _document->template Insert<NonNullAction>(&field)
              : _document->template Insert<irs::Action::INDEX>(&field);
      } else {
        // Column never stores -- skip the per-row check.
        static_assert(NonNullAction == irs::Action::INDEX,
                      "No-store action should be only INDEX");
        r = _document->template Insert<NonNullAction>(&field);
      }
      if (!r) {
        SDB_THROW(ERROR_INTERNAL,
                  "Failed to insert field into IResearch document");
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

SearchSinkInsertBaseImpl::Field& SearchSinkInsertBaseImpl::WriteVectorValue(
  std::string_view full_key, std::span<const rocksdb::Slice> cell_slices,
  SearchSinkInsertBaseImpl::Field& field) {
  // _row_slices layout from WriteFlatSubVector<float>:
  //   [0] header  (varint(count) + ValueFlags)
  //   [1] null bitmap  (only when have_nulls)
  //   [last] raw float data  (count * sizeof(float) bytes)
  // Always use the last slice to get the actual float bytes.
  SDB_ASSERT(!cell_slices.empty());
  field.own_store.value = irs::bytes_view{
    reinterpret_cast<const irs::byte_type*>(cell_slices.back().data()),
    cell_slices.back().size()};
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

void SearchSinkInsertBaseImpl::Field::PrepareForVectorValue() {
  // HNSW vector fields are stored via Action::STORE only (no inverted index).
  // No tokenizer is needed: Action::STORE does not call GetTokens().
  string_analyzer.reset();
  analyzer.reset();
  index_features = irs::IndexFeatures::None;
  own_store.value = {};
  store_attr = &own_store;
}

void SearchSinkInsertBaseImpl::Field::PrepareForVerbatimStringValue() {
  string_analyzer.reset();
  index_features = irs::IndexFeatures::None;
  analyzer = gStringStreamPool.emplace(search::AnalyzerImpl::StringStreamTag{});
  own_store.value = {};
  store_attr = &own_store;
}

void SearchSinkInsertBaseImpl::Field::PrepareForStringValue(
  catalog::FieldTokenizer&& column_analyzer) {
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
