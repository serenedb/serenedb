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

#include <cstdio>
#include <duckdb/common/enum_util.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/analysis/tokenizers.hpp>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "catalog/table_options.h"
#include "connector/common.h"
#include "connector/key_utils.hpp"
#include "search_remove_filter.hpp"

namespace sdb::connector {
namespace {

constexpr size_t kDefaultPoolSize = 8;  // arbitrary value

using StreamPool = irs::UnboundedObjectPool<search::AnalyzerImpl::Builder>;

}  // namespace

SearchSinkInsertBaseImpl::SearchSinkInsertBaseImpl(
  irs::IndexWriter::Transaction& trx, TokenizerProvider&& tokenizer_provider,
  EntryInfoProvider&& entry_info_provider,
  std::span<const catalog::Column::Id> columns,
  std::vector<IndexedExpression>&& indexed_exprs)
  : ColumnSinkWriterImplBase{columns},
    _tokenizer_provider{std::move(tokenizer_provider)},
    _entry_info_provider{std::move(entry_info_provider)},
    _indexed_expressions{std::move(indexed_exprs)},
    _trx{trx} {
  _pk_field.PrepareForVerbatimStringValue();
  _pk_field.id = catalog::term_dict::kPKFieldId;
}

template<duckdb::LogicalTypeId Kind>
void SearchSinkInsertBaseImpl::SetFieldValueFromVector(
  Field& field, const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t idx) {
  if constexpr (Kind == duckdb::LogicalTypeId::VARCHAR ||
                Kind == duckdb::LogicalTypeId::BLOB) {
    const auto& s =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt)[idx];
    field.SetStringValue({s.GetData(), s.GetSize()});
  } else if constexpr (Kind == duckdb::LogicalTypeId::GEOMETRY) {
    const auto& s =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt)[idx];
    const irs::bytes_view wkb{
      reinterpret_cast<const irs::byte_type*>(s.GetData()), s.GetSize()};
    auto& geo =
      basics::downCast<irs::analysis::GeoAnalyzer>(*field.string_analyzer);
    std::ignore = geo.resetWKB(wkb);
  } else if constexpr (Kind == duckdb::LogicalTypeId::BOOLEAN) {
    field.SetBooleanValue(duckdb::UnifiedVectorFormat::GetData<bool>(fmt)[idx]);
  } else if constexpr (Kind == duckdb::LogicalTypeId::TINYINT) {
    field.SetNumericValue(static_cast<int32_t>(
      duckdb::UnifiedVectorFormat::GetData<int8_t>(fmt)[idx]));
  } else if constexpr (Kind == duckdb::LogicalTypeId::SMALLINT) {
    field.SetNumericValue(static_cast<int32_t>(
      duckdb::UnifiedVectorFormat::GetData<int16_t>(fmt)[idx]));
  } else if constexpr (Kind == duckdb::LogicalTypeId::INTEGER ||
                       Kind == duckdb::LogicalTypeId::DATE) {
    field.SetNumericValue(
      duckdb::UnifiedVectorFormat::GetData<int32_t>(fmt)[idx]);
  } else if constexpr (Kind == duckdb::LogicalTypeId::BIGINT ||
                       Kind == duckdb::LogicalTypeId::TIME ||
                       Kind == duckdb::LogicalTypeId::TIME_TZ ||
                       Kind == duckdb::LogicalTypeId::TIME_NS ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_TZ ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_SEC ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_MS ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_NS ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_TZ_NS) {
    field.SetNumericValue(
      duckdb::UnifiedVectorFormat::GetData<int64_t>(fmt)[idx]);
  } else if constexpr (Kind == duckdb::LogicalTypeId::UTINYINT) {
    field.SetNumericValue(static_cast<int32_t>(
      duckdb::UnifiedVectorFormat::GetData<uint8_t>(fmt)[idx]));
  } else if constexpr (Kind == duckdb::LogicalTypeId::USMALLINT) {
    field.SetNumericValue(static_cast<int32_t>(
      duckdb::UnifiedVectorFormat::GetData<uint16_t>(fmt)[idx]));
  } else if constexpr (Kind == duckdb::LogicalTypeId::UINTEGER) {
    field.SetNumericValue(
      duckdb::UnifiedVectorFormat::GetData<uint32_t>(fmt)[idx]);
  } else if constexpr (Kind == duckdb::LogicalTypeId::FLOAT) {
    field.SetNumericValue(
      duckdb::UnifiedVectorFormat::GetData<float>(fmt)[idx]);
  } else {
    static_assert(Kind == duckdb::LogicalTypeId::DOUBLE,
                  "SetFieldValueFromVector: unsupported Kind");
    field.SetNumericValue(
      duckdb::UnifiedVectorFormat::GetData<double>(fmt)[idx]);
  }
}

void SearchSinkInsertBaseImpl::MaybeEmitPk(std::string_view full_row_key) {
  if (!_pk_blob_writer) {
    return;
  }
  const auto row_key = key_utils::ExtractRowKey(full_row_key);
  _pk_field.SetStringValue(row_key);
  if (!_document->Insert(&_pk_field)) {
    SDB_THROW(ERROR_INTERNAL,
              "Failed to insert PK field into IResearch document");
  }
  AppendPkBlob(row_key);
}

void SearchSinkInsertBaseImpl::EmitField(Field* field_to_insert,
                                         std::string_view full_row_key) {
  MaybeEmitPk(full_row_key);
  if (!_document->Insert(field_to_insert)) {
    SDB_THROW(ERROR_INTERNAL, "Failed to insert field into IResearch document");
  }
}

template<duckdb::LogicalTypeId Kind>
void SearchSinkInsertBaseImpl::WriteScalarBatch(
  std::span<const std::string_view> row_keys, duckdb::idx_t count,
  irs::field_id tokenizer_column) {
  auto& fmt = _vec_fmt.unified;

  SDB_ASSERT(_document);
  _document->NextFieldBatch();

  for (duckdb::idx_t i = 0; i < count; ++i) {
    if (i < row_keys.size() && row_keys[i].empty()) {
      continue;  // upstream conflict-resolved row
    }
    const auto full_row_key =
      i < row_keys.size() ? row_keys[i] : std::string_view{};

    if constexpr (Kind == duckdb::LogicalTypeId::SQLNULL) {
      _null_field.SetNullValue();
      EmitField(&_null_field, full_row_key);
    } else {
      const auto sel_idx = fmt.sel->get_index(i);
      if (!fmt.validity.RowIsValid(sel_idx)) {
        _null_field.SetNullValue();
        EmitField(&_null_field, full_row_key);
      } else {
        SetFieldValueFromVector<Kind>(_field, fmt, sel_idx);
        EmitField(&_field, full_row_key);
        if (irs::field_limits::valid(tokenizer_column)) {
          SDB_ASSERT(_field.store_attr);
          AppendPerRowBlob(tokenizer_column, _field.store_attr->value);
        }
      }
    }
    _document->NextDocument();
  }
  _pk_blob_writer = nullptr;
}

template<duckdb::LogicalTypeId ChildKind>
void SearchSinkInsertBaseImpl::WriteListBatch(
  std::span<const std::string_view> row_keys, duckdb::idx_t count,
  duckdb::idx_t array_size) {
  SDB_ASSERT(_document);
  _document->NextFieldBatch();

  const auto& parent_fmt = _vec_fmt.unified;
  const auto& child_fmt = _vec_fmt.children[0].unified;
  for (duckdb::idx_t i = 0; i < count; ++i) {
    if (i < row_keys.size() && row_keys[i].empty()) {
      continue;
    }

    const auto parent_idx = parent_fmt.sel->get_index(i);
    if (!parent_fmt.validity.RowIsValid(parent_idx)) {
      InsertNullValue();
    } else {
      duckdb::idx_t offset;
      duckdb::idx_t length;
      if (array_size == 0) {
        const auto* list_data =
          duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(
            parent_fmt);
        const auto entry = list_data[parent_idx];
        offset = entry.offset;
        length = entry.length;
      } else {
        offset = parent_idx * array_size;
        length = array_size;
      }
      for (duckdb::idx_t k = 0; k < length; ++k) {
        const auto child_flat_idx = offset + k;
        const auto child_idx = child_fmt.sel->get_index(child_flat_idx);
        if (!child_fmt.validity.RowIsValid(child_idx)) {
          InsertNullValue();
          continue;
        }
        SetFieldValueFromVector<ChildKind>(_field, child_fmt, child_idx);
        if (!_document->Insert(&_field)) {
          SDB_THROW(ERROR_INTERNAL,
                    "Failed to insert list element into IResearch document");
        }
      }
    }

    const auto full_row_key =
      i < row_keys.size() ? row_keys[i] : std::string_view{};
    MaybeEmitPk(full_row_key);
    _document->NextDocument();
  }
  _pk_blob_writer = nullptr;
}

void SearchSinkInsertBaseImpl::EmitPkOnlyBatch(
  std::span<const std::string_view> row_keys, duckdb::idx_t count) {
  if (!_pk_blob_writer) {
    return;
  }
  SDB_ASSERT(_document);
  _document->NextFieldBatch();
  for (duckdb::idx_t i = 0; i < count; ++i) {
    if (i < row_keys.size() && row_keys[i].empty()) {
      continue;
    }
    const auto full_row_key =
      i < row_keys.size() ? row_keys[i] : std::string_view{};
    MaybeEmitPk(full_row_key);
    _document->NextDocument();
  }
  _pk_blob_writer = nullptr;
}

bool SearchSinkInsertBaseImpl::DispatchScalarBatch(
  duckdb::LogicalTypeId kind, std::span<const std::string_view> row_keys,
  duckdb::idx_t count, irs::field_id tokenizer_column) {
  using enum duckdb::LogicalTypeId;
  switch (kind) {
    case SQLNULL:
      WriteScalarBatch<SQLNULL>(row_keys, count, tokenizer_column);
      return true;
    case VARCHAR:
      WriteScalarBatch<VARCHAR>(row_keys, count, tokenizer_column);
      return true;
    case BLOB:
      WriteScalarBatch<BLOB>(row_keys, count, tokenizer_column);
      return true;
    case GEOMETRY:
      WriteScalarBatch<GEOMETRY>(row_keys, count, tokenizer_column);
      return true;
    case BOOLEAN:
      WriteScalarBatch<BOOLEAN>(row_keys, count, tokenizer_column);
      return true;
    case TINYINT:
      WriteScalarBatch<TINYINT>(row_keys, count, tokenizer_column);
      return true;
    case SMALLINT:
      WriteScalarBatch<SMALLINT>(row_keys, count, tokenizer_column);
      return true;
    case INTEGER:
      WriteScalarBatch<INTEGER>(row_keys, count, tokenizer_column);
      return true;
    case BIGINT:
      WriteScalarBatch<BIGINT>(row_keys, count, tokenizer_column);
      return true;
    case UTINYINT:
      WriteScalarBatch<UTINYINT>(row_keys, count, tokenizer_column);
      return true;
    case USMALLINT:
      WriteScalarBatch<USMALLINT>(row_keys, count, tokenizer_column);
      return true;
    case UINTEGER:
      WriteScalarBatch<UINTEGER>(row_keys, count, tokenizer_column);
      return true;
    case FLOAT:
      WriteScalarBatch<FLOAT>(row_keys, count, tokenizer_column);
      return true;
    case DOUBLE:
      WriteScalarBatch<DOUBLE>(row_keys, count, tokenizer_column);
      return true;
    case DATE:
      WriteScalarBatch<DATE>(row_keys, count, tokenizer_column);
      return true;
    case TIME:
      WriteScalarBatch<TIME>(row_keys, count, tokenizer_column);
      return true;
    case TIME_TZ:
      WriteScalarBatch<TIME_TZ>(row_keys, count, tokenizer_column);
      return true;
    case TIME_NS:
      WriteScalarBatch<TIME_NS>(row_keys, count, tokenizer_column);
      return true;
    case TIMESTAMP:
      WriteScalarBatch<TIMESTAMP>(row_keys, count, tokenizer_column);
      return true;
    case TIMESTAMP_TZ:
      WriteScalarBatch<TIMESTAMP_TZ>(row_keys, count, tokenizer_column);
      return true;
    case TIMESTAMP_SEC:
      WriteScalarBatch<TIMESTAMP_SEC>(row_keys, count, tokenizer_column);
      return true;
    case TIMESTAMP_MS:
      WriteScalarBatch<TIMESTAMP_MS>(row_keys, count, tokenizer_column);
      return true;
    case TIMESTAMP_NS:
      WriteScalarBatch<TIMESTAMP_NS>(row_keys, count, tokenizer_column);
      return true;
    case TIMESTAMP_TZ_NS:
      WriteScalarBatch<TIMESTAMP_TZ_NS>(row_keys, count, tokenizer_column);
      return true;
    default:
      return false;
  }
}

bool SearchSinkInsertBaseImpl::DispatchListBatch(
  duckdb::LogicalTypeId child_kind, std::span<const std::string_view> row_keys,
  duckdb::idx_t count, duckdb::idx_t array_size) {
  using enum duckdb::LogicalTypeId;
  switch (child_kind) {
    case VARCHAR:
      WriteListBatch<VARCHAR>(row_keys, count, array_size);
      return true;
    case BLOB:
      WriteListBatch<BLOB>(row_keys, count, array_size);
      return true;
    case BOOLEAN:
      WriteListBatch<BOOLEAN>(row_keys, count, array_size);
      return true;
    case TINYINT:
      WriteListBatch<TINYINT>(row_keys, count, array_size);
      return true;
    case SMALLINT:
      WriteListBatch<SMALLINT>(row_keys, count, array_size);
      return true;
    case INTEGER:
      WriteListBatch<INTEGER>(row_keys, count, array_size);
      return true;
    case DATE:
      WriteListBatch<DATE>(row_keys, count, array_size);
      return true;
    case BIGINT:
      WriteListBatch<BIGINT>(row_keys, count, array_size);
      return true;
    case UTINYINT:
      WriteListBatch<UTINYINT>(row_keys, count, array_size);
      return true;
    case USMALLINT:
      WriteListBatch<USMALLINT>(row_keys, count, array_size);
      return true;
    case UINTEGER:
      WriteListBatch<UINTEGER>(row_keys, count, array_size);
      return true;
    case TIME:
      WriteListBatch<TIME>(row_keys, count, array_size);
      return true;
    case TIME_TZ:
      WriteListBatch<TIME_TZ>(row_keys, count, array_size);
      return true;
    case TIME_NS:
      WriteListBatch<TIME_NS>(row_keys, count, array_size);
      return true;
    case TIMESTAMP:
      WriteListBatch<TIMESTAMP>(row_keys, count, array_size);
      return true;
    case TIMESTAMP_TZ:
      WriteListBatch<TIMESTAMP_TZ>(row_keys, count, array_size);
      return true;
    case TIMESTAMP_SEC:
      WriteListBatch<TIMESTAMP_SEC>(row_keys, count, array_size);
      return true;
    case TIMESTAMP_MS:
      WriteListBatch<TIMESTAMP_MS>(row_keys, count, array_size);
      return true;
    case TIMESTAMP_NS:
      WriteListBatch<TIMESTAMP_NS>(row_keys, count, array_size);
      return true;
    case TIMESTAMP_TZ_NS:
      WriteListBatch<TIMESTAMP_TZ_NS>(row_keys, count, array_size);
      return true;
    case FLOAT:
      WriteListBatch<FLOAT>(row_keys, count, array_size);
      return true;
    case DOUBLE:
      WriteListBatch<DOUBLE>(row_keys, count, array_size);
      return true;
    default:
      return false;
  }
}

void SearchSinkInsertBaseImpl::WriteJsonBatch(
  const duckdb::Vector& vec, std::span<const std::string_view> row_keys,
  duckdb::idx_t count) {
  SDB_ASSERT(_document);
  _document->NextFieldBatch();

  auto& fmt = _vec_fmt.unified;
  vec.ToUnifiedFormat(count, fmt);

  auto& jpf = _json_fields;
  const bool has_store = irs::field_limits::valid(jpf.tokenizer_column);

  for (duckdb::idx_t i = 0; i < count; ++i) {
    if (i < row_keys.size() && row_keys[i].empty()) {
      continue;
    }

    const auto sel_idx = fmt.sel->get_index(i);
    const bool is_null = !fmt.validity.RowIsValid(sel_idx);
    bool wrote_string_blob = false;

    if (!is_null) {
      const auto& cell_string =
        duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt)[sel_idx];
      std::string_view json_str{cell_string.GetData(), cell_string.GetSize()};
      if (!json_str.empty() && json_str.front() == kStringPrefix[0]) {
        json_str = json_str.substr(1);
      }
      if (!json_str.empty()) {
        _json_buffer.assign(json_str);
        _json_buffer.append(simdjson::SIMDJSON_PADDING, '\0');
        simdjson::padded_string_view padded_view{
          _json_buffer.data(), json_str.size(), _json_buffer.size()};
        simdjson::ondemand::document doc;
        auto res = _json_parser.iterate(padded_view).get(doc);
        SDB_ASSERT(res == simdjson::SUCCESS);
        simdjson::ondemand::json_type t{};
        if (doc.type().get(t) == simdjson::SUCCESS) {
          auto insert_field = [this](Field& field) {
            if (!_document->Insert(&field)) {
              SDB_THROW(ERROR_INTERNAL,
                        "Failed to insert JSON expression field into IResearch "
                        "document");
            }
          };
          switch (t) {
            case simdjson::ondemand::json_type::string: {
              auto s = doc.get_string();
              if (s.error() == simdjson::SUCCESS) {
                jpf.string_field.SetStringValue(s.value_unsafe());
                insert_field(jpf.string_field);
                if (has_store && jpf.string_field.store_attr) {
                  AppendPerRowBlob(jpf.tokenizer_column,
                                   jpf.string_field.store_attr->value);
                  wrote_string_blob = true;
                }
              }
            } break;
            case simdjson::ondemand::json_type::number: {
              double d;
              if (doc.get_double().get(d) == simdjson::SUCCESS) {
                jpf.numeric_field.SetNumericValue(d);
                insert_field(jpf.numeric_field);
              }
            } break;
            case simdjson::ondemand::json_type::boolean: {
              bool b;
              if (doc.get_bool().get(b) == simdjson::SUCCESS) {
                jpf.bool_field.SetBooleanValue(b);
                insert_field(jpf.bool_field);
              }
            } break;
            case simdjson::ondemand::json_type::null: {
              jpf.null_field.SetNullValue();
              insert_field(jpf.null_field);
            } break;
            case simdjson::ondemand::json_type::object:
            case simdjson::ondemand::json_type::array:
              SDB_THROW(
                ERROR_BAD_PARAMETER,
                "JSON expression indexed by an inverted index must point to a "
                "primitive (string/number/boolean/null) leaf; got an object or "
                "array");
            default:
              break;
          }
        }
      }
    }
    if (has_store && !wrote_string_blob) {
      AppendPerRowBlob(jpf.tokenizer_column, irs::bytes_view{});
    }

    const auto full_row_key =
      i < row_keys.size() ? row_keys[i] : std::string_view{};
    MaybeEmitPk(full_row_key);
    _document->NextDocument();
  }
  _pk_blob_writer = nullptr;
}

void SearchSinkInsertBaseImpl::SwitchFieldImpl(
  irs::field_id field_id, const duckdb::LogicalType& type,
  const duckdb::Vector& vec, std::span<const std::string_view> row_keys,
  duckdb::idx_t count) {
  SDB_ASSERT(irs::field_limits::valid(field_id));
  const auto* entry = _entry_info_provider(field_id);
  auto resolve_tokenizer = [this, field_id] {
    return _tokenizer_provider(field_id);
  };
  const bool is_term_dict = !entry || entry->IsTermDict();
  const bool is_stored = entry && entry->IsStored();
  const auto kind = type.id();

  if (is_stored && !is_term_dict) {
    AppendToColumn(field_id, type, vec, count);
    EmitPkOnlyBatch(row_keys, count);
    return;
  }
  if (type.IsJSONType() && entry && entry->HasJsonLeafFields()) {
    _json_fields.InitForExpression(field_id, entry, resolve_tokenizer());
    if (irs::field_limits::valid(_json_fields.tokenizer_column)) {
      EnsurePerRowBlobWriter(_json_fields.tokenizer_column);
    }
    if (is_stored) {
      AppendToColumn(field_id, type, vec, count);
    }
    WriteJsonBatch(vec, row_keys, count);
    return;
  }

  const bool is_list_or_array =
    kind == duckdb::LogicalTypeId::LIST || kind == duckdb::LogicalTypeId::ARRAY;
  if (is_list_or_array) {
    _vec_fmt.children.clear();
    duckdb::Vector::RecursiveToUnifiedFormat(vec, count, _vec_fmt);
  } else if (kind != duckdb::LogicalTypeId::SQLNULL) {
    vec.ToUnifiedFormat(count, _vec_fmt.unified);
  }

  const bool may_have_nulls = kind == duckdb::LogicalTypeId::SQLNULL ||
                              is_list_or_array ||
                              _vec_fmt.unified.validity.CanHaveNull();
  if (entry && may_have_nulls &&
      irs::field_limits::valid(entry->null_field_id)) {
    _null_field.id = entry->null_field_id;
    if (!_null_field.analyzer) {
      _null_field.PrepareForNullValue();
    }
  }

  if (is_list_or_array) {
    const auto child_kind = (kind == duckdb::LogicalTypeId::LIST
                               ? duckdb::ListType::GetChildType(type)
                               : duckdb::ArrayType::GetChildType(type))
                              .id();
    const duckdb::idx_t array_size =
      (kind == duckdb::LogicalTypeId::ARRAY ? duckdb::ArrayType::GetSize(type)
                                            : 0);
    if (child_kind == duckdb::LogicalTypeId::VARCHAR ||
        child_kind == duckdb::LogicalTypeId::BLOB) {
      _field.PrepareForStringValue(resolve_tokenizer());
    } else if (child_kind == duckdb::LogicalTypeId::BOOLEAN) {
      _field.PrepareForBooleanValue();
    } else if (catalog::term_dict::IsNumeric(
                 catalog::term_dict::Classify(child_kind))) {
      _field.PrepareForNumericValue();
    } else {
      return;
    }
    _field.id = field_id;
    if (is_stored) {
      AppendToColumn(field_id, type, vec, count);
    }
    DispatchListBatch(child_kind, row_keys, count, array_size);
    return;
  }

  irs::field_id tokenizer_column = irs::field_limits::invalid();
  switch (kind) {
    case duckdb::LogicalTypeId::SQLNULL:
      break;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
    case duckdb::LogicalTypeId::GEOMETRY: {
      auto tokenizer = resolve_tokenizer();
      tokenizer_column = tokenizer.tokenizer_column;
      _field.PrepareForStringValue(std::move(tokenizer));
      if (!_field.store_attr) {
        tokenizer_column = irs::field_limits::invalid();
      }
    } break;
    case duckdb::LogicalTypeId::BOOLEAN:
      _field.PrepareForBooleanValue();
      break;
    default:
      if (catalog::term_dict::IsNumeric(catalog::term_dict::Classify(kind))) {
        _field.PrepareForNumericValue();
      } else {
        return;
      }
      break;
  }
  _field.id = field_id;
  if (is_stored) {
    AppendToColumn(field_id, type, vec, count);
  }
  DispatchScalarBatch(kind, row_keys, count, tokenizer_column);
}

void SearchSinkInsertBaseImpl::InitImpl(size_t batch_size) {
  SDB_ASSERT(batch_size > 0);
  if (_document) {
    _document.reset();
  }
  _document.emplace(_trx.Insert(false, batch_size));
  _pk_blob_writer = EnsurePerRowBlobWriter(catalog::term_dict::kPKFieldId);
}

void SearchSinkInsertBaseImpl::InsertNullValue() {
  _null_field.SetNullValue();
  if (!_document->Insert(&_null_field)) {
    SDB_THROW(ERROR_INTERNAL,
              "Failed to insert null field into IResearch document");
  }
}

void SearchSinkInsertBaseImpl::JsonExpressionFields::InitForExpression(
  irs::field_id entry_field_id, const catalog::InvertedIndexEntryInfo* entry,
  catalog::ColumnTokenizer string_analyzer) {
  SDB_ASSERT(entry);
  SDB_ASSERT(irs::field_limits::valid(entry_field_id));
  SDB_ASSERT(irs::field_limits::valid(entry->numeric_field_id));
  SDB_ASSERT(irs::field_limits::valid(entry->bool_field_id));
  SDB_ASSERT(irs::field_limits::valid(entry->null_field_id));
  tokenizer_column = string_analyzer.tokenizer_column;
  string_field.PrepareForStringValue(std::move(string_analyzer));
  string_field.id = entry_field_id;
  numeric_field.PrepareForNumericValue();
  numeric_field.id = entry->numeric_field_id;
  bool_field.PrepareForBooleanValue();
  bool_field.id = entry->bool_field_id;
  null_field.PrepareForNullValue();
  null_field.id = entry->null_field_id;
}

void SearchSinkInsertBaseImpl::FinishImpl() {
  _column_writers.clear();
  _per_row_blob_writers.clear();
  _pk_blob_writer = nullptr;
  _document.reset();
}

void SearchSinkInsertBaseImpl::AppendToColumn(irs::field_id field_id,
                                              const duckdb::LogicalType& type,
                                              const duckdb::Vector& vec,
                                              duckdb::idx_t count) {
  if (!_document) {
    return;
  }
  auto* doc_columnstore = _document->Columnstore();
  if (!doc_columnstore) {
    return;
  }
  auto [it, inserted] = _column_writers.try_emplace(field_id, nullptr);
  if (inserted) {
    it->second = &doc_columnstore->OpenColumn(field_id, type);
  }
  _document->NextFieldBatch();
  const uint64_t start_row = _document->DocId() - irs::doc_limits::min();
  it->second->Append(start_row, vec, count);
}

irs::ColumnWriter* SearchSinkInsertBaseImpl::EnsurePerRowBlobWriter(
  irs::field_id field_id) {
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

void SearchSinkInsertBaseImpl::AppendPkBlob(std::string_view row_key) {
  SDB_ASSERT(_pk_blob_writer);
  const uint64_t row = _document->DocId() - irs::doc_limits::min();
  _pk_blob_writer->PushInStaging(row, [row_key](duckdb::Vector& staging,
                                                duckdb::idx_t slot) {
    auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(staging);
    slots[slot] = duckdb::StringVector::AddStringOrBlob(staging, row_key.data(),
                                                        row_key.size());
  });
}

void SearchSinkInsertBaseImpl::AppendPerRowBlob(irs::field_id field_id,
                                                irs::bytes_view bytes) {
  auto* writer = EnsurePerRowBlobWriter(field_id);
  if (!writer) {
    return;
  }
  const uint64_t row = _document->DocId() - irs::doc_limits::min();
  writer->PushInStaging(row, [bytes](duckdb::Vector& staging,
                                     duckdb::idx_t slot) {
    auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(staging);
    slots[slot] = duckdb::StringVector::AddStringOrBlob(
      staging, reinterpret_cast<const char*>(bytes.data()), bytes.size());
  });
}

void SearchSinkInsertBaseImpl::Field::PrepareForVerbatimStringValue() {
  static StreamPool gPool{kDefaultPoolSize};
  string_analyzer.reset();
  index_features = irs::IndexFeatures::None;
  analyzer = gPool.emplace(search::AnalyzerImpl::StringStreamTag{});
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
  static StreamPool gPool{kDefaultPoolSize};
  string_analyzer.reset();
  index_features = irs::IndexFeatures::None;
  analyzer = gPool.emplace(search::AnalyzerImpl::NumberStreamTag{});
}

template<typename T>
void SearchSinkInsertBaseImpl::Field::SetNumericValue(T value) {
  auto& nstream = basics::downCast<irs::NumericTokenizer>(*analyzer);
  if constexpr (std::is_same_v<T, float>) {
#ifdef FLOAT_T_IS_DOUBLE_T
    nstream.reset(static_cast<double>(value));
#else
    nstream.reset(value);
#endif
  } else if constexpr (std::is_same_v<T, uint32_t>) {
    nstream.reset(static_cast<int64_t>(value));
  } else {
    nstream.reset(value);
  }
}

void SearchSinkInsertBaseImpl::Field::PrepareForBooleanValue() {
  static StreamPool gPool{kDefaultPoolSize};
  string_analyzer.reset();
  index_features = irs::IndexFeatures::None;
  analyzer = gPool.emplace(search::AnalyzerImpl::BoolStreamTag{});
}

void SearchSinkInsertBaseImpl::Field::SetBooleanValue(bool value) {
  auto& bstream = basics::downCast<irs::BooleanTokenizer>(*analyzer);
  bstream.reset(value);
}

void SearchSinkInsertBaseImpl::Field::PrepareForNullValue() {
  static StreamPool gPool{kDefaultPoolSize};
  string_analyzer.reset();
  index_features = irs::IndexFeatures::None;
  analyzer = gPool.emplace(search::AnalyzerImpl::NullStreamTag{});
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
  _remove_filter = std::make_shared<SearchRemoveFilter>(
    batch_size, catalog::term_dict::kPKFieldId);
}

void SearchSinkDeleteBaseImpl::FinishImpl() {
  if (_remove_filter && !_remove_filter->Empty()) {
    _trx.Remove(std::move(_remove_filter));
  }
  _remove_filter.reset();
}

}  // namespace sdb::connector
