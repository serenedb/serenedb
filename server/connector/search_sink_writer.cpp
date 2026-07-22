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
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <iresearch/analysis/batch/numeric_terms.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/analysis/tokenizers.hpp>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "catalog/table_options.h"
#include "connector/common.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search_remove_filter.hpp"

namespace sdb::connector {
namespace {

template<duckdb::LogicalTypeId Kind>
auto ExtractNumericValue(const duckdb::UnifiedVectorFormat& fmt,
                         duckdb::idx_t idx) {
  if constexpr (Kind == duckdb::LogicalTypeId::TINYINT) {
    return static_cast<int32_t>(
      duckdb::UnifiedVectorFormat::GetData<int8_t>(fmt)[idx]);
  } else if constexpr (Kind == duckdb::LogicalTypeId::SMALLINT) {
    return static_cast<int32_t>(
      duckdb::UnifiedVectorFormat::GetData<int16_t>(fmt)[idx]);
  } else if constexpr (Kind == duckdb::LogicalTypeId::INTEGER ||
                       Kind == duckdb::LogicalTypeId::DATE) {
    return duckdb::UnifiedVectorFormat::GetData<int32_t>(fmt)[idx];
  } else if constexpr (Kind == duckdb::LogicalTypeId::TIME_TZ) {
    return TimeTzIndexTerm(
      duckdb::UnifiedVectorFormat::GetData<int64_t>(fmt)[idx]);
  } else if constexpr (Kind == duckdb::LogicalTypeId::BIGINT ||
                       Kind == duckdb::LogicalTypeId::TIME ||
                       Kind == duckdb::LogicalTypeId::TIME_NS ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_TZ ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_SEC ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_MS ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_NS ||
                       Kind == duckdb::LogicalTypeId::TIMESTAMP_TZ_NS) {
    return duckdb::UnifiedVectorFormat::GetData<int64_t>(fmt)[idx];
  } else if constexpr (Kind == duckdb::LogicalTypeId::UTINYINT) {
    return static_cast<int32_t>(
      duckdb::UnifiedVectorFormat::GetData<uint8_t>(fmt)[idx]);
  } else if constexpr (Kind == duckdb::LogicalTypeId::USMALLINT) {
    return static_cast<int32_t>(
      duckdb::UnifiedVectorFormat::GetData<uint16_t>(fmt)[idx]);
  } else if constexpr (Kind == duckdb::LogicalTypeId::UINTEGER) {
    return duckdb::UnifiedVectorFormat::GetData<uint32_t>(fmt)[idx];
  } else if constexpr (Kind == duckdb::LogicalTypeId::FLOAT) {
    return duckdb::UnifiedVectorFormat::GetData<float>(fmt)[idx];
  } else {
    static_assert(Kind == duckdb::LogicalTypeId::DOUBLE,
                  "ExtractNumericValue: unsupported Kind");
    return duckdb::UnifiedVectorFormat::GetData<double>(fmt)[idx];
  }
}

template<typename T>
auto PromoteNumericValue(T value) {
  if constexpr (std::is_same_v<T, float>) {
#ifdef FLOAT_T_IS_DOUBLE_T
    return static_cast<double>(value);
#else
    return value;
#endif
  } else if constexpr (std::is_same_v<T, uint32_t>) {
    return static_cast<int64_t>(value);
  } else {
    return value;
  }
}

duckdb::string_t BoolTerm(bool value) noexcept {
  const auto term = irs::BooleanTerm(value);
  return {term.data(), static_cast<uint32_t>(term.size())};
}

template<duckdb::LogicalTypeId... Kinds, typename F>
bool DispatchKind(duckdb::LogicalTypeId kind, F&& f) {
  return (((kind == Kinds) && (f.template operator()<Kinds>(), true)) || ...);
}

// The kinds every indexable value shares (scalars and list/array children);
// `Extra` carries the scalar-only additions.
template<duckdb::LogicalTypeId... Extra, typename F>
bool DispatchValueKind(duckdb::LogicalTypeId kind, F&& f) {
  using enum duckdb::LogicalTypeId;
  return DispatchKind<Extra..., VARCHAR, BLOB, BOOLEAN, TINYINT, SMALLINT,
                      INTEGER, BIGINT, UTINYINT, USMALLINT, UINTEGER, FLOAT,
                      DOUBLE, DATE, TIME, TIME_TZ, TIME_NS, TIMESTAMP,
                      TIMESTAMP_TZ, TIMESTAMP_SEC, TIMESTAMP_MS, TIMESTAMP_NS,
                      TIMESTAMP_TZ_NS>(kind, std::forward<F>(f));
}

}  // namespace

SearchSinkInsertBaseImpl::SearchSinkInsertBaseImpl(
  irs::IndexWriter::Transaction& trx, TokenizerProvider&& tokenizer_provider,
  EntryInfoProvider&& entry_info_provider,
  std::vector<IndexedExpression>&& indexed_exprs, PkPolicy pk_policy)
  : _tokenizer_provider{std::move(tokenizer_provider)},
    _entry_info_provider{std::move(entry_info_provider)},
    _indexed_expressions{std::move(indexed_exprs)},
    _trx{trx},
    _pk_policy{pk_policy} {
  _pk_field.PrepareForKeywordStringValue();
  _pk_field.id = catalog::term_dict::kPKFieldId;
}

void SearchSinkInsertBaseImpl::EmitPkTerms(
  std::span<const std::string_view> keys) {
  SDB_ASSERT(_document);
  _document->NextFieldBatch();
  const irs::doc_id_t first_doc = _document->DocId();
  if (!_document->InsertKeywordBlock(_pk_field.id, _pk_field.index_features,
                                     keys, first_doc)) {
    THROW_SQL_ERROR(
      ERR_MSG("Failed to insert PK field into IResearch document"));
  }
}

template<typename OnRow>
void SearchSinkInsertBaseImpl::GatherRows(duckdb::idx_t count, OnRow&& on_row) {
  _document->NextFieldBatch();
  _null_docs.clear();
  irs::doc_id_t doc = _document->DocId();
  for (duckdb::idx_t i = 0; i < count; ++i) {
    on_row(i, doc);
    ++doc;
  }
}

template<typename OnValid>
void SearchSinkInsertBaseImpl::GatherValidRows(
  duckdb::idx_t count, const duckdb::UnifiedVectorFormat& fmt,
  OnValid&& on_valid) {
  if (fmt.validity.AllValid()) {
    GatherRows(count, [&](duckdb::idx_t i, irs::doc_id_t doc) {
      on_valid(fmt.sel->get_index(i), doc);
    });
    return;
  }
  GatherRows(count, [&](duckdb::idx_t i, irs::doc_id_t doc) {
    const auto sel_idx = fmt.sel->get_index(i);
    if (!fmt.validity.RowIsValid(sel_idx)) {
      _null_docs.push_back(doc);
      return;
    }
    on_valid(sel_idx, doc);
  });
}

template<typename Fill>
void SearchSinkInsertBaseImpl::GatherKeywordBlock(Fill&& fill) {
  _term_scratch.clear();
  _doc_scratch.clear();
  fill([&](duckdb::string_t term, irs::doc_id_t doc) {
    _term_scratch.push_back(term);
    _doc_scratch.push_back(doc);
  });
  if (!_document->InsertKeywordBlock(_field.Id(), _field.GetIndexFeatures(),
                                     _term_scratch, _doc_scratch)) {
    THROW_SQL_ERROR(ERR_MSG("Failed to insert field into IResearch document"));
  }
}

template<duckdb::LogicalTypeId Kind, typename ForEach>
void SearchSinkInsertBaseImpl::GatherNumericBlock(
  const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t total,
  ForEach&& for_each) {
  using P = decltype(PromoteNumericValue(ExtractNumericValue<Kind>(fmt, 0)));
  _numeric_scratch.resize((total * sizeof(P) + sizeof(uint64_t) - 1) /
                          sizeof(uint64_t));
  auto* vals = reinterpret_cast<P*>(_numeric_scratch.data());
  size_t n = 0;
  _doc_scratch.clear();
  for_each([&](duckdb::idx_t idx, irs::doc_id_t doc) {
    vals[n++] = PromoteNumericValue(ExtractNumericValue<Kind>(fmt, idx));
    _doc_scratch.push_back(doc);
  });
  auto* slot = _document->Field(_field.Id(), _field.GetIndexFeatures());
  SDB_ASSERT(slot);
  InsertNumericColumn(*slot, std::span<const P>{vals, n},
                      std::span<const irs::doc_id_t>{_doc_scratch});
}

template<typename T>
void SearchSinkInsertBaseImpl::InsertNumericColumn(
  irs::FieldInverter& slot, std::span<const T> values,
  std::span<const irs::doc_id_t> docs) {
  SDB_ASSERT(values.size() == docs.size());
  constexpr uint32_t kTerms = irs::NumericTermCount<T>();
  constexpr size_t kMaxValues = irs::TokenBatch::kCapacity / kTerms;
  _term_scratch.resize(kMaxValues * kTerms);
  auto* terms = _term_scratch.data();
  for (size_t i = 0; i < values.size();) {
    const size_t n = std::min(kMaxValues, values.size() - i);
    irs::AppendNumericTermsBlock(terms, values.subspan(i, n));
    if (!_document->InsertBlock(slot, {terms, n * kTerms}, docs.subspan(i, n),
                                kTerms)) {
      THROW_SQL_ERROR(
        ERR_MSG("Failed to insert field into IResearch document"));
    }
    i += n;
  }
}

void SearchSinkInsertBaseImpl::FinishColumnBlocks(const Field& null_field) {
  if (!_null_docs.empty()) {
    if (!_document->InsertNullBlock(
          null_field.id, null_field.GetIndexFeatures(), _null_docs)) {
      THROW_SQL_ERROR(
        ERR_MSG("Failed to insert null field into IResearch document"));
    }
    _null_docs.clear();
  }
}

template<duckdb::LogicalTypeId Kind>
void SearchSinkInsertBaseImpl::WriteAnalyzedColumn(
  duckdb::idx_t count, irs::field_id tokenizer_column) {
  SDB_ASSERT(!_field.keyword);
  auto& fmt = _vec_fmt.unified;

  auto* store_writer = irs::field_limits::valid(tokenizer_column)
                         ? EnsureBlobColumnWriter(tokenizer_column)
                         : nullptr;
  const auto layout = irs::LayoutFromFeatures(_field.GetIndexFeatures());
  auto* slot = _document->Field(_field.Id(), _field.GetIndexFeatures());
  SDB_ASSERT(slot);

  _inverter_sink.Reset();

  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  _term_scratch.clear();
  _doc_scratch.clear();
  GatherValidRows(count, fmt, [&](duckdb::idx_t sel_idx, irs::doc_id_t doc) {
    _term_scratch.push_back(data[sel_idx]);
    _doc_scratch.push_back(doc);
  });
  if (store_writer) {
    SDB_ASSERT(_field.has_store);
    _store_appender.Bind(*this, *store_writer);
  }
  auto* store_sink = store_writer ? &_store_appender : nullptr;
  if constexpr (Kind == duckdb::LogicalTypeId::GEOMETRY) {
    irs::analysis::GeoAnalyzer::Cast(_field.GetTokens()).SetWkbInput(true);
  }
  _field.GetTokens().Fill(_term_scratch, _doc_scratch,
                          _inverter_sink.Bind(*_document, *slot, store_sink),
                          layout);
  _inverter_sink.Flush();
  FinishColumnBlocks(_null_field);
}

void SearchSinkInsertBaseImpl::WriteKeywordColumn(
  duckdb::idx_t count, irs::field_id tokenizer_column) {
  auto& fmt = _vec_fmt.unified;
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);

  auto* store_writer = irs::field_limits::valid(tokenizer_column)
                         ? EnsureBlobColumnWriter(tokenizer_column)
                         : nullptr;

  // Flat, null-free chunk: the vector's string_t array IS the block -- no
  // gather, contiguous docs.
  if (_flat_column && fmt.validity.AllValid()) {
    _document->NextFieldBatch();
    const irs::doc_id_t first_doc = _document->DocId();
    if (store_writer) {
      SDB_ASSERT(_cur_vec);
      duckdb::Vector blob_view{duckdb::LogicalType::BLOB};
      blob_view.Reinterpret(*_cur_vec);
      store_writer->Append(first_doc - irs::doc_limits::min(), blob_view,
                           count);
    }
    if (!_document->InsertKeywordBlock(
          _field.Id(), _field.GetIndexFeatures(),
          std::span<const duckdb::string_t>{data, count}, first_doc)) {
      THROW_SQL_ERROR(
        ERR_MSG("Failed to insert field into IResearch document"));
    }
    return;
  }

  _term_scratch.clear();
  _doc_scratch.clear();
  GatherValidRows(count, fmt, [&](duckdb::idx_t sel_idx, irs::doc_id_t doc) {
    const auto& value = data[sel_idx];
    _term_scratch.push_back(value);
    _doc_scratch.push_back(doc);
    if (store_writer) {
      AppendBlobAt(*store_writer, doc, AsBytesView(value));
    }
  });
  if (!_document->InsertKeywordBlock(_field.Id(), _field.GetIndexFeatures(),
                                     _term_scratch, _doc_scratch)) {
    THROW_SQL_ERROR(ERR_MSG("Failed to insert field into IResearch document"));
  }
  FinishColumnBlocks(_null_field);
}

void SearchSinkInsertBaseImpl::WriteBoolColumn(duckdb::idx_t count) {
  auto& fmt = _vec_fmt.unified;

  SDB_ASSERT(_field.GetIndexFeatures() == irs::IndexFeatures::None);

  const auto* data = duckdb::UnifiedVectorFormat::GetData<bool>(fmt);
  GatherKeywordBlock([&](auto&& emit) {
    GatherValidRows(count, fmt, [&](duckdb::idx_t sel_idx, irs::doc_id_t doc) {
      emit(BoolTerm(data[sel_idx]), doc);
    });
  });
  FinishColumnBlocks(_null_field);
}

template<duckdb::LogicalTypeId Kind>
void SearchSinkInsertBaseImpl::WriteNumericColumn(duckdb::idx_t count) {
  auto& fmt = _vec_fmt.unified;

  SDB_ASSERT(_field.GetIndexFeatures() == irs::IndexFeatures::None);

  GatherNumericBlock<Kind>(fmt, count, [&](auto&& on_value) {
    GatherValidRows(count, fmt, on_value);
  });
  FinishColumnBlocks(_null_field);
}

void SearchSinkInsertBaseImpl::WriteNullColumn(duckdb::idx_t count) {
  GatherRows(count, [&](duckdb::idx_t, irs::doc_id_t doc) {
    _null_docs.push_back(doc);
  });
  FinishColumnBlocks(_null_field);
}

template<duckdb::LogicalTypeId Kind>
void SearchSinkInsertBaseImpl::WriteScalarBatch(
  duckdb::idx_t count, irs::field_id tokenizer_column) {
  SDB_ASSERT(_document);

  if constexpr (Kind == duckdb::LogicalTypeId::SQLNULL) {
    WriteNullColumn(count);
  } else if constexpr (Kind == duckdb::LogicalTypeId::VARCHAR ||
                       Kind == duckdb::LogicalTypeId::BLOB) {
    if (_field.keyword) {
      WriteKeywordColumn(count, tokenizer_column);
    } else {
      WriteAnalyzedColumn<Kind>(count, tokenizer_column);
    }
  } else if constexpr (Kind == duckdb::LogicalTypeId::GEOMETRY) {
    WriteAnalyzedColumn<Kind>(count, tokenizer_column);
  } else if constexpr (Kind == duckdb::LogicalTypeId::BOOLEAN) {
    SDB_ASSERT(!irs::field_limits::valid(tokenizer_column));
    WriteBoolColumn(count);
  } else {
    SDB_ASSERT(!irs::field_limits::valid(tokenizer_column));
    WriteNumericColumn<Kind>(count);
  }
}

template<duckdb::LogicalTypeId ChildKind>
void SearchSinkInsertBaseImpl::WriteListBatch(duckdb::idx_t count,
                                              duckdb::idx_t array_size) {
  SDB_ASSERT(_document);
  _document->NextFieldBatch();

  const auto& parent_fmt = _vec_fmt.unified;
  const auto& child_fmt = _vec_fmt.children[0].unified;
  const auto* list_data =
    array_size == 0
      ? duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(parent_fmt)
      : nullptr;
  _null_docs.clear();

  const auto for_each_element = [&](auto&& on_element) {
    irs::doc_id_t doc = _document->DocId();
    for (duckdb::idx_t i = 0; i < count; ++i, ++doc) {
      const auto parent_idx = parent_fmt.sel->get_index(i);
      if (!parent_fmt.validity.RowIsValid(parent_idx)) {
        _null_docs.push_back(doc);
        continue;
      }
      const auto offset =
        list_data ? list_data[parent_idx].offset : parent_idx * array_size;
      const auto length = list_data ? list_data[parent_idx].length : array_size;
      for (duckdb::idx_t k = 0; k < length; ++k) {
        const auto child_idx = child_fmt.sel->get_index(offset + k);
        if (!child_fmt.validity.RowIsValid(child_idx)) {
          _null_docs.push_back(doc);
          continue;
        }
        on_element(child_idx, doc);
      }
    }
  };

  if constexpr (ChildKind == duckdb::LogicalTypeId::VARCHAR ||
                ChildKind == duckdb::LogicalTypeId::BLOB) {
    const auto* data =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(child_fmt);
    if (_field.keyword) {
      GatherKeywordBlock([&](auto&& emit) {
        for_each_element([&](duckdb::idx_t child_idx, irs::doc_id_t doc) {
          emit(data[child_idx], doc);
        });
      });
    } else {
      const auto layout = irs::LayoutFromFeatures(_field.GetIndexFeatures());
      auto* slot = _document->Field(_field.Id(), _field.GetIndexFeatures());
      SDB_ASSERT(slot);
      _inverter_sink.Reset();
      auto& w = _inverter_sink.Bind(*_document, *slot);
      for_each_element([&](duckdb::idx_t child_idx, irs::doc_id_t doc) {
        w.BeginValue(doc);
        _field.string_analyzer->Fill(AsView(data[child_idx]), w, layout);
        w.EndValue();
      });
      _inverter_sink.Flush();
    }
  } else if constexpr (ChildKind == duckdb::LogicalTypeId::BOOLEAN) {
    const auto* data = duckdb::UnifiedVectorFormat::GetData<bool>(child_fmt);
    GatherKeywordBlock([&](auto&& emit) {
      for_each_element([&](duckdb::idx_t child_idx, irs::doc_id_t doc) {
        emit(BoolTerm(data[child_idx]), doc);
      });
    });
  } else {
    duckdb::idx_t total = count * array_size;
    for (duckdb::idx_t i = 0; list_data && i < count; ++i) {
      const auto parent_idx = parent_fmt.sel->get_index(i);
      if (parent_fmt.validity.RowIsValid(parent_idx)) {
        total += list_data[parent_idx].length;
      }
    }
    GatherNumericBlock<ChildKind>(child_fmt, total, for_each_element);
  }
  FinishColumnBlocks(_null_field);
}

bool SearchSinkInsertBaseImpl::DispatchScalarBatch(
  duckdb::LogicalTypeId kind, duckdb::idx_t count,
  irs::field_id tokenizer_column) {
  using enum duckdb::LogicalTypeId;
  return DispatchValueKind<SQLNULL, GEOMETRY>(
    kind, [&]<duckdb::LogicalTypeId K>() {
      WriteScalarBatch<K>(count, tokenizer_column);
    });
}

bool SearchSinkInsertBaseImpl::DispatchListBatch(
  duckdb::LogicalTypeId child_kind, duckdb::idx_t count,
  duckdb::idx_t array_size) {
  return DispatchValueKind(child_kind, [&]<duckdb::LogicalTypeId K>() {
    WriteListBatch<K>(count, array_size);
  });
}

void SearchSinkInsertBaseImpl::WriteJsonBatch(const duckdb::Vector& vec,
                                              duckdb::idx_t count) {
  SDB_ASSERT(_document);
  _document->NextFieldBatch();

  auto& fmt = _vec_fmt.unified;
  vec.ToUnifiedFormat(count, fmt);

  auto& jpf = _json_fields;
  auto* store_writer = irs::field_limits::valid(jpf.tokenizer_column)
                         ? EnsureBlobColumnWriter(jpf.tokenizer_column)
                         : nullptr;

  const auto str_features = jpf.string_field.GetIndexFeatures();
  const auto str_layout = irs::LayoutFromFeatures(str_features);
  auto* str_slot = _document->Field(jpf.string_field.Id(), str_features);
  SDB_ASSERT(str_slot);
  _inverter_sink.Reset();

  _numeric_scratch.resize(count);
  auto* nums = reinterpret_cast<double*>(_numeric_scratch.data());
  size_t nnums = 0;
  _doc_scratch.clear();
  _term_scratch.clear();
  _bool_docs.clear();
  _null_docs.clear();

  irs::doc_id_t doc = _document->DocId();
  for (duckdb::idx_t i = 0; i < count; ++i, ++doc) {
    const auto sel_idx = fmt.sel->get_index(i);
    const bool is_null = !fmt.validity.RowIsValid(sel_idx);
    bool wrote_string_blob = false;

    if (!is_null) {
      const auto& cell_string =
        duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt)[sel_idx];
      std::string_view json_str = AsView(cell_string);
      if (!json_str.empty() && json_str.front() == kStringPrefix[0]) {
        json_str = json_str.substr(1);
      }
      if (!json_str.empty()) {
        _json_buffer.assign(json_str);
        _json_buffer.append(simdjson::SIMDJSON_PADDING, '\0');
        simdjson::padded_string_view padded_view{
          _json_buffer.data(), json_str.size(), _json_buffer.size()};
        simdjson::ondemand::document json_doc;
        auto res = _json_parser.iterate(padded_view).get(json_doc);
        SDB_ASSERT(res == simdjson::SUCCESS);
        simdjson::ondemand::json_type t{};
        if (json_doc.type().get(t) == simdjson::SUCCESS) {
          switch (t) {
            case simdjson::ondemand::json_type::string: {
              auto s = json_doc.get_string();
              if (s.error() == simdjson::SUCCESS) {
                const std::string_view value = s.value_unsafe();
                irs::TokenConsumer* leaf_store = nullptr;
                if (store_writer && !jpf.string_field.keyword) {
                  _store_appender.Bind(*this, *store_writer);
                  leaf_store = &_store_appender;
                }
                auto& w =
                  _inverter_sink.Bind(*_document, *str_slot, leaf_store);
                w.BeginValue(doc);
                bool ok = true;
                if (jpf.string_field.keyword) {
                  w.buf.dense_pos = true;
                  w.buf.unique = true;
                  const auto i = w.Next();
                  w.buf.terms[i] =
                    w.Intern(irs::ViewCast<irs::byte_type>(value));
                } else {
                  ok = jpf.string_field.string_analyzer->Fill(value, w,
                                                              str_layout);
                }
                w.EndValue();
                if (store_writer) {
                  if (jpf.string_field.keyword) {
                    AppendBlobAt(*store_writer, doc,
                                 irs::ViewCast<irs::byte_type>(value));
                    wrote_string_blob = true;
                  } else {
                    // Store-producing analyzers delivered through OnStore
                    // above; everyone else falls through to the empty-blob
                    // backfill.
                    wrote_string_blob =
                      ok && jpf.string_field.string_analyzer->Traits().store;
                  }
                }
              }
            } break;
            case simdjson::ondemand::json_type::number: {
              double d;
              if (json_doc.get_double().get(d) == simdjson::SUCCESS) {
                nums[nnums++] = d;
                _doc_scratch.push_back(doc);
              }
            } break;
            case simdjson::ondemand::json_type::boolean: {
              bool b;
              if (json_doc.get_bool().get(b) == simdjson::SUCCESS) {
                _term_scratch.push_back(BoolTerm(b));
                _bool_docs.push_back(doc);
              }
            } break;
            case simdjson::ondemand::json_type::null:
              _null_docs.push_back(doc);
              break;
            case simdjson::ondemand::json_type::object:
            case simdjson::ondemand::json_type::array:
              THROW_SQL_ERROR(
                ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                ERR_MSG(
                  "JSON expression indexed by an inverted index must point to "
                  "a primitive (string/number/boolean/null) leaf; got an "
                  "object or array"));
            default:
              break;
          }
        }
      }
    }
    if (store_writer && !wrote_string_blob) {
      AppendBlobAt(*store_writer, doc, irs::bytes_view{});
    }
  }

  _inverter_sink.Flush();
  if (!_term_scratch.empty() &&
      !_document->InsertKeywordBlock(jpf.bool_field.Id(),
                                     jpf.bool_field.GetIndexFeatures(),
                                     _term_scratch, _bool_docs)) {
    THROW_SQL_ERROR(
      ERR_MSG("Failed to insert JSON expression field into IResearch "
              "document"));
  }
  if (nnums) {
    auto* num_slot = _document->Field(jpf.numeric_field.Id(),
                                      jpf.numeric_field.GetIndexFeatures());
    SDB_ASSERT(num_slot);
    InsertNumericColumn(*num_slot, std::span<const double>{nums, nnums},
                        std::span<const irs::doc_id_t>{_doc_scratch});
  }
  FinishColumnBlocks(jpf.null_field);
}

void SearchSinkInsertBaseImpl::SwitchFieldImpl(irs::field_id field_id,
                                               const duckdb::LogicalType& type,
                                               const duckdb::Vector& vec,
                                               duckdb::idx_t count) {
  SDB_ASSERT(irs::field_limits::valid(field_id));
  const auto* entry = _entry_info_provider(field_id);
  const bool is_term_dict = !entry || entry->IsTermDict();
  const bool is_stored = entry && entry->IsStored();
  const auto kind = type.id();

  if (is_stored && !is_term_dict) {
    AppendToColumn(field_id, type, vec, count);
    return;
  }
  if (type.IsJSONType() && entry && entry->HasJsonLeafFields()) {
    _json_fields.InitForExpression(field_id, entry, ResolveTokenizer(field_id));
    if (irs::field_limits::valid(_json_fields.tokenizer_column)) {
      EnsureBlobColumnWriter(_json_fields.tokenizer_column);
    }
    if (is_stored) {
      AppendToColumn(field_id, type, vec, count);
    }
    WriteJsonBatch(vec, count);
    return;
  }

  const bool is_list_or_array =
    kind == duckdb::LogicalTypeId::LIST || kind == duckdb::LogicalTypeId::ARRAY;
  _flat_column =
    !is_list_or_array && vec.GetVectorType() == duckdb::VectorType::FLAT_VECTOR;
  _cur_vec = &vec;
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
    _null_field.PrepareForBlockValue();
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
      _field.PrepareForStringValue(ResolveTokenizer(field_id));
    } else if (child_kind == duckdb::LogicalTypeId::BOOLEAN) {
      _field.PrepareForBlockValue();
    } else if (catalog::term_dict::IsNumeric(
                 catalog::term_dict::Classify(child_kind))) {
      _field.PrepareForBlockValue();
    } else {
      return;
    }
    _field.id = field_id;
    if (is_stored) {
      AppendToColumn(field_id, type, vec, count);
    }
    DispatchListBatch(child_kind, count, array_size);
    return;
  }

  irs::field_id tokenizer_column = irs::field_limits::invalid();
  switch (kind) {
    case duckdb::LogicalTypeId::SQLNULL:
      break;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
    case duckdb::LogicalTypeId::GEOMETRY: {
      auto& tokenizer = ResolveTokenizer(field_id);
      tokenizer_column = tokenizer.tokenizer_column;
      _field.PrepareForStringValue(tokenizer);
      if (!_field.has_store) {
        tokenizer_column = irs::field_limits::invalid();
      }
    } break;
    case duckdb::LogicalTypeId::BOOLEAN:
      _field.PrepareForBlockValue();
      break;
    default:
      if (catalog::term_dict::IsNumeric(catalog::term_dict::Classify(kind))) {
        _field.PrepareForBlockValue();
      } else {
        return;
      }
      break;
  }
  _field.id = field_id;
  if (is_stored) {
    AppendToColumn(field_id, type, vec, count);
  }
  DispatchScalarBatch(kind, count, tokenizer_column);
}

catalog::ColumnTokenizer& SearchSinkInsertBaseImpl::ResolveTokenizer(
  irs::field_id field_id) {
  auto& tokenizer = _tokenizer_cache.try_emplace(field_id).first->second;
  if (!tokenizer.analyzer) {
    tokenizer = _tokenizer_provider(field_id);
  }
  return tokenizer;
}

void SearchSinkInsertBaseImpl::InitImpl(size_t batch_size, const PkChunk& pk) {
  SDB_ASSERT(batch_size > 0);
  if (_document) {
    _document.reset();
  }
  _document.emplace(_trx.Insert(false, batch_size));
  _pk_column_writer = nullptr;
  if (_pk_policy.column == catalog::PkColumnKind::I64 ||
      _pk_policy.column == catalog::PkColumnKind::I64I64) {
    _pk_column_writer = EnsureColumnWriter(catalog::term_dict::kPKFieldId,
                                           PkColumnType(_pk_policy.column));
  }
  if (_pk_column_writer && pk.column) {
    SDB_ASSERT(pk.column->GetType() == PkColumnType(_pk_policy.column));
    AppendPkColumn(*pk.column, batch_size);
  }
  if (_pk_policy.index_term && !pk.keys.empty()) {
    SDB_ASSERT(pk.keys.size() == batch_size);
    EmitPkTerms(pk.keys);
  }
}

void SearchSinkInsertBaseImpl::JsonExpressionFields::InitForExpression(
  irs::field_id entry_field_id, const catalog::InvertedIndexEntryInfo* entry,
  catalog::ColumnTokenizer& string_analyzer) {
  SDB_ASSERT(entry);
  SDB_ASSERT(irs::field_limits::valid(entry_field_id));
  SDB_ASSERT(irs::field_limits::valid(entry->numeric_field_id));
  SDB_ASSERT(irs::field_limits::valid(entry->bool_field_id));
  SDB_ASSERT(irs::field_limits::valid(entry->null_field_id));
  tokenizer_column = string_analyzer.tokenizer_column;
  string_field.PrepareForStringValue(string_analyzer);
  string_field.id = entry_field_id;
  numeric_field.PrepareForBlockValue();
  numeric_field.id = entry->numeric_field_id;
  bool_field.PrepareForBlockValue();
  bool_field.id = entry->bool_field_id;
  null_field.PrepareForBlockValue();
  null_field.id = entry->null_field_id;
}

void SearchSinkInsertBaseImpl::FinishImpl() {
  _column_writers.clear();
  _pk_column_writer = nullptr;
  _document.reset();
}

void SearchSinkInsertBaseImpl::AppendToColumn(irs::field_id field_id,
                                              const duckdb::LogicalType& type,
                                              const duckdb::Vector& vec,
                                              duckdb::idx_t count) {
  auto* writer = EnsureColumnWriter(field_id, type);
  if (!writer) {
    return;
  }
  _document->NextFieldBatch();
  const uint64_t start_row = _document->DocId() - irs::doc_limits::min();
  writer->Append(start_row, vec, count);
}

irs::ColumnWriter* SearchSinkInsertBaseImpl::EnsureColumnWriter(
  irs::field_id field_id, const duckdb::LogicalType& type) {
  auto* col_writer = _document ? _document->GetColWriter() : nullptr;
  if (!col_writer) {
    return nullptr;
  }
  auto [it, inserted] = _column_writers.try_emplace(field_id, nullptr);
  if (!it->second) {
    it->second = &col_writer->OpenColumn(field_id, type);
  }
  return it->second;
}

irs::ColumnWriter* SearchSinkInsertBaseImpl::EnsureBlobColumnWriter(
  irs::field_id field_id) {
  return EnsureColumnWriter(field_id, duckdb::LogicalType::BLOB);
}

void SearchSinkInsertBaseImpl::AppendPkColumn(const duckdb::Vector& pk,
                                              duckdb::idx_t count) {
  SDB_ASSERT(_pk_column_writer);
  SDB_ASSERT(_document);
  _document->NextFieldBatch();
  const uint64_t start_row = _document->DocId() - irs::doc_limits::min();
  _pk_column_writer->Append(start_row, pk, count);
}

void SearchSinkInsertBaseImpl::AppendBlobAt(irs::ColumnWriter& writer,
                                            irs::doc_id_t doc,
                                            irs::bytes_view bytes) {
  const uint64_t row = doc - irs::doc_limits::min();
  writer.PushInStaging(row, [bytes](duckdb::Vector& staging,
                                    duckdb::idx_t slot) {
    auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(staging);
    slots[slot] = duckdb::StringVector::AddStringOrBlob(
      staging, reinterpret_cast<const char*>(bytes.data()), bytes.size());
  });
}

void SearchSinkInsertBaseImpl::Field::PrepareForKeywordStringValue() {
  string_analyzer = nullptr;
  index_features = irs::IndexFeatures::None;
  keyword = true;
  has_store = true;
}

void SearchSinkInsertBaseImpl::Field::PrepareForStringValue(
  catalog::ColumnTokenizer& column_analyzer) {
  index_features = column_analyzer.features;
  keyword = column_analyzer.verbatim;
  SDB_ASSERT(column_analyzer.analyzer);
  string_analyzer = column_analyzer.analyzer.get();
  const auto traits = string_analyzer->Traits();
  if (auto* geo = irs::analysis::GeoAnalyzer::TryCast(*string_analyzer)) {
    geo->SetWkbInput(false);
  }
  if (keyword) {
    has_store = true;
  } else {
    has_store = traits.store;
  }
}

void SearchSinkInsertBaseImpl::Field::PrepareForBlockValue() {
  string_analyzer = nullptr;
  index_features = irs::IndexFeatures::None;
  keyword = false;
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

void WriteChunkToSearchSink(
  SearchSinkInsertBaseImpl& sink, duckdb::DataChunk& chunk,
  std::span<const catalog::Column::Id> column_ids,
  std::span<const duckdb_primary_key::PKColumn> pk_columns,
  bool uses_generated_pk, uint64_t pk_base) {
  const auto num_rows = chunk.size();

  auto& scratch = sink.GetKeyScratch();
  auto& pk_formats = scratch.pk_formats;
  auto& row_keys = scratch.row_keys;
  auto& key_views = scratch.key_views;
  duckdb_primary_key::PreparePKFormats(chunk, pk_columns, pk_formats);
  row_keys.resize(num_rows);
  key_views.clear();
  key_views.reserve(num_rows);
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    auto& key = row_keys[row];
    key.clear();
    if (uses_generated_pk) {
      duckdb_primary_key::AppendGenerated(key, pk_base + row);
    } else {
      duckdb_primary_key::Create(pk_formats, pk_columns, row, key);
    }
    key_views.emplace_back(key);
  }

  sink.InitImpl(num_rows, PkChunk{.keys = key_views});
  for (size_t col = 0; col < column_ids.size(); ++col) {
    sink.SwitchFieldImpl(static_cast<irs::field_id>(column_ids[col]),
                         chunk.data[col].GetType(), chunk.data[col], num_rows);
  }
  if (uses_generated_pk) {
    duckdb::Vector gen_pk(duckdb::LogicalType::BIGINT, num_rows);
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(gen_pk);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      data[row] = static_cast<int64_t>(pk_base + row);
    }
    sink.SwitchFieldImpl(
      static_cast<irs::field_id>(catalog::Column::kGeneratedPKId.id()),
      duckdb::LogicalType::BIGINT, gen_pk, num_rows);
  }
  sink.FinishImpl();
}

}  // namespace sdb::connector
