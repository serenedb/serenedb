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
#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/index/typed_terms.hpp>
#include <iterator>

#include "basics/assert.h"
#include "basics/primary_key.hpp"
#include "catalog/ddl/catalog.h"
#include "catalog/read/duckdb_catalog_sets.h"
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

template<duckdb::LogicalTypeId Kind>
using PromotedNumeric = decltype(PromoteNumericValue(ExtractNumericValue<Kind>(
  std::declval<const duckdb::UnifiedVectorFormat&>(), 0)));

template<duckdb::LogicalTypeId... Kinds, typename F>
bool DispatchKind(duckdb::LogicalTypeId kind, F&& f) {
  return (((kind == Kinds) && (f.template operator()<Kinds>(), true)) || ...);
}

template<duckdb::LogicalTypeId... Extra, typename F>
bool DispatchNumericKind(duckdb::LogicalTypeId kind, F&& f) {
  using enum duckdb::LogicalTypeId;
  return DispatchKind<Extra..., TINYINT, SMALLINT, INTEGER, BIGINT, UTINYINT,
                      USMALLINT, UINTEGER, FLOAT, DOUBLE, DATE, TIME, TIME_TZ,
                      TIME_NS, TIMESTAMP, TIMESTAMP_TZ, TIMESTAMP_SEC,
                      TIMESTAMP_MS, TIMESTAMP_NS, TIMESTAMP_TZ_NS>(
    kind, std::forward<F>(f));
}

// The kinds every indexable list/array child shares.
template<typename F>
bool DispatchValueKind(duckdb::LogicalTypeId kind, F&& f) {
  using enum duckdb::LogicalTypeId;
  return DispatchNumericKind<VARCHAR, BLOB, BOOLEAN>(kind, std::forward<F>(f));
}

}  // namespace

template<typename Func>
void SearchSinkInsertBaseImpl::InvertField(const Field& field, Func&& func) {
  if (!_document->WithField(field.Id(), field.GetIndexFeatures(),
                            std::forward<Func>(func))) {
    THROW_SQL_ERROR(ERR_MSG("Failed to insert field ", field.Id(),
                            " into IResearch document"));
  }
}

template<typename Func>
void SearchSinkInsertBaseImpl::InvertTokens(const Field& field,
                                            irs::StoreSink* store,
                                            Func&& func) {
  if (!_document->WithTokens(field.Id(), field.GetIndexFeatures(), store,
                             std::forward<Func>(func))) {
    THROW_SQL_ERROR(ERR_MSG("Failed to insert field ", field.Id(),
                            " into IResearch document"));
  }
}

SearchSinkInsertBaseImpl::SearchSinkInsertBaseImpl(
  irs::IndexWriter::Transaction& trx, TokenizerProvider&& tokenizer_provider,
  EntryInfoProvider&& entry_info_provider, PkPolicy pk_policy,
  std::vector<IndexedExpression>&& indexed_exprs,
  std::shared_ptr<const search::SearchTable::TermsByColumn> terms_by_column)
  : _tokenizer_provider{std::move(tokenizer_provider)},
    _entry_info_provider{std::move(entry_info_provider)},
    _trx{&trx},
    _pk_policy{pk_policy},
    _indexed_expressions{std::move(indexed_exprs)},
    _terms_by_column{std::move(terms_by_column)} {
  _pk_field.PrepareForKeywordStringValue(catalog::term_dict::kPKFieldId);
}

void SearchSinkInsertBaseImpl::EmitPkTerms(
  const Field& pk_field, std::span<const duckdb::string_t> keys) {
  SDB_ASSERT(_document);
  _document->NextFieldBatch();
  const irs::doc_id_t first_doc = _document->DocId();
  InvertField(pk_field, [&](irs::FieldInverter& fld) {
    return fld.InvertPrimaryKeyBlock(keys, first_doc);
  });
}

template<typename Insert>
void SearchSinkInsertBaseImpl::WriteColumnBlock(const Field& null_field,
                                                duckdb::idx_t count,
                                                Insert&& insert) {
  auto& fmt = _vec_fmt.unified;
  _document->NextFieldBatch();
  const irs::doc_id_t first_doc = _document->DocId();
  insert(fmt, static_cast<uint32_t>(count), first_doc);
  if (irs::analysis::HasInvalidRows(fmt, static_cast<uint32_t>(count))) {
    InvertField(null_field, [&](irs::FieldInverter& fld) {
      return fld.InvertNullBlock(fmt, static_cast<uint32_t>(count), first_doc);
    });
  }
}

void SearchSinkInsertBaseImpl::FinishColumnBlocks(const Field& null_field) {
  if (_null_docs.empty()) {
    return;
  }
  InvertField(null_field, [&](irs::FieldInverter& fld) {
    return fld.InvertKeywords([&](auto&& emit) {
      const auto term = irs::NullTerm();
      for (const auto doc : _null_docs) {
        emit(term, doc);
      }
    });
  });
  _null_docs.clear();
}

void SearchSinkInsertBaseImpl::WriteAnalyzedColumn(const Field& field,
                                                   const Field& null_field,
                                                   duckdb::idx_t count) {
  SDB_ASSERT(!field.keyword);

  auto* store_writer = irs::field_limits::valid(field.store_column)
                         ? EnsureBlobColumnWriter(field.store_column)
                         : nullptr;

  if (store_writer) {
    _store_appender.Bind(*this, *store_writer);
  }
  auto* store_sink = store_writer ? &_store_appender : nullptr;
  WriteColumnBlock(
    null_field, count,
    [&](const duckdb::UnifiedVectorFormat& fmt, uint32_t n,
        irs::doc_id_t first_doc) {
      InvertTokens(
        field, store_sink, [&](irs::FieldInverter& fld, irs::TokenSink& w) {
          fld.Configure(field.GetTokens().Traits());
          field.GetTokens().Fill(fmt, n, first_doc, w, {fld.Layout()});
        });
    });
}

void SearchSinkInsertBaseImpl::WriteKeywordColumn(const Field& field,
                                                  const Field& null_field,
                                                  const duckdb::Vector& vec,
                                                  duckdb::idx_t count) {
  auto& fmt = _vec_fmt.unified;
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);

  auto* store_writer = irs::field_limits::valid(field.store_column)
                         ? EnsureBlobColumnWriter(field.store_column)
                         : nullptr;
  const bool flat = vec.GetVectorType() == duckdb::VectorType::FLAT_VECTOR;

  WriteColumnBlock(
    null_field, count,
    [&](const duckdb::UnifiedVectorFormat& fmt, uint32_t n,
        irs::doc_id_t first_doc) {
      if (store_writer) {
        if (flat && fmt.validity.CheckAllValid(n)) {
          duckdb::Vector blob_view{duckdb::LogicalType::BLOB};
          blob_view.Reinterpret(vec);
          store_writer->Append(first_doc - irs::doc_limits::min(), blob_view,
                               n);
        } else {
          irs::analysis::ForEachValidRow(fmt, n, [&](uint32_t i, uint32_t idx) {
            AppendBlobAt(*store_writer, first_doc + i, data[idx]);
            return true;
          });
        }
      }
      InvertField(field, [&](irs::FieldInverter& fld) {
        return fld.InvertKeywordBlock(fmt, n, first_doc);
      });
    });
}

void SearchSinkInsertBaseImpl::WriteBoolColumn(const Field& field,
                                               const Field& null_field,
                                               duckdb::idx_t count) {
  SDB_ASSERT(field.GetIndexFeatures() == irs::IndexFeatures::None);

  WriteColumnBlock(null_field, count,
                   [&](const duckdb::UnifiedVectorFormat& fmt, uint32_t n,
                       irs::doc_id_t first_doc) {
                     InvertField(field, [&](irs::FieldInverter& fld) {
                       return fld.InvertBoolBlock(fmt, n, first_doc);
                     });
                   });
}

template<duckdb::LogicalTypeId Kind>
void SearchSinkInsertBaseImpl::WriteNumericColumn(const Field& field,
                                                  const Field& null_field,
                                                  duckdb::idx_t count) {
  SDB_ASSERT(field.GetIndexFeatures() == irs::IndexFeatures::None);

  WriteColumnBlock(
    null_field, count,
    [&](const duckdb::UnifiedVectorFormat& fmt, uint32_t n,
        irs::doc_id_t first_doc) {
      InvertField(field, [&](irs::FieldInverter& fld) {
        return fld.InvertNumericBlock(
          fmt, n, first_doc, [&](duckdb::idx_t idx) {
            return PromoteNumericValue(ExtractNumericValue<Kind>(fmt, idx));
          });
      });
    });
}

void SearchSinkInsertBaseImpl::WriteNullColumn(const Field& null_field,
                                               duckdb::idx_t count) {
  _document->NextFieldBatch();
  if (count != 0) {
    InvertField(null_field, [&](irs::FieldInverter& fld) {
      return fld.InvertNullBlock(static_cast<uint32_t>(count),
                                 _document->DocId());
    });
  }
}

template<duckdb::LogicalTypeId ChildKind>
void SearchSinkInsertBaseImpl::WriteListBatch(const Field& field,
                                              const Field& null_field,
                                              duckdb::idx_t count,
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
    const irs::doc_id_t first_doc = _document->DocId();
    irs::analysis::ForEachValidRow(
      parent_fmt, static_cast<uint32_t>(count),
      [&](uint32_t i, uint32_t parent_idx) {
        const irs::doc_id_t doc = first_doc + i;
        const auto offset =
          list_data ? list_data[parent_idx].offset : parent_idx * array_size;
        const auto length =
          list_data ? list_data[parent_idx].length : array_size;
        irs::analysis::ForEachValidRow(
          child_fmt, offset, static_cast<uint32_t>(length),
          [&](uint32_t, uint32_t child_idx) {
            on_element(child_idx, doc);
            return true;
          },
          [&](uint32_t) {
            _null_docs.push_back(doc);
            return true;
          });
        return true;
      },
      [&](uint32_t i) {
        _null_docs.push_back(first_doc + i);
        return true;
      });
  };

  if constexpr (ChildKind == duckdb::LogicalTypeId::VARCHAR ||
                ChildKind == duckdb::LogicalTypeId::BLOB) {
    const auto* data =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(child_fmt);
    if (field.keyword) {
      InvertField(field, [&](irs::FieldInverter& fld) {
        return fld.InvertKeywords([&](auto&& emit) {
          for_each_element([&](duckdb::idx_t child_idx, irs::doc_id_t doc) {
            emit(data[child_idx], doc);
          });
        });
      });
    } else {
      auto traits = field.GetTokens().Traits();
      traits.unique = false;
      InvertTokens(
        field, nullptr, [&](irs::FieldInverter& fld, irs::TokenSink& w) {
          fld.Configure(traits);
          const auto layout = fld.Layout();
          for_each_element([&](duckdb::idx_t child_idx, irs::doc_id_t doc) {
            field.string_analyzer->Fill(data[child_idx], doc, w, {layout});
          });
        });
    }
  } else if constexpr (ChildKind == duckdb::LogicalTypeId::BOOLEAN) {
    const auto* data = duckdb::UnifiedVectorFormat::GetData<bool>(child_fmt);
    InvertField(field, [&](irs::FieldInverter& fld) {
      return fld.InvertKeywords([&](auto&& emit) {
        for_each_element([&](duckdb::idx_t child_idx, irs::doc_id_t doc) {
          emit(irs::BoolTerm(data[child_idx]), doc);
        });
      });
    });
  } else {
    using P = PromotedNumeric<ChildKind>;
    InvertField(field, [&](irs::FieldInverter& fld) {
      return fld.InvertNumerics<P>([&](auto&& emit) {
        for_each_element([&](duckdb::idx_t child_idx, irs::doc_id_t doc) {
          emit(PromoteNumericValue(
                 ExtractNumericValue<ChildKind>(child_fmt, child_idx)),
               doc);
        });
      });
    });
  }
  FinishColumnBlocks(null_field);
}

bool SearchSinkInsertBaseImpl::DispatchListBatch(
  duckdb::LogicalTypeId child_kind, const Field& field, const Field& null_field,
  duckdb::idx_t count, duckdb::idx_t array_size) {
  return DispatchValueKind(child_kind, [&]<duckdb::LogicalTypeId K>() {
    WriteListBatch<K>(field, null_field, count, array_size);
  });
}

void SearchSinkInsertBaseImpl::WriteJsonBatch(const duckdb::Vector& vec,
                                              duckdb::idx_t count) {
  SDB_ASSERT(_document);
  _document->NextFieldBatch();

  auto& fmt = _vec_fmt.unified;
  vec.ToUnifiedFormat(count, fmt);

  auto& jpf = _json_fields;
  auto* store_writer = irs::field_limits::valid(jpf.string_field.store_column)
                         ? EnsureBlobColumnWriter(jpf.string_field.store_column)
                         : nullptr;

  irs::StoreSink* leaf_store = nullptr;
  if (store_writer && !jpf.string_field.keyword) {
    _store_appender.Bind(*this, *store_writer);
    leaf_store = &_store_appender;
  }

  _json_nums.resize(count);
  auto* nums = _json_nums.data();
  size_t nnums = 0;
  _json_num_docs.clear();
  _json_bool_terms.clear();
  _json_bool_docs.clear();
  _null_docs.clear();

  const irs::doc_id_t first_doc = _document->DocId();
  InvertTokens(
    jpf.string_field, leaf_store,
    [&](irs::FieldInverter& fld, irs::TokenSink& w) {
      fld.Configure(jpf.string_field.GetTokens().Traits());
      const auto str_layout = fld.Layout();
      irs::analysis::ForEachValidRow(
        fmt, static_cast<uint32_t>(count),
        [&](uint32_t i, uint32_t sel_idx) {
          const irs::doc_id_t doc = first_doc + i;
          bool wrote_string_blob = false;
          const auto& cell_string =
            duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(
              fmt)[sel_idx];
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
                    bool ok = true;
                    if (jpf.string_field.keyword) {
                      w.BeginValue(doc, static_cast<uint32_t>(value.size()));
                      w.Emit<irs::TokenLayout::Terms>(
                        value.data(), static_cast<uint32_t>(value.size()));
                      w.EndValue();
                    } else {
                      ok = jpf.string_field.string_analyzer->Fill(
                        duckdb::string_t{value.data(),
                                         static_cast<uint32_t>(value.size())},
                        doc, w, {str_layout});
                    }
                    if (store_writer) {
                      if (jpf.string_field.keyword) {
                        AppendBlobAt(
                          *store_writer, doc,
                          duckdb::string_t{
                            value.data(), static_cast<uint32_t>(value.size())});
                        wrote_string_blob = true;
                      } else {
                        // Store-producing analyzers delivered through OnStore
                        // above; everyone else falls through to the empty-blob
                        // backfill.
                        wrote_string_blob =
                          ok &&
                          jpf.string_field.string_analyzer->Traits().store;
                      }
                    }
                  }
                } break;
                case simdjson::ondemand::json_type::number: {
                  double d;
                  if (json_doc.get_double().get(d) == simdjson::SUCCESS) {
                    nums[nnums++] = d;
                    _json_num_docs.push_back(doc);
                  }
                } break;
                case simdjson::ondemand::json_type::boolean: {
                  bool b;
                  if (json_doc.get_bool().get(b) == simdjson::SUCCESS) {
                    _json_bool_terms.push_back(irs::BoolTerm(b));
                    _json_bool_docs.push_back(doc);
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
                      "JSON expression indexed by an inverted index must point "
                      "to "
                      "a primitive (string/number/boolean/null) leaf; got an "
                      "object or array"));
                default:
                  break;
              }
            }
          }
          if (store_writer && !wrote_string_blob) {
            AppendBlobAt(*store_writer, doc, duckdb::string_t{});
          }
          return true;
        },
        [&](uint32_t i) {
          if (store_writer) {
            AppendBlobAt(*store_writer, first_doc + i, duckdb::string_t{});
          }
          return true;
        });
    });

  if (!_json_bool_terms.empty()) {
    InvertField(jpf.bool_field, [&](irs::FieldInverter& fld) {
      return fld.InvertKeywords([&](auto&& emit) {
        for (size_t j = 0; j < _json_bool_terms.size(); ++j) {
          emit(_json_bool_terms[j], _json_bool_docs[j]);
        }
      });
    });
  }
  if (nnums != 0) {
    InvertField(jpf.numeric_field, [&](irs::FieldInverter& fld) {
      return fld.InvertNumerics<double>([&](auto&& emit) {
        for (size_t j = 0; j < nnums; ++j) {
          emit(nums[j], _json_num_docs[j]);
        }
      });
    });
  }
  FinishColumnBlocks(jpf.null_field);
}

void SearchSinkInsertBaseImpl::AppendValueColumn(
  irs::field_id field_id, const duckdb::LogicalType& type,
  const duckdb::Vector& vec, duckdb::idx_t count) {
  AppendToColumn(field_id, type, vec, count);
}

std::span<const irs::field_id> SearchSinkInsertBaseImpl::TermFieldsForColumn(
  catalog::ColumnId col_id) const noexcept {
  if (!_terms_by_column) {
    return {};
  }
  auto it = _terms_by_column->find(col_id);
  if (it == _terms_by_column->end()) {
    return {};
  }
  return it->second;
}

// Emits term postings for one field; stores the value inline only when the
// field's config entry has store_values. Plain-column term fields keep it off
// so the value is not stored once per index; indexed expressions keep it on.
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
    if (irs::field_limits::valid(_json_fields.string_field.store_column)) {
      EnsureBlobColumnWriter(_json_fields.string_field.store_column);
    }
    if (is_stored) {
      AppendToColumn(field_id, type, vec, count);
    }
    WriteJsonBatch(vec, count);
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
    _null_field.PrepareForBlockValue(entry->null_field_id);
  }

  const auto append_stored = [&] {
    if (is_stored) {
      AppendToColumn(field_id, type, vec, count);
    }
  };

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
      _field.PrepareForStringValue(field_id, ResolveTokenizer(field_id));
    } else if (child_kind == duckdb::LogicalTypeId::BOOLEAN ||
               catalog::term_dict::IsNumeric(
                 catalog::term_dict::Classify(child_kind))) {
      _field.PrepareForBlockValue(field_id);
    } else {
      return;
    }
    append_stored();
    [[maybe_unused]] const bool matched =
      DispatchListBatch(child_kind, _field, _null_field, count, array_size);
    SDB_ASSERT(matched);
    return;
  }

  switch (kind) {
    case duckdb::LogicalTypeId::SQLNULL:
      append_stored();
      WriteNullColumn(_null_field, count);
      return;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
    case duckdb::LogicalTypeId::GEOMETRY: {
      auto& tokenizer = ResolveTokenizer(field_id);
      _field.PrepareForStringValue(field_id, tokenizer);
      if (kind == duckdb::LogicalTypeId::GEOMETRY) {
        irs::analysis::GeoAnalyzer::Cast(*tokenizer.analyzer).SetWkbInput(true);
      }
      append_stored();
      if (_field.keyword) {
        WriteKeywordColumn(_field, _null_field, vec, count);
      } else {
        WriteAnalyzedColumn(_field, _null_field, count);
      }
      return;
    }
    case duckdb::LogicalTypeId::BOOLEAN:
      _field.PrepareForBlockValue(field_id);
      append_stored();
      WriteBoolColumn(_field, _null_field, count);
      return;
    default: {
      if (!catalog::term_dict::IsNumeric(catalog::term_dict::Classify(kind))) {
        return;
      }
      _field.PrepareForBlockValue(field_id);
      append_stored();
      [[maybe_unused]] const bool matched =
        DispatchNumericKind(kind, [&]<duckdb::LogicalTypeId K>() {
          WriteNumericColumn<K>(_field, _null_field, count);
        });
      SDB_ASSERT(matched);
      return;
    }
  }
}

catalog::ColumnTokenizer& SearchSinkInsertBaseImpl::ResolveTokenizer(
  irs::field_id field_id) {
  auto& tokenizer = _tokenizer_cache.try_emplace(field_id).first->second;
  if (!tokenizer.analyzer) {
    tokenizer = _tokenizer_provider(field_id);
  }
  return tokenizer;
}

void SearchSinkInsertBaseImpl::InitImpl(size_t batch_size, const PkChunk& pk,
                                        irs::CommitOnFlush* commit_on_flush) {
  SDB_ASSERT(batch_size > 0);
  if (_document) {
    _document.reset();
  }
  _document.emplace(_trx->Insert(false, batch_size, commit_on_flush));
  // Insert may flush the segment mid-transaction (a pooled segment with
  // mismatched options, a full segment): cached column writers then point
  // at the flushed segment while the terms land in the fresh one.
  _column_writers.clear();
  _pk_column_writer = nullptr;
  if (_pk_policy.column == catalog::PkColumnKind::Has && pk.column) {
    _pk_column_writer =
      EnsureColumnWriter(catalog::term_dict::kPKFieldId, pk.column->GetType());
  }
  if (_pk_column_writer && pk.column) {
    AppendPkColumn(*pk.column, batch_size);
  }
  if (_pk_policy.index_term) {
    if (!pk.key_terms.empty()) {
      SDB_ASSERT(pk.key_terms.size() == batch_size);
      EmitPkTerms(_pk_field, pk.key_terms);
    }
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
  string_field.PrepareForStringValue(entry_field_id, string_analyzer);
  string_field.store_column = string_analyzer.tokenizer_column;
  numeric_field.PrepareForBlockValue(entry->numeric_field_id);
  bool_field.PrepareForBlockValue(entry->bool_field_id);
  null_field.PrepareForBlockValue(entry->null_field_id);
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
                                            duckdb::string_t bytes) {
  const uint64_t row = doc - irs::doc_limits::min();
  writer.PushInStaging(row, [bytes](duckdb::Vector& staging,
                                    duckdb::idx_t slot) {
    auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(staging);
    slots[slot] = duckdb::StringVector::AddStringOrBlob(
      staging, bytes.GetData(), bytes.GetSize());
  });
}

void SearchSinkInsertBaseImpl::Field::PrepareForKeywordStringValue(
  irs::field_id field_id) {
  id = field_id;
  string_analyzer = nullptr;
  store_column = irs::field_limits::invalid();
  index_features = irs::IndexFeatures::None;
  keyword = true;
}

void SearchSinkInsertBaseImpl::Field::PrepareForStringValue(
  irs::field_id field_id, catalog::ColumnTokenizer& column_analyzer) {
  id = field_id;
  index_features = column_analyzer.features;
  keyword = column_analyzer.verbatim;
  SDB_ASSERT(column_analyzer.analyzer);
  string_analyzer = column_analyzer.analyzer.get();
  const bool has_store = keyword || string_analyzer->Traits().store;
  store_column =
    has_store ? column_analyzer.tokenizer_column : irs::field_limits::invalid();
}

void SearchSinkInsertBaseImpl::Field::PrepareForBlockValue(
  irs::field_id field_id) {
  id = field_id;
  string_analyzer = nullptr;
  store_column = irs::field_limits::invalid();
  index_features = irs::IndexFeatures::None;
  keyword = false;
}

SearchSinkDeleteBaseImpl::SearchSinkDeleteBaseImpl(
  irs::IndexWriter::Transaction& trx)
  : _trx{&trx} {}

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
    _trx->Remove(std::move(_remove_filter));
  }
  _remove_filter.reset();
}

std::unique_ptr<SearchSinkInsertBaseImpl> MakeSearchTableInsertSink(
  irs::IndexWriter::Transaction& trx, const search::SearchTable& shard,
  duckdb::ClientContext& context) {
  auto config = shard.GetIndexConfig();
  // Each index keeps its own allocated field ids, so unioning every declared
  // index's indexed expressions and text dictionaries is collision-free.
  std::vector<IndexedExpression> indexed_exprs;
  for (const auto& index : catalog::RelationInvertedIndexes(
         &context, shard.GetSchemaId(), shard.GetTableId())) {
    auto exprs = MakeIndexedExpressions(catalog::InvertedInfo(*index), context);
    indexed_exprs.insert(indexed_exprs.end(),
                         std::make_move_iterator(exprs.begin()),
                         std::make_move_iterator(exprs.end()));
  }
  auto dicts = search::ResolveShardTokenizers(shard, &context);
  // Norm-featured fields must get the merged encoding config or the writer
  // asserts.
  trx.SetFieldOptions(shard.GetFieldOptions());
  return std::make_unique<SearchSinkInsertBaseImpl>(
    trx, MakeConfigTokenizerProvider(context, config, std::move(dicts)),
    MakeConfigEntryInfoProvider(std::move(config)),
    PkPolicy{.index_term = true, .column = catalog::PkColumnKind::None},
    std::move(indexed_exprs), shard.GetTermsByColumn());
}

void WriteChunkToSearchSink(
  SearchSinkInsertBaseImpl& sink, duckdb::DataChunk& chunk,
  std::span<const catalog::ColumnId> column_ids,
  std::span<const catalog::duckdb_primary_key::PKColumn> pk_columns,
  bool uses_generated_pk, uint64_t pk_base, ObjectId table_id,
  duckdb::ClientContext& context) {
  const auto num_rows = chunk.size();

  auto& scratch = sink.GetKeyScratch();
  PkChunk pk;
  if (uses_generated_pk) {
    auto& key_terms = scratch.key_terms;
    key_terms.clear();
    key_terms.reserve(num_rows);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      key_terms.push_back(
        catalog::duckdb_primary_key::GeneratedKeyTerm(pk_base + row));
    }
    pk.key_terms = key_terms;
  } else {
    auto& pk_formats = scratch.pk_formats;
    auto& row_keys = scratch.row_keys;
    auto& key_views = scratch.key_views;
    catalog::duckdb_primary_key::PreparePKFormats(chunk, pk_columns,
                                                  pk_formats);
    row_keys.resize(num_rows);
    key_views.clear();
    key_views.reserve(num_rows);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      auto& key = row_keys[row];
      key.clear();
      catalog::duckdb_primary_key::Create(pk_formats, pk_columns, row, key);
      key_views.push_back(
        duckdb::string_t{key.data(), static_cast<uint32_t>(key.size())});
    }
    pk.key_terms = key_views;
  }

  sink.InitImpl(num_rows, pk);
  // The value goes under the column id; the terms go under whatever term fields
  // the declaring indexes allocated for that column.
  auto write_column = [&](catalog::ColumnId col_id,
                          const duckdb::LogicalType& type,
                          const duckdb::Vector& vec) {
    sink.AppendValueColumn(static_cast<irs::field_id>(col_id), type, vec,
                           num_rows);
    for (const auto term_field : sink.TermFieldsForColumn(col_id)) {
      sink.SwitchFieldImpl(term_field, type, vec, num_rows);
    }
  };
  for (size_t col = 0; col < column_ids.size(); ++col) {
    write_column(column_ids[col], chunk.data[col].GetType(), chunk.data[col]);
  }
  if (uses_generated_pk) {
    duckdb::Vector gen_pk(duckdb::LogicalType::BIGINT, num_rows);
    auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(gen_pk);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      data[row] = static_cast<int64_t>(pk_base + row);
    }
    write_column(catalog::kGeneratedPKId, duckdb::LogicalType::BIGINT, gen_pk);
  }
  for (const auto& indexed_expr : sink.IndexedExpressionImpl()) {
    SDB_ASSERT(indexed_expr.normalized_expr);
    auto result =
      EvaluateExprOverChunk(*indexed_expr.normalized_expr, chunk, table_id,
                            column_ids, context, indexed_expr.is_geojson);
    sink.SwitchFieldImpl(indexed_expr.field_id, result.GetType(), result,
                         num_rows);
  }
  sink.FinishImpl();
}

}  // namespace sdb::connector
