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

#include <absl/algorithm/container.h>

#include <cstdio>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <iresearch/analysis/geo_tokenizer.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iterator>
#include <span>

#include "catalog/catalog.h"
#include "connector/common.h"
#include "connector/inet_terms.h"
#include "connector/primary_key.h"
#include "connector/term_dict.h"

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

bool ChildValidWhereverParentIs(const duckdb::ValidityMask& parent,
                                const duckdb::ValidityMask& child,
                                uint32_t count) {
  if (child.CannotHaveNull()) {
    return true;
  }
  if (parent.CannotHaveNull()) {
    return child.CheckAllValid(count);
  }
  constexpr uint32_t kBits = duckdb::ValidityMask::BITS_PER_VALUE;
  const auto* parent_bits = parent.GetData();
  const auto* child_bits = child.GetData();
  const uint32_t words = (count + kBits - 1) / kBits;
  for (uint32_t w = 0; w < words; ++w) {
    auto uncovered = parent_bits[w] & ~child_bits[w];
    if (w + 1 == words && count % kBits != 0) {
      uncovered &= (duckdb::validity_t{1} << (count % kBits)) - 1;
    }
    if (uncovered != 0) {
      return false;
    }
  }
  return true;
}

bool ChildrenAlignWithRows(
  const duckdb::UnifiedVectorFormat& parent,
  std::span<const duckdb::UnifiedVectorFormat> children, uint32_t count) {
  return irs::analysis::IsIdentitySel(parent) &&
         absl::c_all_of(children, [&](const auto& child) {
           return irs::analysis::IsIdentitySel(child) &&
                  ChildValidWhereverParentIs(parent.validity, child.validity,
                                             count);
         });
}

// Collects the terms of consecutive INET rows and hands them to the inverter
// whenever the next row might not fit.
class InetTermWriter {
 public:
  explicit InetTermWriter(irs::TokenBatch& tokens, std::span<irs::DocRun> runs,
                          std::span<uint8_t> bytes,
                          irs::FieldInverter& inverter,
                          irs::doc_id_t first_doc) noexcept
    : _tokens{tokens},
      _runs{runs},
      _bytes{bytes},
      _inverter{inverter},
      _first_doc{first_doc},
      _next_byte{bytes.data()} {}

  IRS_NO_INLINE bool Add(uint32_t row, uint8_t ip_type,
                         duckdb::hugeint_t address, uint16_t mask) {
    if (_term_count + inet_terms::kMaxTerms > irs::TokenBatch::kCapacity &&
        !Flush()) {
      return false;
    }
    const auto family = static_cast<inet_terms::Family>(ip_type);
    auto upper = static_cast<uint64_t>(address.upper);
    if (family == inet_terms::Family::V6) {
      upper ^= pg::kInetV6SortBit;
    }
    const auto written =
      inet_terms::WriteTerms(family, upper, address.lower, mask,
                             _tokens.terms + _term_count, _next_byte);
    SDB_ASSERT(_run_count < _runs.size());
    SDB_ASSERT(_next_byte <= _bytes.data() + _bytes.size());
    _runs[_run_count++] =
      irs::DocRun{.doc = _first_doc + row, .ntokens = written};
    _term_count += written;
    return true;
  }

  bool Flush() {
    if (_run_count == 0) {
      return true;
    }
    _tokens.count = _term_count;
    const bool ok =
      _inverter.InvertBlock(_tokens, irs::DocRuns{{_runs.data(), _run_count}});
    _term_count = 0;
    _run_count = 0;
    _next_byte = _bytes.data();
    return ok;
  }

 private:
  irs::TokenBatch& _tokens;
  std::span<irs::DocRun> _runs;
  std::span<uint8_t> _bytes;
  irs::FieldInverter& _inverter;
  irs::doc_id_t _first_doc;
  uint8_t* _next_byte;
  uint32_t _term_count = 0;
  uint32_t _run_count = 0;
};

[[noreturn]] void ThrowJsonComposite() {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
    ERR_MSG("JSON expression indexed by an inverted index must point to "
            "a primitive (string/number/boolean/null) leaf; got an "
            "object or array"));
}

}  // namespace

std::optional<SearchSinkInsertBaseImpl::DictionaryValues>
SearchSinkInsertBaseImpl::DictionaryOf(const duckdb::Vector& vec) {
  switch (vec.GetVectorType()) {
    case duckdb::VectorType::CONSTANT_VECTOR:
      return DictionaryValues{&vec, {}, 1};
    case duckdb::VectorType::DICTIONARY_VECTOR: {
      static constexpr duckdb::idx_t kMaxDictionarySize = 20000;
      const auto size = duckdb::DictionaryVector::DictionarySize(vec);
      const auto& id = duckdb::DictionaryVector::DictionaryId(vec);
      if (!size.IsValid() || id.empty() ||
          size.GetIndex() >= kMaxDictionarySize) {
        return std::nullopt;
      }
      return DictionaryValues{&duckdb::DictionaryVector::Child(vec), id,
                              static_cast<uint32_t>(size.GetIndex())};
    }
    default:
      return std::nullopt;
  }
}

void SearchSinkInsertBaseImpl::FieldFailed(irs::field_id id) const {
  THROW_SQL_ERROR(
    ERR_MSG("could not index field ", id, ": ", _document->Error()));
}

template<typename Func>
void SearchSinkInsertBaseImpl::InvertField(const Field& field, Func&& func) {
  if (!_document->WithField(_id, field.dict, field.GetIndexFeatures(),
                            std::forward<Func>(func))) {
    FieldFailed(_id);
  }
}

template<typename Func>
void SearchSinkInsertBaseImpl::InvertDocs(Func&& func) {
  if (!_document->WithMemberDocs(_id, _layout, std::forward<Func>(func))) {
    FieldFailed(_id);
  }
}

template<typename Func>
void SearchSinkInsertBaseImpl::InvertTokens(const Field& field,
                                            irs::StoreSink* store,
                                            Func&& func) {
  if (!_document->WithTokens(_id, field.dict, field.GetIndexFeatures(), store,
                             std::forward<Func>(func))) {
    FieldFailed(_id);
  }
}

template<typename Func>
void SearchSinkInsertBaseImpl::InvertEntryTokens(const Field& field,
                                                 Func&& func) {
  if (!_document->WithEntryTokens(_id, field.dict, field.GetIndexFeatures(),
                                  std::forward<Func>(func))) {
    FieldFailed(_id);
  }
}

bool SearchSinkInsertBaseImpl::InvertDictionary(
  const Field& field, const DictionaryValues& dict,
  const duckdb::UnifiedVectorFormat& fmt, uint32_t count,
  irs::doc_id_t first_doc) {
  bool known = false;
  InvertField(field, [&](irs::FieldInverter& fld) {
    known = fld.HasDictionary(dict.id, dict.size);
    return true;
  });
  if (!known) {
    if (2 * count <
        std::min<uint32_t>(dict.size, uint32_t{STANDARD_VECTOR_SIZE})) {
      return false;
    }
    duckdb::UnifiedVectorFormat values;
    dict.values->ToUnifiedFormat(values);
    if (field.keyword) {
      InvertField(field, [&](irs::FieldInverter& fld) {
        return fld.InvertKeywordDictionary(dict.id, values, dict.size);
      });
    } else {
      InvertEntryTokens(field, [&](irs::FieldInverter& fld, irs::TokenSink& w) {
        fld.Configure(field.GetTokens().Traits());
        fld.BeginDictionary(dict.id, dict.size);
        field.GetTokens().Fill(values, dict.size, irs::doc_limits::min(), w,
                               {fld.Layout()});
      });
    }
  }
  InvertField(field, [&](irs::FieldInverter& fld) {
    return fld.InvertDictionaryRows(fmt, count, first_doc);
  });
  return true;
}

SearchSinkInsertBaseImpl::SearchSinkInsertBaseImpl(
  irs::IndexWriter::Transaction& trx, TokenizerProvider&& tokenizer_provider,
  EntryInfoProvider&& entry_info_provider, PkColumnKind pk,
  std::vector<IndexedExpression>&& indexed_exprs,
  const catalog::InvertedIndexConfig* config)
  : _tokenizer_provider{std::move(tokenizer_provider)},
    _entry_info_provider{std::move(entry_info_provider)},
    _trx{&trx},
    _pk{pk},
    _indexed_expressions{std::move(indexed_exprs)} {
  if (!config) {
    return;
  }
  for (const auto& key : config->keys) {
    if (!irs::field_limits::valid(key.column_id)) {
      continue;
    }
    const auto* entry = config->FindEntry(key.field_id);
    if (entry && entry->IsTermDict()) {
      _terms_by_column[key.column_id].push_back(key.field_id);
    }
  }
}

template<typename Insert>
void SearchSinkInsertBaseImpl::WriteColumnBlock(duckdb::idx_t count,
                                                Insert&& insert) {
  auto& fmt = _vec_fmt.unified;
  _document->NextFieldBatch();
  const irs::doc_id_t first_doc = _document->DocId();
  const auto rows = static_cast<uint32_t>(count);
  insert(fmt, rows, first_doc);
  if (irs::analysis::HasInvalidRows(fmt, rows)) {
    InvertDocs([&](irs::MemberDocs& members) {
      members.PushNulls(fmt, rows, first_doc);
    });
  }
}

void SearchSinkInsertBaseImpl::WriteAnalyzedColumn(const Field& field,
                                                   const duckdb::Vector& vec,
                                                   duckdb::idx_t count) {
  SDB_ASSERT(!field.keyword);

  auto* store_writer = irs::field_limits::valid(field.store_column)
                         ? EnsureBlobColumnWriter(field.store_column)
                         : nullptr;

  if (store_writer) {
    _store_appender.Bind(*this, *store_writer);
  }
  auto* store_sink = store_writer ? &_store_appender : nullptr;
  const auto dict = store_writer ? std::nullopt : DictionaryOf(vec);
  WriteColumnBlock(count, [&](const duckdb::UnifiedVectorFormat& fmt,
                              uint32_t n, irs::doc_id_t first_doc) {
    if (dict && InvertDictionary(field, *dict, fmt, n, first_doc)) {
      return;
    }
    InvertTokens(field, store_sink,
                 [&](irs::FieldInverter& fld, irs::TokenSink& w) {
                   fld.Configure(field.GetTokens().Traits());
                   field.GetTokens().Fill(fmt, n, first_doc, w, {fld.Layout()});
                 });
  });
}

void SearchSinkInsertBaseImpl::WriteKeywordColumn(const Field& field,
                                                  const duckdb::Vector& vec,
                                                  duckdb::idx_t count) {
  auto& fmt = _vec_fmt.unified;
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);

  auto* store_writer = irs::field_limits::valid(field.store_column)
                         ? EnsureBlobColumnWriter(field.store_column)
                         : nullptr;
  const bool flat = vec.GetVectorType() == duckdb::VectorType::FLAT_VECTOR;
  const auto dict = DictionaryOf(vec);

  WriteColumnBlock(count, [&](const duckdb::UnifiedVectorFormat& fmt,
                              uint32_t n, irs::doc_id_t first_doc) {
    if (store_writer) {
      if (flat && fmt.validity.CheckAllValid(n)) {
        duckdb::Vector blob_view{duckdb::LogicalType::BLOB};
        blob_view.Reinterpret(vec);
        store_writer->Append(first_doc - irs::doc_limits::min(), blob_view, n);
      } else {
        irs::analysis::ForEachValidRow(fmt, n, [&](uint32_t i, uint32_t idx) {
          AppendBlobAt(*store_writer, first_doc + i, data[idx]);
          return true;
        });
      }
    }
    if (dict && InvertDictionary(field, *dict, fmt, n, first_doc)) {
      return;
    }
    InvertField(field, [&](irs::FieldInverter& fld) {
      return fld.InvertKeywordBlock(fmt, n, first_doc);
    });
  });
}

void SearchSinkInsertBaseImpl::WriteBoolColumn(duckdb::idx_t count) {
  WriteColumnBlock(count, [&](const duckdb::UnifiedVectorFormat& fmt,
                              uint32_t n, irs::doc_id_t first_doc) {
    InvertDocs(
      [&](irs::MemberDocs& members) { members.PushBools(fmt, n, first_doc); });
  });
}

void SearchSinkInsertBaseImpl::WriteInetColumn(const Field& field,
                                               const duckdb::Vector& vec,
                                               duckdb::idx_t count) {
  SDB_ASSERT(field.GetIndexFeatures() == irs::IndexFeatures::None);
  if (!_inet_batch) {
    _inet_batch = std::make_unique_for_overwrite<InetBatch>();
  }
  auto& batch = *_inet_batch;
  // Views over the struct's ip_type, address and mask children; nothing is
  // copied.
  const auto& entries = duckdb::StructVector::GetEntries(vec);
  auto& [types, addresses, masks] = batch.fields;
  entries[pg::kInetIpTypeChild].ToUnifiedFormat(types);
  entries[pg::kInetAddressChild].ToUnifiedFormat(addresses);
  entries[pg::kInetMaskChild].ToUnifiedFormat(masks);
  const auto* type_data = duckdb::UnifiedVectorFormat::GetData<uint8_t>(types);
  const auto* address_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::hugeint_t>(addresses);
  const auto* mask_data = duckdb::UnifiedVectorFormat::GetData<uint16_t>(masks);

  WriteColumnBlock(count, [&](const duckdb::UnifiedVectorFormat& fmt,
                              uint32_t n, irs::doc_id_t first_doc) {
    InvertField(field, [&](irs::FieldInverter& inverter) {
      InetTermWriter writer{batch.tokens, batch.runs, batch.bytes, inverter,
                            first_doc};
      // Fast path: the children are plain arrays, valid wherever the row
      // is, so row i is element i of each.
      if (ChildrenAlignWithRows(fmt, batch.fields, n)) {
        return irs::analysis::ForEachValidRun(
                 fmt, n,
                 [&](uint32_t start, uint32_t length) {
                   for (uint32_t row = start; row != start + length; ++row) {
                     if (!writer.Add(row, type_data[row], address_data[row],
                                     mask_data[row])) {
                       return false;
                     }
                   }
                   return true;
                 }) &&
               writer.Flush();
      }
      // General path: each child is read through its own selection; a row
      // with a NULL child has no terms.
      for (uint32_t row = 0; row < n; ++row) {
        const auto type = types.sel->get_index(row);
        const auto address = addresses.sel->get_index(row);
        const auto mask = masks.sel->get_index(row);
        if (!fmt.validity.RowIsValid(fmt.sel->get_index(row)) ||
            !types.validity.RowIsValid(type) ||
            !addresses.validity.RowIsValid(address) ||
            !masks.validity.RowIsValid(mask)) {
          continue;
        }
        if (!writer.Add(row, type_data[type], address_data[address],
                        mask_data[mask])) {
          return false;
        }
      }
      return writer.Flush();
    });
  });
}

template<duckdb::LogicalTypeId Kind>
void SearchSinkInsertBaseImpl::WriteNumericColumn(const Field& field,
                                                  duckdb::idx_t count) {
  SDB_ASSERT(field.GetIndexFeatures() == irs::IndexFeatures::Ranges);

  WriteColumnBlock(count, [&](const duckdb::UnifiedVectorFormat& fmt,
                              uint32_t n, irs::doc_id_t first_doc) {
    InvertField(field, [&](irs::FieldInverter& fld) {
      return fld.InvertNumericBlock(fmt, n, first_doc, [&](duckdb::idx_t idx) {
        return PromoteNumericValue(ExtractNumericValue<Kind>(fmt, idx));
      });
    });
  });
}

void SearchSinkInsertBaseImpl::WriteNullColumn(duckdb::idx_t count) {
  _document->NextFieldBatch();
  if (count != 0) {
    InvertDocs([&](irs::MemberDocs& members) {
      members.PushAll(irs::DictMember::Nulls, static_cast<uint32_t>(count),
                      _document->DocId());
    });
  }
}

template<duckdb::LogicalTypeId ChildKind>
void SearchSinkInsertBaseImpl::WriteListBatch(const Field& field,
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

  InvertDocs([&](irs::MemberDocs& members) {
    const auto for_each_row = [&](auto&& on_row) {
      const irs::doc_id_t first_doc = _document->DocId();
      irs::analysis::ForEachValidRow(
        parent_fmt, static_cast<uint32_t>(count),
        [&](uint32_t i, uint32_t parent_idx) {
          const auto offset =
            list_data ? list_data[parent_idx].offset : parent_idx * array_size;
          const auto length =
            list_data ? list_data[parent_idx].length : array_size;
          on_row(first_doc + i, offset, static_cast<uint32_t>(length));
          return true;
        },
        [&](uint32_t i) {
          members.Push(irs::DictMember::Nulls, first_doc + i);
          return true;
        });
    };
    const auto for_each_child = [&](irs::doc_id_t doc, duckdb::idx_t offset,
                                    uint32_t length, auto&& on_element) {
      irs::analysis::ForEachValidRow(
        child_fmt, offset, length,
        [&](uint32_t, uint32_t child_idx) {
          on_element(child_idx, doc);
          return true;
        },
        [&](uint32_t) {
          members.Push(irs::DictMember::NullElements, doc);
          return true;
        });
    };
    const auto for_each_element = [&](auto&& on_element) {
      for_each_row(
        [&](irs::doc_id_t doc, duckdb::idx_t offset, uint32_t length) {
          for_each_child(doc, offset, length, on_element);
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
        auto& tokens = field.GetTokens();
        auto traits = tokens.Traits();
        traits.unique = false;
        auto* store_writer = irs::field_limits::valid(field.store_column)
                               ? EnsureBlobColumnWriter(field.store_column)
                               : nullptr;
        if (store_writer) {
          _store_appender.Bind(*this, *store_writer);
        }
        InvertTokens(
          field, store_writer ? &_store_appender : nullptr,
          [&](irs::FieldInverter& fld, irs::TokenSink& w) {
            fld.Configure(traits);
            const auto layout = fld.Layout();
            const bool null_children = child_fmt.validity.CanHaveNull();
            for_each_row(
              [&](irs::doc_id_t doc, duckdb::idx_t offset, uint32_t length) {
                tokens.FillRow(child_fmt, offset, length, doc, w, {layout});
                if (null_children) {
                  for_each_child(doc, offset, length,
                                 [](duckdb::idx_t, irs::doc_id_t) {});
                }
              });
          });
      }
    } else if constexpr (ChildKind == duckdb::LogicalTypeId::BOOLEAN) {
      const auto* data = duckdb::UnifiedVectorFormat::GetData<bool>(child_fmt);
      for_each_element([&](duckdb::idx_t child_idx, irs::doc_id_t doc) {
        members.Push(irs::BoolMember(data[child_idx]), doc);
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
  });
}

bool SearchSinkInsertBaseImpl::DispatchListBatch(
  duckdb::LogicalTypeId child_kind, const Field& field, duckdb::idx_t count,
  duckdb::idx_t array_size) {
  return DispatchValueKind(child_kind, [&]<duckdb::LogicalTypeId K>() {
    WriteListBatch<K>(field, count, array_size);
  });
}

bool SearchSinkInsertBaseImpl::InvertJsonLeaf(
  irs::MemberDocs& members, irs::TokenSink& sink, irs::TokenLayout layout,
  irs::doc_id_t doc, std::string_view json, irs::ColumnWriter* store_writer) {
  const auto& strings = _field;
  const auto leaf = _json_reader.Read(json);
  switch (leaf.kind) {
    case irs::JsonLeafKind::String: {
      const std::string_view value = leaf.string;
      const auto size = static_cast<uint32_t>(value.size());
      if (strings.keyword) {
        sink.BeginValue(doc, size);
        sink.Emit<irs::TokenLayout::Terms>(value.data(), size);
        sink.EndValue();
        if (store_writer) {
          AppendBlobAt(*store_writer, doc,
                       duckdb::string_t{value.data(), size});
          return true;
        }
        return false;
      }
      const bool ok = strings.string_analyzer->Fill(
        duckdb::string_t{value.data(), size}, doc, sink, {layout});
      // Store-producing analyzers delivered through OnStore above; everyone
      // else falls through to the empty-blob backfill.
      return store_writer && ok && strings.string_analyzer->Traits().store;
    }
    case irs::JsonLeafKind::Number:
      _json_nums.push_back(leaf.number);
      _json_num_docs.push_back(doc);
      return false;
    case irs::JsonLeafKind::Bool:
      members.Push(irs::BoolMember(leaf.boolean), doc);
      return false;
    case irs::JsonLeafKind::Null:
      members.Push(irs::DictMember::JsonNulls, doc);
      return false;
    case irs::JsonLeafKind::Composite:
      ThrowJsonComposite();
    case irs::JsonLeafKind::None:
      return false;
  }
  return false;
}

void SearchSinkInsertBaseImpl::InvertJsonNumbers() {
  if (_json_nums.empty()) {
    return;
  }
  Field numbers;
  numbers.PrepareForNumericValue(
    {.layout = _layout, .member = irs::DictMember::Numbers});
  InvertField(numbers, [&](irs::FieldInverter& fld) {
    return fld.InvertNumerics<double>([&](auto&& emit) {
      for (size_t j = 0; j != _json_nums.size(); ++j) {
        emit(_json_nums[j], _json_num_docs[j]);
      }
    });
  });
}

void SearchSinkInsertBaseImpl::WriteJsonBatch(const duckdb::Vector& vec,
                                              duckdb::idx_t count) {
  SDB_ASSERT(_document);
  _document->NextFieldBatch();
  _json_strings.Reset();
  _json_nums.clear();
  _json_num_docs.clear();

  auto& fmt = _vec_fmt.unified;
  vec.ToUnifiedFormat(count, fmt);
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);

  const auto& strings = _field;
  auto* store_writer = irs::field_limits::valid(strings.store_column)
                         ? EnsureBlobColumnWriter(strings.store_column)
                         : nullptr;

  irs::StoreSink* leaf_store = nullptr;
  if (store_writer && !strings.keyword) {
    _store_appender.Bind(*this, *store_writer);
    leaf_store = &_store_appender;
  }

  const irs::doc_id_t first_doc = _document->DocId();
  InvertDocs([&](irs::MemberDocs& members) {
    InvertTokens(
      strings, leaf_store, [&](irs::FieldInverter& fld, irs::TokenSink& w) {
        fld.Configure(strings.GetTokens().Traits());
        const auto layout = fld.Layout();
        irs::analysis::ForEachValidRow(
          fmt, static_cast<uint32_t>(count),
          [&](uint32_t i, uint32_t idx) {
            const irs::doc_id_t doc = first_doc + i;
            if (!InvertJsonLeaf(members, w, layout, doc, AsView(data[idx]),
                                store_writer) &&
                store_writer) {
              AppendBlobAt(*store_writer, doc, duckdb::string_t{});
            }
            return true;
          },
          [&](uint32_t i) {
            members.Push(irs::DictMember::Nulls, first_doc + i);
            if (store_writer) {
              AppendBlobAt(*store_writer, first_doc + i, duckdb::string_t{});
            }
            return true;
          });
      });
  });
  InvertJsonNumbers();
}

void SearchSinkInsertBaseImpl::WriteJsonListBatch(duckdb::idx_t count,
                                                  duckdb::idx_t array_size) {
  SDB_ASSERT(_document);
  _document->NextFieldBatch();
  _json_strings.Reset();
  _json_nums.clear();
  _json_num_docs.clear();

  const auto& parent_fmt = _vec_fmt.unified;
  const auto& child_fmt = _vec_fmt.children[0].unified;
  const auto* list_data =
    array_size == 0
      ? duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(parent_fmt)
      : nullptr;
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(child_fmt);

  const auto& strings = _field;
  auto traits = strings.GetTokens().Traits();
  traits.unique = false;
  const irs::doc_id_t first_doc = _document->DocId();
  InvertDocs([&](irs::MemberDocs& members) {
    InvertTokens(
      strings, nullptr, [&](irs::FieldInverter& fld, irs::TokenSink& w) {
        fld.Configure(traits);
        const auto layout = fld.Layout();
        irs::analysis::ForEachValidRow(
          parent_fmt, static_cast<uint32_t>(count),
          [&](uint32_t i, uint32_t parent_idx) {
            const irs::doc_id_t doc = first_doc + i;
            const auto offset = list_data ? list_data[parent_idx].offset
                                          : parent_idx * array_size;
            const auto length =
              list_data ? list_data[parent_idx].length : array_size;
            irs::analysis::ForEachValidRow(
              child_fmt, offset, static_cast<uint32_t>(length),
              [&](uint32_t, uint32_t child_idx) {
                InvertJsonLeaf(members, w, layout, doc, AsView(data[child_idx]),
                               nullptr);
                return true;
              },
              [&](uint32_t) {
                members.Push(irs::DictMember::NullElements, doc);
                return true;
              });
            return true;
          },
          [&](uint32_t i) {
            members.Push(irs::DictMember::Nulls, first_doc + i);
            return true;
          });
      });
  });
  InvertJsonNumbers();
}

void SearchSinkInsertBaseImpl::RejectJsonComposites(const duckdb::Vector& vec,
                                                    duckdb::idx_t count) {
  duckdb::UnifiedVectorFormat fmt;
  vec.ToUnifiedFormat(count, fmt);
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  irs::analysis::ForEachValidRow(
    fmt, static_cast<uint32_t>(count), [&](uint32_t, uint32_t idx) {
      const auto json = AsView(data[idx]);
      const auto first = json.find_first_not_of(" \t\n\r");
      if (first != std::string_view::npos &&
          (json[first] == '{' || json[first] == '[') &&
          _json_reader.Read(json).kind == irs::JsonLeafKind::Composite) {
        ThrowJsonComposite();
      }
      return true;
    });
}

void SearchSinkInsertBaseImpl::SwitchExpressionImpl(
  irs::field_id field_id, const duckdb::LogicalType& type,
  const duckdb::Vector& vec, duckdb::idx_t count) {
  if (type.IsJSONType()) {
    const auto* entry = _entry_info_provider(field_id);
    if (!entry || (entry->IsTokenized() && !entry->HasJsonLeafFields())) {
      RejectJsonComposites(vec, count);
    }
  }
  SwitchFieldImpl(field_id, type, vec, count);
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
  _id = field_id;
  _layout = term_dict::LayoutOf(type, entry && entry->HasJsonLeafFields());
  const auto append_stored = [&] {
    if (is_stored) {
      AppendToColumn(field_id, type, vec, count);
    }
  };

  if (_layout == irs::DictLayout::Json) {
    _field.PrepareForJsonValue(ResolveTokenizer(field_id), _layout);
    if (irs::field_limits::valid(_field.store_column)) {
      EnsureBlobColumnWriter(_field.store_column);
    }
    append_stored();
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

  if (is_list_or_array) {
    const auto child_kind = (kind == duckdb::LogicalTypeId::LIST
                               ? duckdb::ListType::GetChildType(type)
                               : duckdb::ArrayType::GetChildType(type))
                              .id();
    const duckdb::idx_t array_size =
      (kind == duckdb::LogicalTypeId::ARRAY ? duckdb::ArrayType::GetSize(type)
                                            : 0);
    if (_layout == irs::DictLayout::JsonList) {
      _field.PrepareForJsonValue(ResolveTokenizer(field_id), _layout);
      append_stored();
      WriteJsonListBatch(count, array_size);
      return;
    }
    if (child_kind == duckdb::LogicalTypeId::VARCHAR ||
        child_kind == duckdb::LogicalTypeId::BLOB) {
      _field.PrepareForStringValue(
        ResolveTokenizer(field_id),
        {.layout = _layout, .member = irs::DictMember::Strings});
    } else if (term_dict::IsNumeric(child_kind)) {
      _field.PrepareForNumericValue(
        {.layout = _layout, .member = irs::DictMember::Numbers});
    } else if (child_kind != duckdb::LogicalTypeId::BOOLEAN) {
      return;
    }
    append_stored();
    [[maybe_unused]] const bool matched =
      DispatchListBatch(child_kind, _field, count, array_size);
    SDB_ASSERT(matched);
    return;
  }

  switch (kind) {
    case duckdb::LogicalTypeId::SQLNULL:
      append_stored();
      WriteNullColumn(count);
      return;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
    case duckdb::LogicalTypeId::GEOMETRY: {
      auto& tokenizer = ResolveTokenizer(field_id);
      _field.PrepareForStringValue(tokenizer, irs::StringLayout::kStrings);
      if (kind == duckdb::LogicalTypeId::GEOMETRY) {
        irs::analysis::GeoTokenizer::Cast(*tokenizer.analyzer)
          .SetWkbInput(true);
      }
      append_stored();
      if (_field.keyword) {
        WriteKeywordColumn(_field, vec, count);
      } else {
        WriteAnalyzedColumn(_field, vec, count);
      }
      return;
    }
    case duckdb::LogicalTypeId::BOOLEAN:
      append_stored();
      WriteBoolColumn(count);
      return;
    case duckdb::LogicalTypeId::STRUCT:
      if (!pg::IsInetOrCidr(type)) {
        return;
      }
      _field.PrepareForBlockValue(irs::StringLayout::kStrings);
      append_stored();
      WriteInetColumn(_field, vec, count);
      return;
    default: {
      if (!term_dict::IsNumeric(kind)) {
        return;
      }
      _field.PrepareForNumericValue(irs::NumericLayout::kNumbers);
      append_stored();
      [[maybe_unused]] const bool matched =
        DispatchNumericKind(kind, [&]<duckdb::LogicalTypeId K>() {
          WriteNumericColumn<K>(_field, count);
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

void SearchSinkInsertBaseImpl::InitImpl(size_t batch_size,
                                        const duckdb::Vector* pk) {
  SDB_ASSERT(batch_size > 0);
  _document.reset();
  _document.emplace(_trx->Insert(false, batch_size));
  // Insert may flush the segment mid-transaction (a pooled segment with
  // mismatched options, a full segment): cached column writers then point
  // at the flushed segment while the terms land in the fresh one.
  _column_writers.clear();
  _pk_column_writer = nullptr;
  if (_pk == catalog::PkColumnKind::Has && pk) {
    _pk_column_writer =
      EnsureColumnWriter(term_dict::kPKFieldId, pk->GetType());
    AppendPkColumn(*pk, batch_size);
  }
}

void SearchSinkInsertBaseImpl::FinishImpl() {
  _column_writers.clear();
  _pk_column_writer = nullptr;
  _document.reset();
  _trx->ReserveFlush();
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
  auto it = _column_writers.try_emplace(field_id, nullptr).first;
  if (!it->second) {
    it->second = &col_writer->OpenColumn(field_id, type);
  }
  return it->second;
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

void SearchSinkInsertBaseImpl::Field::PrepareForKeywordStringValue() {
  string_analyzer = nullptr;
  store_column = irs::field_limits::invalid();
  index_features = irs::IndexFeatures::None;
  dict = irs::StringLayout::kStrings;
  keyword = true;
}

void SearchSinkInsertBaseImpl::Field::PrepareForStringValue(
  catalog::ColumnTokenizer& column_analyzer, irs::DictRef value_dict) {
  index_features = column_analyzer.features;
  dict = value_dict;
  SDB_ASSERT(column_analyzer.analyzer);
  string_analyzer = column_analyzer.analyzer.get();
  keyword = string_analyzer->Traits().keyword;
  const bool has_store = keyword || string_analyzer->Traits().store;
  store_column =
    has_store ? column_analyzer.tokenizer_column : irs::field_limits::invalid();
}

void SearchSinkInsertBaseImpl::Field::PrepareForBlockValue(
  irs::DictRef value_dict) {
  string_analyzer = nullptr;
  store_column = irs::field_limits::invalid();
  index_features = irs::IndexFeatures::None;
  dict = value_dict;
  keyword = false;
}

void SearchSinkInsertBaseImpl::Field::PrepareForNumericValue(
  irs::DictRef value_dict) {
  PrepareForBlockValue(value_dict);
  index_features = irs::IndexFeatures::Ranges;
}

void SearchSinkInsertBaseImpl::Field::PrepareForJsonValue(
  catalog::ColumnTokenizer& column_analyzer, irs::DictLayout layout) {
  SDB_ASSERT(irs::IsJson(layout));
  PrepareForStringValue(column_analyzer,
                        {.layout = layout, .member = irs::DictMember::Strings});
  store_column = layout == irs::DictLayout::Json
                   ? column_analyzer.tokenizer_column
                   : irs::field_limits::invalid();
}

std::unique_ptr<SearchSinkInsertBaseImpl> MakeSearchTableInsertSink(
  irs::IndexWriter::Transaction& trx, const search::Store& shard,
  duckdb::Catalog& catalog, duckdb::ClientContext& context) {
  SDB_PARK_ONCE_ON_FAILURE("search_insert_sink_before_config");
  auto config = shard.Config();
  SDB_PARK_ONCE_ON_FAILURE("search_insert_sink_after_config");
  std::vector<IndexedExpression> indexed_exprs;
  for (const auto& key : config->keys) {
    if (key.normalized_expression.empty() || !config->WritesField(key)) {
      continue;
    }
    indexed_exprs.emplace_back(
      DeserializeBoundExpression(key.normalized_expression, context),
      key.field_id);
  }
  auto tokenizers =
    catalog::IndexTokenizers{context, catalog, *config}.AcquireAll(context);
  auto entry_of = [config](irs::field_id field_id) {
    const auto* entry = config->FindEntry(field_id);
    return entry ? entry : AllStoredEntry();
  };
  trx.SetFieldOptions(config);
  return std::make_unique<SearchSinkInsertBaseImpl>(
    trx, BoundTokenizers(std::move(tokenizers)), std::move(entry_of),
    catalog::PkColumnKind::None, std::move(indexed_exprs), config.get());
}

void WriteChunkToSearchSink(SearchSinkInsertBaseImpl& sink,
                            duckdb::DataChunk& chunk,
                            std::span<const ColumnId> column_ids,
                            duckdb::idx_t table_id,
                            duckdb::ClientContext& context) {
  const auto num_rows = chunk.size();
  sink.InitImpl(num_rows);
  for (size_t col = 0; col < column_ids.size(); ++col) {
    const auto& vec = chunk.data[col];
    const auto& type = vec.GetType();
    sink.AppendToColumn(column_ids[col], type, vec, num_rows);
    for (const auto term_field : sink.TermFieldsForColumn(column_ids[col])) {
      sink.SwitchFieldImpl(term_field, type, vec, num_rows);
    }
  }
  for (const auto& indexed_expr : sink.IndexedExpressions()) {
    auto result = EvaluateExprOverChunk(*indexed_expr.normalized_expr, chunk,
                                        table_id, column_ids, context);
    sink.SwitchExpressionImpl(indexed_expr.field_id, result.GetType(), result,
                              num_rows);
  }
  sink.FinishImpl();
}

}  // namespace sdb::connector
