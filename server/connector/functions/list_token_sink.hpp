////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/utils/assert.hpp>
#include <limits>
#include <vector>

namespace sdb::connector {

class ListTokenSink final : public irs::TokenConsumer, public irs::RejectSink {
 public:
  ListTokenSink(duckdb::Vector& result, duckdb::list_entry_t* entries,
                duckdb::ValidityMask& validity, irs::TokenSink& writer)
    : _result{result},
      _entries{entries},
      _validity{validity},
      _writer{writer},
      _offset{duckdb::ListVector::GetListSize(result)} {
    _writer.Discard();
    _writer.Bind(*this, nullptr);
    _writer.BindRejects(this);
  }

  ~ListTokenSink() {
    _writer.BindRejects(nullptr);
    duckdb::ListVector::SetListSize(_result, _offset);
  }

  void ResetRows(uint32_t rows) {
    for (uint32_t r = 0; r < rows; ++r) {
      _entries[r] = {_offset, 0};
    }
  }

  void SetNull(uint32_t row) { _validity.SetInvalid(row); }

  void FillRows(irs::analysis::Tokenizer& tokenizer,
                const duckdb::Vector& source,
                const duckdb::UnifiedVectorFormat& values, uint32_t count) {
    _rows = nullptr;
    _row_ends = nullptr;
    _reject_rows = true;
    Fill(tokenizer, source, values, count);
  }

  void FillElements(irs::analysis::Tokenizer& tokenizer,
                    const duckdb::Vector& source,
                    const duckdb::UnifiedVectorFormat& values, uint32_t count,
                    const uint32_t* row_ends) {
    _rows = nullptr;
    _row_ends = row_ends;
    _reject_rows = false;
    Fill(tokenizer, source, values, count);
  }

  void FillGroup(irs::analysis::Tokenizer& tokenizer,
                 const duckdb::Vector& source,
                 const duckdb::UnifiedVectorFormat& values, uint32_t count,
                 const uint32_t* rows, bool reject_rows) {
    _rows = rows;
    _row_ends = nullptr;
    _reject_rows = reject_rows;
    Fill(tokenizer, source, values, count);
  }

  template<typename ForEachRow>
  void FillTokenLists(irs::analysis::Tokenizer& tokenizer,
                      ForEachRow&& for_each_row) {
    _rows = nullptr;
    _row_ends = nullptr;
    _reject_rows = true;
    _values = nullptr;
    _row = kNoRow;
    _cursor = 0;
    for_each_row([&](uint32_t row, std::span<const duckdb::string_t> tokens) {
      tokenizer.FillTokens(tokens, irs::doc_limits::min() + row, _writer,
                           {irs::TokenLayout::Terms, {}});
    });
    _writer.Finish();
  }

  void Consume(irs::TokenBatch& batch, irs::DocRuns runs) final {
    auto& child = duckdb::ListVector::GetChildMutable(_result);
    const auto needed = _offset + batch.count;
    if (needed > duckdb::ListVector::GetListCapacity(_result)) {
      duckdb::ListVector::SetListSize(_result, _offset);
      duckdb::ListVector::Reserve(_result, needed * 2);
    }
    auto* out = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(child);
    const auto* data =
      _values ? duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(*_values)
              : nullptr;
    uint32_t t = 0;
    for (const auto& run : runs) {
      const auto v = static_cast<uint32_t>(run.doc - irs::doc_limits::min());
      const auto row = RowOf(v);
      if (row != _row) {
        _row = row;
        _entries[row].offset = _offset;
      }
      const char* begin = nullptr;
      const char* end = nullptr;
      if (data) {
        const auto& value = data[_values->sel->get_index(v)];
        begin = value.GetData();
        end = begin + value.GetSize();
      }
      for (uint32_t k = 0; k < run.ntokens; ++k) {
        const auto& term = batch.terms[t++];
        const char* bytes = term.GetData();
        const auto size = term.GetSize();
        if (term.IsInlined() ||
            (begin && bytes >= begin && bytes + size <= end)) {
          out[_offset++] = term;
        } else {
          out[_offset++] =
            duckdb::StringVector::AddStringOrBlob(child, bytes, size);
        }
      }
      _entries[row].length += run.ntokens;
    }
  }

  void OnReject(irs::doc_id_t doc) final {
    if (_reject_rows) {
      _validity.SetInvalid(
        RowOf(static_cast<uint32_t>(doc - irs::doc_limits::min())));
    }
  }

 private:
  static constexpr uint32_t kNoRow = std::numeric_limits<uint32_t>::max();

  void Fill(irs::analysis::Tokenizer& tokenizer, const duckdb::Vector& source,
            const duckdb::UnifiedVectorFormat& values, uint32_t count) {
    if (count == 0) {
      return;
    }
    _values = &values;
    _row = kNoRow;
    _cursor = 0;
    duckdb::StringVector::AddHeapReference(
      duckdb::ListVector::GetChildMutable(_result), source);
    tokenizer.Fill(values, count, irs::doc_limits::min(), _writer,
                   {irs::TokenLayout::Terms, {}});
    _writer.Finish();
  }

  uint32_t RowOf(uint32_t v) noexcept {
    if (_rows) {
      return _rows[v];
    }
    if (_row_ends) {
      while (v >= _row_ends[_cursor]) {
        ++_cursor;
      }
      return _cursor;
    }
    return v;
  }

  duckdb::Vector& _result;
  duckdb::list_entry_t* _entries;
  duckdb::ValidityMask& _validity;
  irs::TokenSink& _writer;
  duckdb::idx_t _offset;
  const duckdb::UnifiedVectorFormat* _values = nullptr;
  const uint32_t* _rows = nullptr;
  const uint32_t* _row_ends = nullptr;
  uint32_t _row = kNoRow;
  uint32_t _cursor = 0;
  bool _reject_rows = true;
};

inline duckdb::UnifiedVectorFormat SliceFormat(
  const duckdb::UnifiedVectorFormat& fmt, const duckdb::SelectionVector& sel) {
  duckdb::UnifiedVectorFormat slice;
  slice.sel = &sel;
  slice.data = fmt.data;
  slice.physical_type = fmt.physical_type;
  slice.validity = fmt.validity;
  return slice;
}

inline void TokenizeRows(irs::analysis::Tokenizer& tokenizer,
                         irs::TokenSink& writer, const duckdb::Vector& input,
                         duckdb::idx_t count, duckdb::Vector& result) {
  duckdb::UnifiedVectorFormat values;
  if (input.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
    result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
    duckdb::ListVector::SetListSize(result, 0);
    if (duckdb::ConstantVector::IsNull(input)) {
      duckdb::ConstantVector::SetNull(result, true);
      return;
    }
    input.ToUnifiedFormat(values);
    ListTokenSink sink{
      result, duckdb::ConstantVector::GetData<duckdb::list_entry_t>(result),
      duckdb::ConstantVector::Validity(result), writer};
    sink.ResetRows(1);
    sink.FillRows(tokenizer, input, values, 1);
    return;
  }
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  input.ToUnifiedFormat(values);
  const auto rows = static_cast<uint32_t>(count);
  ListTokenSink sink{
    result, duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result),
    duckdb::FlatVector::ValidityMutable(result), writer};
  sink.ResetRows(rows);
  irs::analysis::ForEachInvalidRow(values, rows, [&](uint32_t r) {
    sink.SetNull(r);
    return true;
  });
  sink.FillRows(tokenizer, input, values, rows);
}

inline void TokenizeListRows(irs::analysis::Tokenizer& tokenizer,
                             irs::TokenSink& writer,
                             const duckdb::Vector& input, duckdb::idx_t count,
                             duckdb::Vector& result) {
  const bool constant =
    input.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR;
  const auto rows = static_cast<uint32_t>(constant ? 1 : count);
  result.SetVectorType(constant ? duckdb::VectorType::CONSTANT_VECTOR
                                : duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  duckdb::UnifiedVectorFormat lists;
  input.ToUnifiedFormat(lists);
  const auto* entries =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(lists);
  const auto& child = duckdb::ListVector::GetChild(input);
  duckdb::UnifiedVectorFormat elements;
  child.ToUnifiedFormat(elements);

  auto* result_entries =
    constant ? duckdb::ConstantVector::GetData<duckdb::list_entry_t>(result)
             : duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& result_validity = constant
                            ? duckdb::ConstantVector::Validity(result)
                            : duckdb::FlatVector::ValidityMutable(result);
  ListTokenSink sink{result, result_entries, result_validity, writer};
  sink.ResetRows(rows);

  std::vector<uint32_t> ends(rows);
  bool contiguous = true;
  duckdb::idx_t first = 0;
  duckdb::idx_t total = 0;
  for (uint32_t r = 0; r < rows; ++r) {
    const auto idx = lists.sel->get_index(r);
    if (lists.validity.RowIsValid(idx)) {
      const auto& entry = entries[idx];
      if (total == 0) {
        first = entry.offset;
      }
      contiguous &= entry.length == 0 || entry.offset == first + total;
      total += entry.length;
    } else {
      sink.SetNull(r);
    }
    ends[r] = static_cast<uint32_t>(total);
  }
  if (total == 0) {
    return;
  }
  if (contiguous && first == 0) {
    sink.FillElements(tokenizer, child, elements, static_cast<uint32_t>(total),
                      ends.data());
    return;
  }
  duckdb::SelectionVector sel{total};
  duckdb::idx_t k = 0;
  for (uint32_t r = 0; r < rows; ++r) {
    const auto idx = lists.sel->get_index(r);
    if (!lists.validity.RowIsValid(idx)) {
      continue;
    }
    const auto& entry = entries[idx];
    for (auto j = entry.offset; j < entry.offset + entry.length; ++j) {
      sel.set_index(k++, elements.sel->get_index(j));
    }
  }
  sink.FillElements(tokenizer, child, SliceFormat(elements, sel),
                    static_cast<uint32_t>(total), ends.data());
}

}  // namespace sdb::connector
