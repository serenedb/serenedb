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

#include "connector/search_key_unique.h"

#include <duckdb/common/vector/unified_vector_format.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string_view>

#include "connector/term_dict.h"

namespace sdb::connector {
namespace {

// A key is only found again under what the sink wrote, so every column is read
// the way the sink reads it: through the same dispatch, off the same vector,
// into the same member and bytes. A type the sink does not invert as one value
// has no key component to build, so it is refused by name.
void EncodeColumn(duckdb::Vector& column, const primary_key::KeySlot& slot,
                  duckdb::idx_t first_row, duckdb::idx_t count, size_t term,
                  std::vector<KeyTerms>& keys, size_t first_key) {
  duckdb::UnifiedVectorFormat format;
  column.ToUnifiedFormat(count, format);
  const auto encode = [&]<duckdb::LogicalTypeId Kind>() {
    using enum duckdb::LogicalTypeId;
    for (duckdb::idx_t row = first_row; row != count; ++row) {
      const auto idx = format.sel->get_index(row);
      auto& component = keys[first_key + (row - first_row)][term];
      if constexpr (Kind == VARCHAR || Kind == BLOB) {
        const auto& value =
          duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(format)[idx];
        component.dict = {.layout = slot.layout,
                          .member = irs::DictMember::Strings};
        component.term.assign(irs::ViewCast<irs::byte_type>(
          std::string_view{value.GetData(), value.GetSize()}));
      } else if constexpr (Kind == BOOLEAN) {
        const auto* data = duckdb::UnifiedVectorFormat::GetData<bool>(format);
        component.dict = {.layout = slot.layout,
                          .member = irs::BoolMember(data[idx])};
        component.term.clear();
      } else {
        const auto value = term_dict::PromoteNumericValue(
          term_dict::ExtractNumericValue<Kind>(format, idx));
        irs::byte_type buf[irs::numeric_utils::kNumericTermMaxSize];
        component.dict = {.layout = slot.layout,
                          .member = irs::DictMember::Numbers};
        component.term.assign(
          irs::numeric_utils::EncodeNumericTerm(buf, value));
      }
    }
  };
  if (!term_dict::DispatchValueKind(slot.type.id(), encode)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("primary key column \"", slot.name, "\" of type ",
              slot.type.ToString(), " is not indexed as a single value"),
      ERR_HINT("A search table enforces its primary key through the index, so "
               "every key column must be indexed as one value."));
  }
}

}  // namespace

void EncodeChunkKeys(duckdb::DataChunk& chunk,
                     std::span<const primary_key::KeySlot> slots,
                     std::vector<KeyTerms>& keys, duckdb::idx_t first_row) {
  const auto count = chunk.size();
  if (first_row >= count) {
    return;
  }
  const auto first_key = keys.size();
  keys.resize(first_key + (count - first_row), KeyTerms(slots.size()));
  for (size_t term = 0; term != slots.size(); ++term) {
    EncodeColumn(chunk.data[slots[term].input_col_idx], slots[term], first_row,
                 count, term, keys, first_key);
  }
}

void AppendRowKeys(duckdb::ColumnDataCollection& rows,
                   std::span<const primary_key::KeySlot> slots,
                   duckdb::idx_t first_row, std::vector<KeyTerms>& keys) {
  duckdb::ColumnDataScanState state;
  rows.InitializeScan(state);
  duckdb::DataChunk chunk;
  rows.InitializeScanChunk(state, chunk);
  duckdb::idx_t seen = 0;
  while (true) {
    chunk.Reset();
    rows.Scan(state, chunk);
    if (chunk.size() == 0) {
      break;
    }
    const auto next = seen + chunk.size();
    if (next > first_row) {
      EncodeChunkKeys(chunk, slots, keys,
                      first_row > seen ? first_row - seen : 0);
    }
    seen = next;
  }
}

}  // namespace sdb::connector
