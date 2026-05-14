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

#include "iresearch/columnstore/merge.hpp"

#include <algorithm>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/storage/table/scan_state.hpp>

#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/column_writer.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/columnstore/hnsw.hpp"

namespace irs::columnstore {

void MergeInto(std::span<const Reader* const> sources,
               std::span<const DocumentMask* const> source_masks,
               Writer& output) {
  const Reader* schema_src = nullptr;
  for (const auto* src : sources) {
    if (src) {
      schema_src = src;
      break;
    }
  }
  if (!schema_src) {
    return;
  }

  for (const auto& first_col : schema_src->Columns()) {
    const auto field_id_v = first_col->Id();
    auto& cw = output.OpenColumn(field_id_v, first_col->Type());

    uint64_t out_doc = 0;
    for (size_t s = 0; s < sources.size(); ++s) {
      const auto* src = sources[s];
      if (!src) {
        continue;
      }
      const auto* col = src->Column(field_id_v);
      if (!col) {
        continue;
      }
      if (src->HasHNSW(field_id_v)) {
        output.AttachHNSW(field_id_v, src->HNSW(field_id_v)->Info());
      }
      SDB_ASSERT(col->Type() == first_col->Type(),
                 "schema evolution between merge sources not supported");
      const auto* mask = source_masks[s];
      const bool is_array = col->Type().id() == duckdb::LogicalTypeId::ARRAY;
      const uint64_t array_size = is_array ? col->ArraySize() : 1;

      ColumnReader::RangeScan data_range{*col};
      ColumnReader::RangeScan validity_range{*col, /*validity_side=*/true};
      for (size_t rg = 0; rg < col->RowGroupCount(); ++rg) {
        const auto rg_count = col->RowGroupRowCount(rg);
        const auto rg_first_doc = col->RowGroupOffset(rg);

        duckdb::Vector batch{col->Type(), STANDARD_VECTOR_SIZE,
                             duckdb::VectorDataInitialization::ZERO_INITIALIZE};

        duckdb::idx_t scanned = 0;
        while (scanned < rg_count) {
          const auto take =
            std::min<duckdb::idx_t>(rg_count - scanned, STANDARD_VECTOR_SIZE);
          if (is_array) {
            auto& child = duckdb::ArrayVector::GetChildMutable(batch);
            data_range.Scan((rg_first_doc + scanned) * array_size,
                            take * array_size, child, 0);
          } else {
            data_range.Scan(rg_first_doc + scanned, take, batch, 0);
          }
          if (col->HasValidity()) {
            duckdb::FlatVector::ValidityMutable(batch).SetAllValid(take);
          }

          if (!mask || mask->empty()) {
            cw.Append(out_doc, batch, take);
            out_doc += take;
          } else {
            duckdb::SelectionVector sel{take};
            duckdb::idx_t kept = 0;
            for (duckdb::idx_t i = 0; i < take; ++i) {
              const auto src_doc = static_cast<doc_id_t>(
                rg_first_doc + scanned + i + doc_limits::min());
              if (mask->contains(src_doc)) {
                continue;
              }
              sel.set_index(kept++, i);
            }
            if (kept > 0) {
              cw.Append(out_doc, batch, sel, kept);
              out_doc += kept;
            }
          }
          scanned += take;
        }
      }
    }
  }
}

}  // namespace irs::columnstore
