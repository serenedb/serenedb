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
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/columnstore/merge.hpp"

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/table/scan_state.hpp>

#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/column_writer.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/columnstore/hnsw.hpp"
#include "iresearch/columnstore/norm_reader.hpp"
#include "iresearch/columnstore/norm_writer.hpp"

namespace irs::columnstore {

void MergeInto(std::span<const Reader* const> sources,
               std::span<const DocumentMask* const> source_masks,
               Writer& output) {
  absl::flat_hash_map<field_id, const ColumnReader*> first_seen;
  for (const auto* src : sources) {
    if (!src) {
      continue;
    }
    for (const auto* col : src->Columns()) {
      first_seen.try_emplace(col->Id(), col);
    }
  }

  for (auto [field_id_v, first_col] : first_seen) {
    auto& cw =
      output.OpenColumn(field_id_v, first_col->Name(), first_col->Type());

    auto it = absl::c_find_if(sources, [&](const auto& src) {
      return src != nullptr && src->HasHNSW(field_id_v);
    });
    if (it != sources.end()) {
      const auto* src = *it;
      SDB_ASSERT(src);
      output.AttachHNSW(field_id_v, src->HNSW(field_id_v)->Info());
    }

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
      SDB_ASSERT(col->Type() == first_col->Type(),
                 "schema evolution between merge sources not supported");
      const auto* mask = source_masks[s];

      ColumnReader::RangeScan data_range{*col};
      ColumnReader::RangeScan validity_range{*col, /*validity_side=*/true};
      const auto n_rows = col->RowCount();

      duckdb::Vector batch{col->Type(), STANDARD_VECTOR_SIZE,
                           duckdb::VectorDataInitialization::ZERO_INITIALIZE};

      uint64_t scanned = 0;
      while (scanned < n_rows) {
        const auto take =
          std::min<duckdb::idx_t>(n_rows - scanned, STANDARD_VECTOR_SIZE);
        data_range.Scan(scanned, take, batch, 0);
        if (col->HasValidity()) {
          validity_range.Scan(scanned, take, batch, 0);
        } else {
          batch.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
          duckdb::FlatVector::ValidityMutable(batch).SetAllValid(take);
        }

        if (!mask || mask->empty()) {
          cw.Append(out_doc, batch, take);
          out_doc += take;
        } else {
          duckdb::SelectionVector sel{take};
          duckdb::idx_t kept = 0;
          for (duckdb::idx_t i = 0; i < take; ++i) {
            // docs_mask uses 1-indexed iresearch doc_ids; row positions
            // are 0-indexed.
            const auto src_doc =
              static_cast<doc_id_t>(scanned + i + doc_limits::min());
            if (mask->contains(src_doc)) {
              continue;
            }
            sel.set_index(kept++, i);
          }
          if (kept > 0) {
            duckdb::Vector kept_vec{col->Type(), kept};
            kept_vec.Slice(batch, sel, kept);
            kept_vec.Flatten(kept);
            cw.Append(out_doc, kept_vec, kept);
            out_doc += kept;
          }
        }
        scanned += take;
      }
    }
  }

  // Norms are merged in MergeWriter::WriteFields (it walks the merged
  // field set and can allocate fresh ids).
}

}  // namespace irs::columnstore
