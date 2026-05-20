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

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <vector>

#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/column_writer.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/columnstore/hnsw.hpp"
#include "iresearch/columnstore/read_context.hpp"
#include "iresearch/columnstore/scan.hpp"

namespace irs::columnstore {

void MergeInto(std::span<const MergeSource> sources, Writer& output,
               const ColumnOptionsProvider* column_options) {
  absl::flat_hash_map<field_id, const ColumnReader*> first_seen_col;
  std::vector<field_id> ordered_ids;
  for (const auto& s : sources) {
    if (!s.cs_reader) {
      continue;
    }
    for (const auto& col : s.cs_reader->Columns()) {
      auto [it, inserted] = first_seen_col.try_emplace(col->Id(), col.get());
      if (inserted) {
        ordered_ids.push_back(col->Id());
      }
    }
  }
  if (ordered_ids.empty()) {
    return;
  }

  for (const auto field_id_v : ordered_ids) {
    const auto* first_col = first_seen_col[field_id_v];
    const auto opts = (column_options && *column_options)
                        ? (*column_options)(field_id_v)
                        : ColumnOptions{};
    auto& cw =
      output.OpenColumn(field_id_v, first_col->Type(), opts.skip_validity,
                        opts.row_group_size, opts.compression);

    uint64_t out_doc = 0;
    for (const auto& s : sources) {
      const auto* src = s.cs_reader;
      const uint64_t source_target = out_doc + s.alive_count;
      if (!src) {
        out_doc = source_target;
        continue;
      }
      const auto* col = src->Column(field_id_v);
      if (!col) {
        out_doc = source_target;
        continue;
      }
      if (opts.hnsw_info && src->HasHNSW(field_id_v)) {
        output.AttachHNSW(field_id_v, *opts.hnsw_info);
      }
      SDB_ASSERT(col->Type() == first_col->Type(),
                 "schema evolution between merge sources not supported");
      const auto mask = s.mask;

      ReadContext src_ctx{*src};
      auto state = MakeMaterializeState(*col, src_ctx);
      duckdb::Vector batch{col->Type(), STANDARD_VECTOR_SIZE,
                           duckdb::VectorDataInitialization::UNINITIALIZED};
      const bool has_mask = !mask.IsEmpty();
      duckdb::SelectionVector sel;
      if (has_mask) {
        sel.Initialize(STANDARD_VECTOR_SIZE);
      }
      const auto total = col->RowCount();
      uint64_t pos = 0;
      while (pos < total) {
        const auto take =
          std::min<duckdb::idx_t>(total - pos, STANDARD_VECTOR_SIZE);
        MaterializeNode(*col, *state, IotaRange{pos, take}, batch,
                        /*output_start=*/0);

        if (!has_mask) {
          cw.Append(out_doc, batch, take);
          out_doc += take;
        } else {
          duckdb::idx_t kept = 0;
          for (duckdb::idx_t i = 0; i < take; ++i) {
            const auto src_doc =
              static_cast<doc_id_t>(pos + i + doc_limits::min());
            if (mask.IsDeleted(src_doc)) {
              continue;
            }
            sel.set_index(kept++, i);
          }
          if (kept > 0) {
            cw.Append(out_doc, batch, sel, kept);
            out_doc += kept;
          }
        }
        pos += take;
      }
      if (out_doc < source_target) {
        cw.PadNullsTo(source_target);
        out_doc = source_target;
      }
    }
  }
}

}  // namespace irs::columnstore
