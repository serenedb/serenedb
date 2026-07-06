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

#include "iresearch/formats/column/merge.hpp"

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <duckdb/common/types/hyperloglog.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <vector>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/column/scan.hpp"

namespace irs {
namespace {

class HyperLogLogMerger {
 public:
  void Init() {
    _hyperloglog = duckdb::make_shared_ptr<duckdb::HyperLogLog>();
    _hashes = duckdb::make_uniq<duckdb::Vector>(duckdb::LogicalType::HASH,
                                                STANDARD_VECTOR_SIZE);
  }

  void MergeWithoutDelete(const ColumnReader& col) {
    const auto* src = col.HyperLogLog();
    SDB_ASSERT(src);
    _hyperloglog->Merge(*src);
  }

  void MergeWithDelete(const duckdb::Vector& values,
                       const duckdb::SelectionVector& sel,
                       duckdb::idx_t count) {
    // Hash only the survivors (a no-copy view of `values` through `sel`), then
    // drop the null ones, packing the rest to the front so Update sees them.
    duckdb::Vector survivors{values, sel, count};
    duckdb::VectorOperations::Hash(survivors, *_hashes, count);
    auto* h = duckdb::FlatVector::GetDataMutable<duckdb::hash_t>(*_hashes);
    const auto& validity = duckdb::FlatVector::Validity(values);
    duckdb::idx_t kept = 0;
    for (duckdb::idx_t i = 0; i < count; ++i) {
      if (validity.RowIsValid(sel.get_index(i))) {
        h[kept++] = h[i];
      }
    }
    duckdb::FlatVector::SetSize(*_hashes, kept);
    _hyperloglog->Update(*_hashes);
  }

  duckdb::shared_ptr<duckdb::HyperLogLog> Get() && {
    return std::move(_hyperloglog);
  }

 private:
  duckdb::shared_ptr<duckdb::HyperLogLog> _hyperloglog;
  duckdb::unique_ptr<duckdb::Vector> _hashes;
};

}  // namespace

void MergeInto(std::span<const MergeSource> sources, ColWriter& output,
               const IndexFieldOptions* field_options) {
  absl::flat_hash_map<field_id, const ColumnReader*> first_seen_col;
  std::vector<field_id> ordered_ids;
  for (const auto& s : sources) {
    if (!s.col_reader) {
      continue;
    }
    for (const auto& col : s.col_reader->Columns()) {
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
    const auto opts = field_options
                        ? field_options->GetColumnOptions(field_id_v)
                        : ColumnOptions{};

    auto& cw =
      output.OpenColumn(field_id_v, first_col->Type(), opts.skip_validity,
                        opts.row_group_size, opts.compression, false);
    if (opts.ivf_info) {
      output.NoteIvfColumn();
    }

    HyperLogLogMerger hyperloglog;
    if (opts.hyperloglog) {
      hyperloglog.Init();
    }

    uint64_t out_doc = 0;
    for (const auto& s : sources) {
      const auto* src = s.col_reader;
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
      SDB_ASSERT(col->Type() == first_col->Type(),
                 "schema evolution between merge sources not supported");
      const auto* mask = s.mask;
      const bool has_mask = mask && !mask->empty();

      if (opts.hyperloglog && !has_mask) {
        hyperloglog.MergeWithoutDelete(*col);
      }

      ReadContext src_ctx{*src};
      auto state = MakeMaterializeState(*col, src_ctx);
      duckdb::Vector batch{col->Type(), STANDARD_VECTOR_SIZE,
                           duckdb::VectorDataInitialization::UNINITIALIZED};
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
            if (mask->contains(src_doc)) {
              continue;
            }
            sel.set_index(kept++, i);
          }
          if (kept > 0) {
            cw.Append(out_doc, batch, sel, kept);
            if (opts.hyperloglog) {
              hyperloglog.MergeWithDelete(batch, sel, kept);
            }
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

    if (opts.hyperloglog) {
      cw.SetHyperLogLog(std::move(hyperloglog).Get());
    }
  }
}

}  // namespace irs
