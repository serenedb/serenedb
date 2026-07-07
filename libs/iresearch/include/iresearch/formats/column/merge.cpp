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

#include <absl/container/flat_hash_set.h>

#include <algorithm>
#include <cstddef>
#include <duckdb/common/types/hyperloglog.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <optional>
#include <utility>
#include <vector>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"

namespace irs {
namespace {

class HyperLogLogMerger {
 public:
  void Begin() {
    _hyperloglog = duckdb::make_shared_ptr<duckdb::HyperLogLog>();
    if (!_hashes.GetBufferRef()) {
      _hashes.Initialize(duckdb::VectorDataInitialization::UNINITIALIZED,
                         STANDARD_VECTOR_SIZE);
    }
  }

  bool MergeStored(const ColumnReader& col) {
    if (const auto* src = col.HyperLogLog()) {
      _hyperloglog->Merge(*src);
      return true;
    }
    return false;
  }

  void MergeValues(const duckdb::Vector& values, duckdb::idx_t count) {
    UpdateFrom(values, count);
  }

  duckdb::shared_ptr<duckdb::HyperLogLog> Take() {
    SDB_ASSERT(_hyperloglog);
    return std::move(_hyperloglog);
  }

 private:
  void UpdateFrom(const duckdb::Vector& input, duckdb::idx_t count) {
    duckdb::VectorOperations::Hash(input, _hashes, count);
    _hyperloglog->Update(input, _hashes);
  }

  duckdb::shared_ptr<duckdb::HyperLogLog> _hyperloglog;
  duckdb::Vector _hashes{duckdb::LogicalType::HASH, nullptr};
};

}  // namespace

bool MergeInto(std::span<const MergeSource> sources, ColWriter& output,
               const IndexFieldOptions* field_options,
               const std::function<bool()>& progress) {
  std::vector<std::pair<field_id, const ColumnReader*>> ordered_cols;
  absl::flat_hash_set<field_id> seen_ids;
  for (const auto& s : sources) {
    if (!s.col_reader) {
      continue;
    }
    for (const auto& col : s.col_reader->Columns()) {
      if (seen_ids.insert(col->Id()).second) {
        ordered_cols.emplace_back(col->Id(), col.get());
      }
    }
  }
  if (ordered_cols.empty()) {
    return true;
  }

  std::vector<std::optional<ReadContext>> source_ctxs(sources.size());
  for (size_t i = 0; i < sources.size(); ++i) {
    if (sources[i].col_reader) {
      source_ctxs[i].emplace(*sources[i].col_reader);
    }
  }

  HyperLogLogMerger hyperloglog;
  duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
  std::vector<std::vector<std::optional<std::vector<duckdb::sel_t>>>>
    kept_cache(sources.size());
  auto kept_for = [&](size_t si, const DocumentMask& mask,
                      uint64_t pos) -> const std::vector<duckdb::sel_t>& {
    auto& windows = kept_cache[si];
    const size_t w = pos / STANDARD_VECTOR_SIZE;
    if (windows.size() <= w) {
      windows.resize(w + 1);
    }
    auto& slot = windows[w];
    if (!slot) {
      slot.emplace();
      slot->reserve(STANDARD_VECTOR_SIZE);
      for (duckdb::idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
        const auto src_doc = static_cast<doc_id_t>(pos + i + doc_limits::min());
        if (!mask.contains(src_doc)) {
          slot->push_back(static_cast<duckdb::sel_t>(i));
        }
      }
    }
    return *slot;
  };
  for (const auto& [field_id_v, first_col] : ordered_cols) {
    const auto opts = field_options
                        ? field_options->GetColumnOptions(field_id_v)
                        : ColumnOptions{};

    auto& cw =
      output.OpenColumn(field_id_v, first_col->Type(), opts.skip_validity,
                        opts.row_group_size, opts.compression, false);
    if (opts.ivf_info) {
      output.NoteIvfColumn();
    }

    if (opts.hyperloglog) {
      hyperloglog.Begin();
    }

    ColumnReader::VectorScratch batch_scratch{first_col->Type()};

    uint64_t out_doc = 0;
    for (size_t si = 0; si < sources.size(); ++si) {
      const auto& s = sources[si];
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

      const bool stored_hll =
        opts.hyperloglog && !has_mask && hyperloglog.MergeStored(*col);

      auto state = col->InitScan(*source_ctxs[si]);
      cw.PadNullsTo(out_doc);
      const auto total = col->RowCount();
      uint64_t pos = 0;
      while (pos < total) {
        if (progress && !progress()) {
          return false;
        }
        const auto take =
          std::min<duckdb::idx_t>(total - pos, STANDARD_VECTOR_SIZE);

        if (!has_mask) {
          auto& batch = batch_scratch.Reset();
          col->Scan(state, batch, take);
          cw.Append(batch, take);
          if (opts.hyperloglog && !stored_hll) {
            hyperloglog.MergeValues(batch, take);
          }
          out_doc += take;
        } else {
          const auto& kept_idx = kept_for(si, *mask, pos);
          duckdb::idx_t kept = 0;
          for (const auto i : kept_idx) {
            if (i >= take) {
              break;
            }
            sel.set_index(kept++, i);
          }
          if (kept == 0) {
            col->Skip(state, take);
          } else {
            auto& batch = batch_scratch.Reset();
            col->Scan(state, batch, take);
            duckdb::Vector selected{batch, sel, kept};
            cw.Append(out_doc, selected, kept);
            if (opts.hyperloglog) {
              hyperloglog.MergeValues(selected, kept);
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

    cw.PadNullsTo(out_doc);

    if (opts.hyperloglog) {
      cw.SetHyperLogLog(hyperloglog.Take());
    }
  }
  return true;
}

}  // namespace irs
