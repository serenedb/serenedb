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

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/merge_order.hpp"
#include "iresearch/index/merge/merge_progress.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/misc.hpp"

namespace irs {

struct MergeSource {
  const SubReader* reader;
  const ColReader* col_reader;
  uint64_t alive_count;
  const DocRemap* remap = nullptr;
};

class ColumnMerger {
 public:
  ColumnMerger(std::span<const MergeSource> sources,
               const IndexFieldOptions* field_options, const MergeOrder* order,
               duckdb::ObjectCache& cache);

  ColumnMerger(const ColumnMerger&) = delete;
  ColumnMerger& operator=(const ColumnMerger&) = delete;

  std::span<const field_id> Columns() const noexcept { return _ids; }

  uint64_t StoredBytes(field_id column) const;

  uint64_t StoredBytes(field_id column, size_t input) const;

  uint32_t RowGroupSize() const noexcept { return _row_group_size; }

  bool Splittable(field_id column) const;

  bool Copyable(field_id column, size_t input) const;

  void Open(field_id column, ColWriter& output) const;

  bool Merge(field_id column, ColWriter& output, const MergeProgress& progress);

  bool MergeRange(field_id column, uint32_t part, uint64_t begin, uint64_t end,
                  ColWriter& output, const MergeProgress& progress);

  bool CopyRange(field_id column, uint32_t part, size_t input, uint64_t begin,
                 uint64_t end, ColWriter& output,
                 const MergeProgress& progress);

 private:
  struct KeptRows {
    OnceFlag once;
    std::vector<std::vector<duckdb::sel_t>> windows;
  };

  struct ColumnSlot {
    const ColumnReader* first = nullptr;
  };

  struct OrderedInput;

  std::vector<duckdb::sel_t>& KeptFor(size_t si, uint64_t pos);

  auto& Slot(this auto& self, field_id column);

  const ColumnReader& First(field_id column) const;

  ColumnOptions Options(field_id column) const;

  ColCodecParams CodecParams(const ColumnOptions& opts) const;

  ColumnWriterOptions WriterOptions(field_id column) const;

  std::span<const field_id> AnnFields(field_id column) const noexcept;

  ColumnWriter& OpenPart(field_id column, uint32_t part,
                         ColWriter& output) const;

  template<typename Visit>
  bool VisitRows(field_id column, uint64_t begin, uint64_t end,
                 std::span<const std::shared_ptr<ReadContext>> ctxs,
                 const MergeProgress& progress, Visit&& visit);

  void OpenOrdered(OrderedInput& in, size_t si, field_id column, uint64_t live);

  void LoadOrdered(OrderedInput& in, size_t si);

  template<typename Visit>
  bool VisitOrdered(field_id column, uint64_t begin, uint64_t end,
                    const MergeProgress& progress, Visit&& visit);

  std::span<const MergeSource> _sources;
  const IndexFieldOptions* _field_options;
  const MergeOrder* _order;
  duckdb::ObjectCache* _cache;
  duckdb::DatabaseInstance* _db = nullptr;
  uint32_t _row_group_size;
  std::vector<field_id> _ids;
  containers::FlatHashMap<field_id, ColumnSlot> _slots;
  std::unique_ptr<KeptRows[]> _kept_rows;
};

}  // namespace irs
