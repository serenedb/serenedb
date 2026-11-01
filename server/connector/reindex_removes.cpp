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

#include "connector/reindex_removes.h"

#include <absl/algorithm/container.h>
#include <roaring/roaring64.h>

#include <algorithm>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/unordered_map.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <limits>
#include <roaring/roaring.hh>
#include <span>
#include <utility>
#include <vector>

#include "connector/term_dict.h"

namespace sdb::connector {
namespace {

using DeleteMask = IcebergObserve::DeleteMask;
using EqRow = std::pair<uint64_t, int64_t>;
using DeadRows = roaring::api::roaring64_bitmap_t;
using DeletionVector = duckdb::unordered_map<int32_t, roaring::Roaring>;

constexpr int64_t kMinRow = std::numeric_limits<int64_t>::min();
constexpr int64_t kMaxRow = std::numeric_limits<int64_t>::max();

bool AnyIn(const DeadRows* rows, int64_t first, int64_t last) {
  SDB_ASSERT(0 <= first && first <= last);
  return rows &&
         roaring::api::roaring64_bitmap_intersect_with_range(
           rows, static_cast<uint64_t>(first), static_cast<uint64_t>(last) + 1);
}

bool ContainsIn(const DeadRows* rows, int64_t row) {
  SDB_ASSERT(row >= 0);
  return rows && roaring::api::roaring64_bitmap_contains(
                   rows, static_cast<uint64_t>(row));
}

bool AnyIn(const DeletionVector* dv, int64_t first, int64_t last) {
  return dv && absl::c_any_of(*dv, [&](const auto& bucket) {
           const auto base = static_cast<int64_t>(bucket.first) << 32;
           const auto from = std::max(first, base);
           const auto to = std::min(
             last, base + int64_t{std::numeric_limits<uint32_t>::max()});
           return from <= to &&
                  roaring::api::roaring_bitmap_intersect_with_range(
                    &bucket.second.roaring, from - base, to - base + 1);
         });
}

bool ContainsIn(const DeletionVector* dv, int64_t row) {
  if (!dv) {
    return false;
  }
  const auto bucket = dv->find(static_cast<int32_t>(row >> 32));
  return bucket != dv->end() &&
         bucket->second.contains(static_cast<uint32_t>(row));
}

auto RowAtOrAfter(std::span<const EqRow> rows, int64_t row) {
  return absl::c_lower_bound(rows, row, [](const EqRow& eq, int64_t value) {
    return eq.second < value;
  });
}

bool AnyIn(std::span<const EqRow> rows, int64_t first, int64_t last) {
  const auto it = RowAtOrAfter(rows, first);
  return it != rows.end() && it->second <= last;
}

bool ContainsIn(std::span<const EqRow> rows, int64_t row) {
  const auto it = RowAtOrAfter(rows, row);
  return it != rows.end() && it->second == row;
}

auto MaskAtOrAfter(std::span<const DeleteMask> masks, uint64_t file) {
  return absl::c_lower_bound(masks, file,
                             [](const DeleteMask& mask, uint64_t value) {
                               return mask.file_id < value;
                             });
}

struct FileDeletes {
  bool whole = false;
  const DeleteMask* mask = nullptr;
  std::span<const EqRow> eq_rows;

  bool Empty() const {
    SDB_ASSERT(!whole);
    return !mask && eq_rows.empty();
  }

  bool Any(int64_t first, int64_t last) const {
    SDB_ASSERT(!whole);
    return (mask && (AnyIn(mask->rows.get(), first, last) ||
                     AnyIn(mask->dv.get(), first, last))) ||
           AnyIn(eq_rows, first, last);
  }

  bool Contains(int64_t row) const {
    SDB_ASSERT(!whole);
    return (mask && (ContainsIn(mask->rows.get(), row) ||
                     ContainsIn(mask->dv.get(), row))) ||
           ContainsIn(eq_rows, row);
  }
};

struct Deletes {
  std::span<const uint64_t> files;
  std::span<const DeleteMask> masks;
  std::span<const EqRow> eq_rows;

  bool Empty() const {
    return files.empty() && masks.empty() && eq_rows.empty();
  }

  bool AnyFile(uint64_t first, uint64_t last) const {
    const auto file = absl::c_lower_bound(files, first);
    const auto mask = MaskAtOrAfter(masks, first);
    const auto eq = absl::c_lower_bound(eq_rows, EqRow{first, kMinRow});
    return (file != files.end() && *file <= last) ||
           (mask != masks.end() && mask->file_id <= last) ||
           (eq != eq_rows.end() && eq->first <= last);
  }

  FileDeletes Of(uint64_t file) const {
    const auto mask = MaskAtOrAfter(masks, file);
    const auto eq_first = absl::c_lower_bound(eq_rows, EqRow{file, kMinRow});
    const auto eq_last =
      std::upper_bound(eq_first, eq_rows.end(), EqRow{file, kMaxRow});
    return {
      .whole = absl::c_binary_search(files, file),
      .mask = mask != masks.end() && mask->file_id == file ? &*mask : nullptr,
      .eq_rows = {eq_first, eq_last}};
  }
};

template<typename T>
std::pair<T, T> Bounds(const duckdb::BaseStatistics& stats) {
  SDB_ASSERT(duckdb::NumericStats::HasMinMax(stats));
  return {duckdb::NumericStats::GetMin<T>(stats),
          duckdb::NumericStats::GetMax<T>(stats)};
}

irs::doc_id_t DocAt(uint64_t row) {
  return static_cast<irs::doc_id_t>(irs::doc_limits::min() + row);
}

template<typename T>
class KeyHalf {
 public:
  KeyHalf(const irs::ColumnReader& column, irs::ReadContext& ctx)
    : _column{column}, _state{column.InitScan(ctx)}, _scratch{column.Type()} {}

  template<typename Visit>
  void Read(uint64_t first, uint64_t count, const Visit& visit) {
    SDB_ASSERT(_cursor <= first);
    if (first != _cursor) {
      _column.Skip(_state, first - _cursor);
      _cursor = first;
    }
    while (count) {
      const auto take = std::min<uint64_t>(count, STANDARD_VECTOR_SIZE);
      auto& scanned = _scratch.Reset();
      _column.Scan(_state, scanned, take);
      const auto values = scanned.Values<T>();
      for (duckdb::idx_t i = 0; i != take; ++i) {
        visit(_cursor + i, values[i].GetValue());
      }
      _cursor += take;
      count -= take;
    }
  }

 private:
  const irs::ColumnReader& _column;
  irs::ColumnReader::ScanState _state;
  irs::ColumnReader::VectorScratch _scratch;
  uint64_t _cursor = 0;
};

class SegmentRemoves {
 public:
  SegmentRemoves(const Deletes& deletes, uint32_t index,
                 irs::DocRemovalBuilder& removal)
    : _deletes{deletes}, _index{index}, _removal{removal} {}

  void Resolve(const irs::SubReader& segment) {
    const auto* columns = segment.GetColReader();
    SDB_ENSURE(columns, "REINDEX: segment ", segment.Meta().name,
               " has no columnstore");
    const auto* key = columns->Column(term_dict::kPKFieldId);
    SDB_ENSURE(key && key->StructFieldCount() == 2, "REINDEX: segment ",
               segment.Meta().name, " has no row keys");
    const auto& files = key->StructField(0);
    if (const auto [min, max] = Bounds<uint64_t>(files.MergedStatistics());
        !_deletes.AnyFile(min, max)) {
      return;
    }
    irs::ReadContext ctx{*columns};
    KeyHalf<uint64_t> file_half{files, ctx};
    KeyHalf<int64_t> row_half{key->StructField(1), ctx};
    _rows = &key->StructField(1);
    SDB_ASSERT(_rows->DataRgCount() == files.DataRgCount());
    for (size_t block = 0; block != files.DataRgCount(); ++block) {
      const auto first = files.DataBlockFirstRow(block);
      const auto& meta = files.DataBlocks()[block];
      const auto [min, max] = Bounds<uint64_t>(meta.statistics);
      if (!_deletes.AnyFile(min, max)) {
        continue;
      }
      if (min == max) {
        RemoveFileRows(_deletes.Of(min), block, first, meta.tuple_count,
                       row_half);
      } else {
        RemoveMixed(first, meta.tuple_count, file_half, row_half);
      }
    }
    Flush();
  }

 private:
  struct FileRun {
    uint64_t end = 0;
    FileDeletes deletes;
  };

  void Add(uint64_t begin, uint64_t end) {
    if (_range_end != begin) {
      Flush();
      _range_begin = begin;
    }
    _range_end = end;
  }

  void Flush() {
    if (_range_begin != _range_end) {
      _removal.AddRange(_index, DocAt(_range_begin), DocAt(_range_end));
    }
    _range_begin = _range_end = 0;
  }

  void RemoveFileRows(const FileDeletes& file, size_t block, uint64_t first,
                      uint64_t count, KeyHalf<int64_t>& row_half) {
    if (file.whole) {
      Add(first, first + count);
      return;
    }
    const auto& meta = _rows->DataBlocks()[block];
    SDB_ASSERT(_rows->DataBlockFirstRow(block) == first &&
               meta.tuple_count == count);
    if (const auto [min, max] = Bounds<int64_t>(meta.statistics);
        !file.Any(min, max)) {
      return;
    }
    row_half.Read(first, count, [&](uint64_t row, int64_t value) {
      if (file.Contains(value)) {
        Add(row, row + 1);
      }
    });
  }

  void RemoveMixed(uint64_t first, uint64_t count, KeyHalf<uint64_t>& file_half,
                   KeyHalf<int64_t>& row_half) {
    _file_runs.clear();
    uint64_t run_file = 0;
    file_half.Read(first, count, [&](uint64_t row, uint64_t file) {
      if (!_file_runs.empty() && file == run_file) {
        _file_runs.back().end = row + 1;
        return;
      }
      run_file = file;
      _file_runs.push_back({.end = row + 1, .deletes = _deletes.Of(file)});
    });
    auto begin = first;
    for (const auto& run : _file_runs) {
      if (run.deletes.whole) {
        Add(begin, run.end);
      } else if (!run.deletes.Empty()) {
        row_half.Read(begin, run.end - begin, [&](uint64_t row, int64_t value) {
          if (run.deletes.Contains(value)) {
            Add(row, row + 1);
          }
        });
      }
      begin = run.end;
    }
  }

  const Deletes& _deletes;
  uint32_t _index;
  irs::DocRemovalBuilder& _removal;
  const irs::ColumnReader* _rows = nullptr;
  std::vector<FileRun> _file_runs;
  uint64_t _range_begin = 0;
  uint64_t _range_end = 0;
};

}  // namespace

std::shared_ptr<const irs::DocRemoval> ResolveRemoves(
  const irs::DirectoryReader& snapshot, std::vector<uint64_t> deleted_files,
  std::vector<IcebergObserve::DeleteMask> masks,
  std::vector<std::pair<uint64_t, int64_t>> eq_rows) {
  // Delete kind 1 -- deleted files: every row of the file.
  absl::c_sort(deleted_files);
  // Delete kind 2 -- changed bitmasks (iceberg): the dead rows of kept files.
  absl::c_sort(masks, [](const DeleteMask& lhs, const DeleteMask& rhs) {
    return lhs.file_id < rhs.file_id;
  });
  // Delete kind 3 -- equality deletes (iceberg): the rows they matched.
  absl::c_sort(eq_rows);
  const Deletes deletes{
    .files = deleted_files, .masks = masks, .eq_rows = eq_rows};
  if (deletes.Empty()) {
    return std::make_shared<const irs::DocRemoval>();
  }
  irs::DocRemovalBuilder removal;
  for (uint32_t index = 0; index != snapshot.size(); ++index) {
    SegmentRemoves{deletes, index, removal}.Resolve(snapshot[index]);
  }
  return std::move(removal).Finish(snapshot);
}

}  // namespace sdb::connector
