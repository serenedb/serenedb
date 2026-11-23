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

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>

#include <cstdint>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/column/norm_reader.hpp"
#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

inline constexpr uint32_t kNormWindowShift = 16;

struct NormRegion {
  const byte_type* base = nullptr;
  const byte_type* bases = nullptr;
  const byte_type* overflow_rows = nullptr;
  const byte_type* overflow_values = nullptr;
  const byte_type* values = nullptr;
  doc_id_t first_doc = 0;
  doc_id_t end_doc = 0;
  uint32_t bits = 0;
  uint32_t value = 0;
  uint32_t first_code = 0;
  uint32_t shift = 0;
  uint32_t direct = 0;
  uint32_t overflow = 0;
  uint32_t exception_bytes = 0;
  bool exceptions = false;
  size_t window = 0;
  size_t windows = 0;
  size_t page = 0;
  uintptr_t first_page = 0;

  IRS_FORCE_INLINE uint32_t Slot(doc_id_t doc) const noexcept {
    switch (bits) {
      case 0:
        return value;
      case 8:
        return base[doc];
      case 16:
        return absl::little_endian::Load16(base + size_t{doc} * 2);
      default:
        return absl::little_endian::Load32(base + size_t{doc} * 4);
    }
  }

  IRS_FORCE_INLINE uint32_t Exception(doc_id_t doc, uint32_t code) const {
    SDB_ASSERT(exceptions && code >= first_code);
    const uint32_t row = doc - first_doc;
    if (code != first_code + kNormDirect) [[likely]] {
      const uint32_t at =
        absl::little_endian::Load32(bases + size_t{row >> shift} * 4) +
        (code - first_code);
      if (at >= direct) [[unlikely]] {
        Corrupt(doc);
      }
      return exception_bytes == 2
               ? absl::little_endian::Load16(values + size_t{at} * 2)
               : absl::little_endian::Load32(values + size_t{at} * 4);
    }
    return Overflow(row);
  }

  uint32_t Overflow(uint32_t row) const;

  [[noreturn]] void Corrupt(doc_id_t doc) const;
};

class NormColumnReader final {
 public:
  NormColumnReader(field_id id, const NormColumnMeta& meta,
                   const duckdb::InputFile& file);
  NormColumnReader(field_id id, std::vector<NormRegion> regions,
                   std::vector<NormStats> stats,
                   std::vector<std::vector<uint32_t>> owned);

  field_id Id() const noexcept { return _id; }
  uint64_t RowCount() const noexcept { return _row_count; }
  uint64_t Sum() const noexcept { return _sum; }
  uint64_t NonZeroCount() const noexcept { return _non_zero; }
  size_t RegionCount() const noexcept { return _regions.size(); }
  size_t WindowCount() const noexcept { return _windows.size(); }
  size_t PageCount() const noexcept { return _pages; }
  bool Paged() const noexcept { return _paged; }

  const NormRegion& Region(size_t i) const noexcept {
    SDB_ASSERT(i < _regions.size());
    return _regions[i];
  }

  const NormStats& Stats(size_t i) const noexcept {
    SDB_ASSERT(i < _stats.size());
    return _stats[i];
  }

  const NormRegion& Locate(doc_id_t doc) const noexcept;

  IRS_FORCE_INLINE const NormRegion& LocateAfter(const NormRegion& region,
                                                 doc_id_t doc) const noexcept {
    SDB_ASSERT(doc >= region.end_doc);
    const auto next = static_cast<size_t>(&region - _regions.data()) + 1;
    SDB_ASSERT(next < _regions.size());
    if (doc < _ends[next]) [[likely]] {
      return _regions[next];
    }
    const auto rest = std::span{_ends}.subspan(next + 1);
    return _regions[next + 1 +
                    static_cast<size_t>(absl::c_upper_bound(rest, doc) -
                                        rest.begin())];
  }

  std::span<const byte_type> Window(size_t i) const noexcept {
    SDB_ASSERT(i < _windows.size());
    return _windows[i];
  }

  const duckdb::PageResidencyMap& Residency() const noexcept {
    return _residency;
  }

  void Prefetch(const byte_type* from, size_t size) const noexcept {
    SDB_ASSERT(_paged);
    _file->Prefetch(static_cast<uint64_t>(from - _file->Data()), size);
  }

  void Decode(doc_id_t first, size_t n, uint32_t* values) const;

 private:
  const byte_type* Map(const duckdb::InputFile& file, uint64_t offset,
                       uint64_t size);

  void IndexEnds();

  const duckdb::InputFile* _file;
  field_id _id;
  std::vector<NormRegion> _regions;
  std::vector<doc_id_t> _ends;
  std::vector<NormStats> _stats;
  std::vector<std::span<const byte_type>> _windows;
  duckdb::PageResidencyMap _residency;
  std::vector<duckdb::FilePin> _pins;
  std::vector<std::vector<uint32_t>> _owned;
  uint64_t _row_count = 0;
  uint64_t _sum = 0;
  uint64_t _non_zero = 0;
  size_t _pages = 0;
  bool _paged = false;
};

class NormColumnConcat final {
 public:
  void Append(const NormColumnReader& column, size_t region);
  void AppendZeros(uint64_t rows);
  void Append(std::vector<uint32_t> values);

  std::unique_ptr<NormColumnReader> Finish(field_id id) &&;

 private:
  void Add(NormRegion region, const NormStats& stats);

  std::vector<NormRegion> _regions;
  std::vector<NormStats> _stats;
  std::vector<std::vector<uint32_t>> _owned;
  doc_id_t _next = doc_limits::min();
};

std::vector<NormColumnSpec> ReadNormColumns(duckdb::BinaryDeserializer& in,
                                            duckdb::field_id_t field,
                                            uint64_t data_size);

class NormColumns final : public NormProvider {
 public:
  NormColumns() = default;
  NormColumns(duckdb::shared_ptr<duckdb::InputFile> file,
              std::span<const NormColumnSpec> columns);
  explicit NormColumns(std::vector<std::unique_ptr<NormColumnReader>> columns);

  const NormColumnReader* Column(field_id id) const noexcept {
    const auto it = _columns.find(id);
    return it == _columns.end() ? nullptr : it->second.get();
  }

  NormReader::ptr norms(field_id field) const final;

 private:
  duckdb::shared_ptr<duckdb::InputFile> _file;
  containers::FlatHashMap<field_id, std::unique_ptr<NormColumnReader>> _columns;
};

}  // namespace irs
