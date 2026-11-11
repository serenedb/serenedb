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

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/types/interval.hpp>
#include <duckdb/common/types/null_value.hpp>
#include <duckdb/common/types/uhugeint.hpp>
#include <duckdb/common/types/validity_mask.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <limits>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

using duckdb::const_data_ptr_t;
using duckdb::data_ptr_t;
using duckdb::idx_t;
using duckdb::validity_t;

template<typename Appender>
class UncompressedCompress {
 public:
  explicit UncompressedCompress(ColumnDataCheckpointData& checkpoint_data)
    : _checkpoint_data{&checkpoint_data}, _segment{checkpoint_data.type} {
    Appender::Begin(_segment);
  }

  void Append(const duckdb::Vector& input) {
    duckdb::UnifiedVectorFormat data;
    input.ToUnifiedFormat(data);
    idx_t offset = 0;
    idx_t remaining = input.size();
    while (remaining > 0) {
      const auto appended =
        Appender::Append(*_checkpoint_data, _segment, data, offset, remaining);
      if (appended == remaining) {
        return;
      }
      Flush();
      offset += appended;
      remaining -= appended;
    }
  }

  void Flush() {
    _segment.Flush(*_checkpoint_data, Appender::Finish(_segment));
    Appender::Begin(_segment);
  }

 private:
  ColumnDataCheckpointData* _checkpoint_data;
  CompressedSegment _segment;
};

template<typename Appender>
void CompressChunks(ColumnDataCheckpointData& checkpoint_data,
                    std::span<const WriteChunk> chunks) {
  const auto state =
    std::make_unique<UncompressedCompress<Appender>>(checkpoint_data);
  for (const auto& chunk : chunks) {
    state->Append(chunk.data);
  }
  state->Flush();
}

struct PinnedScan final : SegmentScanState {
  explicit PinnedScan(duckdb::BufferHandle handle_p)
    : handle{std::move(handle_p)} {}

  duckdb::BufferHandle handle;
};

std::unique_ptr<SegmentScanState> InitPinnedScan(const ColumnSegment& segment) {
  return std::make_unique<PinnedScan>(segment.Pin());
}

const_data_ptr_t ScanBase(ColumnScanState& state) {
  return state.scan_state->Cast<PinnedScan>().handle.Ptr();
}

template<typename T, bool kList>
struct FixedSizeAppender {
  using Stored = std::conditional_t<kList, uint64_t, T>;

  static void Begin(CompressedSegment&) {}

  static idx_t Append(ColumnDataCheckpointData&, CompressedSegment& segment,
                      const duckdb::UnifiedVectorFormat& data, idx_t offset,
                      idx_t count) {
    constexpr idx_t kMaxRows = kBlockSize / sizeof(Stored);
    const auto copy = std::min<idx_t>(count, kMaxRows - segment.count);
    const auto* source = duckdb::UnifiedVectorFormat::GetData<Stored>(data);
    auto* target = reinterpret_cast<Stored*>(segment.Data()) + segment.count;
    auto& stats = segment.stats;
    if constexpr (kList) {
      for (idx_t i = 0; i < copy; ++i) {
        target[i] = source[data.sel->get_index(offset + i)];
      }
    } else if (data.validity.CanHaveNull()) {
      for (idx_t i = 0; i < copy; ++i) {
        const auto source_idx = data.sel->get_index(offset + i);
        if (!data.validity.RowIsValid(source_idx)) {
          stats.SetHasNullFast();
          target[i] = duckdb::NullValue<T>();
          continue;
        }
        stats.SetHasNoNullFast();
        stats.UpdateNumericStats<T>(source[source_idx]);
        target[i] = source[source_idx];
      }
    } else {
      stats.SetHasNoNullFast();
      for (idx_t i = 0; i < copy; ++i) {
        const auto value = source[data.sel->get_index(offset + i)];
        stats.UpdateNumericStats<T>(value);
        target[i] = value;
      }
    }
    segment.count += copy;
    return copy;
  }

  static uint64_t Finish(CompressedSegment& segment) {
    return segment.count * sizeof(Stored);
  }
};

template<typename T>
const T* FixedSizeRows(const ColumnSegment& segment, const_data_ptr_t base,
                       idx_t start, idx_t count) {
  SDB_ASSERT(start <= segment.count && count <= segment.count - start);
  const auto capacity = segment.size / sizeof(T);
  if (start > capacity || count > capacity - start) [[unlikely]] {
    ThrowCorrupted("uncompressed", "rows exceed the segment size");
  }
  return reinterpret_cast<const T*>(base) + start;
}

template<typename T>
idx_t FixedSizeAnalyze(std::span<const WriteChunk> chunks) {
  return sizeof(T) * CountRows(chunks);
}

template<typename T>
void FixedSizeScanPartial(const ColumnSegment& segment, ColumnScanState& state,
                          idx_t scan_count, duckdb::Vector& result,
                          idx_t result_offset) {
  const auto* source = FixedSizeRows<T>(
    segment, ScanBase(state), state.GetPositionInSegment(), scan_count);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* target = duckdb::FlatVector::GetDataMutable<T>(result);
  std::memcpy(target + result_offset, source, scan_count * sizeof(T));
}

template<typename T>
void FixedSizeScan(const ColumnSegment& segment, ColumnScanState& state,
                   idx_t scan_count, duckdb::Vector& result) {
  const auto* source = FixedSizeRows<T>(
    segment, ScanBase(state), state.GetPositionInSegment(), scan_count);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::FlatVector::SetData(result,
                              duckdb::data_ptr_cast(const_cast<T*>(source)),
                              duckdb::count_t(scan_count));
}

template<typename T>
void FixedSizeFetchRows(const ColumnSegment& segment, ColumnFetchState& state,
                        const idx_t* rows, idx_t count, duckdb::Vector& result,
                        const duckdb::SelectionVector& result_sel,
                        idx_t result_offset) {
  const auto* source = FixedSizeRows<T>(
    segment, state.GetOrInsertHandle(segment).Ptr(), 0, segment.count);
  auto* target = duckdb::FlatVector::GetDataMutable<T>(result);
  for (idx_t i = 0; i < count; ++i) {
    SDB_ASSERT(rows[i] < segment.count);
    target[result_offset + result_sel.get_index(i)] = source[rows[i]];
  }
}

template<typename T, bool kList, duckdb::PhysicalType kPhysical>
constexpr CompressionFunction kFixedSize{
  .type = CodecType::Uncompressed,
  .analyze = FixedSizeAnalyze<std::conditional_t<kList, uint64_t, T>>,
  .compress = CompressChunks<FixedSizeAppender<T, kList>>,
  .init_scan = InitPinnedScan,
  .scan_vector = FixedSizeScan<std::conditional_t<kList, uint64_t, T>>,
  .scan_partial = FixedSizeScanPartial<std::conditional_t<kList, uint64_t, T>>,
  .fetch_rows = FixedSizeFetchRows<std::conditional_t<kList, uint64_t, T>>,
  .skip = EmptySkip,
};

constexpr idx_t kBitsPerEntry = duckdb::ValidityMask::BITS_PER_VALUE;
constexpr validity_t kAllValid = ~validity_t{0};

constexpr validity_t LowerMask(idx_t bits) noexcept {
  return bits >= kBitsPerEntry ? kAllValid : (validity_t{1} << bits) - 1;
}

constexpr validity_t UpperMask(idx_t bits) noexcept {
  return bits == 0 ? 0 : kAllValid << (kBitsPerEntry - bits);
}

struct ValidityAppender {
  static void Begin(CompressedSegment& segment) {
    std::memset(segment.Data(), 0xFF, kBlockSize);
  }

  static idx_t Append(ColumnDataCheckpointData&, CompressedSegment& segment,
                      const duckdb::UnifiedVectorFormat& data, idx_t offset,
                      idx_t count) {
    constexpr idx_t kMaxRows = kBlockSize /
                               duckdb::ValidityMask::STANDARD_MASK_SIZE *
                               STANDARD_VECTOR_SIZE;
    const auto append = std::min<idx_t>(count, kMaxRows - segment.count);
    auto& stats = segment.stats;
    if (data.validity.CannotHaveNull()) {
      segment.count += append;
      stats.SetHasNoNullFast();
      return append;
    }
    auto* entries = reinterpret_cast<validity_t*>(segment.Data());
    for (idx_t i = 0; i < append; ++i) {
      const auto idx = data.sel->get_index(offset + i);
      if (data.validity.RowIsValidUnsafe(idx)) {
        stats.SetHasNoNullFast();
        continue;
      }
      const auto row = segment.count + i;
      entries[row / kBitsPerEntry] &= ~(validity_t{1} << (row % kBitsPerEntry));
      stats.SetHasNullFast();
    }
    segment.count += append;
    return append;
  }

  static uint64_t Finish(CompressedSegment& segment) {
    return (segment.count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE *
           duckdb::ValidityMask::STANDARD_MASK_SIZE;
  }
};

idx_t ValidityAnalyze(std::span<const WriteChunk> chunks) {
  return (CountRows(chunks) + 7) / 8;
}

const validity_t* ValidityEntries(const ColumnSegment& segment,
                                  const_data_ptr_t base, idx_t start,
                                  idx_t count) {
  SDB_ASSERT(start <= segment.count && count <= segment.count - start);
  const auto last_entry = (start + count - 1) / kBitsPerEntry;
  if ((last_entry + 1) * sizeof(validity_t) > segment.size) [[unlikely]] {
    ThrowCorrupted("validity", "rows exceed the segment size");
  }
  return reinterpret_cast<const validity_t*>(base);
}

bool RowValid(const validity_t* entries, idx_t row) noexcept {
  return duckdb::ValidityMask::RowIsValid(entries[row / kBitsPerEntry],
                                          row % kBitsPerEntry);
}

void UnalignedScan(const validity_t* input, idx_t input_start,
                   duckdb::ValidityMask& result_mask, idx_t result_offset,
                   idx_t scan_count) {
  auto* result_data = result_mask.GetData();
  auto result_entry = result_offset / kBitsPerEntry;
  auto result_idx = result_offset % kBitsPerEntry;
  auto input_entry = input_start / kBitsPerEntry;
  auto input_idx = input_start % kBitsPerEntry;
  idx_t pos = 0;
  while (pos < scan_count) {
    auto input_mask = input[input_entry];
    const auto bits_left = scan_count - pos;
    const auto window_size = std::min(
      {bits_left, kBitsPerEntry - input_idx, kBitsPerEntry - result_idx});
    if (result_idx < input_idx) {
      input_mask >>= input_idx - result_idx;
    } else {
      const auto shift = result_idx - input_idx;
      input_mask = (input_mask & ~UpperMask(shift)) << shift;
    }
    input_mask |= UpperMask(kBitsPerEntry - result_idx - window_size);
    input_mask |= LowerMask(result_idx);
    if (input_mask != kAllValid) {
      if (!result_data) {
        result_mask.Initialize();
        result_data = result_mask.GetData();
      }
      result_data[result_entry] &= input_mask;
    }
    pos += window_size;
    input_idx = (input_idx + window_size) % kBitsPerEntry;
    result_idx = (result_idx + window_size) % kBitsPerEntry;
    input_entry += input_idx == 0;
    result_entry += result_idx == 0;
  }
}

void ValidityScanPartial(const ColumnSegment& segment, ColumnScanState& state,
                         idx_t scan_count, duckdb::Vector& result,
                         idx_t result_offset) {
  if (scan_count == 0) {
    return;
  }
  const auto start = state.GetPositionInSegment();
  const auto* input =
    ValidityEntries(segment, ScanBase(state), start, scan_count);
  UnalignedScan(input, start, duckdb::FlatVector::ValidityMutable(result),
                result_offset, scan_count);
}

void ValidityScan(const ColumnSegment& segment, ColumnScanState& state,
                  idx_t scan_count, duckdb::Vector& result) {
  if (scan_count == 0) {
    return;
  }
  result.Flatten();
  const auto start = state.GetPositionInSegment();
  if (start % kBitsPerEntry != 0) {
    ValidityScanPartial(segment, state, scan_count, result, 0);
    return;
  }
  const auto* input =
    ValidityEntries(segment, ScanBase(state), start, scan_count) +
    start / kBitsPerEntry;
  auto& result_mask = duckdb::FlatVector::ValidityMutable(result);
  auto* result_data = result_mask.GetData();
  const auto entries = (scan_count + kBitsPerEntry - 1) / kBitsPerEntry;
  for (idx_t i = 0; i < entries; ++i) {
    const auto entry = input[i];
    if (!result_data && entry == kAllValid) {
      continue;
    }
    if (!result_data) {
      result_mask.Initialize();
      result_data = result_mask.GetData();
    }
    result_data[i] = entry;
  }
}

void ValiditySelect(const ColumnSegment& segment, ColumnScanState& state,
                    idx_t vector_count, duckdb::Vector& result,
                    const duckdb::SelectionVector& sel, idx_t sel_count) {
  if (sel_count == 0) {
    return;
  }
  result.Flatten();
  const auto start = state.GetPositionInSegment();
  const auto* input =
    ValidityEntries(segment, ScanBase(state), start, vector_count);
  auto& result_mask = duckdb::FlatVector::ValidityMutable(result);
  for (idx_t i = 0; i < sel_count; ++i) {
    if (!RowValid(input, start + sel.get_index(i))) {
      result_mask.SetInvalid(i);
    }
  }
}

void ValidityFetchRows(const ColumnSegment& segment, ColumnFetchState& state,
                       const idx_t* rows, idx_t count, duckdb::Vector& result,
                       const duckdb::SelectionVector& result_sel,
                       idx_t result_offset) {
  const auto* input = ValidityEntries(
    segment, state.GetOrInsertHandle(segment).Ptr(), 0, segment.count);
  auto& result_mask = duckdb::FlatVector::ValidityMutable(result);
  for (idx_t i = 0; i < count; ++i) {
    SDB_ASSERT(rows[i] < segment.count);
    if (!RowValid(input, rows[i])) {
      result_mask.SetInvalid(result_offset + result_sel.get_index(i));
    }
  }
}

constexpr CompressionFunction kValidity{
  .type = CodecType::Uncompressed,
  .analyze = ValidityAnalyze,
  .compress = CompressChunks<ValidityAppender>,
  .init_scan = InitPinnedScan,
  .scan_vector = ValidityScan,
  .scan_partial = ValidityScanPartial,
  .select = ValiditySelect,
  .fetch_rows = ValidityFetchRows,
  .skip = EmptySkip,
};

constexpr uint32_t kDictionaryHeaderSize = 2 * sizeof(uint32_t);
constexpr idx_t kOverflowMarkerSize =
  sizeof(duckdb::block_id_t) + sizeof(int32_t);
constexpr idx_t kStringBlockLimit = 4096;
constexpr idx_t kCompactionFlushLimit = kBlockSize / 5 * 4;

static_assert(kStringBlockLimit ==
              std::min<idx_t>(kBlockSize / 4 / 8 * 8, kStringBlockLimit));

struct StringAppender {
  static void Begin(CompressedSegment& segment) {
    duckdb::Store<uint32_t>(0, segment.Data());
    duckdb::Store<uint32_t>(static_cast<uint32_t>(kBlockSize),
                            segment.Data() + sizeof(uint32_t));
  }

  static idx_t Append(ColumnDataCheckpointData& checkpoint_data,
                      CompressedSegment& segment,
                      const duckdb::UnifiedVectorFormat& data, idx_t offset,
                      idx_t count) {
    auto* base = segment.Data();
    const auto* source =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(data);
    auto* offsets = reinterpret_cast<int32_t*>(base + kDictionaryHeaderSize);
    auto* dictionary_size = reinterpret_cast<uint32_t*>(base);
    const auto dictionary_end = duckdb::Load<uint32_t>(base + sizeof(uint32_t));
    idx_t remaining =
      kBlockSize - (*dictionary_size + segment.count * sizeof(int32_t) +
                    kDictionaryHeaderSize);
    const auto base_count = segment.count;
    duckdb::StatsWriter<duckdb::string_t> stats_writer{segment.stats.GetType()};
    for (idx_t i = 0; i < count; ++i) {
      const auto source_idx = data.sel->get_index(offset + i);
      const auto target_idx = base_count + i;
      if (remaining < sizeof(int32_t)) {
        count = i;
        break;
      }
      remaining -= sizeof(int32_t);
      if (!data.validity.RowIsValid(source_idx)) {
        stats_writer.SetHasNull();
        offsets[target_idx] = target_idx > 0 ? offsets[target_idx - 1] : 0;
        continue;
      }
      const auto& value = source[source_idx];
      const idx_t length = value.GetSize();
      const bool overflow = length >= kStringBlockLimit;
      const idx_t required = overflow ? kOverflowMarkerSize : length;
      if (required > remaining) {
        count = i;
        break;
      }
      stats_writer.Update(value);
      *dictionary_size += static_cast<uint32_t>(required);
      remaining -= required;
      auto* position = base + dictionary_end - *dictionary_size;
      if (overflow) {
        const auto block = checkpoint_data.ctx.WriteOverflowString(value);
        const int32_t block_offset = 0;
        std::memcpy(position, &block, sizeof(block));
        std::memcpy(position + sizeof(block), &block_offset,
                    sizeof(block_offset));
        offsets[target_idx] = -static_cast<int32_t>(*dictionary_size);
      } else {
        std::memcpy(position, value.GetData(), length);
        offsets[target_idx] = static_cast<int32_t>(*dictionary_size);
      }
    }
    stats_writer.Merge(segment.stats);
    segment.count += count;
    return count;
  }

  static uint64_t Finish(CompressedSegment& segment) {
    auto* base = segment.Data();
    const auto dictionary_size = duckdb::Load<uint32_t>(base);
    auto dictionary_end = duckdb::Load<uint32_t>(base + sizeof(uint32_t));
    SDB_ASSERT(dictionary_end == kBlockSize);
    const auto offset_size =
      kDictionaryHeaderSize + segment.count * sizeof(int32_t);
    const auto total_size = offset_size + dictionary_size;
    if (total_size >= kCompactionFlushLimit) {
      std::memset(base + offset_size, 0,
                  dictionary_end - dictionary_size - offset_size);
      return kBlockSize;
    }
    std::memmove(base + offset_size, base + dictionary_end - dictionary_size,
                 dictionary_size);
    dictionary_end = static_cast<uint32_t>(total_size);
    duckdb::Store<uint32_t>(dictionary_end, base + sizeof(uint32_t));
    return total_size;
  }
};

uint32_t DictionaryOffset(int32_t encoded) noexcept {
  return static_cast<uint32_t>(std::abs(static_cast<int64_t>(encoded)));
}

bool ValidEntry(int32_t current, int32_t previous,
                uint32_t dictionary_size) noexcept {
  const auto current_offset = DictionaryOffset(current);
  const auto previous_offset = DictionaryOffset(previous);
  const auto length = current_offset - previous_offset;
  uint32_t invalid = current == std::numeric_limits<int32_t>::min();
  invalid |= current_offset > dictionary_size;
  invalid |= current_offset < previous_offset;
  const uint32_t invalid_null = (length == 0) & (current != previous);
  const uint32_t invalid_overflow =
    (length != 0) & (length != kOverflowMarkerSize);
  invalid |=
    static_cast<uint32_t>(current < 0) & (invalid_null | invalid_overflow);
  return invalid == 0;
}

struct StringEntry {
  const_data_ptr_t data;
  uint32_t length;
  bool overflow;
};

class StringLayout {
 public:
  StringLayout(const ColumnSegment& segment, const_data_ptr_t base) {
    if (segment.size < kDictionaryHeaderSize) [[unlikely]] {
      ThrowCorrupted("uncompressed string", "header exceeds the segment");
    }
    const auto dictionary_end = duckdb::Load<uint32_t>(base + sizeof(uint32_t));
    if (dictionary_end > segment.size ||
        dictionary_end < kDictionaryHeaderSize ||
        segment.count > (dictionary_end - kDictionaryHeaderSize) /
                          sizeof(int32_t)) [[unlikely]] {
      ThrowCorrupted("uncompressed string",
                     "dictionary is outside the segment or overlaps offsets");
    }
    const auto dictionary_start =
      kDictionaryHeaderSize + segment.count * sizeof(int32_t);
    _offsets = reinterpret_cast<const int32_t*>(base + kDictionaryHeaderSize);
    _dictionary_end = base + dictionary_end;
    _dictionary_size = static_cast<uint32_t>(dictionary_end - dictionary_start);
    _count = segment.count;
  }

  void Validate(idx_t start, idx_t count) const {
    SDB_ASSERT(start <= _count && count <= _count - start);
    auto previous = start > 0 ? _offsets[start - 1] : 0;
    bool valid = previous != std::numeric_limits<int32_t>::min() &&
                 DictionaryOffset(previous) <= _dictionary_size;
    for (idx_t i = 0; i < count; ++i) {
      const auto current = _offsets[start + i];
      valid &= ValidEntry(current, previous, _dictionary_size);
      previous = current;
    }
    if (!valid) [[unlikely]] {
      ThrowCorrupted("uncompressed string", "dictionary offsets are invalid");
    }
  }

  StringEntry Entry(idx_t row) const noexcept {
    const auto current = _offsets[row];
    const auto offset = DictionaryOffset(current);
    const auto previous = row > 0 ? DictionaryOffset(_offsets[row - 1]) : 0;
    const auto length = offset - previous;
    return {_dictionary_end - offset, length, current < 0 && length > 0};
  }

 private:
  const int32_t* _offsets = nullptr;
  const_data_ptr_t _dictionary_end = nullptr;
  uint32_t _dictionary_size = 0;
  uint64_t _count = 0;
};

duckdb::string_t ReadEntry(const ColumnSegment& segment, duckdb::Vector& result,
                           const StringEntry& entry) {
  if (!entry.overflow) [[likely]] {
    return {reinterpret_cast<const char*>(entry.data), entry.length};
  }
  const auto block = duckdb::Load<duckdb::block_id_t>(entry.data);
  const auto offset =
    duckdb::Load<int32_t>(entry.data + sizeof(duckdb::block_id_t));
  if (block < 0 || offset != 0) [[unlikely]] {
    ThrowCorrupted("uncompressed string", "invalid overflow string marker");
  }
  return segment.ctx.ReadString(result, block, offset);
}

void StringScanPartial(const ColumnSegment& segment, ColumnScanState& state,
                       idx_t scan_count, duckdb::Vector& result,
                       idx_t result_offset) {
  const StringLayout layout{segment, ScanBase(state)};
  const auto start = state.GetPositionInSegment();
  layout.Validate(start, scan_count);
  auto* target = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  for (idx_t i = 0; i < scan_count; ++i) {
    target[result_offset + i] =
      ReadEntry(segment, result, layout.Entry(start + i));
  }
}

void StringScan(const ColumnSegment& segment, ColumnScanState& state,
                idx_t scan_count, duckdb::Vector& result) {
  StringScanPartial(segment, state, scan_count, result, 0);
}

void StringSelect(const ColumnSegment& segment, ColumnScanState& state,
                  idx_t vector_count, duckdb::Vector& result,
                  const duckdb::SelectionVector& sel, idx_t sel_count) {
  const StringLayout layout{segment, ScanBase(state)};
  const auto start = state.GetPositionInSegment();
  layout.Validate(start, vector_count);
  auto* target = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  for (idx_t i = 0; i < sel_count; ++i) {
    target[i] =
      ReadEntry(segment, result, layout.Entry(start + sel.get_index(i)));
  }
}

void StringFetchRows(const ColumnSegment& segment, ColumnFetchState& state,
                     const idx_t* rows, idx_t count, duckdb::Vector& result,
                     const duckdb::SelectionVector& result_sel,
                     idx_t result_offset) {
  const StringLayout layout{segment, state.GetOrInsertHandle(segment).Ptr()};
  auto* target = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  for (idx_t i = 0; i < count; ++i) {
    SDB_ASSERT(rows[i] < segment.count);
    layout.Validate(rows[i], 1);
    target[result_offset + result_sel.get_index(i)] =
      ReadEntry(segment, result, layout.Entry(rows[i]));
  }
}

constexpr CompressionFunction kString{
  .type = CodecType::Uncompressed,
  .compress = CompressChunks<StringAppender>,
  .init_scan = InitPinnedScan,
  .scan_vector = StringScan,
  .scan_partial = StringScanPartial,
  .select = StringSelect,
  .fetch_rows = StringFetchRows,
  .skip = EmptySkip,
};

}  // namespace

const CompressionFunction* UncompressedFunction(duckdb::PhysicalType physical) {
  using enum duckdb::PhysicalType;
  switch (physical) {
    case BOOL:
      return &kFixedSize<int8_t, false, BOOL>;
    case INT8:
      return &kFixedSize<int8_t, false, INT8>;
    case INT16:
      return &kFixedSize<int16_t, false, INT16>;
    case INT32:
      return &kFixedSize<int32_t, false, INT32>;
    case INT64:
      return &kFixedSize<int64_t, false, INT64>;
    case UINT8:
      return &kFixedSize<uint8_t, false, UINT8>;
    case UINT16:
      return &kFixedSize<uint16_t, false, UINT16>;
    case UINT32:
      return &kFixedSize<uint32_t, false, UINT32>;
    case UINT64:
      return &kFixedSize<uint64_t, false, UINT64>;
    case INT128:
      return &kFixedSize<duckdb::hugeint_t, false, INT128>;
    case UINT128:
      return &kFixedSize<duckdb::uhugeint_t, false, UINT128>;
    case FLOAT:
      return &kFixedSize<float, false, FLOAT>;
    case DOUBLE:
      return &kFixedSize<double, false, DOUBLE>;
    case INTERVAL:
      return &kFixedSize<duckdb::interval_t, false, INTERVAL>;
    case LIST:
      return &kFixedSize<uint64_t, true, LIST>;
    case BIT:
      return &kValidity;
    case VARCHAR:
      return &kString;
    default:
      return nullptr;
  }
}

}  // namespace irs::codecs
