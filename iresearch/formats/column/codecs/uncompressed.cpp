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
#include <duckdb/common/serializer/write_stream.hpp>
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/types/interval.hpp>
#include <duckdb/common/types/null_value.hpp>
#include <duckdb/common/types/uhugeint.hpp>
#include <duckdb/common/types/validity_mask.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/storage/segment/uncompressed.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <limits>

#include "iresearch/formats/column/codecs/codecs.hpp"
#include "iresearch/formats/column/codecs/numeric_axis.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs {
namespace {

using duckdb::const_data_ptr_t;
using duckdb::data_ptr_t;
using duckdb::idx_t;
using duckdb::validity_t;

duckdb::block_id_t WriteOverflowString(duckdb::WriteStream& out,
                                       duckdb::string_t string) {
  const auto size = string.GetSize();
  SDB_ENSURE(size <= std::numeric_limits<uint32_t>::max(),
             "string too long for overflow format");
  const auto block = static_cast<duckdb::block_id_t>(out.Position());
  out.Write<uint32_t>(static_cast<uint32_t>(size));
  out.WriteData(duckdb::const_data_ptr_cast(string.GetData()), size);
  return block;
}

template<typename Appender>
bool CompressChunks(const Codec& codec,
                    ColumnDataCheckpointData& checkpoint_data) {
  CompressedSegment segment{codec, checkpoint_data.type};
  Appender::Begin(segment);
  for (const auto& chunk : checkpoint_data.chunks) {
    duckdb::UnifiedVectorFormat data;
    chunk.data.ToUnifiedFormat(data);
    idx_t offset = 0;
    idx_t remaining = chunk.data.size();
    while (true) {
      const auto appended =
        Appender::Append(checkpoint_data, segment, data, offset, remaining);
      if (appended == remaining) {
        break;
      }
      segment.Flush(checkpoint_data, Appender::Finish(segment));
      Appender::Begin(segment);
      offset += appended;
      remaining -= appended;
    }
  }
  segment.Flush(checkpoint_data, Appender::Finish(segment));
  return true;
}

template<auto kBytes>
double AnalyzeBytes([[maybe_unused]] ColumnDataCheckpointData& data, double) {
  if constexpr (kBytes == nullptr) {
    return kNoScore;
  } else {
    return ScoreOfBytes(kBytes(data.chunks), data);
  }
}

template<auto kBytes, typename Appender>
constexpr CompressionFunction kChunks{
  .analyze = AnalyzeBytes<kBytes>,
  .compress = CompressChunks<Appender>,
};

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

template<typename T, bool kStats>
struct FixedSizeAppender {
  struct SegmentStats {
    void SetHasNull() { stats.SetHasNullFast(); }
    void SetHasValid() { stats.SetHasNoNullFast(); }
    void UpdateMinMax(const T& value) { stats.UpdateNumericStats<T>(value); }

    duckdb::BaseStatistics& stats;
  };

  template<typename Stats>
  IRS_FORCE_INLINE static void Copy(Stats& stats,
                                    const duckdb::UnifiedVectorFormat& data,
                                    const T* source, T* target, idx_t offset,
                                    idx_t count) {
    if (data.validity.CanHaveNull()) {
      for (idx_t i = 0; i < count; ++i) {
        const auto source_idx = data.sel->get_index(offset + i);
        if (!data.validity.RowIsValid(source_idx)) {
          stats.SetHasNull();
          target[i] = duckdb::NullValue<T>();
          continue;
        }
        stats.SetHasValid();
        stats.UpdateMinMax(source[source_idx]);
        target[i] = source[source_idx];
      }
      return;
    }
    for (idx_t i = 0; i < count; ++i) {
      const auto value = source[data.sel->get_index(offset + i)];
      stats.UpdateMinMax(value);
      target[i] = value;
    }
    if (count != 0) {
      stats.SetHasValid();
    }
  }

  static void Begin(CompressedSegment&) {}

  static idx_t Append(ColumnDataCheckpointData&, CompressedSegment& segment,
                      const duckdb::UnifiedVectorFormat& data, idx_t offset,
                      idx_t count) {
    constexpr idx_t kMaxRows = kBlockSize / sizeof(T);
    const auto copy = std::min<idx_t>(count, kMaxRows - segment.count);
    const auto* source = duckdb::UnifiedVectorFormat::GetData<T>(data);
    auto* target = reinterpret_cast<T*>(segment.Data()) + segment.count;
    if constexpr (!kStats) {
      for (idx_t i = 0; i < copy; ++i) {
        target[i] = source[data.sel->get_index(offset + i)];
      }
    } else if constexpr (std::is_integral_v<T>) {
      duckdb::StatsWriter<T> stats;
      Copy(stats, data, source, target, offset, copy);
      stats.Merge(segment.stats);
    } else {
      SegmentStats stats{segment.stats};
      Copy(stats, data, source, target, offset, copy);
    }
    segment.count += copy;
    return copy;
  }

  static uint64_t Finish(CompressedSegment& segment) {
    return segment.count * sizeof(T);
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
idx_t FixedSizeBytes(std::span<const WriteChunk> chunks) {
  return sizeof(T) * CountRows(chunks);
}

template<typename T>
void FixedSizeScan(const ColumnSegment& segment, ColumnScanState& state,
                   idx_t scan_count, duckdb::Vector& result,
                   idx_t result_offset, duckdb::ScanVectorType scan_type) {
  const auto* source = FixedSizeRows<T>(
    segment, ScanBase(state), state.GetPositionInSegment(), scan_count);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  if (scan_type == duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR) {
    duckdb::FlatVector::SetData(result,
                                duckdb::data_ptr_cast(const_cast<T*>(source)),
                                duckdb::count_t(scan_count));
    return;
  }
  auto* target = duckdb::FlatVector::GetDataMutable<T>(result);
  std::memcpy(target + result_offset, source, scan_count * sizeof(T));
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

template<typename T>
constexpr DecompressionFunction kFixedSizeDecompression{
  .init_scan = InitPinnedScan,
  .scan = FixedSizeScan<T>,
  .fetch_rows = FixedSizeFetchRows<T>,
};

template<typename T, bool kStats = true>
constexpr Codec kFixedSize{
  .type = CodecType::Uncompressed,
  .compression = {&kChunks<FixedSizeBytes<T>, FixedSizeAppender<T, kStats>>, 1},
  .decompression = kFixedSizeDecompression<T>,
};

constexpr idx_t kBitsPerEntry = duckdb::ValidityMask::BITS_PER_VALUE;

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

idx_t ValidityBytes(std::span<const WriteChunk> chunks) {
  return (CountRows(chunks) + 7) / 8;
}

const validity_t* ValidityEntries(const ColumnSegment& segment,
                                  const_data_ptr_t base, idx_t start,
                                  idx_t count) {
  SDB_ASSERT(count != 0);
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

void ValidityScan(const ColumnSegment& segment, ColumnScanState& state,
                  idx_t scan_count, duckdb::Vector& result, idx_t result_offset,
                  duckdb::ScanVectorType scan_type) {
  const auto start = state.GetPositionInSegment();
  const bool entire = scan_type == duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR;
  if (entire) {
    result.Flatten();
  }
  const duckdb::unsafe_array_ptr<const validity_t> input{
    ValidityEntries(segment, ScanBase(state), start, scan_count),
    segment.size / sizeof(validity_t)};
  auto& result_mask = duckdb::FlatVector::ValidityMutable(result);
  if (entire && start % kBitsPerEntry == 0) {
    duckdb::ValidityUncompressed::AlignedScan(input, start, result_mask,
                                              scan_count);
    return;
  }
  duckdb::ValidityUncompressed::UnalignedScan(
    input, segment.count, start, result_mask, result_offset, scan_count);
}

void ValiditySelect(const ColumnSegment& segment, ColumnScanState& state,
                    idx_t vector_count, duckdb::Vector& result,
                    idx_t result_offset, const duckdb::SelectionVector& sel,
                    idx_t sel_count, duckdb::ScanVectorType) {
  result.Flatten();
  const auto start = state.GetPositionInSegment();
  const auto* input =
    ValidityEntries(segment, ScanBase(state), start, vector_count);
  auto& result_mask = duckdb::FlatVector::ValidityMutable(result);
  for (idx_t i = 0; i < sel_count; ++i) {
    if (!RowValid(input, start + sel.get_index(i))) {
      result_mask.SetInvalid(result_offset + i);
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

constexpr DecompressionFunction kValidityDecompression{
  .init_scan = InitPinnedScan,
  .scan = ValidityScan,
  .select = ValiditySelect,
  .fetch_rows = ValidityFetchRows,
};

constexpr Codec kValidity{
  .type = CodecType::Uncompressed,
  .compression = {&kChunks<ValidityBytes, ValidityAppender>, 1},
  .decompression = kValidityDecompression,
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
        const auto block =
          WriteOverflowString(checkpoint_data.ctx->Out(), value);
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

class StringLayout {
 public:
  StringLayout(const ColumnSegment& segment, const_data_ptr_t base)
    : _ctx{segment.ctx} {
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

  duckdb::string_t Read(duckdb::Vector& result, idx_t row) const {
    const auto current = _offsets[row];
    const auto offset = DictionaryOffset(current);
    const auto previous = row > 0 ? DictionaryOffset(_offsets[row - 1]) : 0;
    const auto length = offset - previous;
    const auto* data = _dictionary_end - offset;
    if (current >= 0 || length == 0) [[likely]] {
      return {reinterpret_cast<const char*>(data), length};
    }
    const auto block = duckdb::Load<duckdb::block_id_t>(data);
    const auto block_offset =
      duckdb::Load<int32_t>(data + sizeof(duckdb::block_id_t));
    if (block < 0 || block_offset != 0) [[unlikely]] {
      ThrowCorrupted("uncompressed string", "invalid overflow string marker");
    }
    return _ctx.ReadString(result, block, block_offset);
  }

 private:
  ReadContext& _ctx;
  const int32_t* _offsets = nullptr;
  const_data_ptr_t _dictionary_end = nullptr;
  uint32_t _dictionary_size = 0;
  uint64_t _count = 0;
};

template<typename Row>
void ReadStrings(const ColumnSegment& segment, ColumnScanState& state,
                 idx_t span, idx_t count, Row row_at, duckdb::Vector& result,
                 idx_t result_offset) {
  const StringLayout layout{segment, ScanBase(state)};
  const auto start = state.GetPositionInSegment();
  layout.Validate(start, span);
  auto* target = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);
  for (idx_t i = 0; i < count; ++i) {
    target[result_offset + i] = layout.Read(result, start + row_at(i));
  }
}

void StringScan(const ColumnSegment& segment, ColumnScanState& state,
                idx_t scan_count, duckdb::Vector& result, idx_t result_offset,
                duckdb::ScanVectorType) {
  ReadStrings(
    segment, state, scan_count, scan_count, [](idx_t i) { return i; }, result,
    result_offset);
}

void StringSelect(const ColumnSegment& segment, ColumnScanState& state,
                  idx_t vector_count, duckdb::Vector& result,
                  idx_t result_offset, const duckdb::SelectionVector& sel,
                  idx_t sel_count, duckdb::ScanVectorType) {
  ReadStrings(
    segment, state, vector_count, sel_count,
    [&](idx_t i) { return sel.get_index(i); }, result, result_offset);
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
      layout.Read(result, rows[i]);
  }
}

constexpr DecompressionFunction kStringDecompression{
  .init_scan = InitPinnedScan,
  .scan = StringScan,
  .select = StringSelect,
  .fetch_rows = StringFetchRows,
};

constexpr Codec kString{
  .type = CodecType::Uncompressed,
  .compression = {&kChunks<nullptr, StringAppender>, 1},
  .decompression = kStringDecompression,
};

}  // namespace

const Codec* UncompressedCodec(duckdb::PhysicalType physical) {
  using enum duckdb::PhysicalType;
  switch (physical) {
    case INTERVAL:
      return &kFixedSize<duckdb::interval_t>;
    case LIST:
      return &kFixedSize<uint64_t, false>;
    case BIT:
      return &kValidity;
    case VARCHAR:
      return &kString;
    default:
      return Resolve<NumericAxis>(
        physical, [&]<typename T>() -> const Codec* { return &kFixedSize<T>; });
  }
}

}  // namespace irs::codecs
