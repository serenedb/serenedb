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

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/limits.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/operator/add.hpp>
#include <duckdb/common/operator/multiply.hpp>
#include <duckdb/common/operator/subtract.hpp>
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/types/uhugeint.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/planner/filter/zonemap_checker.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/compression/bitpacking.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/segment_reader.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

using duckdb::bitpacking_width_t;
using duckdb::BitpackingMode;
using duckdb::BitpackingPrimitives;
using duckdb::FilterPropagateResult;
using duckdb::idx_t;

template<typename T>
using Signed = typename duckdb::MakeSigned<T>::type;

template<typename T>
using Unsigned = typename duckdb::MakeUnsigned<T>::type;

constexpr idx_t kMetadataGroupSize =
  STANDARD_VECTOR_SIZE > 512 ? STANDARD_VECTOR_SIZE : 2048;
constexpr idx_t kAlgorithmGroupSize =
  BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
constexpr idx_t kAlgorithmGroups = kMetadataGroupSize / kAlgorithmGroupSize;
constexpr idx_t kHeaderSize = BitpackingPrimitives::BITPACKING_HEADER_SIZE;
constexpr idx_t kMetaSize = sizeof(uint32_t);
constexpr idx_t kDecodeDistance = kAlgorithmGroupSize / 4;
constexpr idx_t kNone = duckdb::DConstants::INVALID_INDEX;
constexpr bool kSkipSignExtend = true;

struct GroupMeta {
  BitpackingMode mode = BitpackingMode::INVALID;
  uint32_t offset = 0;
};

uint32_t EncodeMeta(GroupMeta meta) noexcept {
  SDB_ASSERT(meta.offset <= 0x00FFFFFF);
  return meta.offset |
         static_cast<uint32_t>(static_cast<uint8_t>(meta.mode) << 24);
}

GroupMeta DecodeMeta(uint32_t encoded) noexcept {
  return {
    .mode = static_cast<BitpackingMode>((encoded >> 24) & 0xFF),
    .offset = encoded & 0x00FFFFFF,
  };
}

[[noreturn]] void Corrupted(std::string_view what) {
  ThrowCorrupted("bitpacking", what);
}

[[noreturn]] void UnknownMode() { Corrupted("unknown bitpacking mode"); }

[[noreturn]] void GroupOffsetsInvalid() {
  Corrupted("group offsets do not describe a range in the data region");
}

[[noreturn]] void ReadPastEnd() {
  Corrupted("read exceeds the segment row count");
}

void ValidateMode(BitpackingMode mode) {
  switch (mode) {
    case BitpackingMode::CONSTANT:
    case BitpackingMode::CONSTANT_DELTA:
    case BitpackingMode::FOR:
    case BitpackingMode::DELTA_FOR:
      return;
    default:
      UnknownMode();
  }
}

template<typename T>
bitpacking_width_t ValidateWidth(T stored_width) {
  if (static_cast<Unsigned<T>>(stored_width) > sizeof(T) * 8) {
    Corrupted("bit width exceeds the physical type width");
  }
  return static_cast<bitpacking_width_t>(stored_width);
}

BitpackingMode ForcedMode(duckdb::DatabaseInstance& db) {
  return duckdb::Settings::Get<duckdb::ForceBitpackingModeSetting>(
    duckdb::DBConfig::GetConfig(db));
}

template<typename T>
struct NoWriter {
  void WriteConstant(T, idx_t) {}
  void WriteConstantDelta(Signed<T>, T, idx_t) {}
  void WriteDeltaFor(T*, bitpacking_width_t, T, Signed<T>, idx_t) {}
  void WriteFor(T*, bitpacking_width_t, T, idx_t) {}
};

template<typename T>
struct BitpackingState {
  using S = Signed<T>;

  explicit BitpackingState(BitpackingMode mode_p) : mode{mode_p} {
    compression_buffer_internal[0] = T(0);
    Reset();
  }

  T* Buffer() noexcept { return compression_buffer_internal + 1; }

  void Reset() {
    minimum = duckdb::NumericLimits<T>::Maximum();
    minimum_delta = duckdb::NumericLimits<S>::Maximum();
    maximum = duckdb::NumericLimits<T>::Minimum();
    maximum_delta = duckdb::NumericLimits<S>::Minimum();
    delta_offset = 0;
    all_valid = true;
    all_invalid = true;
    has_valid = false;
    has_invalid = false;
    can_do_delta = false;
    can_do_for = false;
    compression_buffer_idx = 0;
    min_max_diff = 0;
    min_max_delta_diff = 0;
  }

  void CalculateForStats() {
    can_do_for =
      duckdb::TrySubtractOperator::Operation(maximum, minimum, min_max_diff);
  }

  void CalculateDeltaStats() {
    if (maximum > static_cast<T>(duckdb::NumericLimits<S>::Maximum())) {
      return;
    }
    if (compression_buffer_idx < 2) {
      return;
    }
    if (!all_valid) {
      return;
    }
    bool can_do_all = true;
    if (duckdb::NumericLimits<T>::IsSigned()) {
      S bogus{};
      can_do_all = duckdb::TrySubtractOperator::Operation(
                     static_cast<S>(minimum), static_cast<S>(maximum), bogus) &&
                   duckdb::TrySubtractOperator::Operation(
                     static_cast<S>(maximum), static_cast<S>(minimum), bogus);
    }
    const T* buffer = Buffer();
    const auto count = static_cast<int64_t>(compression_buffer_idx);
    if (can_do_all) {
      for (int64_t i = 0; i < count; i++) {
        delta_buffer[i] =
          static_cast<S>(buffer[i]) - static_cast<S>(buffer[i - 1]);
      }
    } else {
      for (int64_t i = 0; i < count; i++) {
        const bool success = duckdb::TrySubtractOperator::Operation(
          static_cast<S>(buffer[i]), static_cast<S>(buffer[i - 1]),
          delta_buffer[i]);
        if (!success) {
          return;
        }
      }
    }
    can_do_delta = true;
    for (idx_t i = 1; i < compression_buffer_idx; i++) {
      maximum_delta = duckdb::MaxValue<S>(maximum_delta, delta_buffer[i]);
      minimum_delta = duckdb::MinValue<S>(minimum_delta, delta_buffer[i]);
    }
    delta_buffer[0] = minimum_delta;
    can_do_delta =
      can_do_delta && duckdb::TrySubtractOperator::Operation(
                        maximum_delta, minimum_delta, min_max_delta_diff);
    can_do_delta =
      can_do_delta && duckdb::TrySubtractOperator::Operation(
                        static_cast<S>(buffer[0]), minimum_delta, delta_offset);
  }

  template<typename V>
  void SubtractFrameOfReference(V* buffer, V frame_of_reference) {
    static_assert(duckdb::NumericLimits<V>::IsIntegral());
    for (idx_t i = 0; i < compression_buffer_idx; i++) {
      reinterpret_cast<Unsigned<V>*>(buffer)[i] -=
        static_cast<Unsigned<V>>(frame_of_reference);
    }
  }

  template<typename Writer>
  bool Flush(Writer& writer) {
    if (compression_buffer_idx == 0) {
      return true;
    }
    if ((all_invalid || maximum == minimum) &&
        (mode == BitpackingMode::AUTO || mode == BitpackingMode::CONSTANT)) {
      writer.WriteConstant(maximum, compression_buffer_idx);
      total_size += sizeof(T) + kMetaSize;
      return true;
    }
    CalculateForStats();
    CalculateDeltaStats();
    T* buffer = Buffer();
    if (can_do_delta) {
      if (maximum_delta == minimum_delta && mode != BitpackingMode::FOR &&
          mode != BitpackingMode::DELTA_FOR) {
        const T frame_of_reference = buffer[0];
        writer.WriteConstantDelta(maximum_delta, frame_of_reference,
                                  compression_buffer_idx);
        total_size += sizeof(T) + sizeof(T) + kMetaSize;
        return true;
      }
      const auto delta_required_bitwidth =
        BitpackingPrimitives::MinimumBitWidth<T, false>(
          static_cast<T>(min_max_delta_diff));
      const auto regular_required_bitwidth =
        BitpackingPrimitives::MinimumBitWidth(min_max_diff);
      const bool prefer_for =
        can_do_for && delta_required_bitwidth >= regular_required_bitwidth;
      if (!prefer_for && mode != BitpackingMode::FOR) {
        SubtractFrameOfReference(delta_buffer, minimum_delta);
        writer.WriteDeltaFor(
          reinterpret_cast<T*>(delta_buffer), delta_required_bitwidth,
          static_cast<T>(minimum_delta), delta_offset, compression_buffer_idx);
        total_size += sizeof(T);
        total_size += duckdb::AlignValue(sizeof(bitpacking_width_t));
        total_size += sizeof(T);
        total_size += BitpackingPrimitives::GetRequiredSize(
          compression_buffer_idx, delta_required_bitwidth);
        return true;
      }
    }
    if (can_do_for) {
      const auto width =
        BitpackingPrimitives::MinimumBitWidth<T, false>(min_max_diff);
      SubtractFrameOfReference(buffer, minimum);
      writer.WriteFor(buffer, width, minimum, compression_buffer_idx);
      total_size +=
        BitpackingPrimitives::GetRequiredSize(compression_buffer_idx, width);
      total_size += sizeof(T);
      total_size += duckdb::AlignValue(sizeof(bitpacking_width_t));
      return true;
    }
    return false;
  }

  template<typename Writer>
  bool Update(const typename duckdb::VectorIterator<T>::ValueEntry& entry,
              Writer& writer) {
    const bool is_valid = entry.IsValid();
    has_valid = has_valid || is_valid;
    has_invalid = has_invalid || !is_valid;
    all_valid = all_valid && is_valid;
    all_invalid = all_invalid && !is_valid;
    T* buffer = Buffer();
    if (is_valid) {
      const T value = entry.GetValue();
      buffer[compression_buffer_idx] = value;
      minimum = duckdb::MinValue<T>(minimum, value);
      maximum = duckdb::MaxValue<T>(maximum, value);
    } else if constexpr (!std::is_same_v<Writer, NoWriter<T>>) {
      buffer[compression_buffer_idx] = T(0);
    }
    compression_buffer_idx++;
    if (compression_buffer_idx == kMetadataGroupSize) {
      const bool success = Flush(writer);
      Reset();
      return success;
    }
    return true;
  }

  T compression_buffer_internal[kMetadataGroupSize + 1];
  S delta_buffer[kMetadataGroupSize];
  idx_t compression_buffer_idx = 0;
  idx_t total_size = 0;
  T minimum{};
  T maximum{};
  T min_max_diff{};
  S minimum_delta{};
  S maximum_delta{};
  S min_max_delta_diff{};
  S delta_offset{};
  bool all_valid = true;
  bool all_invalid = true;
  bool has_valid = false;
  bool has_invalid = false;
  bool can_do_delta = false;
  bool can_do_for = false;
  BitpackingMode mode = BitpackingMode::AUTO;
};

template<typename T>
struct BitpackingAnalyze final : AnalyzeState {
  explicit BitpackingAnalyze(BitpackingMode mode) : state{mode} {}

  BitpackingState<T> state;
};

template<typename T>
std::unique_ptr<AnalyzeState> InitAnalyze(CompressionAnalyzeContext& ctx,
                                          duckdb::PhysicalType) {
  return std::make_unique<BitpackingAnalyze<T>>(ForcedMode(ctx.ctx.Database()));
}

template<typename T>
bool Analyze(AnalyzeState& state, const duckdb::Vector& input) {
  const auto type_size = duckdb::GetTypeIdSize(input.GetType().InternalType());
  if (type_size * kMetadataGroupSize * 2 > kBlockSize) {
    return false;
  }
  auto& analyze = state.Cast<BitpackingAnalyze<T>>();
  NoWriter<T> writer;
  for (auto entry : input.Values<T>()) {
    if (!analyze.state.Update(entry, writer)) {
      return false;
    }
  }
  return true;
}

template<typename T>
idx_t FinalAnalyze(AnalyzeState& state) {
  auto& analyze = state.Cast<BitpackingAnalyze<T>>();
  NoWriter<T> writer;
  if (!analyze.state.Flush(writer)) {
    return kNone;
  }
  return analyze.state.total_size;
}

template<typename T, bool kStats>
class BitpackingCompress final : public CompressionState {
 public:
  using S = Signed<T>;

  explicit BitpackingCompress(ColumnDataCheckpointData& checkpoint_data)
    : _checkpoint_data{&checkpoint_data},
      _segment{checkpoint_data.type},
      _state{ForcedMode(checkpoint_data.ctx.Database())} {}

  void Append(const duckdb::Vector& input) {
    for (auto entry : input.Values<T>()) {
      _state.Update(entry, *this);
    }
  }

  void Finalize() {
    _state.Flush(*this);
    FlushSegment();
  }

  void WriteConstant(T constant, idx_t count) {
    ReserveSpace(sizeof(T));
    WriteMeta(BitpackingMode::CONSTANT);
    WriteData(constant);
    UpdateStats(count);
  }

  void WriteConstantDelta(S constant, T frame_of_reference, idx_t count) {
    ReserveSpace(2 * sizeof(T));
    WriteMeta(BitpackingMode::CONSTANT_DELTA);
    WriteData(frame_of_reference);
    WriteData(constant);
    UpdateStats(count);
  }

  void WriteDeltaFor(T* values, bitpacking_width_t width, T frame_of_reference,
                     S delta_offset, idx_t count) {
    const auto bp_size = BitpackingPrimitives::GetRequiredSize(count, width);
    ReserveSpace(bp_size + 3 * sizeof(T));
    WriteMeta(BitpackingMode::DELTA_FOR);
    WriteData(frame_of_reference);
    WriteData(static_cast<T>(width));
    WriteData(delta_offset);
    WritePacked(values, count, width, bp_size);
    UpdateStats(count);
  }

  void WriteFor(T* values, bitpacking_width_t width, T frame_of_reference,
                idx_t count) {
    const auto bp_size = BitpackingPrimitives::GetRequiredSize(count, width);
    ReserveSpace(bp_size + 2 * sizeof(T));
    WriteMeta(BitpackingMode::FOR);
    WriteData(frame_of_reference);
    WriteData(static_cast<T>(width));
    WritePacked(values, count, width, bp_size);
    UpdateStats(count);
  }

 private:
  template<typename V>
  void WriteData(V value) {
    duckdb::Store<V>(value, _segment.Data() + _data_offset);
    _data_offset += sizeof(V);
  }

  void WritePacked(T* values, idx_t count, bitpacking_width_t width,
                   idx_t bp_size) {
    BitpackingPrimitives::PackBuffer<T, false>(_segment.Data() + _data_offset,
                                               values, count, width);
    _data_offset += bp_size;
  }

  void WriteMeta(BitpackingMode mode) {
    const GroupMeta meta{.mode = mode,
                         .offset = static_cast<uint32_t>(_data_offset)};
    _metadata_offset -= kMetaSize;
    duckdb::Store<uint32_t>(EncodeMeta(meta),
                            _segment.Data() + _metadata_offset);
  }

  void ReserveSpace(idx_t data_bytes) {
    if (!CanStore(data_bytes, kMetaSize)) {
      FlushSegment();
    }
    SDB_ASSERT(CanStore(data_bytes, kMetaSize));
  }

  void UpdateStats(idx_t count) {
    _segment.count += count;
    if constexpr (kStats) {
      if (_state.has_valid) {
        _stats.SetHasValid();
        _stats.UpdateMinMax(_state.minimum);
        _stats.UpdateMinMax(_state.maximum);
      }
      if (_state.has_invalid) {
        _stats.SetHasNull();
      }
    }
  }

  bool CanStore(idx_t data_bytes, idx_t meta_bytes) const noexcept {
    const auto required_data_bytes = duckdb::AlignValue<idx_t>(data_bytes);
    const auto required_meta_bytes =
      kBlockSize - (_metadata_offset - _data_offset) + meta_bytes;
    return required_data_bytes + required_meta_bytes <=
           kBlockSize - kHeaderSize;
  }

  void FlushSegment() {
    auto* base = _segment.Data();
    const idx_t unaligned_offset = _data_offset;
    const idx_t metadata_offset = duckdb::AlignValue(unaligned_offset);
    const idx_t metadata_size = kBlockSize - _metadata_offset;
    const idx_t total_segment_size = metadata_offset + metadata_size;
    SDB_ENSURE(CanStore(0, 0), "Error in bitpacking size calculation");
    if (unaligned_offset != metadata_offset) {
      std::memset(base + unaligned_offset, 0,
                  metadata_offset - unaligned_offset);
    }
    std::memmove(base + metadata_offset, base + _metadata_offset,
                 metadata_size);
    duckdb::Store<idx_t>(metadata_offset + metadata_size, base);
    _stats.Merge(_segment.stats);
    _segment.Flush(*_checkpoint_data, total_segment_size);
    _stats.Clear();
    _data_offset = kHeaderSize;
    _metadata_offset = kBlockSize;
  }

  ColumnDataCheckpointData* _checkpoint_data;
  CompressedSegment _segment;
  duckdb::StatsWriter<T> _stats;
  idx_t _data_offset = kHeaderSize;
  idx_t _metadata_offset = kBlockSize;
  BitpackingState<T> _state;
};

template<typename T, bool kStats>
std::unique_ptr<CompressionState> InitCompression(
  ColumnDataCheckpointData& checkpoint_data, std::unique_ptr<AnalyzeState>) {
  return std::make_unique<BitpackingCompress<T, kStats>>(checkpoint_data);
}

template<typename T, bool kStats>
void Compress(CompressionState& state, const duckdb::Vector& input) {
  state.Cast<BitpackingCompress<T, kStats>>().Append(input);
}

template<typename T, bool kStats>
void FinalizeCompress(CompressionState& state) {
  state.Cast<BitpackingCompress<T, kStats>>().Finalize();
}

template<typename T>
void ApplyFrameOfReference(std::span<T> values, T frame_of_reference) {
  if (!frame_of_reference) {
    return;
  }
  auto* unsigned_values = reinterpret_cast<Unsigned<T>*>(values.data());
  for (idx_t i = 0; i < values.size(); i++) {
    unsigned_values[i] += static_cast<Unsigned<T>>(frame_of_reference);
  }
}

template<typename T>
T DeltaDecode(std::span<T> values, T previous_value) {
  using U = Unsigned<T>;
  SDB_ASSERT(!values.empty());
  auto* udata = reinterpret_cast<U*>(values.data());
  U carry = static_cast<U>(previous_value);
  const idx_t count = values.size();
  constexpr idx_t kUnroll = 4;
  const idx_t unrolled = (count / kUnroll) * kUnroll;
  idx_t i = 0;
  for (; i < unrolled; i += kUnroll) {
    carry = udata[i] += carry;
    carry = udata[i + 1] += carry;
    carry = udata[i + 2] += carry;
    carry = udata[i + 3] += carry;
  }
  for (; i < count; ++i) {
    carry = udata[i] += carry;
  }
  return values[count - 1];
}

template<typename T>
struct Group {
  idx_t Remaining() const noexcept {
    SDB_ASSERT(offset <= count);
    return count - offset;
  }

  bool AtEnd() const noexcept { return offset == count; }

  void Advance(idx_t advance_count) noexcept {
    SDB_ASSERT(advance_count <= Remaining());
    offset += advance_count;
  }

  const uint8_t* AlgorithmGroup() const {
    SDB_ASSERT(!AtEnd());
    SDB_ASSERT(width > 0);
    SDB_ENSURE(payload, "Bitpacking algorithm group has no payload");
    const auto algorithm_group_index = offset / kAlgorithmGroupSize;
    SDB_ASSERT(algorithm_group_index < algorithm_group_count);
    return payload + algorithm_group_index * algorithm_group_size;
  }

  void UnpackAlgorithmGroup(T* target, bool skip_sign_extend) const {
    SDB_ASSERT(meta.mode == BitpackingMode::FOR ||
               meta.mode == BitpackingMode::DELTA_FOR);
    if (width == 0) {
      std::fill_n(target, kAlgorithmGroupSize, T(0));
      return;
    }
    BitpackingPrimitives::UnPackBlock<T>(
      duckdb::data_ptr_cast(target), AlgorithmGroup(), width, skip_sign_extend);
  }

  idx_t index = 0;
  idx_t offset = 0;
  idx_t count = 0;
  GroupMeta meta;
  bitpacking_width_t width = 0;
  T frame_of_reference{};
  T constant{};
  T delta_offset{};
  const uint8_t* payload = nullptr;
  idx_t algorithm_group_count = 0;
  idx_t algorithm_group_size = 0;
};

std::span<const uint32_t> MetadataTable(const SegmentReader& reader,
                                        idx_t start, idx_t count) {
  const auto table = reader.GetArray<uint32_t>(start, count);
  return {table.data(), table.size()};
}

template<typename T>
struct ScanState final : SegmentScanState {
  using S = Signed<T>;
  using Packed =
    std::conditional_t<(sizeof(T) <= sizeof(uint32_t)), T, uint32_t>;

  struct Initial {};
  struct Finished {};

  ScanState(duckdb::BufferHandle handle_p, const ColumnSegment& segment)
    : handle{std::move(handle_p)},
      reader{handle.Ptr(), segment.size, "bitpacking segment"},
      segment_count{segment.count},
      group_count{segment_count / kMetadataGroupSize +
                  (segment_count % kMetadataGroupSize != 0)},
      metadata_table_start{reader.Get<idx_t>(0) - group_count * kMetaSize},
      metadata_table{MetadataTable(reader, metadata_table_start, group_count)} {
    if (metadata_table_start < kHeaderSize) {
      GroupOffsetsInvalid();
    }
    reader = reader.GetSubReader(
      kHeaderSize, metadata_table_start - kHeaderSize, "bitpacking group data");
  }

  GroupMeta GroupMetadata(idx_t group_index) const {
    if (group_index >= group_count) {
      Corrupted("group index exceeds the metadata table");
    }
    return DecodeMeta(metadata_table[group_count - 1 - group_index]);
  }

  Group<T>& CurrentGroup() { return std::get<Group<T>>(group_state); }

  bool IsInitial() const noexcept {
    return std::holds_alternative<Initial>(group_state);
  }

  bool HasCurrentGroup() const noexcept {
    return std::holds_alternative<Group<T>>(group_state);
  }

  bool IsFinished() const noexcept {
    return std::holds_alternative<Finished>(group_state);
  }

  void Finish() { group_state = Finished{}; }

  Group<T> ReadGroup(idx_t group_index) const {
    const auto meta = GroupMetadata(group_index);
    const idx_t group_start = meta.offset;
    const idx_t group_end = group_index + 1 < group_count
                              ? GroupMetadata(group_index + 1).offset
                              : metadata_table_start;
    if (group_start < kHeaderSize || group_start > group_end ||
        group_end > metadata_table_start) {
      GroupOffsetsInvalid();
    }
    auto group_reader =
      reader.GetSubReader(group_start - kHeaderSize, group_end - group_start,
                          "bitpacking group data");
    Group<T> group{.index = group_index, .meta = meta};
    switch (meta.mode) {
      case BitpackingMode::CONSTANT:
        group.constant = group_reader.template Read<T>();
        break;
      case BitpackingMode::FOR:
      case BitpackingMode::CONSTANT_DELTA:
      case BitpackingMode::DELTA_FOR:
        group.frame_of_reference = group_reader.template Read<T>();
        break;
      default:
        UnknownMode();
    }
    switch (meta.mode) {
      case BitpackingMode::CONSTANT_DELTA:
        group.constant = group_reader.template Read<T>();
        break;
      case BitpackingMode::FOR:
      case BitpackingMode::DELTA_FOR:
        group.width = ValidateWidth<T>(group_reader.template Read<T>());
        break;
      case BitpackingMode::CONSTANT:
        break;
      default:
        UnknownMode();
    }
    if (meta.mode == BitpackingMode::DELTA_FOR) {
      group.delta_offset = group_reader.template Read<T>();
    }
    group.count = std::min<idx_t>(
      kMetadataGroupSize, segment_count - group_index * kMetadataGroupSize);
    idx_t expected_payload_size = 0;
    if (meta.mode == BitpackingMode::FOR ||
        meta.mode == BitpackingMode::DELTA_FOR) {
      group.algorithm_group_count = group.count / kAlgorithmGroupSize +
                                    (group.count % kAlgorithmGroupSize != 0);
      group.algorithm_group_size =
        BitpackingPrimitives::GetRequiredSize(kAlgorithmGroupSize, group.width);
      expected_payload_size =
        group.algorithm_group_count * group.algorithm_group_size;
    }
    if (expected_payload_size > 0) {
      group.payload =
        group_reader.template ReadBytesAligned<Packed>(expected_payload_size)
          .data();
    }
    return group;
  }

  void LoadGroup(idx_t group_index) { group_state = ReadGroup(group_index); }

  void Skip(idx_t skip_count) {
    if (IsFinished()) {
      if (skip_count > 0) {
        ReadPastEnd();
      }
      return;
    }
    const auto* active_group = std::get_if<Group<T>>(&group_state);
    SDB_ASSERT(active_group || IsInitial());
    const idx_t group_index = active_group ? active_group->index : 0;
    const idx_t group_offset = active_group ? active_group->offset : 0;
    idx_t skipped = 0;
    const idx_t meta_groups_to_skip =
      (skip_count + group_offset) / kMetadataGroupSize;
    if (meta_groups_to_skip) {
      const idx_t target_group_index = group_index + meta_groups_to_skip;
      const bool skip_lands_exactly_on_group_boundary =
        (skip_count + group_offset) % kMetadataGroupSize == 0;
      if (target_group_index > group_count ||
          (target_group_index == group_count &&
           !skip_lands_exactly_on_group_boundary)) {
        ReadPastEnd();
      }
      const auto skipped_group_rows =
        meta_groups_to_skip * kMetadataGroupSize - group_offset;
      SDB_ASSERT(skipped_group_rows <= skip_count);
      skipped += skipped_group_rows;
      if (target_group_index == group_count) {
        Finish();
        SDB_ASSERT(skipped == skip_count);
        return;
      }
      LoadGroup(target_group_index);
    }
    if (IsInitial()) {
      LoadGroup(0);
    }
    auto& current_group = CurrentGroup();
    if (skipped > skip_count) {
      ReadPastEnd();
    }
    const auto remaining_to_skip = skip_count - skipped;
    if (current_group.offset > current_group.count ||
        remaining_to_skip > current_group.count - current_group.offset) {
      ReadPastEnd();
    }
    if (current_group.meta.mode == BitpackingMode::CONSTANT ||
        current_group.meta.mode == BitpackingMode::CONSTANT_DELTA ||
        current_group.meta.mode == BitpackingMode::FOR) {
      current_group.Advance(remaining_to_skip);
      skipped += remaining_to_skip;
    } else {
      SDB_ASSERT(current_group.meta.mode == BitpackingMode::DELTA_FOR);
      while (skipped < skip_count) {
        const idx_t offset_in_compression_group =
          current_group.offset % kAlgorithmGroupSize;
        const idx_t skipping_this_algorithm_group =
          std::min(skip_count - skipped,
                   kAlgorithmGroupSize - offset_in_compression_group);
        current_group.UnpackAlgorithmGroup(decompression_buffer,
                                           kSkipSignExtend);
        const std::span<S> decompression_values{
          reinterpret_cast<S*>(decompression_buffer), kAlgorithmGroupSize};
        const auto skipped_values = decompression_values.subspan(
          offset_in_compression_group, skipping_this_algorithm_group);
        ApplyFrameOfReference<S>(
          skipped_values, static_cast<S>(current_group.frame_of_reference));
        current_group.delta_offset = static_cast<T>(DeltaDecode<S>(
          skipped_values, static_cast<S>(current_group.delta_offset)));
        current_group.Advance(skipping_this_algorithm_group);
        skipped += skipping_this_algorithm_group;
      }
    }
    SDB_ASSERT(skipped == skip_count);
    if (current_group.AtEnd() && current_group.index + 1 == group_count) {
      Finish();
    }
  }

  duckdb::BufferHandle handle;
  SegmentReader reader;
  idx_t segment_count;
  idx_t group_count;
  idx_t metadata_table_start;
  std::span<const uint32_t> metadata_table;
  T decompression_buffer[kAlgorithmGroupSize];
  std::variant<Initial, Group<T>, Finished> group_state = Initial{};
  duckdb::unique_ptr<duckdb::BaseStatistics> filter_group_stats;
};

template<typename T>
std::unique_ptr<SegmentScanState> InitScan(const ColumnSegment& segment) {
  return std::make_unique<ScanState<T>>(segment.Pin(), segment);
}

template<typename T>
void ScanPartial(const ColumnSegment&, ColumnScanState& state, idx_t scan_count,
                 duckdb::Vector& result, idx_t result_offset) {
  using S = Signed<T>;
  using U = Unsigned<T>;
  auto& scan_state = state.scan_state->Cast<ScanState<T>>();
  if (scan_count == 0) {
    return;
  }
  T* result_data = duckdb::FlatVector::GetDataMutable<T>(result);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  if (scan_state.IsInitial()) {
    scan_state.LoadGroup(0);
  }
  if (!scan_state.HasCurrentGroup()) {
    ReadPastEnd();
  }
  idx_t scanned = 0;
  while (scanned < scan_count) {
    auto& current_group = scan_state.CurrentGroup();
    if (current_group.AtEnd()) {
      SDB_ASSERT(current_group.index + 1 < scan_state.group_count);
      scan_state.LoadGroup(current_group.index + 1);
      continue;
    }
    const idx_t offset_in_compression_group =
      current_group.offset % kAlgorithmGroupSize;
    if (current_group.meta.mode == BitpackingMode::CONSTANT) {
      const idx_t remaining = scan_count - scanned;
      const idx_t to_scan = std::min(remaining, current_group.Remaining());
      std::fill_n(result_data + result_offset + scanned, to_scan,
                  current_group.constant);
      scanned += to_scan;
      current_group.Advance(to_scan);
      continue;
    }
    if (current_group.meta.mode == BitpackingMode::CONSTANT_DELTA) {
      const idx_t remaining = scan_count - scanned;
      const idx_t to_scan = std::min(remaining, current_group.Remaining());
      T* target_ptr = result_data + result_offset + scanned;
      for (idx_t i = 0; i < to_scan; i++) {
        const idx_t multiplier = current_group.offset + i;
        target_ptr[i] =
          static_cast<T>((static_cast<U>(current_group.constant) * multiplier) +
                         static_cast<U>(current_group.frame_of_reference));
      }
      scanned += to_scan;
      current_group.Advance(to_scan);
      continue;
    }
    SDB_ASSERT(current_group.meta.mode == BitpackingMode::FOR ||
               current_group.meta.mode == BitpackingMode::DELTA_FOR);
    idx_t to_scan = std::min<idx_t>(
      scan_count - scanned, kAlgorithmGroupSize - offset_in_compression_group);
    to_scan = std::min(to_scan, current_group.Remaining());
    T* current_result_ptr = result_data + result_offset + scanned;
    if (to_scan == kAlgorithmGroupSize && offset_in_compression_group == 0) {
      current_group.UnpackAlgorithmGroup(current_result_ptr, kSkipSignExtend);
    } else {
      current_group.UnpackAlgorithmGroup(scan_state.decompression_buffer,
                                         kSkipSignExtend);
      std::memcpy(current_result_ptr,
                  scan_state.decompression_buffer + offset_in_compression_group,
                  to_scan * sizeof(T));
    }
    if (current_group.meta.mode == BitpackingMode::DELTA_FOR) {
      const std::span<S> current_results{
        reinterpret_cast<S*>(current_result_ptr), to_scan};
      ApplyFrameOfReference<S>(
        current_results, static_cast<S>(current_group.frame_of_reference));
      current_group.delta_offset = static_cast<T>(DeltaDecode<S>(
        current_results, static_cast<S>(current_group.delta_offset)));
    } else {
      ApplyFrameOfReference<T>(std::span<T>{current_result_ptr, to_scan},
                               current_group.frame_of_reference);
    }
    scanned += to_scan;
    current_group.Advance(to_scan);
  }
  const auto& current_group = scan_state.CurrentGroup();
  if (current_group.AtEnd() &&
      current_group.index + 1 == scan_state.group_count) {
    scan_state.Finish();
  }
}

template<typename T>
void Scan(const ColumnSegment& segment, ColumnScanState& state,
          idx_t scan_count, duckdb::Vector& result) {
  ScanPartial<T>(segment, state, scan_count, result, 0);
}

bool Near(idx_t row, idx_t other) noexcept {
  return other != kNone &&
         row / kAlgorithmGroupSize == other / kAlgorithmGroupSize &&
         (row > other ? row - other : other - row) <= kDecodeDistance;
}

template<typename T>
struct FetchState final : SegmentScanState {
  using S = Signed<T>;
  using U = Unsigned<T>;

  FetchState(duckdb::BufferHandle handle, const ColumnSegment& segment)
    : scan_state{std::move(handle), segment} {}

  Group<T>& LoadGroup(idx_t group_index) {
    if (group_index != current_group) {
      current = scan_state.ReadGroup(group_index);
      current_group = group_index;
      decoded_algorithm_group = kNone;
      known_current_starts = 0;
    }
    return current;
  }

  T FetchValue(Group<T>& group, idx_t offset_in_group) {
    const auto algorithm_group = offset_in_group / kAlgorithmGroupSize;
    if (group.meta.mode == BitpackingMode::DELTA_FOR) {
      uint8_t* known = nullptr;
      const T* starts = StartsUpTo(group, algorithm_group, known);
      return static_cast<T>(
        static_cast<U>(starts[algorithm_group]) +
        DeltaSum(group, algorithm_group,
                 offset_in_group % kAlgorithmGroupSize + 1));
    }
    SDB_ASSERT(group.meta.mode == BitpackingMode::FOR);
    U value = 0;
    if (group.width > 0) {
      group.offset = offset_in_group;
      value = static_cast<U>(BitpackingPrimitives::UnPackValue<T>(
        group.AlgorithmGroup(), offset_in_group % kAlgorithmGroupSize,
        group.width));
    }
    return static_cast<T>(value + static_cast<U>(group.frame_of_reference));
  }

  void Decode(Group<T>& group, idx_t algorithm_group) {
    if (decoded_algorithm_group == algorithm_group) {
      return;
    }
    if (group.meta.mode != BitpackingMode::DELTA_FOR) {
      DecodeAlgorithmGroup(group, algorithm_group, T(0));
      return;
    }
    uint8_t* known = nullptr;
    T* starts = StartsUpTo(group, algorithm_group, known);
    DecodeAlgorithmGroup(group, algorithm_group, starts[algorithm_group]);
    if (*known == algorithm_group + 1) {
      starts[*known] = scan_state.decompression_buffer[kAlgorithmGroupSize - 1];
      (*known)++;
    }
  }

  T* StartsUpTo(Group<T>& group, idx_t algorithm_group, uint8_t*& known) {
    T* starts = current_starts;
    known = &known_current_starts;
    if (calls > 1) {
      if (block_starts.empty()) {
        block_starts.resize(scan_state.group_count * (kAlgorithmGroups + 1));
        known_block_starts.resize(scan_state.group_count, 0);
      }
      starts = block_starts.data() + group.index * (kAlgorithmGroups + 1);
      known = &known_block_starts[group.index];
    }
    if (*known == 0) {
      starts[0] = group.delta_offset;
      *known = 1;
    }
    while (*known <= algorithm_group) {
      starts[*known] =
        static_cast<T>(static_cast<U>(starts[*known - 1]) +
                       DeltaSum(group, *known - 1, kAlgorithmGroupSize));
      (*known)++;
    }
    return starts;
  }

  U DeltaSum(Group<T>& group, idx_t algorithm_group, idx_t count) {
    group.offset = algorithm_group * kAlgorithmGroupSize;
    T unpacked[kAlgorithmGroupSize];
    group.UnpackAlgorithmGroup(unpacked, kSkipSignExtend);
    const auto* deltas = reinterpret_cast<const U*>(unpacked);
    U sum = static_cast<U>(group.frame_of_reference) * static_cast<U>(count);
    for (idx_t i = 0; i < count; i++) {
      sum += deltas[i];
    }
    return sum;
  }

  void DecodeAlgorithmGroup(Group<T>& group, idx_t algorithm_group,
                            T block_start) {
    group.offset = algorithm_group * kAlgorithmGroupSize;
    group.UnpackAlgorithmGroup(scan_state.decompression_buffer,
                               kSkipSignExtend);
    const std::span<S> values{
      reinterpret_cast<S*>(scan_state.decompression_buffer),
      kAlgorithmGroupSize};
    ApplyFrameOfReference<S>(values, static_cast<S>(group.frame_of_reference));
    if (group.meta.mode == BitpackingMode::DELTA_FOR) {
      DeltaDecode<S>(values, static_cast<S>(block_start));
    }
    decoded_algorithm_group = algorithm_group;
  }

  ScanState<T> scan_state;
  idx_t calls = 0;
  Group<T> current;
  idx_t current_group = kNone;
  idx_t decoded_algorithm_group = kNone;
  idx_t previous_row = kNone;
  uint8_t known_current_starts = 0;
  std::vector<T> block_starts;
  std::vector<uint8_t> known_block_starts;
  T current_starts[kAlgorithmGroups + 1];
};

template<typename T>
void FetchRows(const ColumnSegment& segment, ColumnFetchState& state,
               const idx_t* rows, idx_t count, duckdb::Vector& result,
               const duckdb::SelectionVector& result_sel, idx_t result_offset) {
  using U = Unsigned<T>;
  auto& fetch_state = state.GetOrCreateSegmentState<FetchState<T>>(
    [&] { return std::make_unique<FetchState<T>>(segment.Pin(), segment); });
  fetch_state.calls++;
  SDB_ASSERT(result.GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
  T* result_data = duckdb::FlatVector::GetDataMutable<T>(result);
  for (idx_t i = 0; i < count; i++) {
    const auto offset_in_group = rows[i] % kMetadataGroupSize;
    auto& group = fetch_state.LoadGroup(rows[i] / kMetadataGroupSize);
    SDB_ASSERT(offset_in_group < group.count);
    T* current_result_ptr =
      result_data + result_offset + result_sel.get_index(i);
    if (group.meta.mode == BitpackingMode::CONSTANT) {
      *current_result_ptr = group.constant;
      continue;
    }
    if (group.meta.mode == BitpackingMode::CONSTANT_DELTA) {
      const idx_t multiplier = offset_in_group;
      *current_result_ptr =
        static_cast<T>((static_cast<U>(group.constant) * multiplier) +
                       static_cast<U>(group.frame_of_reference));
      continue;
    }
    SDB_ASSERT(group.meta.mode == BitpackingMode::FOR ||
               group.meta.mode == BitpackingMode::DELTA_FOR);
    const auto algorithm_group = offset_in_group / kAlgorithmGroupSize;
    if (fetch_state.decoded_algorithm_group != algorithm_group) {
      const auto previous_row = fetch_state.previous_row;
      fetch_state.previous_row = rows[i];
      if (!Near(rows[i], previous_row) &&
          (i + 1 == count || !Near(rows[i], rows[i + 1]))) {
        *current_result_ptr = fetch_state.FetchValue(group, offset_in_group);
        continue;
      }
    }
    fetch_state.Decode(group, algorithm_group);
    *current_result_ptr =
      fetch_state.scan_state
        .decompression_buffer[offset_in_group % kAlgorithmGroupSize];
  }
}

template<typename T>
void Skip(const ColumnSegment&, ColumnScanState& state, idx_t skip_count) {
  if (skip_count == 0) {
    return;
  }
  state.scan_state->Cast<ScanState<T>>().Skip(skip_count);
}

template<typename T>
bool GroupBounds(const Group<T>& group, T& minimum, T& maximum) {
  using S = Signed<T>;
  using U = Unsigned<T>;
  if constexpr (sizeof(T) > sizeof(uint64_t)) {
    return false;
  } else {
    switch (group.meta.mode) {
      case BitpackingMode::CONSTANT:
        minimum = group.constant;
        maximum = minimum;
        return true;
      case BitpackingMode::CONSTANT_DELTA: {
        const T frame_of_reference = group.frame_of_reference;
        const T constant = group.constant;
        const idx_t last = group.count - 1;
        const T first_value = frame_of_reference;
        const T last_value = static_cast<T>((static_cast<U>(constant) * last) +
                                            static_cast<U>(frame_of_reference));
        minimum = duckdb::MinValue(first_value, last_value);
        maximum = duckdb::MaxValue(first_value, last_value);
        return true;
      }
      case BitpackingMode::FOR: {
        const auto width = group.width;
        minimum = group.frame_of_reference;
        maximum = duckdb::NumericLimits<T>::Maximum();
        if (width < sizeof(T) * 8) {
          const uint64_t packed_span = (uint64_t{1} << width) - 1;
          if (packed_span >
                static_cast<uint64_t>(duckdb::NumericLimits<T>::Maximum()) ||
              !duckdb::TryAddOperator::Operation(
                minimum, static_cast<T>(packed_span), maximum)) {
            maximum = duckdb::NumericLimits<T>::Maximum();
          }
        }
        return true;
      }
      case BitpackingMode::DELTA_FOR: {
        if (!duckdb::NumericLimits<T>::IsSigned()) {
          return false;
        }
        const auto width = group.width;
        if (width >= sizeof(T) * 8) {
          return false;
        }
        const auto delta_min =
          static_cast<int64_t>(static_cast<S>(group.frame_of_reference));
        const uint64_t packed_span = (uint64_t{1} << width) - 1;
        if (packed_span >
            static_cast<uint64_t>(duckdb::NumericLimits<int64_t>::Maximum())) {
          return false;
        }
        int64_t delta_max = 0;
        if (!duckdb::TryAddOperator::Operation(
              delta_min, static_cast<int64_t>(packed_span), delta_max)) {
          return false;
        }
        const auto base =
          static_cast<int64_t>(static_cast<S>(group.delta_offset));
        const auto steps = static_cast<int64_t>(group.count);
        int64_t lo = delta_min;
        int64_t hi = delta_max;
        if (delta_min < 0 &&
            !duckdb::TryMultiplyOperator::Operation(delta_min, steps, lo)) {
          return false;
        }
        if (delta_max > 0 &&
            !duckdb::TryMultiplyOperator::Operation(delta_max, steps, hi)) {
          return false;
        }
        if (!duckdb::TryAddOperator::Operation(base, lo, lo) ||
            !duckdb::TryAddOperator::Operation(base, hi, hi)) {
          return false;
        }
        if (lo < static_cast<int64_t>(duckdb::NumericLimits<T>::Minimum()) ||
            hi > static_cast<int64_t>(duckdb::NumericLimits<T>::Maximum())) {
          return false;
        }
        minimum = static_cast<T>(lo);
        maximum = static_cast<T>(hi);
        return true;
      }
      default:
        return false;
    }
  }
}

template<typename T>
void Filter(const ColumnSegment& segment, ColumnScanState& state,
            idx_t vector_count, duckdb::Vector& result,
            duckdb::SelectionVector& sel, idx_t& sel_count,
            const duckdb::TableFilter&,
            duckdb::TableFilterState& filter_state) {
  auto& scan_state = state.scan_state->Cast<ScanState<T>>();
  const auto& checker =
    *filter_state.Cast<duckdb::ExpressionFilterState>().zonemap_checker;
  auto verdict = FilterPropagateResult::NO_PRUNING_POSSIBLE;
  if (checker.IsFullyCompiled() && !scan_state.IsFinished()) {
    idx_t group_index = 0;
    idx_t offset_in_group = 0;
    if (scan_state.HasCurrentGroup()) {
      const auto& current_group = scan_state.CurrentGroup();
      group_index = current_group.index;
      offset_in_group = current_group.offset;
      if (current_group.AtEnd()) {
        group_index++;
        offset_in_group = 0;
      }
    }
    if (!scan_state.filter_group_stats) {
      scan_state.filter_group_stats =
        duckdb::NumericStats::CreateEmpty(segment.stats.GetType()).ToUnique();
      scan_state.filter_group_stats->SetHasNoNullFast();
    }
    auto& group_stats = *scan_state.filter_group_stats;
    idx_t remaining = vector_count;
    bool first = true;
    while (remaining > 0 && group_index < scan_state.group_count) {
      const auto group = scan_state.ReadGroup(group_index);
      auto group_verdict = FilterPropagateResult::NO_PRUNING_POSSIBLE;
      T minimum{};
      T maximum{};
      if (GroupBounds<T>(group, minimum, maximum)) {
        duckdb::NumericStats::SetMin<T>(group_stats, minimum);
        duckdb::NumericStats::SetMax<T>(group_stats, maximum);
        group_verdict = checker.Check(group_stats, nullptr);
      }
      if (first) {
        verdict = group_verdict;
        first = false;
      } else if (group_verdict != verdict) {
        verdict = FilterPropagateResult::NO_PRUNING_POSSIBLE;
      }
      if (verdict == FilterPropagateResult::NO_PRUNING_POSSIBLE) {
        break;
      }
      remaining -= std::min<idx_t>(remaining, group.count - offset_in_group);
      group_index++;
      offset_in_group = 0;
    }
    if (remaining > 0) {
      verdict = FilterPropagateResult::NO_PRUNING_POSSIBLE;
    }
  }
  if (verdict == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
      verdict == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
    scan_state.Skip(vector_count);
    sel_count = 0;
    return;
  }
  ScanPartial<T>(segment, state, vector_count, result, 0);
  duckdb::FlatVector::SetSize(result, vector_count);
  if (verdict == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
    return;
  }
  duckdb::ColumnSegment::FilterSelection(sel, result, filter_state,
                                         vector_count, sel_count);
}

template<typename T>
duckdb::InsertionOrderPreservingMap<std::string> SegmentInfo(
  const ColumnSegment& segment) {
  const auto scan_state =
    std::make_unique<ScanState<T>>(segment.Pin(), segment);
  std::array<uint64_t, magic_enum::enum_count<BitpackingMode>()> counts{};
  for (idx_t group_index = 0; group_index < scan_state->group_count;
       group_index++) {
    const auto mode = scan_state->GroupMetadata(group_index).mode;
    ValidateMode(mode);
    counts[*magic_enum::enum_index(mode)]++;
  }
  duckdb::InsertionOrderPreservingMap<std::string> result;
  for (const auto mode : magic_enum::enum_values<BitpackingMode>()) {
    if (const auto count = counts[*magic_enum::enum_index(mode)]) {
      result[magic_enum::enum_name(mode)] = absl::StrCat(count);
    }
  }
  return result;
}

template<typename T, duckdb::PhysicalType kPhysical, bool kStats, bool kFilter>
constexpr CompressionFunction kFunction{
  .type = duckdb::CompressionType::COMPRESSION_BITPACKING,
  .data_type = kPhysical,
  .init_analyze = InitAnalyze<T>,
  .analyze = Analyze<T>,
  .final_analyze = FinalAnalyze<T>,
  .init_compression = InitCompression<T, kStats>,
  .compress = Compress<T, kStats>,
  .compress_finalize = FinalizeCompress<T, kStats>,
  .init_scan = InitScan<T>,
  .scan_vector = Scan<T>,
  .scan_partial = ScanPartial<T>,
  .filter = kFilter ? Filter<T> : nullptr,
  .fetch_rows = FetchRows<T>,
  .skip = Skip<T>,
  .get_segment_info = SegmentInfo<T>,
};

}  // namespace

const CompressionFunction* BitpackingFunction(duckdb::PhysicalType physical) {
  using enum duckdb::PhysicalType;
  switch (physical) {
    case BOOL:
      return &kFunction<int8_t, BOOL, true, false>;
    case INT8:
      return &kFunction<int8_t, INT8, true, true>;
    case INT16:
      return &kFunction<int16_t, INT16, true, true>;
    case INT32:
      return &kFunction<int32_t, INT32, true, true>;
    case INT64:
      return &kFunction<int64_t, INT64, true, true>;
    case UINT8:
      return &kFunction<uint8_t, UINT8, true, true>;
    case UINT16:
      return &kFunction<uint16_t, UINT16, true, true>;
    case UINT32:
      return &kFunction<uint32_t, UINT32, true, true>;
    case UINT64:
      return &kFunction<uint64_t, UINT64, true, true>;
    case INT128:
      return &kFunction<duckdb::hugeint_t, INT128, true, true>;
    case UINT128:
      return &kFunction<duckdb::uhugeint_t, UINT128, true, true>;
    case LIST:
      return &kFunction<uint64_t, LIST, false, false>;
    default:
      return nullptr;
  }
}

}  // namespace irs::codecs
