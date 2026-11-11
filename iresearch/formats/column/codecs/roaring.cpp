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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/types/validity_mask.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

using duckdb::BitpackingPrimitives;
using duckdb::const_data_ptr_t;
using duckdb::data_ptr_t;
using duckdb::idx_t;
using duckdb::validity_t;
using duckdb::ValidityMask;

constexpr uint16_t kCompressedSegmentSize = 256;
constexpr uint16_t kCompressedSegmentShift = 8;
constexpr idx_t kContainerSize = 2048;
constexpr bool kNulls = true;
constexpr bool kNonNulls = false;
constexpr uint16_t kUncompressedSize = kContainerSize / sizeof(validity_t);
constexpr uint16_t kCompressedSegmentCount =
  kContainerSize / kCompressedSegmentSize;
constexpr uint16_t kMaxRunIdx =
  (kUncompressedSize - kCompressedSegmentCount) / (sizeof(uint8_t) * 2);
constexpr uint16_t kMaxArrayIdx =
  (kUncompressedSize - kCompressedSegmentCount) / sizeof(uint8_t);
constexpr uint16_t kBitsetSentinel = kMaxArrayIdx + 1;
constexpr uint16_t kCompressedArrayThreshold = 8;
constexpr uint16_t kCompressedRunThreshold = 4;
constexpr uint8_t kContainerTypeWidth = 2;
constexpr uint8_t kRunContainerSizeWidth = 7;
constexpr uint8_t kArrayContainerSizeWidth = 8;
constexpr uint8_t kIsRunFlag = 1 << 1;
constexpr uint8_t kIsInvertedFlag = 1 << 0;
constexpr idx_t kBitsPerValue = ValidityMask::BITS_PER_VALUE;
constexpr idx_t kPackGroupSize =
  BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
constexpr validity_t kAllValid = ~validity_t{0};
constexpr double kCompressPenalty = 2.0;

static_assert(kMaxRunIdx < std::numeric_limits<uint8_t>::max());
static_assert(kBitsetSentinel < std::numeric_limits<uint8_t>::max());
static_assert(kContainerSize % kCompressedSegmentSize == 0);
static_assert((1 << kRunContainerSizeWidth) - 1 >= kMaxRunIdx);
static_assert((1 << kArrayContainerSizeWidth) - 1 >= kMaxArrayIdx + 1);

constexpr validity_t LowerMask(idx_t bits) noexcept {
  return bits >= kBitsPerValue ? kAllValid : (validity_t{1} << bits) - 1;
}

constexpr validity_t UpperMask(idx_t bits) noexcept {
  return bits == 0 ? 0 : kAllValid << (kBitsPerValue - bits);
}

void SetInvalidRange(ValidityMask& result, idx_t start, idx_t end) {
  SDB_ASSERT(end > start);
  SDB_ASSERT(result.Capacity() >= end);
  result.EnsureWritable();
  auto* result_data = result.GetData();
  idx_t index = start;
  if (index % kBitsPerValue != 0) {
    const idx_t right_bits = index % kBitsPerValue;
    idx_t bits_to_set = kBitsPerValue - right_bits;
    idx_t left_bits = 0;
    if (index + bits_to_set > end) {
      left_bits = (index + bits_to_set) - end;
      bits_to_set = end - index;
    }
    validity_t mask = LowerMask(right_bits);
    if (left_bits) {
      mask |= UpperMask(left_bits);
    }
    const idx_t entry_idx = index / kBitsPerValue;
    index += bits_to_set;
    result_data[entry_idx] &= mask;
  }
  const idx_t remaining_bits = end - index;
  const idx_t full_entries = remaining_bits / kBitsPerValue;
  const idx_t entry_idx = index / kBitsPerValue;
  for (idx_t i = 0; i < full_entries; ++i) {
    result_data[entry_idx + i] = 0;
  }
  if (remaining_bits % kBitsPerValue != 0) {
    const idx_t bits_to_set = end % kBitsPerValue;
    result_data[end / kBitsPerValue] &= UpperMask(kBitsPerValue - bits_to_set);
  }
}

struct RunContainerRLEPair {
  uint16_t start = 0;
  uint16_t length = 0;
};

static_assert(sizeof(RunContainerRLEPair) == 2 * sizeof(uint16_t));

enum class ContainerType : uint8_t {
  Run,
  Array,
  Bitset,
};

struct ContainerMetadata {
  ContainerType type = ContainerType::Run;
  bool nulls = false;
  uint16_t count = 0;
};

ContainerMetadata RunContainer(uint16_t runs) {
  return {.type = ContainerType::Run, .nulls = true, .count = runs};
}

ContainerMetadata ArrayContainer(uint16_t array_size, bool nulls) {
  return {.type = ContainerType::Array, .nulls = nulls, .count = array_size};
}

ContainerMetadata BitsetContainer(uint16_t container_size) {
  return {
    .type = ContainerType::Bitset,
    .nulls = true,
    .count = container_size,
  };
}

ContainerMetadata CreateMetadata(uint16_t count, uint16_t array_null,
                                 uint16_t array_non_null, uint16_t runs) {
  const bool can_use_null_array = array_null < kMaxArrayIdx;
  const bool can_use_non_null_array = array_non_null < kMaxArrayIdx;
  const bool can_use_run = runs < kMaxRunIdx;
  const bool can_use_array = can_use_null_array || can_use_non_null_array;
  if (!can_use_array && !can_use_run) {
    return BitsetContainer(count);
  }
  const auto null_array_cost = static_cast<uint16_t>(
    array_null < kCompressedArrayThreshold
      ? array_null * sizeof(uint16_t)
      : kCompressedSegmentCount + array_null * sizeof(uint8_t));
  const auto non_null_array_cost = static_cast<uint16_t>(
    array_non_null < kCompressedArrayThreshold
      ? array_non_null * sizeof(uint16_t)
      : kCompressedSegmentCount + array_non_null * sizeof(uint8_t));
  const auto lowest_array_cost = std::min(null_array_cost, non_null_array_cost);
  const auto lowest_run_cost = static_cast<uint16_t>(
    runs < kCompressedRunThreshold
      ? runs * sizeof(uint32_t)
      : kCompressedSegmentCount + runs * sizeof(uint16_t));
  const auto bitset_cost =
    static_cast<uint16_t>(duckdb::AlignValue<uint16_t, kBitsPerValue>(count) /
                          kBitsPerValue * sizeof(validity_t));
  if (std::min(lowest_array_cost, lowest_run_cost) > bitset_cost) {
    return BitsetContainer(count);
  }
  if (lowest_array_cost > lowest_run_cost) {
    return RunContainer(runs);
  }
  if (array_null <= array_non_null) {
    return ArrayContainer(array_null, kNulls);
  }
  return ArrayContainer(array_non_null, kNonNulls);
}

idx_t DataSizeInBytes(const ContainerMetadata& metadata, idx_t container_size) {
  if (metadata.type == ContainerType::Bitset) {
    return container_size / kBitsPerValue * sizeof(validity_t);
  }
  if (metadata.type == ContainerType::Run) {
    return metadata.count >= kCompressedRunThreshold
             ? kCompressedSegmentCount + sizeof(uint8_t) * metadata.count * 2
             : sizeof(RunContainerRLEPair) * metadata.count;
  }
  return metadata.count >= kCompressedArrayThreshold
           ? kCompressedSegmentCount + sizeof(uint8_t) * metadata.count
           : sizeof(uint16_t) * metadata.count;
}

struct ContainerMetadataCollection {
  std::vector<uint8_t> container_type;
  std::vector<uint8_t> number_of_runs;
  std::vector<uint8_t> cardinality;
  idx_t count_in_segment = 0;
  idx_t runs_in_segment = 0;
  idx_t arrays_in_segment = 0;
};

idx_t MetadataSize(idx_t container_count, idx_t run_containers,
                   idx_t array_containers) {
  return BitpackingPrimitives::GetRequiredSize(container_count,
                                               kContainerTypeWidth) +
         BitpackingPrimitives::GetRequiredSize(run_containers,
                                               kRunContainerSizeWidth) +
         sizeof(uint8_t) * array_containers;
}

idx_t MetadataSizeForSegment(const ContainerMetadataCollection& collection) {
  return MetadataSize(collection.runs_in_segment + collection.arrays_in_segment,
                      collection.runs_in_segment, collection.arrays_in_segment);
}

void AddContainerType(ContainerMetadataCollection& collection, bool is_run,
                      bool is_inverted) {
  uint8_t type = 0;
  if (is_run) {
    type |= kIsRunFlag;
  }
  if (is_inverted) {
    type |= kIsInvertedFlag;
  }
  collection.container_type.emplace_back(type);
}

void AddMetadata(ContainerMetadataCollection& collection,
                 const ContainerMetadata& metadata) {
  if (metadata.type == ContainerType::Run) {
    AddContainerType(collection, true, metadata.nulls);
    SDB_ASSERT(metadata.count < kMaxRunIdx);
    collection.number_of_runs.emplace_back(
      duckdb::NumericCast<uint8_t>(metadata.count));
    ++collection.runs_in_segment;
  } else if (metadata.type == ContainerType::Bitset) {
    AddContainerType(collection, false, false);
    collection.cardinality.emplace_back(static_cast<uint8_t>(kBitsetSentinel));
    ++collection.arrays_in_segment;
  } else {
    AddContainerType(collection, false, metadata.nulls);
    SDB_ASSERT(metadata.count < kMaxArrayIdx);
    collection.cardinality.emplace_back(
      duckdb::NumericCast<uint8_t>(metadata.count));
    ++collection.arrays_in_segment;
  }
  ++collection.count_in_segment;
}

void FlushSegment(ContainerMetadataCollection& collection) {
  collection.runs_in_segment = 0;
  collection.count_in_segment = 0;
  collection.arrays_in_segment = 0;
}

idx_t Serialize(ContainerMetadataCollection& collection, data_ptr_t dest) {
  const idx_t types_size = BitpackingPrimitives::GetRequiredSize(
    collection.count_in_segment, kContainerTypeWidth);
  const idx_t runs_size = BitpackingPrimitives::GetRequiredSize(
    collection.runs_in_segment, kRunContainerSizeWidth);
  const idx_t arrays_size = sizeof(uint8_t) * collection.arrays_in_segment;

  const idx_t types_offset =
    collection.container_type.size() - collection.count_in_segment;
  BitpackingPrimitives::PackBuffer<uint8_t>(
    dest, collection.container_type.data() + types_offset,
    collection.count_in_segment, kContainerTypeWidth);
  dest += types_size;

  if (!collection.number_of_runs.empty()) {
    const idx_t runs_offset =
      collection.number_of_runs.size() - collection.runs_in_segment;
    BitpackingPrimitives::PackBuffer<uint8_t>(
      dest, collection.number_of_runs.data() + runs_offset,
      collection.runs_in_segment, kRunContainerSizeWidth);
    dest += runs_size;
  }

  if (!collection.cardinality.empty()) {
    const idx_t arrays_offset =
      collection.cardinality.size() - collection.arrays_in_segment;
    std::memcpy(dest, collection.cardinality.data() + arrays_offset,
                arrays_size);
  }
  return types_size + runs_size + arrays_size;
}

void Deserialize(ContainerMetadataCollection& collection, const_data_ptr_t src,
                 idx_t container_count, idx_t available) {
  const auto take = [&](idx_t size) {
    if (size > available) [[unlikely]] {
      ThrowCorrupted("roaring", "container metadata extends past the segment");
    }
    available -= size;
  };
  collection.container_type.resize(
    duckdb::AlignValue<idx_t, kPackGroupSize>(container_count));
  collection.count_in_segment = container_count;

  const idx_t types_size = BitpackingPrimitives::GetRequiredSize(
    collection.container_type.size(), kContainerTypeWidth);
  take(types_size);
  BitpackingPrimitives::UnPackBuffer<uint8_t>(collection.container_type.data(),
                                              src, container_count,
                                              kContainerTypeWidth, true);
  src += types_size;

  idx_t runs_count = 0;
  for (idx_t i = 0; i < container_count; ++i) {
    runs_count += (collection.container_type[i] & kIsRunFlag) != 0;
  }
  collection.runs_in_segment = runs_count;
  collection.number_of_runs.resize(
    duckdb::AlignValue<idx_t, kPackGroupSize>(runs_count));
  collection.cardinality.resize(container_count - runs_count);

  if (runs_count) {
    const idx_t runs_size =
      BitpackingPrimitives::GetRequiredSize(runs_count, kRunContainerSizeWidth);
    take(runs_size);
    BitpackingPrimitives::UnPackBuffer<uint8_t>(
      collection.number_of_runs.data(), src, runs_count, kRunContainerSizeWidth,
      true);
    src += runs_size;
  }

  if (!collection.cardinality.empty()) {
    const idx_t arrays_size = sizeof(uint8_t) * collection.cardinality.size();
    take(arrays_size);
    collection.arrays_in_segment = arrays_size;
    std::memcpy(collection.cardinality.data(), src, arrays_size);
  }
}

struct ContainerMetadataCollectionScanner {
  const ContainerMetadataCollection& collection;
  idx_t array_idx = 0;
  idx_t run_idx = 0;
  idx_t idx = 0;
};

ContainerMetadata GetNext(ContainerMetadataCollectionScanner& scanner) {
  const auto& collection = scanner.collection;
  SDB_ASSERT(scanner.idx < collection.count_in_segment);
  const auto type = collection.container_type[scanner.idx++];
  if ((type & kIsRunFlag) != 0) {
    return RunContainer(collection.number_of_runs[scanner.run_idx++]);
  }
  const uint8_t amount = collection.cardinality[scanner.array_idx++];
  if (amount == kBitsetSentinel) {
    return BitsetContainer(amount);
  }
  return ArrayContainer(amount, (type & kIsInvertedFlag) != 0);
}

struct BitmaskTableEntry {
  bool first_bit_set = false;
  bool last_bit_set = false;
  uint8_t valid_count = 0;
  uint8_t run_count = 0;
};

constexpr auto kBitmaskTable = [] {
  std::array<BitmaskTableEntry, std::numeric_limits<uint8_t>::max() + 1> table;
  for (size_t val = 0; val < table.size(); ++val) {
    bool previous_bit = false;
    auto& entry = table[val];
    for (uint8_t i = 0; i < 8; ++i) {
      const bool bit_set = (val & (size_t{1} << i)) != 0;
      if (i == 0) {
        entry.first_bit_set = bit_set;
      } else if (i == 7) {
        entry.last_bit_set = bit_set;
      }
      entry.valid_count += bit_set;
      if (!bit_set && previous_bit) {
        ++entry.run_count;
      }
      previous_bit = bit_set;
    }
  }
  return table;
}();

template<bool kAllValid>
void BitPackBooleans(data_ptr_t dst, const duckdb::UnifiedVectorFormat& input,
                     idx_t count) {
  const auto* src = duckdb::UnifiedVectorFormat::GetData<bool>(input);
  uint8_t byte = 0;
  uint32_t bit_pos = 0;
  uint8_t src_bit = false;
  if constexpr (kAllValid) {
    for (idx_t i = 0; i < count; ++i) {
      src_bit = src[input.sel->get_index(i)];
      byte |= src_bit << bit_pos;
      ++bit_pos;
      if (bit_pos == 8) {
        *dst++ = byte;
        byte = 0;
        bit_pos = 0;
      }
    }
  } else {
    bool last_bit_value = false;
    for (idx_t i = 0; i < count; ++i) {
      const auto idx = input.sel->get_index(i);
      const uint8_t valid = input.validity.RowIsValid(idx);
      src_bit = valid ? src[idx] : src_bit;
      const uint8_t bit = (src_bit & valid) | (last_bit_value & ~valid);
      byte |= bit << bit_pos;
      ++bit_pos;
      last_bit_value = src_bit;
      if (bit_pos == 8) {
        *dst++ = byte;
        byte = 0;
        bit_pos = 0;
      }
    }
  }
  if (bit_pos != 0) {
    *dst = byte;
  }
}

void PackBooleans(ValidityMask& packed,
                  const duckdb::UnifiedVectorFormat& input, idx_t count) {
  packed.EnsureWritable();
  auto* dst = reinterpret_cast<data_ptr_t>(packed.GetData());
  if (input.validity.CannotHaveNull()) {
    BitPackBooleans<true>(dst, input, count);
  } else {
    BitPackBooleans<false>(dst, input, count);
  }
}

struct RoaringAnalyzeState {
  uint16_t one_count = 0;
  uint16_t zero_count = 0;
  uint16_t run_count = 0;
  bool last_bit_set = false;
  uint16_t count = 0;
  idx_t data_size = 0;
  idx_t metadata_size = 0;
  idx_t current_count = 0;
  idx_t total_count = 0;
  idx_t total_size = 0;
  ContainerMetadataCollection metadata_collection;
  std::vector<ContainerMetadata> container_metadata;
};

bool HasEnoughSpaceInSegment(const RoaringAnalyzeState& state,
                             idx_t required_space) {
  const auto space_used = state.data_size + state.metadata_size;
  SDB_ASSERT(space_used <= kBlockSize);
  return required_space <= kBlockSize - space_used;
}

void FlushSegment(RoaringAnalyzeState& state) {
  const auto space_used = state.data_size + state.metadata_size;
  if (!state.current_count) {
    SDB_ASSERT(!space_used);
    return;
  }
  FlushSegment(state.metadata_collection);
  state.total_size += space_used;
  state.data_size = 0;
  state.metadata_size = 0;
  state.current_count = 0;
}

void FlushContainer(RoaringAnalyzeState& state) {
  if (!state.count) {
    return;
  }
  const auto metadata = CreateMetadata(state.count, state.zero_count,
                                       state.one_count, state.run_count);
  idx_t runs_count = state.metadata_collection.runs_in_segment;
  idx_t arrays_count = state.metadata_collection.arrays_in_segment;
  if (metadata.type == ContainerType::Run) {
    ++runs_count;
  } else {
    ++arrays_count;
  }
  state.metadata_size =
    MetadataSize(runs_count + arrays_count, runs_count, arrays_count);
  state.data_size += DataSizeInBytes(metadata, state.count);
  if (!HasEnoughSpaceInSegment(state, state.metadata_size + state.data_size)) {
    FlushSegment(state);
  }
  state.container_metadata.emplace_back(metadata);
  AddMetadata(state.metadata_collection, metadata);
  state.current_count += state.count;

  state.one_count = 0;
  state.zero_count = 0;
  state.run_count = 0;
  state.last_bit_set = false;
  state.count = 0;
}

void HandleByte(RoaringAnalyzeState& state, uint8_t array_index) {
  const auto& bit_info = kBitmaskTable[array_index];
  state.run_count +=
    bit_info.run_count +
    (!bit_info.first_bit_set && (!state.count || state.last_bit_set));
  state.one_count += bit_info.valid_count;
  SDB_ASSERT(bit_info.valid_count <= 8);
  state.zero_count += 8 - bit_info.valid_count;
  state.last_bit_set = bit_info.last_bit_set;
  state.count += 8;
}

void HandleBit(RoaringAnalyzeState& state, bool bit_set) {
  if (!bit_set && (state.count == 0 || state.last_bit_set)) {
    ++state.run_count;
  }
  state.one_count += bit_set;
  state.zero_count += !bit_set;
  state.last_bit_set = bit_set;
  ++state.count;
}

void HandleRaggedByte(RoaringAnalyzeState& state, uint8_t array_index,
                      idx_t relevant_bits) {
  SDB_ASSERT(relevant_bits <= 8);
  for (idx_t i = 0; i < relevant_bits; ++i) {
    HandleBit(state, (array_index & (1U << i)) != 0);
  }
}

void HandleAllValid(RoaringAnalyzeState& state, idx_t amount) {
  state.one_count += amount;
  state.last_bit_set = true;
  state.count += amount;
}

void HandleNoneValid(RoaringAnalyzeState& state, idx_t amount) {
  if (!state.count || state.last_bit_set) {
    ++state.run_count;
  }
  state.zero_count += amount;
  state.last_bit_set = false;
  state.count += amount;
}

idx_t Count(const RoaringAnalyzeState& state) { return state.count; }

void Flush(RoaringAnalyzeState& state) { FlushContainer(state); }

struct ContainerCompressionState {
  using AppendFunction = void (*)(ContainerCompressionState&, bool, uint16_t);

  ContainerCompressionState() = default;
  ContainerCompressionState(const ContainerCompressionState&) = delete;
  ContainerCompressionState& operator=(const ContainerCompressionState&) =
    delete;

  uint16_t length = 0;
  bool last_bit_set = false;
  uint16_t appended_count = 0;
  uint16_t null_count = 0;
  bool last_is_null = false;

  RunContainerRLEPair base_runs[kCompressedRunThreshold] = {};
  uint16_t base_arrays[2][kCompressedArrayThreshold] = {};
  uint8_t base_compressed_arrays[2][kMaxArrayIdx] = {};
  uint8_t base_compressed_runs[kMaxRunIdx * 2] = {};
  uint8_t base_array_counts[2][kCompressedSegmentCount] = {};
  uint8_t base_run_counts[kCompressedSegmentCount] = {};

  RunContainerRLEPair* runs = base_runs;
  uint8_t* compressed_runs = base_compressed_runs;
  uint8_t* compressed_arrays[2] = {base_compressed_arrays[0],
                                   base_compressed_arrays[1]};
  uint16_t* arrays[2] = {base_arrays[0], base_arrays[1]};
  uint8_t* array_counts[2] = {base_array_counts[0], base_array_counts[1]};
  uint8_t* run_counts = base_run_counts;

  uint16_t run_idx = 0;
  uint16_t array_idx[2] = {};

  validity_t* uncompressed = nullptr;
  idx_t uncompressed_size = 0;
  bool finalized = false;
  AppendFunction append_function = nullptr;
};

void AppendBitset(ContainerCompressionState& state, bool null,
                  uint16_t amount) {
  SDB_ASSERT(state.uncompressed);
  if (null) {
    ValidityMask mask{state.uncompressed, state.uncompressed_size};
    SetInvalidRange(mask, state.appended_count, state.appended_count + amount);
  }
}

void AppendRun(ContainerCompressionState& state, bool null, uint16_t) {
  const auto run_idx = state.run_idx;
  const auto appended_count = state.appended_count;
  if (!null && run_idx < kMaxRunIdx && appended_count &&
      null != state.last_is_null) {
    if (run_idx < kCompressedRunThreshold) {
      auto& last_run = state.runs[run_idx];
      last_run.length =
        static_cast<uint16_t>((appended_count - last_run.start) - 1);
    }
    state.compressed_runs[(run_idx * 2) + 1] =
      static_cast<uint8_t>(appended_count & (kCompressedSegmentSize - 1));
    ++state.run_counts[appended_count >> kCompressedSegmentShift];
    ++state.run_idx;
  } else if (null && run_idx < kMaxRunIdx &&
             (!appended_count || null != state.last_is_null)) {
    if (run_idx < kCompressedRunThreshold) {
      state.runs[run_idx].start = appended_count;
    }
    state.compressed_runs[(run_idx * 2) + 0] =
      static_cast<uint8_t>(appended_count & (kCompressedSegmentSize - 1));
    ++state.run_counts[appended_count >> kCompressedSegmentShift];
  }
}

template<bool kInverted>
void AppendToArray(ContainerCompressionState& state, bool null,
                   uint16_t amount) {
  if (kInverted != null) [[likely]] {
    return;
  }
  const auto current_array_idx = state.array_idx[null];
  if (current_array_idx + amount > kMaxArrayIdx) {
    return;
  }
  const auto appended_count = state.appended_count;
  auto* array_count = state.array_counts[null];
  auto* compressed_array = state.compressed_arrays[null];
  uint16_t appended = 0;
  while (appended < amount) {
    const auto remaining = static_cast<uint16_t>(amount - appended);
    const auto segment_offset = static_cast<uint8_t>(
      appended ? 0
               : (appended_count + appended) & (kCompressedSegmentSize - 1));
    const auto to_append = static_cast<uint8_t>(
      std::min<uint16_t>(remaining, kCompressedSegmentSize - segment_offset));
    for (uint8_t i = 0; i < to_append; ++i) {
      const auto index = current_array_idx + appended + i;
      compressed_array[index] = static_cast<uint8_t>(segment_offset + i);
    }
    const idx_t segment_index =
      (appended_count + appended) / kCompressedSegmentSize;
    array_count[segment_index] += to_append;
    appended += to_append;
  }

  if (current_array_idx + amount < kCompressedArrayThreshold) {
    auto* array = state.arrays[null];
    for (uint16_t i = 0; i < amount; ++i) {
      array[current_array_idx + i] = static_cast<uint16_t>(appended_count + i);
    }
  }
  state.array_idx[null] += amount;
}

template<idx_t kAlignment>
void AlignZeroed(data_ptr_t base, data_ptr_t& destination) {
  const auto offset = static_cast<idx_t>(destination - base);
  auto* aligned = base + duckdb::AlignValue<idx_t, kAlignment>(offset);
  std::memset(destination, 0, static_cast<size_t>(aligned - destination));
  destination = aligned;
}

void Append(ContainerCompressionState& state, bool null, uint16_t amount) {
  state.append_function(state, null, amount);
  state.last_is_null = null;
  state.null_count += null * amount;
  state.appended_count += amount;
}

void OverrideArray(ContainerCompressionState& state, data_ptr_t base,
                   data_ptr_t& destination, bool nulls, idx_t count) {
  if (nulls) {
    state.append_function = AppendToArray<true>;
  } else {
    state.append_function = AppendToArray<false>;
  }
  if (count >= kCompressedArrayThreshold) {
    std::memset(destination, 0, sizeof(uint8_t) * kCompressedSegmentCount);
    state.array_counts[nulls] = destination;
    state.compressed_arrays[nulls] =
      destination + sizeof(uint8_t) * kCompressedSegmentCount;
  } else {
    AlignZeroed<sizeof(uint16_t)>(base, destination);
    state.arrays[nulls] = reinterpret_cast<uint16_t*>(destination);
  }
}

void OverrideRun(ContainerCompressionState& state, data_ptr_t base,
                 data_ptr_t& destination, idx_t count) {
  state.append_function = AppendRun;
  if (count >= kCompressedRunThreshold) {
    std::memset(destination, 0, sizeof(uint8_t) * kCompressedSegmentCount);
    state.run_counts = destination;
    state.compressed_runs =
      destination + sizeof(uint8_t) * kCompressedSegmentCount;
  } else {
    AlignZeroed<sizeof(RunContainerRLEPair)>(base, destination);
    state.runs = reinterpret_cast<RunContainerRLEPair*>(destination);
  }
}

void OverrideUncompressed(ContainerCompressionState& state, data_ptr_t base,
                          data_ptr_t& destination, idx_t container_size) {
  state.append_function = AppendBitset;
  AlignZeroed<sizeof(idx_t)>(base, destination);
  state.uncompressed = reinterpret_cast<validity_t*>(destination);
  state.uncompressed_size = container_size;
}

void Finalize(ContainerCompressionState& state) {
  SDB_ASSERT(!state.finalized);
  if (state.appended_count && state.last_is_null &&
      state.run_idx < kMaxRunIdx) {
    if (state.run_idx < kCompressedRunThreshold) {
      auto& last_run = state.runs[state.run_idx];
      last_run.length =
        static_cast<uint16_t>(state.appended_count - last_run.start);
    }
    state.compressed_runs[(state.run_idx * 2) + 1] =
      static_cast<uint8_t>(state.appended_count % kCompressedSegmentSize);
    if (state.appended_count != kContainerSize) {
      ++state.run_counts[state.appended_count >> kCompressedSegmentShift];
    }
    ++state.run_idx;
  }
  state.finalized = true;
}

void Reset(ContainerCompressionState& state) {
  state.length = 0;
  state.appended_count = 0;
  state.null_count = 0;
  state.run_idx = 0;
  state.array_idx[kNonNulls] = 0;
  state.array_idx[kNulls] = 0;
  state.finalized = false;
  state.last_is_null = false;

  state.arrays[kNulls] = state.base_arrays[kNulls];
  state.arrays[kNonNulls] = state.base_arrays[kNonNulls];
  state.runs = state.base_runs;

  state.compressed_arrays[kNulls] = state.base_compressed_arrays[kNulls];
  state.compressed_arrays[kNonNulls] = state.base_compressed_arrays[kNonNulls];
  state.compressed_runs = state.base_compressed_runs;

  state.array_counts[kNulls] = state.base_array_counts[kNulls];
  state.array_counts[kNonNulls] = state.base_array_counts[kNonNulls];
  state.run_counts = state.base_run_counts;

  std::memset(state.array_counts[kNulls], 0,
              sizeof(uint8_t) * kCompressedSegmentCount);
  std::memset(state.array_counts[kNonNulls], 0,
              sizeof(uint8_t) * kCompressedSegmentCount);
  std::memset(state.run_counts, 0, sizeof(uint8_t) * kCompressedSegmentCount);
  state.uncompressed = nullptr;
}

struct RoaringCompressState {
  RoaringCompressState(ColumnDataCheckpointData& checkpoint_data,
                       const RoaringAnalyzeState& analyze_state);

  ColumnDataCheckpointData& checkpoint_data;
  const RoaringAnalyzeState& analyze_state;
  ContainerCompressionState container_state;
  ContainerMetadataCollection metadata_collection;
  CompressedSegment segment;
  data_ptr_t data_ptr = nullptr;
  idx_t total_count = 0;
  duckdb::StatsWriter<void> stats_writer;
  duckdb::StatsWriter<bool> bool_stats_writer;
};

idx_t GetContainerIndex(const RoaringCompressState& state) {
  return state.total_count / kContainerSize;
}

idx_t GetUsedDataSpace(RoaringCompressState& state) {
  return static_cast<idx_t>(state.data_ptr -
                            (state.segment.Data() + sizeof(idx_t)));
}

constexpr idx_t kAvailableSpace = kBlockSize - sizeof(idx_t);

bool CanStore(RoaringCompressState& state, idx_t container_size,
              const ContainerMetadata& metadata) {
  idx_t runs_count = state.metadata_collection.runs_in_segment;
  idx_t arrays_count = state.metadata_collection.arrays_in_segment;
  SDB_ASSERT(
    (duckdb::AlignValue<idx_t, 8>(GetUsedDataSpace(state)) +
       MetadataSize(runs_count + arrays_count, runs_count, arrays_count) <=
     kAvailableSpace));
  idx_t new_data_space = 0;
  if (metadata.type == ContainerType::Bitset) {
    const auto offset =
      static_cast<idx_t>(state.data_ptr - state.segment.Data());
    new_data_space += duckdb::AlignValue<idx_t>(offset) - offset;
  }
  new_data_space += DataSizeInBytes(metadata, container_size);
  if (metadata.type == ContainerType::Run) {
    ++runs_count;
  } else {
    ++arrays_count;
  }
  const idx_t new_metadata_space =
    MetadataSize(runs_count + arrays_count, runs_count, arrays_count);
  const idx_t required_data_space =
    duckdb::AlignValue<idx_t, 8>(GetUsedDataSpace(state) + new_data_space);
  return required_data_space + new_metadata_space <= kAvailableSpace;
}

void CreateEmptySegment(RoaringCompressState& state) {
  state.data_ptr = state.segment.Data() + sizeof(idx_t);
}

template<typename Writer>
void FlushCurrentSegment(RoaringCompressState& state, Writer& stats_writer,
                         idx_t segment_size) {
  stats_writer.Merge(state.segment.stats);
  state.segment.Flush(state.checkpoint_data, segment_size);
  stats_writer.Clear();
}

void FlushSegment(RoaringCompressState& state) {
  auto* base_ptr = state.segment.Data() + sizeof(idx_t);
  const auto unaligned_data_size =
    static_cast<idx_t>(state.data_ptr - base_ptr);
  const auto data_size = duckdb::AlignValue<idx_t, 8>(unaligned_data_size);
  std::memset(state.data_ptr, 0, data_size - unaligned_data_size);
  state.data_ptr += data_size - unaligned_data_size;

  const auto metadata_size = MetadataSizeForSegment(state.metadata_collection);
  if (state.segment.count == 0) {
    SDB_ASSERT(metadata_size == 0);
    return;
  }

  const auto serialized_metadata_size =
    Serialize(state.metadata_collection, state.data_ptr);
  SDB_ENSURE(metadata_size == serialized_metadata_size,
             "roaring: mismatch in metadata size during FlushSegment");

  FlushSegment(state.metadata_collection);
  const auto metadata_start = static_cast<idx_t>(state.data_ptr - base_ptr);
  SDB_ENSURE(metadata_start <= kBlockSize,
             "roaring: metadata start outside of block size during "
             "FlushSegment");

  duckdb::Store<idx_t>(metadata_start, state.segment.Data());
  const auto total_segment_size = sizeof(idx_t) + data_size + metadata_size;
  if (state.checkpoint_data.type.id() == duckdb::LogicalTypeId::BOOLEAN) {
    FlushCurrentSegment(state, state.bool_stats_writer, total_segment_size);
  } else {
    FlushCurrentSegment(state, state.stats_writer, total_segment_size);
  }
}

void FlushContainer(RoaringCompressState& state) {
  auto& container = state.container_state;
  if (container.length) {
    Append(container, !container.last_bit_set, container.length);
    container.length = 0;
  }
  if (!container.appended_count) {
    return;
  }
  Finalize(container);
  state.total_count += container.appended_count;
  const bool has_nulls = container.null_count != 0;
  const bool has_non_nulls = container.null_count != container.appended_count;
  if (has_nulls || container.uncompressed) {
    state.stats_writer.SetHasNull();
  }
  if (has_non_nulls || container.uncompressed) {
    state.stats_writer.SetHasValid();
  }
  state.segment.count += container.appended_count;
  Reset(container);
}

void InitializeContainer(RoaringCompressState& state) {
  if (state.total_count == state.analyze_state.total_count) {
    return;
  }
  const auto container_index = GetContainerIndex(state);
  SDB_ASSERT(container_index < state.analyze_state.container_metadata.size());
  const auto metadata = state.analyze_state.container_metadata[container_index];

  const auto container_size =
    duckdb::AlignValue<idx_t, kBitsPerValue>(std::min<idx_t>(
      state.analyze_state.total_count - state.container_state.appended_count,
      kContainerSize));
  if (!CanStore(state, container_size, metadata)) {
    FlushSegment(state);
    CreateEmptySegment(state);
  }

  auto* base = state.segment.Data();
  if (metadata.type == ContainerType::Bitset) {
    OverrideUncompressed(state.container_state, base, state.data_ptr,
                         container_size);
    std::memset(state.data_ptr, ~0,
                sizeof(validity_t) * (container_size / kBitsPerValue));
  } else if (metadata.type == ContainerType::Run) {
    OverrideRun(state.container_state, base, state.data_ptr, metadata.count);
  } else {
    OverrideArray(state.container_state, base, state.data_ptr, metadata.nulls,
                  metadata.count);
  }
  state.data_ptr += DataSizeInBytes(metadata, container_size);
  AddMetadata(state.metadata_collection, metadata);
}

void NextContainer(RoaringCompressState& state) {
  FlushContainer(state);
  InitializeContainer(state);
}

void Finalize(RoaringCompressState& state) {
  FlushContainer(state);
  FlushSegment(state);
}

RoaringCompressState::RoaringCompressState(
  ColumnDataCheckpointData& checkpoint_data,
  const RoaringAnalyzeState& analyze_state)
  : checkpoint_data{checkpoint_data},
    analyze_state{analyze_state},
    segment{checkpoint_data.type} {
  CreateEmptySegment(*this);
  InitializeContainer(*this);
}

void HandleBit(RoaringCompressState& state, bool bit_set) {
  auto& container = state.container_state;
  if (container.length && container.last_bit_set != bit_set) {
    Append(container, !container.last_bit_set, container.length);
    container.length = 0;
  }
  ++container.length;
  container.last_bit_set = bit_set;
}

void HandleRaggedByte(RoaringCompressState& state, uint8_t array_index,
                      idx_t relevant_bits) {
  SDB_ASSERT(relevant_bits <= 8);
  for (idx_t i = 0; i < relevant_bits; ++i) {
    HandleBit(state, (array_index & (1U << i)) != 0);
  }
}

void HandleAllValid(RoaringCompressState& state, idx_t amount) {
  auto& container = state.container_state;
  if (container.length && !container.last_bit_set) {
    Append(container, !container.last_bit_set, container.length);
    container.length = 0;
  }
  container.length += amount;
  container.last_bit_set = true;
}

void HandleNoneValid(RoaringCompressState& state, idx_t amount) {
  auto& container = state.container_state;
  if (container.length && container.last_bit_set) {
    Append(container, !container.last_bit_set, container.length);
    container.length = 0;
  }
  container.length += amount;
  container.last_bit_set = false;
}

void HandleByte(RoaringCompressState& state, uint8_t array_index) {
  if (array_index == std::numeric_limits<uint8_t>::max()) {
    HandleAllValid(state, 8);
  } else if (array_index == 0) {
    HandleNoneValid(state, 8);
  } else {
    HandleRaggedByte(state, array_index, 8);
  }
}

idx_t Count(const RoaringCompressState& state) {
  return state.container_state.appended_count + state.container_state.length;
}

void Flush(RoaringCompressState& state) { NextContainer(state); }

template<typename State>
void AppendBytes(State& state, validity_t entry, idx_t bits) {
  SDB_ASSERT(bits <= kBitsPerValue);
  const idx_t full_bytes = bits / 8;
  const idx_t last_bits = bits % 8;
  for (idx_t i = 0; i < full_bytes; ++i) {
    HandleByte(state, static_cast<uint8_t>(entry >> (i * 8)));
  }
  if (last_bits != 0) [[unlikely]] {
    HandleRaggedByte(state, static_cast<uint8_t>(entry >> (full_bytes * 8)),
                     last_bits);
  }
}

template<typename State, typename OnRows>
void AppendValidity(State& state, const ValidityMask& validity,
                    idx_t input_size, OnRows&& on_rows) {
  if (validity.CannotHaveNull()) {
    idx_t appended = 0;
    while (appended < input_size) {
      const idx_t to_append =
        std::min<idx_t>(kContainerSize - Count(state), input_size - appended);
      on_rows(appended, to_append);
      HandleAllValid(state, to_append);
      if (Count(state) == kContainerSize) {
        Flush(state);
      }
      appended += to_append;
    }
    return;
  }
  idx_t appended = 0;
  while (appended < input_size) {
    idx_t to_append =
      std::min<idx_t>(kContainerSize - Count(state), input_size - appended);
    on_rows(appended, to_append);

    idx_t entry_offset = appended / kBitsPerValue;
    const idx_t previous_entry_remainder = appended % kBitsPerValue;
    if (previous_entry_remainder != 0) [[unlikely]] {
      const auto validity_entry = validity.GetValidityEntry(entry_offset);
      idx_t to_process = kBitsPerValue - previous_entry_remainder;
      validity_t mask = UpperMask(to_process);
      if (to_append < to_process) {
        const idx_t left_bits = to_process - to_append;
        to_process = to_append;
        mask = UpperMask(to_process) >> left_bits;
      }
      const auto masked_entry =
        (validity_entry & mask) >> previous_entry_remainder;
      if (masked_entry == LowerMask(to_process)) {
        HandleAllValid(state, to_process);
      } else if (masked_entry == 0) {
        HandleNoneValid(state, to_process);
      } else {
        AppendBytes(state, masked_entry, to_process);
      }
      to_append -= to_process;
      appended += to_process;
      ++entry_offset;
    }

    const idx_t entry_count = to_append / kBitsPerValue;
    for (idx_t entry_index = 0; entry_index < entry_count; ++entry_index) {
      const auto validity_entry =
        validity.GetValidityEntry(entry_offset + entry_index);
      if (ValidityMask::AllValid(validity_entry)) {
        HandleAllValid(state, kBitsPerValue);
      } else if (ValidityMask::NoneValid(validity_entry)) {
        HandleNoneValid(state, kBitsPerValue);
      } else {
        AppendBytes(state, validity_entry, kBitsPerValue);
      }
    }

    const idx_t remainder = to_append % kBitsPerValue;
    if (remainder != 0) [[unlikely]] {
      const auto validity_entry =
        validity.GetValidityEntry(entry_offset + entry_count);
      const auto masked = validity_entry & LowerMask(remainder);
      if (masked == LowerMask(remainder)) {
        HandleAllValid(state, remainder);
      } else if (masked == 0) {
        HandleNoneValid(state, remainder);
      } else {
        AppendBytes(state, validity_entry, remainder);
      }
    }

    if (Count(state) == kContainerSize) {
      Flush(state);
    }
    appended += to_append;
  }
}

class ContainerSegmentScan {
 public:
  explicit ContainerSegmentScan(const_data_ptr_t segments)
    : _segments{segments} {}

  uint16_t Next() {
    while (_index < kCompressedSegmentCount && _count >= _segments[_index]) {
      _count = 0;
      ++_index;
    }
    ++_count;
    SDB_ASSERT(_index <= kCompressedSegmentCount);
    SDB_ASSERT(_index == kCompressedSegmentCount || _segments[_index] != 0);
    return static_cast<uint16_t>(_index * kCompressedSegmentSize);
  }

 private:
  const_data_ptr_t _segments;
  uint8_t _index = 0;
  uint8_t _count = 0;
};

void UnalignedScan(const validity_t* input, idx_t input_size, idx_t input_start,
                   ValidityMask& result_mask, idx_t result_offset,
                   idx_t scan_count) {
  SDB_ASSERT(input_start < input_size);
  SDB_ASSERT(scan_count <= input_size - input_start);
  auto* result_data = result_mask.GetData();
  idx_t result_entry = result_offset / kBitsPerValue;
  idx_t result_idx = result_offset - result_entry * kBitsPerValue;
  idx_t input_entry = input_start / kBitsPerValue;
  idx_t input_idx = input_start - input_entry * kBitsPerValue;
  idx_t pos = 0;
  while (pos < scan_count) {
    validity_t input_mask = input[input_entry];
    const idx_t bits_left = scan_count - pos;
    const idx_t input_window_size =
      std::min(bits_left, kBitsPerValue - input_idx);
    const idx_t result_window_size =
      std::min(bits_left, kBitsPerValue - result_idx);
    const idx_t window_size = std::min(input_window_size, result_window_size);
    if (result_idx < input_idx) {
      input_mask >>= input_idx - result_idx;
    } else {
      const idx_t shift_amount = result_idx - input_idx;
      input_mask = (input_mask & ~UpperMask(shift_amount)) << shift_amount;
    }
    input_mask |= UpperMask(kBitsPerValue - result_idx - window_size);
    input_mask |= LowerMask(result_idx);
    if (input_mask != kAllValid) {
      if (!result_data) {
        result_mask.Initialize();
        result_data = result_mask.GetData();
      }
      result_data[result_entry] &= input_mask;
    }
    pos += window_size;
    input_idx = (input_idx + window_size) % kBitsPerValue;
    result_idx = (result_idx + window_size) % kBitsPerValue;
    if (input_idx == 0) {
      ++input_entry;
    }
    if (result_idx == 0) {
      ++result_entry;
    }
  }
}

void AlignedScan(const validity_t* input, idx_t input_start,
                 ValidityMask& result_mask, idx_t scan_count) {
  SDB_ASSERT(input_start % kBitsPerValue == 0);
  auto* result_data = result_mask.GetData();
  const idx_t start_offset = input_start / kBitsPerValue;
  const idx_t entry_scan_count = ValidityMask::EntryCount(scan_count);
  for (idx_t i = 0; i < entry_scan_count; ++i) {
    const auto input_entry = input[start_offset + i];
    if (!result_data && input_entry == kAllValid) {
      continue;
    }
    if (!result_data) {
      result_mask.Initialize();
      result_data = result_mask.GetData();
    }
    result_data[i] = input_entry;
  }
}

struct ContainerScanState {
  explicit ContainerScanState(idx_t container_index, idx_t container_size)
    : container_index{container_index}, container_size{container_size} {}
  virtual ~ContainerScanState() = default;

  virtual void ScanPartial(ValidityMask& result_mask, idx_t result_offset,
                           idx_t to_scan) = 0;
  virtual void Skip(idx_t to_skip) = 0;

  idx_t container_index;
  idx_t container_size;
  idx_t scanned_count = 0;
};

struct RunContainerScanState : ContainerScanState {
  explicit RunContainerScanState(idx_t container_index, idx_t container_size,
                                 idx_t count, const_data_ptr_t data)
    : ContainerScanState{container_index, container_size},
      count{count},
      data{data} {}

  void ScanPartial(ValidityMask& result_mask, idx_t result_offset,
                   idx_t to_scan) final {
    idx_t result_idx = 0;
    if (!run_index) {
      LoadNextRun();
    }
    while (!finished && result_idx < to_scan) {
      const auto start_of_run =
        std::max<idx_t>(std::min<idx_t>(run.start, scanned_count + to_scan),
                        scanned_count + result_idx);
      result_idx = start_of_run - scanned_count;

      const idx_t run_end = run.start + 1 + run.length;
      const auto run_or_scan_end =
        std::min<idx_t>(run_end, scanned_count + to_scan);

      if (run_or_scan_end < start_of_run) [[unlikely]] {
        ThrowCorrupted("roaring", "runs overlap or are out of order");
      }
      if (run_or_scan_end > start_of_run) {
        const idx_t amount = run_or_scan_end - start_of_run;
        const idx_t start = result_offset + result_idx;
        SetInvalidRange(result_mask, start, start + amount);
      }

      result_idx += run_or_scan_end - start_of_run;
      if (scanned_count + result_idx == run_end) {
        LoadNextRun();
      }
    }
    scanned_count += to_scan;
  }

  void Skip(idx_t to_skip) final {
    const idx_t end = scanned_count + to_skip;
    if (!run_index) {
      LoadNextRun();
    }
    while (scanned_count < end && !finished) {
      const idx_t run_end = run.start + 1 + run.length;
      scanned_count = std::min<idx_t>(run_end, end);
      if (scanned_count == run_end) {
        LoadNextRun();
      }
    }
    scanned_count = end;
  }

  virtual void LoadNextRun() {
    if (run_index >= count) {
      finished = true;
      return;
    }
    run = duckdb::Load<RunContainerRLEPair>(
      data + run_index * sizeof(RunContainerRLEPair));
    ++run_index;
  }

  RunContainerRLEPair run;
  bool finished = false;
  idx_t run_index = 0;
  idx_t count;
  const_data_ptr_t data;
};

struct CompressedRunContainerScanState final : RunContainerScanState {
  explicit CompressedRunContainerScanState(idx_t container_index,
                                           idx_t container_size, idx_t count,
                                           const_data_ptr_t segments,
                                           const_data_ptr_t data)
    : RunContainerScanState{container_index, container_size, count, data},
      segment{segments} {
    SDB_ASSERT(count >= kCompressedRunThreshold);
  }

  void LoadNextRun() final {
    if (run_index >= count) {
      finished = true;
      return;
    }
    auto start = segment.Next();
    start += data[(run_index * 2) + 0];

    auto end = segment.Next();
    end += data[(run_index * 2) + 1];

    if (end <= start) [[unlikely]] {
      ThrowCorrupted("roaring", "run ends before it starts");
    }
    run = RunContainerRLEPair{
      .start = start,
      .length = static_cast<uint16_t>(end - 1 - start),
    };
    ++run_index;
  }

  ContainerSegmentScan segment;
};

template<bool kInverted>
struct ArrayContainerScanState : ContainerScanState {
  explicit ArrayContainerScanState(idx_t container_index, idx_t container_size,
                                   idx_t count, const_data_ptr_t data)
    : ContainerScanState{container_index, container_size},
      data{data},
      count{count} {}

  virtual void LoadNextValue() {
    if (array_index >= count) {
      finished = true;
      return;
    }
    value = duckdb::Load<uint16_t>(data + array_index * sizeof(uint16_t));
    ++array_index;
  }

  void ScanPartial(ValidityMask& result_mask, idx_t result_offset,
                   idx_t to_scan) final {
    if constexpr (!kInverted) {
      SetInvalidRange(result_mask, result_offset, result_offset + to_scan);
    }
    if (!array_index) {
      LoadNextValue();
    }
    while (!finished) {
      if (value >= scanned_count + to_scan) {
        break;
      }
      if (value < scanned_count) {
        LoadNextValue();
        continue;
      }
      const auto index = value - scanned_count;
      if constexpr (kInverted) {
        result_mask.SetInvalid(result_offset + index);
      } else {
        result_mask.SetValid(result_offset + index);
      }
      LoadNextValue();
    }
    scanned_count += to_scan;
  }

  void Skip(idx_t to_skip) final {
    const idx_t end = scanned_count + to_skip;
    if (!array_index) {
      LoadNextValue();
    }
    while (!finished && value < end) {
      LoadNextValue();
    }
    scanned_count = end;
  }

  uint16_t value = 0;
  const_data_ptr_t data;
  bool finished = false;
  idx_t count;
  idx_t array_index = 0;
};

template<bool kInverted>
struct CompressedArrayContainerScanState final
  : ArrayContainerScanState<kInverted> {
  explicit CompressedArrayContainerScanState(idx_t container_index,
                                             idx_t container_size, idx_t count,
                                             const_data_ptr_t segments,
                                             const_data_ptr_t data)
    : ArrayContainerScanState<kInverted>{container_index, container_size, count,
                                         data},
      segment{segments} {
    SDB_ASSERT(count >= kCompressedArrayThreshold);
  }

  void LoadNextValue() final {
    if (this->array_index >= this->count) {
      this->finished = true;
      return;
    }
    this->value = segment.Next();
    this->value += this->data[this->array_index];
    ++this->array_index;
  }

  ContainerSegmentScan segment;
};

struct BitsetContainerScanState final : ContainerScanState {
  explicit BitsetContainerScanState(idx_t container_index, idx_t container_size,
                                    const validity_t* bitset)
    : ContainerScanState{container_index, container_size}, bitset{bitset} {}

  void ScanPartial(ValidityMask& result_mask, idx_t result_offset,
                   idx_t to_scan) final {
    if (!result_offset && to_scan % kBitsPerValue == 0 &&
        scanned_count % kBitsPerValue == 0) {
      AlignedScan(bitset, scanned_count, result_mask, to_scan);
    } else {
      UnalignedScan(bitset, container_size, scanned_count, result_mask,
                    result_offset, to_scan);
    }
    scanned_count += to_scan;
  }

  void Skip(idx_t to_skip) final { scanned_count += to_skip; }

  const validity_t* bitset;
};

struct RoaringScanState final : SegmentScanState {
  explicit RoaringScanState(const ColumnSegment& segment)
    : handle{segment.Pin()}, count{segment.count} {
    const auto segment_size = segment.size;
    if (segment_size < sizeof(idx_t)) [[unlikely]] {
      ThrowCorrupted("roaring", "segment is smaller than its header");
    }
    const auto* base_ptr = handle.Ptr();
    data_ptr = base_ptr + sizeof(idx_t);

    const auto metadata_offset = duckdb::Load<idx_t>(base_ptr);
    if (metadata_offset > segment_size - sizeof(idx_t)) [[unlikely]] {
      ThrowCorrupted("roaring", "metadata offset exceeds the segment");
    }
    const auto* metadata_ptr = data_ptr + metadata_offset;
    const idx_t metadata_size = segment_size - sizeof(idx_t) - metadata_offset;

    auto container_count = count / kContainerSize;
    if (count % kContainerSize != 0) {
      ++container_count;
    }
    if (container_count > metadata_size) [[unlikely]] {
      ThrowCorrupted("roaring",
                     "container count exceeds available metadata space");
    }

    ContainerMetadataCollection metadata_collection;
    Deserialize(metadata_collection, metadata_ptr, container_count,
                metadata_size);
    ContainerMetadataCollectionScanner scanner{metadata_collection};
    container_metadata.reserve(container_count);
    data_start_position.reserve(container_count);
    idx_t position = 0;
    for (idx_t i = 0; i < container_count; ++i) {
      const auto metadata = GetNext(scanner);
      container_metadata.emplace_back(metadata);
      if (metadata.type == ContainerType::Bitset) {
        position = duckdb::AlignValue<idx_t>(position);
      } else if (metadata.type == ContainerType::Array &&
                 metadata.count < kCompressedArrayThreshold) {
        position = duckdb::AlignValue<idx_t, sizeof(uint16_t)>(position);
      } else if (metadata.type == ContainerType::Run &&
                 metadata.count < kCompressedRunThreshold) {
        position =
          duckdb::AlignValue<idx_t, sizeof(RunContainerRLEPair)>(position);
      }
      data_start_position.emplace_back(position);
      const auto rows =
        std::min<idx_t>(kContainerSize, count - i * kContainerSize);
      position += DataSizeInBytes(
        metadata, duckdb::AlignValue<idx_t, kBitsPerValue>(rows));
      if (position > metadata_offset) [[unlikely]] {
        ThrowCorrupted("roaring", "container data extends into the metadata");
      }
    }
  }

  duckdb::BufferHandle handle;
  uint64_t count;
  const_data_ptr_t data_ptr = nullptr;
  std::unique_ptr<ContainerScanState> current_container;
  std::vector<ContainerMetadata> container_metadata;
  std::vector<idx_t> data_start_position;
  idx_t lookup_container = duckdb::DConstants::INVALID_INDEX;
  std::array<idx_t, kCompressedSegmentCount + 1> lookup_starts = {};
};

void SkipContainer(ContainerScanState& container, idx_t skip_count) {
  SDB_ASSERT(container.scanned_count + skip_count <= container.container_size);
  if (container.scanned_count + skip_count == container.container_size) {
    container.scanned_count = container.container_size;
    return;
  }
  container.Skip(skip_count);
}

ContainerScanState& LoadContainer(RoaringScanState& scan, idx_t container_index,
                                  idx_t internal_offset) {
  auto& current = scan.current_container;
  if (current && current->container_index == container_index &&
      current->scanned_count <= internal_offset) {
    if (current->scanned_count < internal_offset) {
      SkipContainer(*current, internal_offset - current->scanned_count);
    }
    return *current;
  }
  const auto metadata = scan.container_metadata[container_index];
  const auto* data = scan.data_ptr + scan.data_start_position[container_index];

  const auto start_of_container = container_index * kContainerSize;
  const auto container_size =
    std::min<idx_t>(scan.count - start_of_container, kContainerSize);
  if (metadata.type == ContainerType::Bitset) {
    current = std::make_unique<BitsetContainerScanState>(
      container_index, container_size,
      reinterpret_cast<const validity_t*>(data));
  } else if (metadata.type == ContainerType::Run) {
    SDB_ASSERT(metadata.nulls);
    if (metadata.count >= kCompressedRunThreshold) {
      current = std::make_unique<CompressedRunContainerScanState>(
        container_index, container_size, metadata.count, data,
        data + kCompressedSegmentCount);
    } else {
      current = std::make_unique<RunContainerScanState>(
        container_index, container_size, metadata.count, data);
    }
  } else if (metadata.count >= kCompressedArrayThreshold) {
    if (metadata.nulls) {
      current = std::make_unique<CompressedArrayContainerScanState<kNulls>>(
        container_index, container_size, metadata.count, data,
        data + kCompressedSegmentCount);
    } else {
      current = std::make_unique<CompressedArrayContainerScanState<kNonNulls>>(
        container_index, container_size, metadata.count, data,
        data + kCompressedSegmentCount);
    }
  } else if (metadata.nulls) {
    current = std::make_unique<ArrayContainerScanState<kNulls>>(
      container_index, container_size, metadata.count, data);
  } else {
    current = std::make_unique<ArrayContainerScanState<kNonNulls>>(
      container_index, container_size, metadata.count, data);
  }

  if (internal_offset) {
    SkipContainer(*current, internal_offset);
  }
  return *current;
}

void ScanPartial(RoaringScanState& scan, idx_t start_idx, ValidityMask& result,
                 idx_t offset, idx_t count) {
  idx_t remaining = count;
  idx_t scanned = 0;
  while (remaining) {
    const idx_t row = start_idx + scanned;
    auto& container =
      LoadContainer(scan, row / kContainerSize, row % kContainerSize);
    const idx_t remaining_in_container =
      container.container_size - container.scanned_count;
    const idx_t to_scan = std::min(remaining, remaining_in_container);
    container.ScanPartial(result, offset + scanned, to_scan);
    remaining -= to_scan;
    scanned += to_scan;
  }
  SDB_ASSERT(scanned == count);
}

const std::array<idx_t, kCompressedSegmentCount + 1>& CompressedSegmentStarts(
  RoaringScanState& scan, idx_t container_index, const_data_ptr_t segments,
  idx_t count) {
  if (scan.lookup_container != container_index) {
    idx_t start = 0;
    scan.lookup_starts[0] = 0;
    for (idx_t i = 0; i < kCompressedSegmentCount; ++i) {
      start += segments[i];
      scan.lookup_starts[i + 1] = std::min<idx_t>(start, count);
    }
    scan.lookup_container = container_index;
  }
  return scan.lookup_starts;
}

bool RowIsValid(RoaringScanState& scan, idx_t row) {
  const idx_t container_index = row / kContainerSize;
  const idx_t offset = row % kContainerSize;
  const auto metadata = scan.container_metadata[container_index];
  const auto* data = scan.data_ptr + scan.data_start_position[container_index];
  if (metadata.type == ContainerType::Bitset) {
    const auto entry = duckdb::Load<validity_t>(data + offset / kBitsPerValue *
                                                         sizeof(validity_t));
    return ValidityMask::RowIsValid(entry, offset % kBitsPerValue);
  }
  const auto segment = offset / kCompressedSegmentSize;
  const auto low = static_cast<uint8_t>(offset % kCompressedSegmentSize);
  if (metadata.type == ContainerType::Run) {
    SDB_ASSERT(metadata.nulls);
    const idx_t number_of_runs = metadata.count;
    if (number_of_runs >= kCompressedRunThreshold) {
      const auto& starts = CompressedSegmentStarts(scan, container_index, data,
                                                   number_of_runs * 2);
      const std::span<const uint8_t> values{
        data + kCompressedSegmentCount + starts[segment],
        data + kCompressedSegmentCount + starts[segment + 1]};
      const auto last = absl::c_upper_bound(values, low);
      return (starts[segment] + static_cast<idx_t>(last - values.begin())) %
               2 ==
             0;
    }
    const std::span runs{reinterpret_cast<const RunContainerRLEPair*>(data),
                         number_of_runs};
    const auto next = absl::c_upper_bound(
      runs, offset,
      [](idx_t value, const auto& run) { return value < run.start; });
    if (next == runs.begin()) {
      return true;
    }
    const auto& run = *(next - 1);
    return offset >= idx_t{run.start} + 1 + run.length;
  }
  const idx_t cardinality = metadata.count;
  bool listed = false;
  if (cardinality >= kCompressedArrayThreshold) {
    const auto& starts =
      CompressedSegmentStarts(scan, container_index, data, cardinality);
    const std::span<const uint8_t> values{
      data + kCompressedSegmentCount + starts[segment],
      data + kCompressedSegmentCount + starts[segment + 1]};
    listed = absl::c_binary_search(values, low);
  } else {
    const std::span array{reinterpret_cast<const uint16_t*>(data), cardinality};
    listed = absl::c_binary_search(array, static_cast<uint16_t>(offset));
  }
  return metadata.nulls ? !listed : listed;
}

void ExtractValidityMaskToData(const ValidityMask& validity,
                               duckdb::Vector& dst, idx_t offset,
                               idx_t scan_count) {
  auto* write_ptr = duckdb::FlatVector::GetDataMutable<uint8_t>(dst) + offset;
  if (validity.CannotHaveNull()) {
    std::memset(write_ptr, 1, scan_count);
    return;
  }
  const auto* src = reinterpret_cast<const_data_ptr_t>(validity.GetData());
  const idx_t aligned =
    duckdb::AlignValueFloor<idx_t, kPackGroupSize>(scan_count);
  BitpackingPrimitives::UnPackBuffer<uint8_t>(write_ptr, src, aligned, 1);
  if (aligned == scan_count) {
    return;
  }
  std::array<uint8_t, kPackGroupSize> tail = {};
  BitpackingPrimitives::UnPackBlock<uint8_t>(tail.data(), src + aligned / 8, 1);
  std::memcpy(write_ptr + aligned, tail.data(), scan_count - aligned);
}

template<duckdb::PhysicalType kPhysical>
void CountContainers(RoaringAnalyzeState& state,
                     std::span<const WriteChunk> chunks) {
  const auto no_stats = [](idx_t, idx_t) {};
  for (const auto& chunk : chunks) {
    const auto count = chunk.data.size();
    duckdb::UnifiedVectorFormat unified;
    chunk.data.ToUnifiedFormat(unified);
    if constexpr (kPhysical == duckdb::PhysicalType::BOOL) {
      ValidityMask packed{count};
      PackBooleans(packed, unified, count);
      AppendValidity(state, packed, count, no_stats);
    } else {
      AppendValidity(state, unified.validity, count, no_stats);
    }
    state.total_count += count;
  }
  FlushContainer(state);
  FlushSegment(state);
}

idx_t Price(const RoaringAnalyzeState& state) noexcept {
  return static_cast<idx_t>(static_cast<double>(state.total_size) *
                            kCompressPenalty);
}

template<duckdb::PhysicalType kPhysical>
idx_t AnalyzeChunks(std::span<const WriteChunk> chunks) {
  RoaringAnalyzeState state;
  CountContainers<kPhysical>(state, chunks);
  return Price(state);
}

template<duckdb::PhysicalType kPhysical>
void EncodeChunks(ColumnDataCheckpointData& checkpoint_data,
                  std::span<const WriteChunk> chunks,
                  const RoaringAnalyzeState& analyze_state) {
  const auto compress =
    std::make_unique<RoaringCompressState>(checkpoint_data, analyze_state);
  auto& state = *compress;
  for (const auto& chunk : chunks) {
    const auto count = chunk.data.size();
    duckdb::UnifiedVectorFormat unified;
    chunk.data.ToUnifiedFormat(unified);
    if constexpr (kPhysical == duckdb::PhysicalType::BOOL) {
      ValidityMask packed{count};
      PackBooleans(packed, unified, count);
      const auto* values = duckdb::UnifiedVectorFormat::GetData<bool>(unified);
      AppendValidity(state, packed, count, [&](idx_t begin, idx_t rows) {
        for (idx_t i = begin; i < begin + rows; ++i) {
          const auto idx = unified.sel->get_index(i);
          if (unified.validity.RowIsValid(idx)) {
            state.bool_stats_writer.Update(values[idx]);
          } else {
            state.bool_stats_writer.SetHasNull();
          }
        }
      });
    } else {
      AppendValidity(state, unified.validity, count, [](idx_t, idx_t) {});
    }
  }
  Finalize(state);
}

template<duckdb::PhysicalType kPhysical>
void CompressChunks(ColumnDataCheckpointData& checkpoint_data,
                    std::span<const WriteChunk> chunks) {
  RoaringAnalyzeState analyze_state;
  CountContainers<kPhysical>(analyze_state, chunks);
  EncodeChunks<kPhysical>(checkpoint_data, chunks, analyze_state);
}

std::unique_ptr<SegmentScanState> InitScan(const ColumnSegment& segment) {
  return std::make_unique<RoaringScanState>(segment);
}

void ScanPartialValidity(const ColumnSegment&, ColumnScanState& state,
                         idx_t scan_count, duckdb::Vector& result,
                         idx_t result_offset) {
  ScanPartial(
    state.scan_state->Cast<RoaringScanState>(), state.GetPositionInSegment(),
    duckdb::FlatVector::ValidityMutable(result), result_offset, scan_count);
}

void ScanValidity(const ColumnSegment& segment, ColumnScanState& state,
                  idx_t scan_count, duckdb::Vector& result) {
  result.Flatten();
  ScanPartialValidity(segment, state, scan_count, result, 0);
}

void ScanPartialBoolean(const ColumnSegment&, ColumnScanState& state,
                        idx_t scan_count, duckdb::Vector& result,
                        idx_t result_offset) {
  ValidityMask mask{scan_count};
  ScanPartial(state.scan_state->Cast<RoaringScanState>(),
              state.GetPositionInSegment(), mask, 0, scan_count);
  ExtractValidityMaskToData(mask, result, result_offset, scan_count);
}

void ScanBoolean(const ColumnSegment& segment, ColumnScanState& state,
                 idx_t scan_count, duckdb::Vector& result) {
  ScanPartialBoolean(segment, state, scan_count, result, 0);
}

RoaringScanState& FetchScanState(const ColumnSegment& segment,
                                 ColumnFetchState& state, const idx_t* rows,
                                 idx_t count) {
  SDB_ASSERT(count == 0 || rows[count - 1] < segment.count);
  return state.GetOrCreateSegmentState<RoaringScanState>(
    [&] { return std::make_unique<RoaringScanState>(segment); });
}

void FetchRowsValidity(const ColumnSegment& segment, ColumnFetchState& state,
                       const idx_t* rows, idx_t count, duckdb::Vector& result,
                       const duckdb::SelectionVector& result_sel,
                       idx_t result_offset) {
  auto& scan = FetchScanState(segment, state, rows, count);
  auto& result_mask = duckdb::FlatVector::ValidityMutable(result);
  for (idx_t i = 0; i < count; ++i) {
    if (!RowIsValid(scan, rows[i])) {
      result_mask.SetInvalid(result_offset + result_sel.get_index(i));
    }
  }
}

void FetchRowsBoolean(const ColumnSegment& segment, ColumnFetchState& state,
                      const idx_t* rows, idx_t count, duckdb::Vector& result,
                      const duckdb::SelectionVector& result_sel,
                      idx_t result_offset) {
  auto& scan = FetchScanState(segment, state, rows, count);
  auto* result_data = duckdb::FlatVector::GetDataMutable<uint8_t>(result);
  for (idx_t i = 0; i < count; ++i) {
    result_data[result_offset + result_sel.get_index(i)] =
      RowIsValid(scan, rows[i]);
  }
}

constexpr CompressionFunction kValidity{
  .type = CodecType::Roaring,
  .analyze = AnalyzeChunks<duckdb::PhysicalType::BIT>,
  .compress = CompressChunks<duckdb::PhysicalType::BIT>,
  .init_scan = InitScan,
  .scan_vector = ScanValidity,
  .scan_partial = ScanPartialValidity,
  .fetch_rows = FetchRowsValidity,
  .skip = EmptySkip,
};

constexpr CompressionFunction kBoolean{
  .type = CodecType::Roaring,
  .analyze = AnalyzeChunks<duckdb::PhysicalType::BOOL>,
  .compress = CompressChunks<duckdb::PhysicalType::BOOL>,
  .init_scan = InitScan,
  .scan_vector = ScanBoolean,
  .scan_partial = ScanPartialBoolean,
  .fetch_rows = FetchRowsBoolean,
  .skip = EmptySkip,
};

}  // namespace

bool CompressRoaringValidity(ColumnDataCheckpointData& checkpoint_data,
                             std::span<const WriteChunk> chunks,
                             duckdb::idx_t limit) {
  RoaringAnalyzeState analyze_state;
  CountContainers<duckdb::PhysicalType::BIT>(analyze_state, chunks);
  if (Price(analyze_state) >= limit) {
    return false;
  }
  EncodeChunks<duckdb::PhysicalType::BIT>(checkpoint_data, chunks,
                                          analyze_state);
  return true;
}

const CompressionFunction* RoaringFunction(duckdb::PhysicalType physical) {
  using enum duckdb::PhysicalType;
  switch (physical) {
    case BIT:
      return &kValidity;
    case BOOL:
      return &kBoolean;
    default:
      return nullptr;
  }
}

}  // namespace irs::codecs
