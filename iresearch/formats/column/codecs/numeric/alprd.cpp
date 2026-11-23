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
#include <absl/base/optimization.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <functional>
#include <limits>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/numeric/alp_primitives.hpp"
#include "iresearch/formats/column/codecs/numeric/alp_scan.tpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs {
namespace {

using alp::kMetadataPointerSize;
using alp::kVectorSize;
using duckdb::idx_t;

constexpr uint8_t kMaxDictionaryBitWidth = 3;
constexpr uint8_t kMaxDictionarySize = 1 << kMaxDictionaryBitWidth;
constexpr idx_t kCuttingLimit = 16;
constexpr uint32_t kDictionaryElementSize = sizeof(uint16_t);
constexpr uint32_t kMaxDictionarySizeBytes =
  kMaxDictionarySize * kDictionaryElementSize;
constexpr uint32_t kExceptionSize = sizeof(uint16_t);
constexpr uint32_t kExceptionsCountSize = sizeof(uint16_t);
constexpr uint16_t kUncompressedModeSentinel =
  std::numeric_limits<uint16_t>::max();
constexpr uint32_t kExceptionPositionSize = sizeof(uint16_t);
constexpr uint32_t kRightBitWidthSize = sizeof(uint8_t);
constexpr uint32_t kLeftBitWidthSize = sizeof(uint8_t);
constexpr uint32_t kNDictionaryElementsSize = sizeof(uint8_t);
constexpr uint32_t kHeaderSize = kMetadataPointerSize + kRightBitWidthSize +
                                 kLeftBitWidthSize + kNDictionaryElementsSize;

[[noreturn]] IRS_NO_INLINE void ThrowAlpRdRightBitWidthOutOfRange(
  uint8_t bit_width, uint8_t max_bit_width) {
  ThrowCorrupted("ALPRD", absl::StrCat("right bit width ", bit_width,
                                       " exceeds ", max_bit_width));
}

[[noreturn]] IRS_NO_INLINE void ThrowAlpRdLeftBitWidthOutOfRange(
  uint8_t bit_width) {
  ThrowCorrupted("ALPRD", absl::StrCat("left bit width ", bit_width,
                                       " exceeds ", kMaxDictionaryBitWidth));
}

[[noreturn]] IRS_NO_INLINE void ThrowAlpRdDictionarySizeExceedsMaximum() {
  ThrowCorrupted("ALPRD", "dictionary size exceeds maximum");
}

[[noreturn]] IRS_NO_INLINE void ThrowAlpRdExceptionCountOutOfRange(
  uint16_t exception_count, idx_t vector_size) {
  ThrowCorrupted("ALPRD", absl::StrCat("exception count ", exception_count,
                                       " exceeds vector size ", vector_size));
}

[[noreturn]] IRS_NO_INLINE void ThrowAlpRdExceptionPositionOutOfRange(
  uint16_t position, idx_t vector_size) {
  ThrowCorrupted("ALPRD",
                 absl::StrCat("exception position ", position,
                              " is outside vector size ", vector_size));
}

[[noreturn]] IRS_NO_INLINE void ThrowAlpRdVectorOutOfRange() {
  ThrowCorrupted("ALPRD", "vector exceeds its data range");
}

[[noreturn]] IRS_NO_INLINE void ThrowAlpRdVector(uint16_t exceptions_count,
                                                 idx_t vector_size) {
  if (exceptions_count > vector_size) {
    ThrowAlpRdExceptionCountOutOfRange(exceptions_count, vector_size);
  }
  ThrowAlpRdVectorOutOfRange();
}

struct AlpRdLeftPartInfo {
  uint32_t count = 0;
  uint64_t hash = 0;
};

struct AlpRdDictionary {
  uint8_t right_bit_width = 0;
  uint8_t left_bit_width = 0;
  uint8_t actual_dictionary_size = 0;
  uint16_t left_parts_dict[kMaxDictionarySize] = {};
  containers::FlatHashMap<uint16_t, uint16_t> left_parts_dict_map;
};

double EstimateCompressionSize(uint8_t right_bit_width, uint8_t left_bit_width,
                               uint32_t exceptions_count,
                               uint64_t sample_count) {
  const auto exceptions_size =
    exceptions_count * ((static_cast<uint64_t>(kExceptionPositionSize) +
                         static_cast<uint64_t>(kExceptionSize)) *
                        8);
  const auto estimated_size =
    static_cast<double>(right_bit_width + left_bit_width) +
    (static_cast<double>(exceptions_size) / static_cast<double>(sample_count));
  return estimated_size;
}

std::vector<AlpRdLeftPartInfo> CountLeftParts(
  std::span<const uint16_t> sorted_tops, idx_t left_bits) {
  std::vector<AlpRdLeftPartInfo> left_parts;
  for (const auto top : sorted_tops) {
    const uint64_t left_part = top >> (kCuttingLimit - left_bits);
    if (left_parts.empty() || left_parts.back().hash != left_part) {
      left_parts.emplace_back(0, left_part);
    }
    ++left_parts.back().count;
  }
  return left_parts;
}

void DropLowestBit(std::vector<AlpRdLeftPartInfo>& left_parts) {
  size_t merged = 0;
  for (const auto part : left_parts) {
    const auto left_part = part.hash >> 1;
    if (merged && left_parts[merged - 1].hash == left_part) {
      left_parts[merged - 1].count += part.count;
    } else {
      left_parts[merged++] = {part.count, left_part};
    }
  }
  left_parts.resize(merged);
}

uint8_t LeftBitWidth(uint64_t dictionary_size) {
  return std::max<uint8_t>(1, duckdb::ExactNumericCast<uint8_t>(
                                std::ceil(std::log2(dictionary_size))));
}

double EstimateLeftParts(std::span<const AlpRdLeftPartInfo> left_parts,
                         std::vector<uint32_t>& counts, idx_t right_bit_width,
                         uint64_t sample_count) {
  uint32_t exceptions_count = 0;
  if (left_parts.size() > kMaxDictionarySize) {
    counts.clear();
    for (const auto& part : left_parts) {
      counts.push_back(part.count);
    }
    absl::c_nth_element(counts, counts.begin() + kMaxDictionarySize,
                        std::greater{});
    for (const auto count : std::span{counts}.subspan(kMaxDictionarySize)) {
      exceptions_count += count;
    }
  }
  return EstimateCompressionSize(
    static_cast<uint8_t>(right_bit_width),
    LeftBitWidth(std::min<uint64_t>(kMaxDictionarySize, left_parts.size())),
    exceptions_count, sample_count);
}

template<typename T>
double FindBestDictionary(std::span<const alp::ExactType<T>> values,
                          AlpRdDictionary& dictionary) {
  constexpr idx_t kExactTypeBitSize = sizeof(alp::ExactType<T>) * 8;
  std::vector<uint16_t> sorted_tops(values.size());
  absl::c_transform(values, sorted_tops.begin(), [](auto value) {
    return static_cast<uint16_t>(value >> (kExactTypeBitSize - kCuttingLimit));
  });
  absl::c_sort(sorted_tops);

  auto left_parts = CountLeftParts(sorted_tops, kCuttingLimit);
  std::vector<uint32_t> counts;
  idx_t left_bits = kCuttingLimit;
  double best_dict_size = EstimateLeftParts(
    left_parts, counts, kExactTypeBitSize - kCuttingLimit, values.size());
  for (idx_t i = kCuttingLimit - 1; i != 0; --i) {
    DropLowestBit(left_parts);
    const double estimated_size = EstimateLeftParts(
      left_parts, counts, kExactTypeBitSize - i, values.size());
    if (estimated_size < best_dict_size) {
      left_bits = i;
      best_dict_size = estimated_size;
    }
  }

  left_parts = CountLeftParts(sorted_tops, left_bits);
  absl::c_sort(
    left_parts, [](const AlpRdLeftPartInfo& a, const AlpRdLeftPartInfo& b) {
      return a.count > b.count || (a.count == b.count && a.hash < b.hash);
    });
  const uint64_t actual_dictionary_size =
    std::min<uint64_t>(kMaxDictionarySize, left_parts.size());
  for (idx_t dict_idx = 0; dict_idx < actual_dictionary_size; ++dict_idx) {
    dictionary.left_parts_dict[dict_idx] =
      static_cast<uint16_t>(left_parts[dict_idx].hash);
    dictionary.left_parts_dict_map.try_emplace(
      dictionary.left_parts_dict[dict_idx], static_cast<uint16_t>(dict_idx));
  }
  for (idx_t i = actual_dictionary_size + 1; i < left_parts.size(); ++i) {
    dictionary.left_parts_dict_map.try_emplace(
      static_cast<uint16_t>(left_parts[i].hash), static_cast<uint16_t>(i));
  }
  dictionary.left_bit_width = LeftBitWidth(actual_dictionary_size);
  dictionary.right_bit_width =
    static_cast<uint8_t>(kExactTypeBitSize - left_bits);
  dictionary.actual_dictionary_size =
    static_cast<uint8_t>(actual_dictionary_size);
  SDB_ASSERT(dictionary.left_bit_width > 0 && dictionary.right_bit_width > 0 &&
             dictionary.left_bit_width <= kMaxDictionaryBitWidth &&
             dictionary.actual_dictionary_size <= kMaxDictionarySize);
  return best_dict_size;
}

template<typename T>
void AlpRdDecompress(duckdb::const_data_ptr_t left_encoded,
                     duckdb::const_data_ptr_t right_encoded,
                     const uint16_t* left_parts_dict, alp::ExactType<T>* output,
                     idx_t values_count, uint16_t exceptions_count,
                     const uint16_t* exceptions,
                     const uint16_t* exceptions_positions,
                     uint8_t left_bit_width, uint8_t right_bit_width) {
  using Exact = alp::ExactType<T>;
  ABSL_CACHELINE_ALIGNED uint16_t left_decoded[kVectorSize] = {};
  ABSL_CACHELINE_ALIGNED Exact right_decoded[kVectorSize] = {};

  duckdb::BitpackingPrimitives::UnPackBuffer<uint16_t>(
    reinterpret_cast<duckdb::data_ptr_t>(left_decoded), left_encoded,
    values_count, left_bit_width);
  duckdb::BitpackingPrimitives::UnPackBuffer<Exact>(
    reinterpret_cast<duckdb::data_ptr_t>(right_decoded), right_encoded,
    values_count, right_bit_width);

  for (idx_t i = 0; i < values_count; ++i) {
    const uint16_t left = left_parts_dict[left_decoded[i]];
    const Exact right = right_decoded[i];
    output[i] = (static_cast<Exact>(left) << right_bit_width) | right;
  }

  for (idx_t i = 0; i < exceptions_count; ++i) {
    const Exact right = right_decoded[exceptions_positions[i]];
    const uint16_t left = exceptions[i];
    output[exceptions_positions[i]] =
      (static_cast<Exact>(left) << right_bit_width) | right;
  }
}

template<typename T>
struct AlpRdAnalyzeState {
  idx_t vectors_count = 0;
  idx_t total_values_count = 0;
  idx_t vectors_sampled_count = 0;
  std::vector<alp::ExactType<T>> rowgroup_sample;
  AlpRdDictionary dictionary;
};

template<typename T>
void AlpRdSample(AlpRdAnalyzeState<T>& analyze_state,
                 const duckdb::Vector& input) {
  using Exact = alp::ExactType<T>;

  const auto count = input.size();
  const bool must_skip_current_vector = alp::MustSkipSamplingFromCurrentVector(
    analyze_state.vectors_count, analyze_state.vectors_sampled_count, count);
  analyze_state.vectors_count += 1;
  analyze_state.total_values_count += count;
  if (must_skip_current_vector) {
    return;
  }

  const auto values = input.Values<T>();
  const auto sampling_params = alp::GetSamplingParameters(count);

  uint16_t current_vector_null_positions[alp::kSamplesPerVector] = {};
  auto& rowgroup_sample = analyze_state.rowgroup_sample;
  const auto sample_begin = rowgroup_sample.size();
  rowgroup_sample.resize(sample_begin + sampling_params.n_sampled_values);
  auto* current_vector_sample = rowgroup_sample.data() + sample_begin;

  idx_t sample_idx = 0;
  idx_t nulls_idx = 0;
  if (!values.CanHaveNull()) {
    for (idx_t i = 0; i < sampling_params.n_lookup_values;
         i += sampling_params.n_sampled_increments) {
      current_vector_sample[sample_idx++] =
        std::bit_cast<Exact>(values[i].GetValueUnsafe());
    }
  } else {
    for (idx_t i = 0; i < sampling_params.n_lookup_values;
         i += sampling_params.n_sampled_increments) {
      const auto entry = values[i];
      current_vector_sample[sample_idx] =
        std::bit_cast<Exact>(entry.GetValueUnsafe());
      current_vector_null_positions[nulls_idx] =
        static_cast<uint16_t>(sample_idx);
      nulls_idx += !entry.IsValid();
      ++sample_idx;
    }
    alp::FindAndReplaceNullsInVector<Exact>(
      current_vector_sample, current_vector_null_positions,
      sampling_params.n_sampled_values, nulls_idx);
  }
  SDB_ASSERT(sample_idx == sampling_params.n_sampled_values);

  ++analyze_state.vectors_sampled_count;
}

template<typename T>
idx_t AlpRdAnalyzeChunks(std::span<const WriteChunk> chunks) {
  AlpRdAnalyzeState<T> analyze_state;
  for (const auto& chunk : chunks) {
    AlpRdSample(analyze_state, chunk.data);
  }
  if (!analyze_state.total_values_count) {
    return duckdb::DConstants::INVALID_INDEX;
  }
  const double factor_of_sampling =
    1 / (static_cast<double>(analyze_state.rowgroup_sample.size()) /
         static_cast<double>(analyze_state.total_values_count));

  const double estimated_bits_per_value = FindBestDictionary<T>(
    analyze_state.rowgroup_sample, analyze_state.dictionary);
  const double estimated_compressed_bits =
    estimated_bits_per_value *
    static_cast<double>(analyze_state.rowgroup_sample.size());
  const double estimated_compressed_bytes = estimated_compressed_bits / 8;

  const double per_segment_overhead = kHeaderSize + kMaxDictionarySizeBytes;
  const double per_vector_overhead =
    kMetadataPointerSize + kExceptionsCountSize;

  const uint32_t n_vectors = duckdb::LossyNumericCast<uint32_t>(std::ceil(
    static_cast<double>(analyze_state.total_values_count) / kVectorSize));

  const auto estimated_size =
    (estimated_compressed_bytes * factor_of_sampling) +
    (n_vectors * per_vector_overhead);
  const uint32_t estimated_n_blocks = duckdb::LossyNumericCast<uint32_t>(
    std::ceil(estimated_size /
              (static_cast<double>(kBlockSize) - per_segment_overhead)));

  const auto final_analyze_size =
    estimated_size + (estimated_n_blocks * per_segment_overhead);
  return duckdb::LossyNumericCast<idx_t>(final_analyze_size);
}

template<typename T>
class AlpRdCompressionState {
 public:
  using Exact = alp::ExactType<T>;

  AlpRdCompressionState(ColumnDataCheckpointData& checkpoint_data,
                        AlpRdAnalyzeState<T>& analyze_state)
    : _checkpoint_data{checkpoint_data},
      _segment{CodecType::Alprd, checkpoint_data.type},
      _dictionary{std::move(analyze_state.dictionary)},
      _actual_dictionary_size_bytes{_dictionary.actual_dictionary_size *
                                    kDictionaryElementSize},
      _next_vector_byte_index_start{kHeaderSize +
                                    _actual_dictionary_size_bytes} {}

  void Append(const duckdb::Vector& input) {
    const auto values = input.Values<T>();
    idx_t values_left_in_data = input.size();
    idx_t offset_in_data = 0;
    while (values_left_in_data) {
      const auto values_to_fill_alp_input =
        std::min<idx_t>(kVectorSize - _vector_idx, values_left_in_data);
      if (!values.CanHaveNull()) {
        for (idx_t i = 0; i < values_to_fill_alp_input; ++i) {
          _input_vector[_vector_idx + i] =
            std::bit_cast<Exact>(values[offset_in_data + i].GetValueUnsafe());
        }
      } else {
        for (idx_t i = 0; i < values_to_fill_alp_input; ++i) {
          const auto entry = values[offset_in_data + i];
          _vector_null_positions[_nulls_idx] =
            static_cast<uint16_t>(_vector_idx + i);
          _nulls_idx += !entry.IsValid();
          _input_vector[_vector_idx + i] =
            std::bit_cast<Exact>(entry.GetValueUnsafe());
        }
      }
      offset_in_data += values_to_fill_alp_input;
      values_left_in_data -= values_to_fill_alp_input;
      _vector_idx += values_to_fill_alp_input;
      if (_vector_idx == kVectorSize) {
        CompressVector();
      }
    }
  }

  void Finalize() {
    if (_vector_idx) {
      CompressVector();
    }
    FlushSegment();
  }

 private:
  idx_t UsedSpace() const noexcept {
    return kHeaderSize + _actual_dictionary_size_bytes + _data_bytes_used;
  }

  bool HasEnoughSpace(idx_t vector_size) const noexcept {
    return duckdb::AlignValue(UsedSpace() + vector_size) <
           _metadata_offset - kMetadataPointerSize;
  }

  idx_t RequiredSpace() const noexcept {
    return _left_bit_packed_size + _right_bit_packed_size +
           static_cast<idx_t>(_exceptions_count) *
             (kExceptionSize + kExceptionPositionSize) +
           kExceptionsCountSize;
  }

  void CompressVectorData() {
    ABSL_CACHELINE_ALIGNED uint64_t right_parts[kVectorSize];
    ABSL_CACHELINE_ALIGNED uint16_t left_parts[kVectorSize];
    const auto right_bit_width = _dictionary.right_bit_width;
    const auto left_bit_width = _dictionary.left_bit_width;
    const auto actual_dictionary_size = _dictionary.actual_dictionary_size;

    for (idx_t i = 0; i < _vector_idx; ++i) {
      const Exact tmp = _input_vector[i];
      right_parts[i] = tmp & ((1ULL << right_bit_width) - 1);
      left_parts[i] = static_cast<uint16_t>(tmp >> right_bit_width);
    }

    _exceptions_count = 0;
    for (idx_t i = 0; i < _vector_idx; ++i) {
      const auto dictionary_key = left_parts[i];
      const auto it = _dictionary.left_parts_dict_map.find(dictionary_key);
      const uint16_t dictionary_index =
        it == _dictionary.left_parts_dict_map.end() ? actual_dictionary_size
                                                    : it->second;
      left_parts[i] = dictionary_index;
      if (dictionary_index >= actual_dictionary_size) {
        _exceptions[_exceptions_count] = dictionary_key;
        _exceptions_positions[_exceptions_count] = static_cast<uint16_t>(i);
        ++_exceptions_count;
      }
    }

    _right_bit_packed_size = duckdb::BitpackingPrimitives::GetRequiredSize(
      _vector_idx, right_bit_width);
    _left_bit_packed_size = duckdb::BitpackingPrimitives::GetRequiredSize(
      _vector_idx, left_bit_width);
    duckdb::BitpackingPrimitives::PackBuffer<uint16_t, false>(
      reinterpret_cast<duckdb::data_ptr_t>(_left_parts_encoded), left_parts,
      _vector_idx, left_bit_width);
    duckdb::BitpackingPrimitives::PackBuffer<uint64_t, false>(
      reinterpret_cast<duckdb::data_ptr_t>(_right_parts_encoded), right_parts,
      _vector_idx, right_bit_width);
  }

  void CompressVector() {
    if (_nulls_idx) {
      alp::FindAndReplaceNullsInVector<Exact>(
        _input_vector, _vector_null_positions, _vector_idx, _nulls_idx);
    }
    CompressVectorData();
    const idx_t uncompressed_size =
      kExceptionsCountSize + sizeof(Exact) * _vector_idx;
    const idx_t compressed_size = RequiredSpace();
    const bool should_compress = compressed_size < uncompressed_size;
    const idx_t vector_size =
      should_compress ? compressed_size : uncompressed_size;

    if (!HasEnoughSpace(vector_size)) {
      FlushSegment();
    }

    if (_nulls_idx) {
      _stats_writer.SetHasNull();
    }
    if (_vector_idx != _nulls_idx) {
      _stats_writer.SetHasValid();
      for (idx_t i = 0; i < _vector_idx; ++i) {
        _stats_writer.UpdateMinMax(std::bit_cast<T>(_input_vector[i]));
      }
    }
    _segment.count += _vector_idx;

    if (should_compress) {
      FlushCompressedVector();
    } else {
      FlushUncompressedVector();
    }
  }

  void FlushCompressedVector() {
    auto* data_ptr = _segment.Data() + UsedSpace();
    duckdb::Store<uint16_t>(_exceptions_count, data_ptr);
    data_ptr += kExceptionsCountSize;
    std::memcpy(data_ptr, _left_parts_encoded, _left_bit_packed_size);
    data_ptr += _left_bit_packed_size;
    std::memcpy(data_ptr, _right_parts_encoded, _right_bit_packed_size);
    data_ptr += _right_bit_packed_size;
    if (_exceptions_count) {
      std::memcpy(data_ptr, _exceptions, kExceptionSize * _exceptions_count);
      data_ptr += kExceptionSize * _exceptions_count;
      std::memcpy(data_ptr, _exceptions_positions,
                  kExceptionPositionSize * _exceptions_count);
    }

    _data_bytes_used += RequiredSpace();
    FlushVectorMetadata();
  }

  void FlushUncompressedVector() {
    auto* data_ptr = _segment.Data() + UsedSpace();
    duckdb::Store<uint16_t>(kUncompressedModeSentinel, data_ptr);
    data_ptr += kExceptionsCountSize;
    std::memcpy(data_ptr, _input_vector, sizeof(Exact) * _vector_idx);

    _data_bytes_used += kExceptionsCountSize + (sizeof(Exact) * _vector_idx);
    FlushVectorMetadata();
  }

  void FlushVectorMetadata() {
    _metadata_offset -= kMetadataPointerSize;
    duckdb::Store<uint32_t>(_next_vector_byte_index_start,
                            _segment.Data() + _metadata_offset);
    _next_vector_byte_index_start = duckdb::NumericCast<uint32_t>(UsedSpace());
    _vector_idx = 0;
    _nulls_idx = 0;
  }

  void FlushSegment() {
    auto* dataptr = _segment.Data();
    const idx_t metadata_offset = duckdb::AlignValue(UsedSpace());
    SDB_ASSERT(metadata_offset <= _metadata_offset);
    const idx_t bytes_used_by_metadata = kBlockSize - _metadata_offset;
    idx_t total_segment_size = kBlockSize;

    const auto used_space_percentage =
      static_cast<float>(metadata_offset + bytes_used_by_metadata) /
      static_cast<float>(total_segment_size);
    if (used_space_percentage < alp::kCompactBlockThreshold) {
      std::memmove(dataptr + metadata_offset, dataptr + _metadata_offset,
                   bytes_used_by_metadata);
      total_segment_size = metadata_offset + bytes_used_by_metadata;
    }
    std::memset(dataptr + UsedSpace(), 0,
                total_segment_size - bytes_used_by_metadata - UsedSpace());

    duckdb::Store<uint32_t>(duckdb::NumericCast<uint32_t>(total_segment_size),
                            dataptr);
    dataptr += kMetadataPointerSize;
    duckdb::Store<uint8_t>(_dictionary.right_bit_width, dataptr);
    dataptr += kRightBitWidthSize;
    duckdb::Store<uint8_t>(_dictionary.left_bit_width, dataptr);
    dataptr += kLeftBitWidthSize;
    duckdb::Store<uint8_t>(_dictionary.actual_dictionary_size, dataptr);
    dataptr += kNDictionaryElementsSize;
    std::memcpy(dataptr, _dictionary.left_parts_dict,
                _actual_dictionary_size_bytes);

    _stats_writer.Merge(_segment.stats);
    _segment.Flush(_checkpoint_data, total_segment_size);
    _stats_writer.Clear();

    _data_bytes_used = 0;
    _metadata_offset = kBlockSize;
    _next_vector_byte_index_start = kHeaderSize + _actual_dictionary_size_bytes;
  }

  ColumnDataCheckpointData& _checkpoint_data;
  CompressedSegment _segment;
  AlpRdDictionary _dictionary;
  uint32_t _actual_dictionary_size_bytes = 0;
  uint32_t _next_vector_byte_index_start = 0;
  duckdb::StatsWriter<T> _stats_writer;
  idx_t _vector_idx = 0;
  idx_t _nulls_idx = 0;
  idx_t _data_bytes_used = 0;
  idx_t _metadata_offset = kBlockSize;
  Exact _input_vector[kVectorSize] = {};
  uint16_t _vector_null_positions[kVectorSize] = {};
  uint16_t _exceptions_count = 0;
  idx_t _left_bit_packed_size = 0;
  idx_t _right_bit_packed_size = 0;
  uint32_t _right_parts_encoded[kVectorSize * 2] = {};
  uint16_t _left_parts_encoded[kVectorSize * 4] = {};
  uint16_t _exceptions[kVectorSize] = {};
  uint16_t _exceptions_positions[kVectorSize] = {};
};

template<typename T>
void AlpRdCompressChunks(ColumnDataCheckpointData& checkpoint_data,
                         std::span<const WriteChunk> chunks) {
  AlpRdAnalyzeState<T> analyze_state;
  for (const auto& chunk : chunks) {
    AlpRdSample(analyze_state, chunk.data);
  }
  FindBestDictionary<T>(analyze_state.rowgroup_sample,
                        analyze_state.dictionary);
  AlpRdCompressionState<T> state{checkpoint_data, analyze_state};
  for (const auto& chunk : chunks) {
    state.Append(chunk.data);
  }
  state.Finalize();
}

template<typename T>
struct AlpRdVectorState {
  using Exact = alp::ExactType<T>;

  idx_t index = 0;
  uint16_t left_encoded[kVectorSize];
  Exact right_encoded[kVectorSize];
  Exact decoded_values[kVectorSize];
  uint16_t exceptions[kVectorSize];
  uint16_t exceptions_positions[kVectorSize];
};

struct AlpRdHeader {
  uint8_t right_bit_width = 0;
  uint8_t left_bit_width = 0;
  uint16_t left_parts_dict[kMaxDictionarySize];
};

template<typename T>
alp::SegmentLayout ReadAlpRdSegmentLayout(duckdb::const_data_ptr_t base,
                                          idx_t size, idx_t count,
                                          AlpRdHeader& header) {
  constexpr auto kMaxRightBitWidth =
    static_cast<uint8_t>(sizeof(alp::ExactType<T>) * 8 - 1);
  const auto metadata_end = alp::MetadataEnd(base, size, kHeaderSize, "ALPRD");
  const auto* at = base + kMetadataPointerSize;

  header.right_bit_width = duckdb::Load<uint8_t>(at);
  if (header.right_bit_width > kMaxRightBitWidth) {
    ThrowAlpRdRightBitWidthOutOfRange(header.right_bit_width,
                                      kMaxRightBitWidth);
  }
  at += kRightBitWidthSize;

  header.left_bit_width = duckdb::Load<uint8_t>(at);
  if (header.left_bit_width > kMaxDictionaryBitWidth) {
    ThrowAlpRdLeftBitWidthOutOfRange(header.left_bit_width);
  }
  at += kLeftBitWidthSize;

  const auto dictionary_count = duckdb::Load<uint8_t>(at);
  if (dictionary_count > kMaxDictionarySize) {
    ThrowAlpRdDictionarySizeExceedsMaximum();
  }
  const uint32_t dictionary_size = dictionary_count * kDictionaryElementSize;
  const auto layout = alp::ReadLayout(base, kHeaderSize + dictionary_size,
                                      metadata_end, count, "ALPRD");
  std::memcpy(header.left_parts_dict, base + kHeaderSize, dictionary_size);
  std::memset(header.left_parts_dict + dictionary_count, 0,
              kMaxDictionarySizeBytes - dictionary_size);
  return layout;
}

struct AlpRdVector {
  duckdb::const_data_ptr_t values = nullptr;
  duckdb::const_data_ptr_t left = nullptr;
  duckdb::const_data_ptr_t right = nullptr;
  duckdb::const_data_ptr_t exceptions = nullptr;
  duckdb::const_data_ptr_t positions = nullptr;
  uint16_t exceptions_count = 0;
};

template<typename T>
IRS_FORCE_INLINE AlpRdVector OpenVector(const alp::SegmentLayout& layout,
                                        uint8_t left_bit_width,
                                        uint8_t right_bit_width,
                                        idx_t vector_index, idx_t vector_size) {
  const auto bytes =
    alp::VectorBytes(layout, vector_index, kExceptionsCountSize, "ALPRD");
  const auto* at = bytes.data() + kExceptionsCountSize;
  AlpRdVector vector{.exceptions_count = duckdb::Load<uint16_t>(bytes.data())};
  if (vector.exceptions_count == kUncompressedModeSentinel) {
    if (bytes.size() < kExceptionsCountSize +
                         vector_size * sizeof(alp::ExactType<T>)) [[unlikely]] {
      ThrowAlpRdVectorOutOfRange();
    }
    vector.values = at;
    return vector;
  }
  const idx_t left_size =
    duckdb::BitpackingPrimitives::GetRequiredSize(vector_size, left_bit_width);
  const idx_t right_size =
    duckdb::BitpackingPrimitives::GetRequiredSize(vector_size, right_bit_width);
  const idx_t exceptions_size = vector.exceptions_count * kExceptionSize;
  if (!(vector.exceptions_count <= vector_size &&
        kExceptionsCountSize + left_size + right_size + exceptions_size +
            vector.exceptions_count * kExceptionPositionSize <=
          bytes.size())) [[unlikely]] {
    ThrowAlpRdVector(vector.exceptions_count, vector_size);
  }
  vector.left = at;
  vector.right = at + left_size;
  vector.exceptions = vector.right + right_size;
  vector.positions = vector.exceptions + exceptions_size;
  return vector;
}

template<typename T>
struct AlpRdScanState final : SegmentScanState {
  using Value = alp::ExactType<T>;
  using Vector = AlpRdVector;
  using VectorState = AlpRdVectorState<T>;

  AlpRdScanState(duckdb::BufferHandle handle_p, const ColumnSegment& segment,
                 std::unique_ptr<VectorState> vector_state_p = nullptr)
    : handle{std::move(handle_p)},
      count{segment.count},
      layout{
        ReadAlpRdSegmentLayout<T>(handle.Ptr(), segment.size, count, header)},
      vector_state{std::move(vector_state_p)} {}

  duckdb::BufferHandle handle;
  idx_t count = 0;
  AlpRdHeader header;
  alp::SegmentLayout layout;
  idx_t total_value_count = 0;
  std::unique_ptr<VectorState> vector_state;
};

template<typename T>
AlpRdVector OpenVector(const AlpRdScanState<T>& state, idx_t vector_index) {
  return OpenVector<T>(
    state.layout, state.header.left_bit_width, state.header.right_bit_width,
    vector_index,
    std::min<idx_t>(kVectorSize, state.count - vector_index * kVectorSize));
}

template<typename T>
alp::ExactType<T>* ResultValues(const AlpRdScanState<T>&,
                                duckdb::Vector& result) {
  return duckdb::FlatVector::GetDataMutableUnsafe<alp::ExactType<T>>(result);
}

template<typename T>
void LoadVector(AlpRdScanState<T>& state, alp::ExactType<T>* value_buffer) {
  auto& vector_state = *state.vector_state;
  const auto& header = state.header;
  vector_state.index = 0;

  const idx_t vector_size =
    std::min<idx_t>(kVectorSize, state.count - state.total_value_count);
  const auto vector =
    OpenVector<T>(state.layout, header.left_bit_width, header.right_bit_width,
                  state.total_value_count / kVectorSize, vector_size);
  if (vector.exceptions_count == kUncompressedModeSentinel) {
    std::memcpy(value_buffer, vector.values, sizeof(T) * vector_size);
    return;
  }

  static_assert(kVectorSize * kMaxDictionaryBitWidth / 8 <=
                sizeof(vector_state.left_encoded));
  static_assert(sizeof(vector_state.right_encoded) == kVectorSize * sizeof(T));
  std::memcpy(vector_state.left_encoded, vector.left,
              vector.right - vector.left);
  std::memcpy(vector_state.right_encoded, vector.right,
              vector.exceptions - vector.right);
  std::memcpy(vector_state.exceptions, vector.exceptions,
              vector.exceptions_count * kExceptionSize);
  std::memcpy(vector_state.exceptions_positions, vector.positions,
              vector.exceptions_count * kExceptionPositionSize);
  for (idx_t i = 0; i < vector.exceptions_count; ++i) {
    if (vector_state.exceptions_positions[i] >= vector_size) {
      ThrowAlpRdExceptionPositionOutOfRange(
        vector_state.exceptions_positions[i], vector_size);
    }
  }

  AlpRdDecompress<T>(
    reinterpret_cast<duckdb::const_data_ptr_t>(vector_state.left_encoded),
    reinterpret_cast<duckdb::const_data_ptr_t>(vector_state.right_encoded),
    header.left_parts_dict, value_buffer, vector_size, vector.exceptions_count,
    vector_state.exceptions, vector_state.exceptions_positions,
    header.left_bit_width, header.right_bit_width);
}

template<typename T>
IRS_FORCE_INLINE alp::ExactType<T> ValueAt(const AlpRdScanState<T>& state,
                                           const AlpRdVector& vector,
                                           idx_t index) {
  using Exact = alp::ExactType<T>;
  const auto& header = state.header;
  if (vector.exceptions_count == kUncompressedModeSentinel) {
    return duckdb::Load<Exact>(vector.values + index * sizeof(Exact));
  }
  static_assert(kHeaderSize + kExceptionsCountSize >= alp::kWordReach);
  const auto right_value =
    alp::PackedValue<Exact>(vector.right, index, header.right_bit_width);
  if (vector.exceptions_count) {
    const auto position_at = [&](idx_t i) {
      return duckdb::Load<uint16_t>(vector.positions +
                                    i * kExceptionPositionSize);
    };
    const auto found =
      PartitionIndex(0, vector.exceptions_count,
                     [&](size_t i) { return position_at(i) < index; });
    if (found < vector.exceptions_count && position_at(found) == index) {
      const auto exception =
        duckdb::Load<uint16_t>(vector.exceptions + found * kExceptionSize);
      return (static_cast<Exact>(exception) << header.right_bit_width) |
             right_value;
    }
  }
  const auto code =
    alp::PackedValue<uint16_t>(vector.left, index, header.left_bit_width);
  const auto left_value = header.left_parts_dict[code];
  return (static_cast<Exact>(left_value) << header.right_bit_width) |
         right_value;
}

template<typename T>
void AlpRdFetchRows(const ColumnSegment& segment, ColumnFetchState& state,
                    const idx_t* rows, idx_t count, duckdb::Vector& result,
                    const duckdb::SelectionVector& result_sel,
                    idx_t result_offset) {
  SDB_ASSERT(!count || rows[count - 1] < segment.count);
  auto& fetch_state =
    state.GetOrCreateSegmentState<alp::FetchState<AlpRdScanState<T>>>([&] {
      return std::make_unique<alp::FetchState<AlpRdScanState<T>>>(segment.Pin(),
                                                                  segment);
    });
  auto* result_data =
    duckdb::FlatVector::GetDataMutableUnsafe<alp::ExactType<T>>(result) +
    result_offset;
  if (alp::CopyOneDecoded(fetch_state, rows, count, result_data, result_sel)) {
    return;
  }
  const idx_t last_vector = count ? rows[count - 1] / kVectorSize : 0;
  for (idx_t i = 0; i < count;) {
    if (rows[i] / kVectorSize == fetch_state.decoded_vector ||
        alp::Dense(fetch_state, rows, i, count, last_vector)) {
      i =
        alp::CopyDecoded(fetch_state, rows, i, count, result_data, result_sel);
      continue;
    }
    result_data[result_sel.get_index(i)] =
      alp::FetchValue(fetch_state, rows[i]);
    ++i;
  }
}

template<typename T>
constexpr DecompressionFunction kDecompression{
  .init_scan = alp::InitScan<AlpRdScanState<T>>,
  .scan = alp::Scan<AlpRdScanState<T>>,
  .fetch_rows = AlpRdFetchRows<T>,
  .skip = alp::Skip<AlpRdScanState<T>>,
};

template<typename T>
constexpr Codec kCodec{
  .type = CodecType::Alprd,
  .compression = &kStreamed<CodecType::Alprd, AlpRdAnalyzeChunks<T>,
                            AlpRdCompressChunks<T>, HasRows>,
  .decompression = &kDecompression<T>,
};

}  // namespace

const Codec* AlpRdCodec(duckdb::PhysicalType physical) {
  switch (physical) {
    case duckdb::PhysicalType::FLOAT:
      return &kCodec<float>;
    case duckdb::PhysicalType::DOUBLE:
      return &kCodec<double>;
    default:
      return nullptr;
  }
}

}  // namespace irs::codecs
