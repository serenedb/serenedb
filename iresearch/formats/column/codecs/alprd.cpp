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
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <duckdb/common/array_ptr.hpp>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <limits>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/alp_primitives.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/segment_reader.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs {
namespace {

using alp::kVectorSize;
using duckdb::idx_t;

constexpr uint8_t kMaxDictionaryBitWidth = 3;
constexpr uint8_t kMaxDictionarySize = 1 << kMaxDictionaryBitWidth;
constexpr idx_t kCuttingLimit = 16;
constexpr uint32_t kDictionaryElementSize = sizeof(uint16_t);
constexpr uint32_t kMaxDictionarySizeBytes =
  kMaxDictionarySize * kDictionaryElementSize;
constexpr uint32_t kExceptionSize = sizeof(uint16_t);
constexpr uint32_t kMetadataPointerSize = sizeof(uint32_t);
constexpr uint32_t kExceptionsCountSize = sizeof(uint16_t);
constexpr uint16_t kUncompressedModeSentinel =
  std::numeric_limits<uint16_t>::max();
constexpr uint32_t kExceptionPositionSize = sizeof(uint16_t);
constexpr uint32_t kRightBitWidthSize = sizeof(uint8_t);
constexpr uint32_t kLeftBitWidthSize = sizeof(uint8_t);
constexpr uint32_t kNDictionaryElementsSize = sizeof(uint8_t);
constexpr uint32_t kHeaderSize = kMetadataPointerSize + kRightBitWidthSize +
                                 kLeftBitWidthSize + kNDictionaryElementsSize;
constexpr idx_t kDecodeDistance = kVectorSize / 64;

[[noreturn]] IRS_NO_INLINE void ThrowAlpRdMetadataBeforeHeader() {
  ThrowCorrupted("ALPRD", "metadata ends before the segment header");
}

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

[[noreturn]] IRS_NO_INLINE void ThrowAlpRdMetadataTableOutOfBounds() {
  ThrowCorrupted("ALPRD", "metadata offset table exceeds the segment");
}

[[noreturn]] IRS_NO_INLINE void ThrowAlpRdVectorOffsetOutOfBounds() {
  ThrowCorrupted("ALPRD", "vector offset is outside the data region");
}

[[noreturn]] IRS_NO_INLINE void ThrowAlpRdVectorOffsetsInvalid() {
  ThrowCorrupted("ALPRD", "vector offsets do not describe a data range");
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

template<typename T, bool kPersistDict>
double BuildLeftPartsDictionary(std::span<const alp::ExactType<T>> values,
                                uint8_t right_bit_width,
                                AlpRdDictionary& dictionary) {
  containers::FlatHashMap<alp::ExactType<T>, int32_t> left_parts_hash;
  for (const auto value : values) {
    ++left_parts_hash[value >> right_bit_width];
  }

  std::vector<AlpRdLeftPartInfo> left_parts_sorted_repetitions;
  left_parts_sorted_repetitions.reserve(left_parts_hash.size());
  for (const auto& [left_part, count] : left_parts_hash) {
    left_parts_sorted_repetitions.emplace_back(static_cast<uint32_t>(count),
                                               left_part);
  }
  absl::c_sort(left_parts_sorted_repetitions, [](const AlpRdLeftPartInfo& a,
                                                 const AlpRdLeftPartInfo& b) {
    return a.count > b.count || (a.count == b.count && a.hash < b.hash);
  });

  uint32_t exceptions_count = 0;
  for (idx_t i = kMaxDictionarySize; i < left_parts_sorted_repetitions.size();
       ++i) {
    exceptions_count += left_parts_sorted_repetitions[i].count;
  }

  const uint64_t actual_dictionary_size = std::min<uint64_t>(
    kMaxDictionarySize, left_parts_sorted_repetitions.size());
  const uint8_t left_bit_width =
    std::max<uint8_t>(1, duckdb::ExactNumericCast<uint8_t>(
                           std::ceil(std::log2(actual_dictionary_size))));

  if constexpr (kPersistDict) {
    for (idx_t dict_idx = 0; dict_idx < actual_dictionary_size; ++dict_idx) {
      dictionary.left_parts_dict[dict_idx] =
        static_cast<uint16_t>(left_parts_sorted_repetitions[dict_idx].hash);
      dictionary.left_parts_dict_map.try_emplace(
        dictionary.left_parts_dict[dict_idx], static_cast<uint16_t>(dict_idx));
    }
    for (idx_t i = actual_dictionary_size + 1;
         i < left_parts_sorted_repetitions.size(); ++i) {
      dictionary.left_parts_dict_map.try_emplace(
        static_cast<uint16_t>(left_parts_sorted_repetitions[i].hash),
        static_cast<uint16_t>(i));
    }
    dictionary.left_bit_width = left_bit_width;
    dictionary.right_bit_width = right_bit_width;
    dictionary.actual_dictionary_size =
      static_cast<uint8_t>(actual_dictionary_size);
    SDB_ASSERT(dictionary.left_bit_width > 0 &&
               dictionary.right_bit_width > 0 &&
               dictionary.left_bit_width <= kMaxDictionaryBitWidth &&
               dictionary.actual_dictionary_size <= kMaxDictionarySize);
  }

  return EstimateCompressionSize(right_bit_width, left_bit_width,
                                 exceptions_count, values.size());
}

template<typename T>
double FindBestDictionary(std::span<const alp::ExactType<T>> values,
                          AlpRdDictionary& dictionary) {
  constexpr idx_t kExactTypeBitSize = sizeof(alp::ExactType<T>) * 8;
  uint8_t right_bit_width = 0;
  auto best_dict_size =
    static_cast<double>(std::numeric_limits<int32_t>::max());
  for (idx_t i = 1; i <= kCuttingLimit; ++i) {
    const auto candidate_right_bit_width =
      static_cast<uint8_t>(kExactTypeBitSize - i);
    const double estimated_size = BuildLeftPartsDictionary<T, false>(
      values, candidate_right_bit_width, dictionary);
    if (estimated_size <= best_dict_size) {
      right_bit_width = candidate_right_bit_width;
      best_dict_size = estimated_size;
    }
  }
  return BuildLeftPartsDictionary<T, true>(values, right_bit_width, dictionary);
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
  uint16_t left_decoded[kVectorSize] = {};
  Exact right_decoded[kVectorSize] = {};

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
struct AlpRdAnalyzeState final : AnalyzeState {
  idx_t vectors_count = 0;
  idx_t total_values_count = 0;
  idx_t vectors_sampled_count = 0;
  std::vector<alp::ExactType<T>> rowgroup_sample;
  AlpRdDictionary dictionary;
};

template<typename T>
std::unique_ptr<AnalyzeState> AlpRdInitAnalyze(CompressionAnalyzeContext&,
                                               duckdb::PhysicalType) {
  return std::make_unique<AlpRdAnalyzeState<T>>();
}

template<typename T>
bool AlpRdAnalyze(AnalyzeState& state, const duckdb::Vector& input) {
  using Exact = alp::ExactType<T>;
  auto& analyze_state = state.Cast<AlpRdAnalyzeState<T>>();

  const auto count = input.size();
  const bool must_skip_current_vector = alp::MustSkipSamplingFromCurrentVector(
    analyze_state.vectors_count, analyze_state.vectors_sampled_count, count);
  analyze_state.vectors_count += 1;
  analyze_state.total_values_count += count;
  if (must_skip_current_vector) {
    return true;
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
  return true;
}

template<typename T>
idx_t AlpRdFinalAnalyze(AnalyzeState& state) {
  auto& analyze_state = state.Cast<AlpRdAnalyzeState<T>>();
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
class AlpRdCompressionState final : public CompressionState {
 public:
  using Exact = alp::ExactType<T>;

  AlpRdCompressionState(ColumnDataCheckpointData& checkpoint_data,
                        AlpRdAnalyzeState<T>& analyze_state)
    : _checkpoint_data{checkpoint_data},
      _segment{checkpoint_data.type},
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
    uint64_t right_parts[kVectorSize];
    uint16_t left_parts[kVectorSize];
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
std::unique_ptr<CompressionState> AlpRdInitCompression(
  ColumnDataCheckpointData& checkpoint_data,
  std::unique_ptr<AnalyzeState> state) {
  SDB_ASSERT(state);
  return std::make_unique<AlpRdCompressionState<T>>(
    checkpoint_data, state->Cast<AlpRdAnalyzeState<T>>());
}

template<typename T>
void AlpRdCompress(CompressionState& state, const duckdb::Vector& input) {
  state.Cast<AlpRdCompressionState<T>>().Append(input);
}

template<typename T>
void AlpRdFinalizeCompress(CompressionState& state) {
  state.Cast<AlpRdCompressionState<T>>().Finalize();
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
  uint16_t exceptions_count = 0;
  uint8_t right_bit_width = 0;
  uint8_t left_bit_width = 0;
  uint16_t left_parts_dict[kMaxDictionarySize];
};

struct AlpRdSegmentLayout {
  SegmentReader data;
  duckdb::unsafe_array_ptr<const uint32_t> vector_offsets;
  idx_t data_start = 0;
};

template<typename T>
AlpRdSegmentLayout ReadAlpRdSegmentLayout(duckdb::const_data_ptr_t base,
                                          idx_t size, idx_t count,
                                          AlpRdVectorState<T>& vector_state) {
  constexpr auto kMaxRightBitWidth =
    static_cast<uint8_t>(sizeof(alp::ExactType<T>) * 8 - 1);
  SegmentReader reader{base, size, "ALPRD segment"};
  const auto metadata_end = reader.Read<uint32_t>();
  if (metadata_end < kHeaderSize) {
    ThrowAlpRdMetadataBeforeHeader();
  }
  reader = reader.GetSubReader(0, metadata_end, "ALPRD segment");
  reader.Skip(kMetadataPointerSize);

  vector_state.right_bit_width = reader.Read<uint8_t>();
  if (vector_state.right_bit_width > kMaxRightBitWidth) {
    ThrowAlpRdRightBitWidthOutOfRange(vector_state.right_bit_width,
                                      kMaxRightBitWidth);
  }

  vector_state.left_bit_width = reader.Read<uint8_t>();
  if (vector_state.left_bit_width > kMaxDictionaryBitWidth) {
    ThrowAlpRdLeftBitWidthOutOfRange(vector_state.left_bit_width);
  }

  const auto dictionary_count = reader.Read<uint8_t>();
  if (dictionary_count > kMaxDictionarySize) {
    ThrowAlpRdDictionarySizeExceedsMaximum();
  }
  reader.ReadIntoArray(vector_state.left_parts_dict, dictionary_count);
  std::memset(vector_state.left_parts_dict + dictionary_count, 0,
              (kMaxDictionarySize - dictionary_count) * kDictionaryElementSize);

  const idx_t data_start = reader.Position();
  const idx_t vector_count = count / kVectorSize + (count % kVectorSize != 0);
  if (vector_count > (metadata_end - data_start) / kMetadataPointerSize) {
    ThrowAlpRdMetadataTableOutOfBounds();
  }
  const idx_t metadata_size = vector_count * kMetadataPointerSize;
  const idx_t metadata_start = metadata_end - metadata_size;
  return {
    .data = reader.GetSubReader(data_start, metadata_start - data_start,
                                "ALPRD data"),
    .vector_offsets = reader.GetArray<uint32_t>(metadata_start, vector_count),
    .data_start = data_start,
  };
}

uint32_t GetVectorOffset(const AlpRdSegmentLayout& layout, idx_t vector_index) {
  SDB_ASSERT(vector_index < layout.vector_offsets.size());
  return layout.vector_offsets[layout.vector_offsets.size() - 1 - vector_index];
}

SegmentReader GetVectorReader(const AlpRdSegmentLayout& layout,
                              idx_t vector_index) {
  const auto vector_start = GetVectorOffset(layout, vector_index);
  if (vector_start < layout.data_start) {
    ThrowAlpRdVectorOffsetOutOfBounds();
  }
  const idx_t data_start = vector_start - layout.data_start;

  idx_t data_end = layout.data.Size();
  if (vector_index + 1 < layout.vector_offsets.size()) {
    const auto vector_end = GetVectorOffset(layout, vector_index + 1);
    if (vector_end < layout.data_start) {
      ThrowAlpRdVectorOffsetOutOfBounds();
    }
    data_end = vector_end - layout.data_start;
  }
  if (data_start > data_end || data_end > layout.data.Size()) {
    ThrowAlpRdVectorOffsetsInvalid();
  }
  return layout.data.GetSubReader(data_start, data_end - data_start,
                                  "ALPRD vector");
}

template<typename T>
struct AlpRdScanState final : SegmentScanState {
  AlpRdScanState(duckdb::BufferHandle handle_p, const ColumnSegment& segment)
    : handle{std::move(handle_p)},
      count{segment.count},
      layout{ReadAlpRdSegmentLayout(handle.Ptr(), segment.size, count,
                                    vector_state)} {}

  duckdb::BufferHandle handle;
  idx_t count = 0;
  AlpRdVectorState<T> vector_state;
  AlpRdSegmentLayout layout;
  idx_t total_value_count = 0;
};

template<typename T>
idx_t LeftInVector(const AlpRdScanState<T>& state) {
  return kVectorSize - (state.total_value_count % kVectorSize);
}

template<typename T>
bool VectorFinished(const AlpRdScanState<T>& state) {
  return !(state.total_value_count % kVectorSize);
}

template<typename T, bool kSkip>
void LoadVector(AlpRdScanState<T>& state, alp::ExactType<T>* value_buffer) {
  auto& vector_state = state.vector_state;
  vector_state.index = 0;

  const auto vector_index = state.total_value_count / kVectorSize;
  SegmentReader vector_reader = GetVectorReader(state.layout, vector_index);
  const idx_t vector_size =
    std::min<idx_t>(kVectorSize, state.count - state.total_value_count);

  vector_state.exceptions_count = vector_reader.Read<uint16_t>();
  if (vector_state.exceptions_count == kUncompressedModeSentinel) {
    const idx_t value_buffer_copy_size = sizeof(T) * vector_size;
    if constexpr (kSkip) {
      vector_reader.Skip(value_buffer_copy_size);
    } else {
      vector_reader.ReadBytesInto(
        reinterpret_cast<duckdb::data_ptr_t>(value_buffer),
        value_buffer_copy_size);
    }
    return;
  }

  if (vector_state.exceptions_count > vector_size) {
    ThrowAlpRdExceptionCountOutOfRange(vector_state.exceptions_count,
                                       vector_size);
  }

  const auto left_bp_size = duckdb::BitpackingPrimitives::GetRequiredSize(
    vector_size, vector_state.left_bit_width);
  const auto right_bp_size = duckdb::BitpackingPrimitives::GetRequiredSize(
    vector_size, vector_state.right_bit_width);
  vector_reader.ReadBytesIntoArray(vector_state.left_encoded, left_bp_size);
  vector_reader.ReadBytesIntoArray(vector_state.right_encoded, right_bp_size);

  if (vector_state.exceptions_count) {
    vector_reader.ReadIntoArray(vector_state.exceptions,
                                vector_state.exceptions_count);
    vector_reader.ReadIntoArray(vector_state.exceptions_positions,
                                vector_state.exceptions_count);
    for (idx_t i = 0; i < vector_state.exceptions_count; ++i) {
      if (vector_state.exceptions_positions[i] >= vector_size) {
        ThrowAlpRdExceptionPositionOutOfRange(
          vector_state.exceptions_positions[i], vector_size);
      }
    }
  }

  if constexpr (!kSkip) {
    AlpRdDecompress<T>(
      reinterpret_cast<duckdb::const_data_ptr_t>(vector_state.left_encoded),
      reinterpret_cast<duckdb::const_data_ptr_t>(vector_state.right_encoded),
      vector_state.left_parts_dict, value_buffer, vector_size,
      vector_state.exceptions_count, vector_state.exceptions,
      vector_state.exceptions_positions, vector_state.left_bit_width,
      vector_state.right_bit_width);
  }
}

template<typename T, bool kSkip>
void ScanVector(AlpRdScanState<T>& state, alp::ExactType<T>* values,
                idx_t vector_size) {
  SDB_ASSERT(vector_size <= kVectorSize);
  SDB_ASSERT(vector_size <= LeftInVector(state));
  if (VectorFinished(state) && state.total_value_count < state.count) {
    if (vector_size == kVectorSize) {
      LoadVector<T, kSkip>(state, values);
      state.total_value_count += vector_size;
      return;
    }
    LoadVector<T, false>(state, state.vector_state.decoded_values);
  }
  if constexpr (!kSkip) {
    std::memcpy(values,
                state.vector_state.decoded_values + state.vector_state.index,
                sizeof(T) * vector_size);
  }
  state.vector_state.index += vector_size;
  state.total_value_count += vector_size;
}

template<typename T>
void SkipVector(AlpRdScanState<T>& state) {
  SDB_ASSERT(state.total_value_count < state.count);
  state.total_value_count +=
    std::min<idx_t>(kVectorSize, state.count - state.total_value_count);
}

template<typename T>
void SkipValues(AlpRdScanState<T>& state, idx_t skip_count) {
  SDB_ASSERT(state.total_value_count <= state.count);
  SDB_ASSERT(skip_count <= state.count - state.total_value_count);
  if (state.total_value_count && !VectorFinished(state)) {
    const idx_t to_skip = std::min<idx_t>(skip_count, LeftInVector(state));
    ScanVector<T, true>(state, nullptr, to_skip);
    skip_count -= to_skip;
  }
  const idx_t vectors_to_skip = skip_count / kVectorSize;
  for (idx_t i = 0; i < vectors_to_skip; ++i) {
    SkipVector(state);
  }
  skip_count -= kVectorSize * vectors_to_skip;
  if (!skip_count) {
    return;
  }
  ScanVector<T, true>(state, nullptr, skip_count);
}

template<typename T>
std::unique_ptr<SegmentScanState> AlpRdInitScan(const ColumnSegment& segment) {
  return std::make_unique<AlpRdScanState<T>>(segment.Pin(), segment);
}

template<typename T>
void AlpRdScanPartial(const ColumnSegment&, ColumnScanState& state,
                      idx_t scan_count, duckdb::Vector& result,
                      idx_t result_offset) {
  auto& scan_state = state.scan_state->Cast<AlpRdScanState<T>>();
  SDB_ASSERT(scan_state.total_value_count <= scan_state.count);
  SDB_ASSERT(scan_count <= scan_state.count - scan_state.total_value_count);

  auto* current_result_ptr =
    duckdb::FlatVector::GetDataMutableUnsafe<alp::ExactType<T>>(result);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  current_result_ptr += result_offset;

  idx_t scanned = 0;
  while (scanned < scan_count) {
    const auto remaining = scan_count - scanned;
    const idx_t to_scan = std::min(remaining, LeftInVector(scan_state));
    ScanVector<T, false>(scan_state, current_result_ptr + scanned, to_scan);
    scanned += to_scan;
  }
}

template<typename T>
void AlpRdScan(const ColumnSegment& segment, ColumnScanState& state,
               idx_t scan_count, duckdb::Vector& result) {
  AlpRdScanPartial<T>(segment, state, scan_count, result, 0);
}

template<typename T>
void AlpRdSkip(const ColumnSegment&, ColumnScanState& state, idx_t skip_count) {
  SkipValues(state.scan_state->Cast<AlpRdScanState<T>>(), skip_count);
}

template<typename T>
struct AlpRdFetchState final : SegmentScanState {
  AlpRdFetchState(duckdb::BufferHandle handle, const ColumnSegment& segment)
    : scan_state{std::move(handle), segment} {}

  AlpRdScanState<T> scan_state;
  idx_t decoded_vector = duckdb::DConstants::INVALID_INDEX;
  idx_t previous_row = duckdb::DConstants::INVALID_INDEX;
};

template<typename T>
IRS_NO_INLINE alp::ExactType<T> FetchValue(const AlpRdScanState<T>& scan_state,
                                           idx_t row) {
  using Exact = alp::ExactType<T>;
  SDB_ASSERT(row < scan_state.count);
  const idx_t vector_index = row / kVectorSize;
  const idx_t index = row % kVectorSize;
  SegmentReader reader = GetVectorReader(scan_state.layout, vector_index);
  const idx_t vector_size =
    std::min<idx_t>(kVectorSize, scan_state.count - vector_index * kVectorSize);
  const auto exceptions_count = reader.Read<uint16_t>();
  if (exceptions_count == kUncompressedModeSentinel) {
    return reader.Get<Exact>(reader.Position() + index * sizeof(Exact));
  }
  if (exceptions_count > vector_size) {
    ThrowAlpRdExceptionCountOutOfRange(exceptions_count, vector_size);
  }
  const auto& vector_state = scan_state.vector_state;
  const auto left =
    reader.ReadBytes(duckdb::BitpackingPrimitives::GetRequiredSize(
      vector_size, vector_state.left_bit_width));
  const auto right =
    reader.ReadBytes(duckdb::BitpackingPrimitives::GetRequiredSize(
      vector_size, vector_state.right_bit_width));
  const auto right_value = duckdb::BitpackingPrimitives::UnPackValue<Exact>(
    right.data(), index, vector_state.right_bit_width);
  if (exceptions_count) {
    const auto exceptions = reader.ReadBytes(exceptions_count * kExceptionSize);
    const auto positions =
      reader.ReadBytes(exceptions_count * kExceptionPositionSize);
    idx_t found = exceptions_count;
    for (idx_t i = 0; i < exceptions_count; ++i) {
      const auto position =
        duckdb::Load<uint16_t>(positions.data() + i * kExceptionPositionSize);
      if (position >= vector_size) {
        ThrowAlpRdExceptionPositionOutOfRange(position, vector_size);
      }
      if (position == index) {
        found = i;
      }
    }
    if (found < exceptions_count) {
      const auto exception =
        duckdb::Load<uint16_t>(exceptions.data() + found * kExceptionSize);
      return (static_cast<Exact>(exception) << vector_state.right_bit_width) |
             right_value;
    }
  }
  const auto code = duckdb::BitpackingPrimitives::UnPackValue<uint16_t>(
    left.data(), index, vector_state.left_bit_width);
  const auto left_value = vector_state.left_parts_dict[code];
  return (static_cast<Exact>(left_value) << vector_state.right_bit_width) |
         right_value;
}

bool Near(idx_t row, idx_t other) {
  return other != duckdb::DConstants::INVALID_INDEX &&
         row / kVectorSize == other / kVectorSize &&
         (row > other ? row - other : other - row) <= kDecodeDistance;
}

template<typename T>
bool Alone(AlpRdFetchState<T>& fetch_state, const idx_t* rows, idx_t i,
           idx_t count) {
  const auto previous = fetch_state.previous_row;
  fetch_state.previous_row = rows[i];
  return !Near(rows[i], previous) &&
         (i + 1 == count || !Near(rows[i], rows[i + 1]));
}

template<typename T>
void AlpRdFetchRows(const ColumnSegment& segment, ColumnFetchState& state,
                    const idx_t* rows, idx_t count, duckdb::Vector& result,
                    const duckdb::SelectionVector& result_sel,
                    idx_t result_offset) {
  SDB_ASSERT(!count || rows[count - 1] < segment.count);
  auto& fetch_state = state.GetOrCreateSegmentState<AlpRdFetchState<T>>([&] {
    return std::make_unique<AlpRdFetchState<T>>(segment.Pin(), segment);
  });
  auto& scan_state = fetch_state.scan_state;
  auto* result_data =
    duckdb::FlatVector::GetDataMutableUnsafe<alp::ExactType<T>>(result);
  for (idx_t i = 0; i < count; ++i) {
    const auto vector_index = rows[i] / kVectorSize;
    if (vector_index != fetch_state.decoded_vector) {
      if (Alone(fetch_state, rows, i, count)) {
        result_data[result_offset + result_sel.get_index(i)] =
          FetchValue(scan_state, rows[i]);
        continue;
      }
      scan_state.total_value_count = vector_index * kVectorSize;
      LoadVector<T, false>(scan_state, scan_state.vector_state.decoded_values);
      fetch_state.decoded_vector = vector_index;
    }
    result_data[result_offset + result_sel.get_index(i)] =
      scan_state.vector_state.decoded_values[rows[i] % kVectorSize];
  }
}

template<typename T, duckdb::PhysicalType kPhysical>
constexpr CompressionFunction kFunction{
  .type = duckdb::CompressionType::COMPRESSION_ALPRD,
  .init_analyze = AlpRdInitAnalyze<T>,
  .analyze = AlpRdAnalyze<T>,
  .final_analyze = AlpRdFinalAnalyze<T>,
  .init_compression = AlpRdInitCompression<T>,
  .compress = AlpRdCompress<T>,
  .compress_finalize = AlpRdFinalizeCompress<T>,
  .init_scan = AlpRdInitScan<T>,
  .scan_vector = AlpRdScan<T>,
  .scan_partial = AlpRdScanPartial<T>,
  .fetch_rows = AlpRdFetchRows<T>,
  .skip = AlpRdSkip<T>,
};

}  // namespace

const CompressionFunction* AlpRdFunction(duckdb::PhysicalType physical) {
  switch (physical) {
    case duckdb::PhysicalType::FLOAT:
      return &kFunction<float, duckdb::PhysicalType::FLOAT>;
    case duckdb::PhysicalType::DOUBLE:
      return &kFunction<double, duckdb::PhysicalType::DOUBLE>;
    default:
      return nullptr;
  }
}

}  // namespace irs::codecs
