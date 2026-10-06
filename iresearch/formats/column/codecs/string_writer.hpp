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

#include <absl/functional/function_ref.h>

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/string_choice.hpp"
#include "iresearch/formats/column/codecs/trained_dictionary.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs::codecs {

struct RatioHistory {
  uint64_t raw = 0;
  uint64_t comp = 0;
};

enum class FrameLayout : uint8_t {
  Dictionary = 0,
  Wide = 1,
  FirstFrame = 2,
};

struct StringTuning {
  std::optional<StringChoice> choice;
  double bytes_per_input = 0;
  bool levels_tuned = false;
  uint32_t calibration_gap = 1;
  uint32_t since_calibration = 0;
  uint64_t last_distinct = 0;
  uint8_t level[kByteCodecCount]{};
  FrameLayout layout[kByteCodecCount]{};
  RatioHistory history[kByteCodecCount][2]{};
  DictionarySampler sampler;
  bool sampling_done = false;
  std::shared_ptr<const TrainedDictionary> dictionary;
  uint16_t dictionary_id = 0;
};

struct SealOutcome {
  bool sealed = false;
  bool all_dedup = true;
};

class StringAccumulator {
 public:
  explicit StringAccumulator(bool dedup) noexcept : _dedup{dedup} {}

  void Reserve(uint64_t rows, uint64_t distinct);
  void Add(const duckdb::Vector& input);

  uint64_t row_count = 0;
  uint64_t null_count = 0;
  std::vector<std::string_view> entries;
  std::vector<uint32_t> codes;

 private:
  containers::FlatHashMap<std::string_view, uint32_t> _map;
  bool _dedup;
  uint32_t _last_code = 0;
  duckdb::string_t _last;
};

using SegmentSink = absl::FunctionRef<void(
  StringChoice choice, duckdb::BaseStatistics stats, uint64_t rows,
  std::span<const std::string_view> parts)>;

using DictionarySink = absl::FunctionRef<uint16_t(std::string_view bytes)>;

bool TrainsDictionary(std::optional<StringChoice> named,
                      const ColCodecParams& params) noexcept;

SealOutcome SealSegments(const StringAccumulator& acc,
                         std::optional<StringChoice> named,
                         const ColCodecParams& params,
                         const duckdb::LogicalType& type, StringTuning& tuning,
                         SegmentSink sink, DictionarySink dictionaries);

}  // namespace irs::codecs
