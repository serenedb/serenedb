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

#include <absl/container/flat_hash_map.h>
#include <absl/functional/function_ref.h>

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/index/column_info.hpp"

namespace irs::codecs {

struct PricedChoice {
  StringChoice choice;
  uint64_t bytes;
  double ratio;
};

class StringAccumulator {
 public:
  explicit StringAccumulator(bool dedup) noexcept : _dedup{dedup} {}

  void Add(const duckdb::Vector& input);

  uint64_t row_count = 0;
  uint64_t null_count = 0;
  uint64_t raw_bytes = 0;
  uint64_t entry_bytes = 0;
  uint64_t runs = 0;
  uint32_t max_len = 0;
  absl::flat_hash_map<std::string_view, uint32_t> dedup;
  std::vector<std::string_view> entries;
  std::vector<uint32_t> codes;

 private:
  bool _dedup;
};

PricedChoice Price(const StringAccumulator& acc, StringChoice choice,
                   const ColCodecParams& params);

PricedChoice ChooseAuto(const StringAccumulator& acc,
                        const ColCodecParams& params);

using SegmentSink =
  absl::FunctionRef<void(duckdb::BaseStatistics stats, uint64_t rows,
                         std::span<const std::string_view> parts)>;

void SealSegments(const StringAccumulator& acc, const PricedChoice& priced,
                  const ColCodecParams& params, const duckdb::LogicalType& type,
                  SegmentSink sink);

}  // namespace irs::codecs
