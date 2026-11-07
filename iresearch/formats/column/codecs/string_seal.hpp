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
#include <absl/hash/hash.h>

#include <array>
#include <bit>
#include <cstdint>
#include <duckdb/common/string_map_set.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <span>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"
#include "iresearch/formats/column/list_ingest.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/default_init_allocator.hpp"

namespace irs::codecs {

struct SealOutcome {
  bool sealed = false;
  bool all_dedup = true;
};

class StringAccumulator {
 public:
  void Reserve(uint64_t rows, uint64_t distinct);
  void Add(const duckdb::Vector& input);

  uint64_t Rows() const noexcept { return codes.size(); }

  uint64_t null_count = 0;
  std::vector<duckdb::string_t> entries;
  ScratchVector<uint32_t> codes;

 private:
  struct Hash {
    size_t operator()(const duckdb::string_t& value) const noexcept {
      if (value.IsInlined()) {
        return absl::HashOf(std::bit_cast<std::array<uint64_t, 2>>(value));
      }
      return absl::HashOf(std::string_view{value.GetData(), value.GetSize()});
    }
  };

  containers::FlatHashMap<duckdb::string_t, uint32_t, Hash,
                          duckdb::StringEquality>
    _map;
  uint32_t _last_code = 0;
  duckdb::string_t _last;
};

using SegmentSink = absl::FunctionRef<void(
  StringChoice choice, duckdb::BaseStatistics stats, uint64_t rows,
  std::span<const std::string_view> parts)>;

SealOutcome SealSegments(const StringAccumulator& acc,
                         const StringChoice* named,
                         const ColCodecParams& params,
                         const duckdb::LogicalType& type, StringTuning& tuning,
                         SegmentSink sink);

SealOutcome SealSegments(std::span<const WriteChunk> chunks,
                         const StringChoice& named,
                         const ColCodecParams& params,
                         const duckdb::LogicalType& type, StringTuning& tuning,
                         SegmentSink sink);

}  // namespace irs::codecs
