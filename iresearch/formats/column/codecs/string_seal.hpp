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
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <span>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"
#include "iresearch/formats/column/list_ingest.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/utils/containers/flat_hash_set.hpp"
#include "iresearch/utils/default_init_allocator.hpp"

namespace irs::codecs {

struct SealOutcome {
  bool sealed = false;
  bool all_dedup = true;
};

class StringAccumulator {
 public:
  StringAccumulator() = default;
  StringAccumulator(const StringAccumulator&) = delete;
  StringAccumulator& operator=(const StringAccumulator&) = delete;

  void Reserve(uint64_t rows, uint64_t distinct);
  void Add(const duckdb::Vector& input);

  uint64_t Rows() const noexcept { return codes.size(); }

  uint64_t null_count = 0;
  std::vector<const duckdb::string_t*> values;
  ScratchVector<uint32_t> codes;

 private:
  using Values = std::vector<const duckdb::string_t*>;

  static constexpr size_t kFirstValues = 64;

  struct Hash {
    using is_transparent = void;

    size_t operator()(const duckdb::string_t& value) const noexcept {
      if (value.IsInlined()) {
        return absl::HashOf(std::bit_cast<std::array<uint64_t, 2>>(value));
      }
      return absl::HashOf(std::string_view{value.GetData(), value.GetSize()});
    }

    size_t operator()(uint32_t code) const noexcept {
      return (*this)(*(*values)[code - 1]);
    }

    const Values* values;
  };

  struct Eq {
    using is_transparent = void;

    bool operator()(uint32_t lhs, uint32_t rhs) const noexcept {
      return lhs == rhs;
    }

    bool operator()(uint32_t code,
                    const duckdb::string_t& value) const noexcept {
      return *(*values)[code - 1] == value;
    }

    bool operator()(const duckdb::string_t& value,
                    uint32_t code) const noexcept {
      return *(*values)[code - 1] == value;
    }

    const Values* values;
  };

  containers::FlatHashSet<uint32_t, Hash, Eq> _distinct{0, Hash{&values},
                                                        Eq{&values}};
  uint32_t _last_code = 0;
  const duckdb::string_t* _last = nullptr;
};

using SegmentSink =
  absl::FunctionRef<void(StringChoice choice, duckdb::BaseStatistics stats,
                         uint64_t rows, std::string_view bytes)>;

SealOutcome SealSegments(const StringAccumulator& acc,
                         const StringChoice* named,
                         const ColCodecParams& params,
                         const duckdb::LogicalType& type, StringTuning& tuning,
                         duckdb::BufferManager& buffers, SegmentSink sink);

SealOutcome SealSegments(std::span<const WriteChunk> chunks,
                         const StringChoice& named,
                         const ColCodecParams& params,
                         const duckdb::LogicalType& type, StringTuning& tuning,
                         duckdb::BufferManager& buffers, SegmentSink sink);

}  // namespace irs::codecs
