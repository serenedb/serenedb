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
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "iresearch/analysis/text/dict/string_table.hpp"
#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"
#include "iresearch/formats/column/list_ingest.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/utils/default_init_allocator.hpp"

namespace irs::codecs {

struct SealOutcome {
  bool sealed = false;
  bool all_dedup = true;
};

class StringAccumulator {
 public:
  class Cursor {
   public:
    static constexpr bool kCodes = true;

    Cursor(const StringAccumulator& acc, uint64_t row) noexcept
      : _codes{acc.codes.data() + row}, _entries{acc.entries.data()} {}

    uint32_t Code() noexcept { return *_codes++; }

    const duckdb::string_t& Entry(uint32_t code) const noexcept {
      return _entries[code - 1];
    }

    const duckdb::string_t* Value() noexcept {
      const auto code = Code();
      return code == 0 ? nullptr : &Entry(code);
    }

   private:
    const uint32_t* _codes;
    const duckdb::string_t* _entries;
  };

  void Reserve(uint64_t rows, const analysis::dict::ClassSizes& distinct);
  void Add(const duckdb::Vector& input);

  uint64_t Rows() const noexcept { return codes.size(); }

  analysis::dict::ClassSizes Distinct() const noexcept { return _map.Sizes(); }

  Cursor At(uint64_t row) const noexcept { return {*this, row}; }

  uint64_t null_count = 0;
  std::vector<duckdb::string_t> entries;
  ScratchVector<uint32_t> codes;

 private:
  analysis::dict::StringMap<std::string_view, uint32_t> _map;
  uint32_t _last_code = 0;
  duckdb::string_t _last;
};

class ChunkCursor {
 public:
  static constexpr bool kCodes = false;

  explicit ChunkCursor(std::span<const WriteChunk> chunks) noexcept
    : _chunks{chunks} {}

  const duckdb::string_t* Value() {
    while (_index == _count) {
      Enter();
    }
    const auto entry = (*_values)[_index++];
    return entry.IsValid() ? &entry.GetValueUnsafe() : nullptr;
  }

 private:
  void Enter();

  std::span<const WriteChunk> _chunks;
  size_t _next_chunk = 0;
  std::optional<duckdb::VectorIterator<duckdb::string_t>> _values;
  uint64_t _count = 0;
  uint64_t _index = 0;
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
