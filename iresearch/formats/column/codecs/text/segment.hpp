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

#include <cstdint>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <string_view>

#include "iresearch/formats/column/codecs/text/segment_buffer.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/utils/tracked.hpp"

namespace irs::codecs::text {

struct Segment {
  Segment(LocalBudget& budget, const duckdb::LogicalType& type)
    : bytes{budget}, stats{duckdb::BaseStatistics::CreateEmpty(type)} {}

  Config config;
  uint64_t begin = 0;
  uint64_t rows = 0;
  uint64_t entries = 0;
  uint64_t nulls = 0;
  uint64_t input = 0;
  uint32_t max_len = 0;
  SegmentBuffer bytes;
  std::string_view shared;
  duckdb::BaseStatistics stats;

  uint64_t Size() const noexcept { return bytes.Size(); }
};

}  // namespace irs::codecs::text
