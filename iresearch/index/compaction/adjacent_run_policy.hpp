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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <optional>

#include "iresearch/index/index_writer.hpp"

namespace irs {

struct SegmentRange {
  int64_t first = 0;
  int64_t last = 0;
};

using SegmentRangeOf =
  std::function<std::optional<SegmentRange>(const SubReader& segment)>;

struct AdjacentRunOptions {
  size_t max_segments = 10;
  uint64_t max_bytes = uint64_t{5} << 30;
  uint64_t max_docs = std::numeric_limits<uint64_t>::max();
  uint64_t segment_cost = uint64_t{2} << 20;
  double base = 6;
  double min_base = 2;
  size_t relax_from = 10;
  size_t relax_to = 50;
  double max_deleted_share = 0.25;
};

CompactionPolicy MakeAdjacentRunPolicy(AdjacentRunOptions options,
                                       SegmentRangeOf range_of);

}  // namespace irs
