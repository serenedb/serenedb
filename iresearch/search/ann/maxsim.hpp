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
#include <deque>
#include <span>
#include <vector>

#include "iresearch/formats/ann_entries.hpp"
#include "iresearch/search/ann/collector.hpp"
#include "iresearch/search/ann/shared_floor.hpp"
#include "iresearch/search/detail/ann_index.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs::ann {

struct MaxSimBound {
  doc_id_t doc;
  score_t upper;
};

class MaxSimCandidates {
 public:
  void Search(const AnnIndex& ann, const AnnEntries& entries,
              const SubReader& segment, const PrepareContext& ctx,
              const VectorFilterOptions& opts, uint32_t effort,
              std::span<const float> vectors,
              std::span<const uint32_t> group_ends);

  std::span<const MaxSimBound> Group(size_t group) const noexcept {
    return std::span<const MaxSimBound>{_bounds}.subspan(
      _group_first[group], _group_first[group + 1] - _group_first[group]);
  }

 private:
  std::deque<SharedFloor> _floors;
  std::vector<Collector> _collectors;
  struct RowScore {
    uint32_t row;
    score_t score;
  };

  std::vector<RowScore> _rows;
  irs::containers::FlatHashMap<uint32_t, score_t> _excess;
  std::vector<MaxSimBound> _bounds;
  std::vector<size_t> _group_first;
};

}  // namespace irs::ann
