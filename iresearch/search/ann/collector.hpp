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

#include <algorithm>
#include <cstdint>
#include <span>
#include <vector>

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/ann/shared_floor.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::ann {

class Collector {
 public:
  Collector(SharedFloor& floor, uint32_t k) : _floor{&floor}, _k{k} {
    SDB_ASSERT(k != 0);
  }

  uint32_t K() const noexcept { return _k; }

  score_t Floor() const noexcept { return _floor->Load(); }

  void Append(const doc_id_t* docs, const score_t* scores,
              const score_t* spreads, size_t n) {
    const auto floor = Floor();
    for (size_t i = 0; i < n; ++i) {
      const auto upper = scores[i] + spreads[i];
      if (upper < floor) {
        continue;
      }
      if (const auto lower = scores[i] - spreads[i]; lower > floor) {
        _lower.push_back(lower);
      }
      _hits.push_back({.score = upper, .doc = docs[i]});
    }
    if (_lower.size() >= _k) {
      Merge();
    }
    if (_hits.size() >= _prune_at) {
      Prune();
    }
  }

  std::span<ScoreDoc> Flush() {
    Merge();
    return _hits;
  }

  void Clear() noexcept {
    _hits.clear();
    _prune_at = kPruneAt;
  }

 private:
  static constexpr size_t kPruneAt = 1024;

  void Merge() {
    _floor->Merge(_lower);
    _lower.clear();
  }

  void Prune() {
    const auto floor = Floor();
    std::erase_if(_hits,
                  [floor](const ScoreDoc& hit) { return hit.score < floor; });
    _prune_at = std::max(kPruneAt, 2 * _hits.size());
  }

  SharedFloor* _floor;
  uint32_t _k;
  size_t _prune_at = kPruneAt;
  std::vector<score_t> _lower;
  std::vector<ScoreDoc> _hits;
};

}  // namespace irs::ann
