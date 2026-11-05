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
#include <duckdb/common/sorting/presorted_merge.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <limits>
#include <optional>
#include <span>
#include <vector>

#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/merge_order.hpp"
#include "iresearch/index/merge/merge_progress.hpp"
#include "iresearch/index/merge/sort_spec.hpp"

namespace irs {

class KeyOrder {
 public:
  static constexpr uint64_t kNotIsolated = std::numeric_limits<uint64_t>::max();

  KeyOrder(MergeDocMap& doc_map, const std::vector<SortKey>& spec,
           duckdb::ObjectCache* cache)
    : _doc_map{doc_map}, _spec{spec}, _cache{cache} {}

  KeyOrder(const KeyOrder&) = delete;
  KeyOrder& operator=(const KeyOrder&) = delete;

  [[nodiscard]] bool Prepare(size_t partitions, const MergeProgress& progress,
                             duckdb::TaskScheduler& scheduler, size_t threads);

  uint64_t IsolatedBegin(size_t input) const noexcept {
    return _isolated[input];
  }

  const MergeOrder& Order() const noexcept { return *_order; }

 private:
  struct Span {
    uint64_t first = std::numeric_limits<uint64_t>::max();
    uint64_t last = 0;
  };

  struct Part {
    uint64_t next = 0;
    uint64_t end = 0;
    std::vector<Span> spans;
  };

  const SubReader& Reader(size_t input) const {
    return *_doc_map[input].reader;
  }

  void PlanParts(const duckdb::PresortedMerge& merge);

  void ConcatOrder();

  bool Expand(const duckdb::PresortedMerge& merge, size_t partition,
              const MergeProgress& progress);

  void Place(const duckdb::PresortedMerge& merge, size_t partition,
             std::span<const duckdb::PresortedRun> runs);

  void Finish();

  MergeDocMap& _doc_map;
  const std::vector<SortKey>& _spec;
  duckdb::ObjectCache* _cache;
  std::vector<uint64_t> _isolated;
  std::optional<MergeOrder> _order;
  std::vector<Part> _parts;
};

}  // namespace irs
