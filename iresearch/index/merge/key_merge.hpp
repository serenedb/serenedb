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
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "iresearch/index/merge/doc_map.hpp"
#include "iresearch/index/merge/merge_order.hpp"
#include "iresearch/index/merge/sort_spec.hpp"

namespace irs {

class KeyOrder {
 public:
  KeyOrder(MergeDocMap& doc_map, const SortSpec& spec);
  ~KeyOrder();

  KeyOrder(const KeyOrder&) = delete;
  KeyOrder& operator=(const KeyOrder&) = delete;

  bool Prepare(size_t partitions);

  bool Pending() const noexcept;

  duckdb::shared_ptr<duckdb::PresortedMerge> KeyMerge() const noexcept;

  void Expand(size_t partition, std::span<const duckdb::PresortedRun> runs);

  bool Merge(const std::function<bool()>& proceed);

  bool Finish();

  std::optional<uint64_t> IsolatedBegin(size_t input) const noexcept;

  const MergeOrder& Order() const noexcept;

 private:
  struct State;
  std::unique_ptr<State> _state;
};

}  // namespace irs
