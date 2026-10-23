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

#include <absl/container/btree_map.h>

#include <cstdint>
#include <deque>
#include <iresearch/index/doc_removal.hpp>
#include <span>
#include <utility>
#include <vector>

namespace sdb::search {

struct RowBatch {
  int64_t begin = 0;
  int64_t end = 0;
  uint32_t segment = 0;
  irs::doc_id_t doc = 0;
  std::vector<uint32_t> offsets;
  uint64_t generation = 0;
};

class RowBatches {
 public:
  void Add(std::vector<RowBatch>&& batches, uint64_t generation);
  void Find(std::span<const int64_t> rows, irs::DocRemovalBuilder& found,
            std::vector<int64_t>& missing) const;
  void Prune(uint64_t committed);

 private:
  absl::btree_map<int64_t, RowBatch> _batches;
  std::deque<std::pair<uint64_t, int64_t>> _order;
};

}  // namespace sdb::search
