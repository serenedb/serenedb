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

#include "search/row_batches.h"

#include <absl/algorithm/container.h>

#include <iterator>

namespace sdb::search {

void RowBatches::Add(std::vector<RowBatch>&& batches, uint64_t generation) {
  for (auto& batch : batches) {
    batch.generation = generation;
    _order.emplace_back(generation, batch.begin);
    _batches.insert_or_assign(batch.begin, std::move(batch));
  }
}

void RowBatches::Find(std::span<const int64_t> rows,
                      irs::DocRemovalBuilder& found,
                      std::vector<int64_t>& missing) const {
  for (const auto row : rows) {
    if (const auto next = _batches.upper_bound(row); next != _batches.begin()) {
      const auto& batch = std::prev(next)->second;
      const auto offset = static_cast<uint32_t>(row - batch.begin);
      if (row < batch.end && batch.offsets.empty()) {
        found.Add(batch.segment, batch.doc + offset);
        continue;
      }
      if (row < batch.end) {
        const auto it = absl::c_lower_bound(batch.offsets, offset);
        if (it != batch.offsets.end() && *it == offset) {
          found.Add(batch.segment, static_cast<irs::doc_id_t>(
                                     batch.doc + (it - batch.offsets.begin())));
          continue;
        }
      }
    }
    missing.push_back(row);
  }
}

void RowBatches::Prune(uint64_t committed) {
  while (!_order.empty() && _order.front().first <= committed) {
    const auto it = _batches.find(_order.front().second);
    if (it != _batches.end() && it->second.generation <= committed) {
      _batches.erase(it);
    }
    _order.pop_front();
  }
}

}  // namespace sdb::search
