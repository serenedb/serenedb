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

#include "iresearch/index/merge/merge_order.hpp"

#include <algorithm>

#include "iresearch/utils/assert.hpp"

namespace irs {

MergeOrder::MergeOrder(size_t inputs, uint64_t rows, IResourceManager& memory)
  : _inputs{{memory}}, _live(inputs, 0) {
  SDB_ASSERT(inputs <= kSortedMergeInputsMax);
  _inputs.resize(rows);
}

void MergeOrder::Finish() {
  std::ranges::fill(_live, 0);
  _checkpoints.assign(_live.size(), 0);
  for (uint64_t row = 0; row != Rows(); ++row) {
    ++_live[_inputs[row]];
    if ((row + 1) % kCheckpointRows == 0) {
      _checkpoints.insert(_checkpoints.end(), _live.begin(), _live.end());
    }
  }
}

void MergeOrder::LiveRowsBefore(uint64_t row, std::span<uint64_t> live) const {
  SDB_ASSERT(row <= Rows());
  SDB_ASSERT(live.size() == _live.size());
  const auto checkpoint = row / kCheckpointRows;
  std::copy_n(_checkpoints.begin() + checkpoint * _live.size(), _live.size(),
              live.begin());
  for (auto r = checkpoint * kCheckpointRows; r != row; ++r) {
    ++live[_inputs[r]];
  }
}

}  // namespace irs
