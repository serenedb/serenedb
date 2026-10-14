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
#include <span>
#include <vector>

#include "iresearch/utils/resource_manager.hpp"

namespace irs {

inline constexpr size_t kSortedMergeInputsMax = 256;

class MergeOrder {
 public:
  MergeOrder(size_t inputs, uint64_t rows, IResourceManager& memory);

  void Set(uint64_t row, size_t input) noexcept {
    _inputs[row] = static_cast<uint8_t>(input);
  }

  void Finish();

  uint64_t Rows() const noexcept { return _inputs.size(); }

  std::span<const uint8_t> Inputs() const noexcept { return _inputs; }

  void LiveRowsBefore(uint64_t row, std::span<uint64_t> live) const;

 private:
  static constexpr uint64_t kCheckpointRows = uint64_t{1} << 16;

  ManagedVector<uint8_t> _inputs;
  std::vector<uint64_t> _checkpoints;
  std::vector<uint64_t> _live;
};

}  // namespace irs
