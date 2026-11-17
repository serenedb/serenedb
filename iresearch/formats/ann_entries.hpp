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

#include <absl/algorithm/container.h>

#include <cstdint>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"

namespace irs {

class AnnEntries {
 public:
  AnnEntries(const ColumnReader& list, ReadContext& ctx);

  uint32_t Rows() const noexcept {
    return static_cast<uint32_t>(_row_first.size() - 1);
  }
  uint32_t Count() const noexcept { return _row_first.back(); }
  uint32_t First(uint32_t row) const noexcept { return _row_first[row]; }
  uint32_t End(uint32_t row) const noexcept { return _row_first[row + 1]; }
  uint32_t Row(uint32_t entry) const noexcept {
    return static_cast<uint32_t>(absl::c_upper_bound(_row_first, entry) -
                                 _row_first.begin() - 1);
  }

 private:
  std::vector<uint32_t> _row_first;
};

}  // namespace irs
