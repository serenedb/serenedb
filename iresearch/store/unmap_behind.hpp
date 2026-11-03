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
#include <limits>

#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/file_utils_ext.hpp"

namespace irs {

class UnmapBehind {
 public:
  void Advance(const IndexInput& in, uint64_t at, uint64_t end) noexcept {
    if (at < _from) {
      _from = Behind(at);
      _end = end;
      return;
    }
    _end = std::max(_end, end);
    const auto page = at / file_utils::kPage * file_utils::kPage;
    if (page >= _from + kChunk) {
      in.Unmap(_from, page - _from);
      _from = Behind(page);
    }
  }

  void Finish(const IndexInput& in) const noexcept {
    if (_from < _end) {
      in.Unmap(_from, _end + kFaultAround - _from);
    }
  }

 private:
  static constexpr uint64_t kChunk = file_utils::kPrefetchChunk;
  static constexpr uint64_t kFaultAround = 16 * file_utils::kPage;

  static uint64_t Behind(uint64_t at) noexcept {
    return at > kFaultAround ? at - kFaultAround : 0;
  }

  uint64_t _from = std::numeric_limits<uint64_t>::max();
  uint64_t _end = 0;
};

}  // namespace irs
