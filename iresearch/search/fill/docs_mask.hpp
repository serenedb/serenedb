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

#include "iresearch/index/document_mask.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

class DocsMask {
 public:
  DocsMask(const DocumentMask* mask, doc_id_t uncommitted) noexcept
    : _it{mask, doc_limits::eof()}, _uncommitted{uncommitted} {}

  explicit DocsMask(const SubReader& segment) noexcept
    : DocsMask{segment.docs_mask(), segment.Meta().uncommitted_begin} {}

  doc_id_t FillOr(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT words) {
    auto next = _it.Seek(min);
    while (next < max) {
      Set(words, next - min);
      next = _it.Next();
    }
    if (_uncommitted < max) {
      for (auto doc = std::max(min, _uncommitted); doc < max; ++doc) {
        Set(words, doc - min);
      }
      return max;
    }
    return std::min(next, _uncommitted);
  }

 private:
  static IRS_FORCE_INLINE void Set(uint64_t* IRS_RESTRICT words,
                                   doc_id_t offset) noexcept {
    words[offset / 64] |= uint64_t{1} << (offset % 64);
  }

  DocumentMask::Iterator _it;
  doc_id_t _uncommitted;
};

}  // namespace irs::fill
