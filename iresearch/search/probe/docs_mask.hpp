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
#include <bit>

#include "iresearch/index/document_mask.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

class DocsMask {
 public:
  static constexpr doc_id_t kBits = 64;
  static constexpr doc_id_t kMin = doc_limits::min();

  DocsMask(const DocumentMask* mask, doc_id_t uncommitted) noexcept
    : _words{mask != nullptr ? mask->Words() : nullptr},
      _count{mask != nullptr ? static_cast<uint32_t>(mask->WordCount()) : 0},
      _uncommitted{uncommitted} {}

  explicit DocsMask(const SubReader& segment) noexcept
    : DocsMask{segment.docs_mask(), segment.Meta().uncommitted_begin} {}

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) const noexcept {
    if (target >= _uncommitted) [[unlikely]] {
      return target;
    }
    const auto offset = target - kMin;
    const auto word = offset / kBits;
    if (word >= _count) [[unlikely]] {
      return _uncommitted;
    }
    const auto rest = _words[word] & (~uint64_t{0} << (offset % kBits));
    const auto found = static_cast<doc_id_t>(
      kMin + word * kBits + static_cast<doc_id_t>(std::countr_zero(rest)));
    return std::min(found, _uncommitted);
  }

  IRS_FORCE_INLINE bool Test(doc_id_t doc) const noexcept {
    if (doc >= _uncommitted) [[unlikely]] {
      return true;
    }
    const auto offset = doc - kMin;
    const auto word = offset / kBits;
    if (word >= _count) [[unlikely]] {
      return false;
    }
    return ((_words[word] >> (offset % kBits)) & 1) != 0;
  }

 private:
  const uint64_t* _words;
  uint32_t _count;
  doc_id_t _uncommitted;
};

}  // namespace irs::probe
