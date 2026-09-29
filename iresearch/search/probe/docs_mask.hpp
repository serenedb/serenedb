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
#include "iresearch/search/detail/window.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::probe {

class DocsMask {
 public:
  DocsMask(const DocumentMask* mask, doc_id_t visible_end) noexcept
    : _words{mask != nullptr ? mask->Words() : nullptr},
      _count{mask != nullptr ? static_cast<uint32_t>(mask->WordCount()) : 0},
      _visible_end{visible_end} {}

  explicit DocsMask(const SubReader& segment) noexcept
    : DocsMask{segment.docs_mask(), segment.Meta().visible_end} {}

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) const noexcept {
    if (target >= _visible_end) [[unlikely]] {
      return target;
    }
    const auto word = target / kBits;
    if (word >= _count) [[unlikely]] {
      return _visible_end;
    }
    const auto rest = _words[word] & (~uint64_t{0} << (target % kBits));
    const auto found = static_cast<doc_id_t>(
      word * kBits + static_cast<doc_id_t>(std::countr_zero(rest)));
    if (const auto end = uint64_t{word + 1} * kBits; end <= _visible_end)
      [[likely]] {
      return found;
    }
    return std::min(found, _visible_end);
  }

 private:
  static constexpr auto kBits = detail::kWindowBits;

  const uint64_t* _words;
  uint32_t _count;
  doc_id_t _visible_end;
};

}  // namespace irs::probe
