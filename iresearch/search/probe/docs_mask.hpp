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
  explicit DocsMask(const DocumentMask* mask) noexcept
    : _words{mask != nullptr ? mask->Words() : nullptr},
      _count{mask != nullptr ? static_cast<uint32_t>(mask->WordCount()) : 0} {}

  explicit DocsMask(const SubReader& segment) noexcept
    : DocsMask{segment.docs_mask()} {}

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) const noexcept {
    const auto word = target / kBits;
    if (word >= _count) [[unlikely]] {
      return doc_limits::eof();
    }
    const auto rest = _words[word] & (~uint64_t{0} << (target % kBits));
    return static_cast<doc_id_t>(word * kBits +
                                 static_cast<doc_id_t>(std::countr_zero(rest)));
  }

 private:
  static constexpr auto kBits = detail::kWindowBits;

  const uint64_t* _words;
  uint32_t _count;
};

}  // namespace irs::probe
