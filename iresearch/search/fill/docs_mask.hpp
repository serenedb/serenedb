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
#include "iresearch/search/detail/window.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

class DocsMask {
 public:
  DocsMask(const DocumentMask* mask, doc_id_t uncommitted) noexcept
    : _it{mask, doc_limits::eof()},
      _words{mask != nullptr ? mask->Words() : nullptr},
      _word_count{mask != nullptr ? static_cast<uint32_t>(mask->WordCount())
                                  : 0},
      _uncommitted{uncommitted} {}

  explicit DocsMask(const SubReader& segment) noexcept
    : DocsMask{segment.docs_mask(), segment.Meta().uncommitted_begin} {}

  doc_id_t FillOr(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT words) {
    const auto base = static_cast<int64_t>(min - doc_limits::min());
    const auto len = static_cast<uint32_t>(max - min);
    const auto full = len / kBits;
    for (uint32_t w = 0; w != full; ++w) {
      words[w] |=
        detail::WordAt(_words, _word_count, base + int64_t{w} * kBits);
    }
    if (const auto rest = len % kBits; rest != 0) {
      words[full] |=
        detail::WordAt(_words, _word_count, base + int64_t{full} * kBits) &
        (~uint64_t{0} >> (kBits - rest));
    }
    if (_uncommitted < max) {
      for (auto doc = std::max(min, _uncommitted); doc < max; ++doc) {
        Set(words, doc - min);
      }
      return max;
    }
    return std::min(_it.Seek(max), _uncommitted);
  }

  uint32_t FillLive(doc_id_t min, uint32_t count, uint32_t* IRS_RESTRICT out) {
    SDB_ASSERT(count != 0);
    SDB_ASSERT(count <= detail::kWindowDocs);
    const auto used = (count + kBits - 1) / kBits;
    uint64_t dead[detail::kWindowWords];
    std::fill_n(dead, used, uint64_t{0});
    FillOr(min, min + count, dead);
    if (const auto rest = count % kBits; rest != 0) {
      dead[used - 1] |= ~uint64_t{0} << rest;
    }
    auto* const begin = out;
    for (uint32_t w = 0; w != used; ++w) {
      out = MaterializeWord(w * kBits, ~dead[w], out);
    }
    return static_cast<uint32_t>(out - begin);
  }

 private:
  static constexpr auto kBits = detail::kWindowBits;

  static IRS_FORCE_INLINE void Set(uint64_t* IRS_RESTRICT words,
                                   doc_id_t offset) noexcept {
    words[offset / kBits] |= uint64_t{1} << (offset % kBits);
  }

  DocumentMask::Iterator _it;
  const uint64_t* _words;
  uint32_t _word_count;
  doc_id_t _uncommitted;
};

}  // namespace irs::fill
