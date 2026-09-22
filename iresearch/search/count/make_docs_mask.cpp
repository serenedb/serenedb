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

#include <algorithm>
#include <bit>

#include "iresearch/index/document_mask.hpp"
#include "iresearch/search/count/make.hpp"
#include "iresearch/search/detail/window.hpp"

namespace irs::count {
namespace {

class MaskCount : public Root {
 public:
  MaskCount(const DocumentMask* mask, doc_id_t uncommitted,
            doc_id_t docs_count) noexcept
    : _words{mask != nullptr ? mask->Words() : nullptr},
      _word_count{mask != nullptr ? static_cast<uint32_t>(mask->WordCount())
                                  : 0},
      _uncommitted{uncommitted},
      _end{doc_limits::min() + docs_count} {}

  uint64_t Run(doc_id_t min, doc_id_t max) final {
    const auto stop = std::min(max, _end);
    if (stop <= min) {
      return 0;
    }
    const auto committed = std::min(stop, _uncommitted);
    uint64_t count = committed > min ? Popcount(min, committed) : 0;
    if (const auto from = std::max(min, _uncommitted); stop > from) {
      count += stop - from;
    }
    return count;
  }

 private:
  uint64_t Popcount(doc_id_t min, doc_id_t max) const noexcept {
    const auto base = static_cast<int64_t>(min - doc_limits::min());
    const auto len = static_cast<uint32_t>(max - min);
    const auto full = len / detail::kWindowBits;
    uint64_t count = 0;
    for (uint32_t w = 0; w != full; ++w) {
      count += static_cast<uint64_t>(std::popcount(detail::WordAt(
        _words, _word_count, base + int64_t{w} * detail::kWindowBits)));
    }
    if (const auto rest = len % detail::kWindowBits; rest != 0) {
      const auto word =
        detail::WordAt(_words, _word_count,
                       base + int64_t{full} * detail::kWindowBits) &
        (~uint64_t{0} >> (detail::kWindowBits - rest));
      count += static_cast<uint64_t>(std::popcount(word));
    }
    return count;
  }

  const uint64_t* _words;
  uint32_t _word_count;
  doc_id_t _uncommitted;
  doc_id_t _end;
};

}  // namespace

Root::ptr MakeMaskCount(const DocumentMask* mask, doc_id_t uncommitted,
                        doc_id_t docs_count) {
  return memory::make_managed<MaskCount>(mask, uncommitted, docs_count);
}

}  // namespace irs::count
